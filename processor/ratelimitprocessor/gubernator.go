// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package ratelimitprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor"

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/uptrace/opentelemetry-go-extra/otellogrus"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/gubernator-io/gubernator/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// maxDynamicLimit is a sufficiently large number to capture all traffic for dynamic sampling.
// We set the limit to a very high number so that we always get a response from gubernator.
// We are only interested in the number of hits in the current window, not in actually limiting the traffic.
const maxDynamicLimit int64 = 1 << 53

var _ RateLimiter = (*gubernatorRateLimiter)(nil)

type gubernatorRateLimiter struct {
	cfg      *Config
	set      processor.Settings
	behavior gubernator.Behavior

	daemonCfg  gubernator.DaemonConfig
	daemon     *gubernator.Daemon
	client     gubernator.V1Client
	clientConn *grpc.ClientConn
}

func newGubernatorDaemonConfig(logger *zap.Logger) (gubernator.DaemonConfig, error) {
	l, err := logrus.ParseLevel(logger.Level().String())
	if err != nil {
		return gubernator.DaemonConfig{}, err
	}
	log := logrus.New()
	log.SetLevel(l)
	log.SetFormatter(&logrus.JSONFormatter{})
	log.AddHook(otellogrus.NewHook(
		otellogrus.WithLevels(
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
			logrus.WarnLevel,
			logrus.InfoLevel,
		)))

	conf, err := gubernator.SetupDaemonConfig(log, nil)
	if err != nil {
		return gubernator.DaemonConfig{}, fmt.Errorf("failed to setup gubernator daemon config: %w", err)
	}

	return conf, nil
}

func newGubernatorRateLimiter(cfg *Config, set processor.Settings) (*gubernatorRateLimiter, error) {
	daemonCfg, err := newGubernatorDaemonConfig(set.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create gubernator daemon config: %w", err)
	}

	return &gubernatorRateLimiter{
		cfg:       cfg,
		set:       set,
		behavior:  gubernator.Behavior_BATCHING,
		daemonCfg: daemonCfg,
	}, nil
}

func (r *gubernatorRateLimiter) Start(ctx context.Context, _ component.Host) error {
	daemon, err := gubernator.SpawnDaemon(ctx, r.daemonCfg)
	if err != nil {
		return fmt.Errorf("failed to spawn gubernator daemon: %w", err)
	}
	r.daemon = daemon

	conn, err := grpc.NewClient(
		r.daemonCfg.GRPCListenAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return fmt.Errorf("failed to create gRPC client connection: %w", err)
	}
	r.client = gubernator.NewV1Client(conn)
	r.clientConn = conn
	return nil
}

func (r *gubernatorRateLimiter) Shutdown(_ context.Context) error {
	if r.daemon != nil {
		r.daemon.Close()
		r.daemon = nil
	}
	if r.clientConn != nil {
		_ = r.clientConn.Close()
		r.clientConn = nil
	}
	r.client = nil
	return nil
}

func (r *gubernatorRateLimiter) RateLimit(ctx context.Context, hits int) error {
	uniqueKey := getUniqueKey(ctx, r.cfg.MetadataKeys)
	cfg := resolveRateLimitSettings(r.cfg, uniqueKey)
	now := time.Now()

	// If dynamic rate limiting is enabled, we calculate the rate limit dynamically.
	// Otherwise, we use the static rate limit.
	rate, burst := cfg.Rate, cfg.Burst
	if r.cfg.DynamicRateLimiting.Enabled {
		limit, err := r.getDynamicLimit(ctx, uniqueKey, hits, now)
		if err != nil {
			r.set.Logger.Error("failed to get dynamic limit from gubernator",
				zap.Error(err),
				zap.String("unique_key", uniqueKey),
				zap.Strings("metadata_keys", r.cfg.MetadataKeys),
			)
			return errRateLimitInternalError
		}
		rate, burst = int(math.Round(limit)), int(math.Round(limit))
	}
	createdAt := now.UnixMilli()
	getRateLimitsResp, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{
			{
				Name:      cfg.Strategy.String(),
				UniqueKey: uniqueKey,
				Hits:      int64(hits),
				Behavior:  r.behavior,
				Algorithm: gubernator.Algorithm_LEAKY_BUCKET,
				Limit:     int64(rate), // rate is per second
				Burst:     int64(burst),
				Duration:  cfg.ThrottleInterval.Milliseconds(),
				CreatedAt: &createdAt,
			},
		},
	})
	if err != nil {
		r.set.Logger.Error("error executing gubernator rate limit request", zap.Error(err))
		return errRateLimitInternalError
	}

	responses := getRateLimitsResp.GetResponses()
	if err := validateGubernatorResponse(r.set.Logger, responses, 1, "rate limit request",
		zap.String("unique_key", uniqueKey),
		zap.Strings("metadata_keys", r.cfg.MetadataKeys),
	); err != nil {
		return err
	}

	resp := responses[0]
	if resp.GetStatus() != gubernator.Status_OVER_LIMIT {
		return nil
	}
	// Same logic as local
	switch r.cfg.ThrottleBehavior {
	case ThrottleBehaviorError:
		r.set.Logger.Error("request is over the limits defined by the rate limiter",
			zap.Error(errTooManyRequests),
			zap.String("processor_id", r.set.ID.String()),
			zap.Strings("metadata_keys", r.cfg.MetadataKeys),
		)
		return status.Error(codes.ResourceExhausted, errTooManyRequests.Error())
	case ThrottleBehaviorDelay:
		delay := time.Duration(resp.GetResetTime()-createdAt) * time.Millisecond
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

func (r *gubernatorRateLimiter) getDynamicLimit(ctx context.Context,
	uniqueKey string, hits int, now time.Time,
) (float64, error) {
	createdAt := now.UnixMilli()
	cfg := r.cfg.DynamicRateLimiting
	rate := float64(r.cfg.Rate)
	currentWindow := now.Truncate(cfg.EWMAWindow)
	previousWindow := currentWindow.Add(-cfg.EWMAWindow)

	currentMetricsKey := fmt.Sprintf("%s-%d", uniqueKey, currentWindow.UnixMilli())
	previousMetricsKey := fmt.Sprintf("%s-%d", uniqueKey, previousWindow.UnixMilli())

	// ----------------------- PEEK PHASE -----------------------
	peekResp, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{
			{
				Name:      "dynamic",
				UniqueKey: currentMetricsKey,
				Hits:      0, // current peek
				Behavior:  r.behavior,
				Algorithm: gubernator.Algorithm_TOKEN_BUCKET,
				Limit:     maxDynamicLimit,
				Burst:     maxDynamicLimit,
				Duration:  cfg.EWMAWindow.Milliseconds() * 2,
				CreatedAt: &createdAt,
			},
			{
				Name:      "dynamic",
				UniqueKey: previousMetricsKey,
				Hits:      0, // previous peek
				Behavior:  r.behavior,
				Algorithm: gubernator.Algorithm_TOKEN_BUCKET,
				Limit:     maxDynamicLimit,
				Burst:     maxDynamicLimit,
				Duration:  cfg.EWMAWindow.Milliseconds() * 2,
				CreatedAt: &createdAt,
			},
		},
	})
	if err != nil {
		r.set.Logger.Error("dynamic: error executing gubernator dynamic peek request",
			zap.Error(err),
		)
		return 0, errRateLimitInternalError
	}

	peekResponses := peekResp.GetResponses()
	if err := validateGubernatorResponse(r.set.Logger, peekResponses, 2, "dynamic peek request",
		zap.String("current_unique_key", currentMetricsKey),
		zap.String("previous_unique_key", previousMetricsKey),
	); err != nil {
		return 0, err
	}

	currentBytes, currentRate := bytesAndRateFromResponse(peekResponses[0], cfg.EWMAWindow)
	_, previousRate := bytesAndRateFromResponse(peekResponses[1], cfg.EWMAWindow)

	dynamicLimit := computeDynamicLimit(currentRate, previousRate, rate, cfg)

	// If this request would exceed the dynamic limit, return the limit early.
	if float64(currentBytes)+float64(hits) > dynamicLimit {
		return dynamicLimit, nil
	}

	// ----------------------- ACT PHASE -----------------------
	gubResp, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{
			{
				Name:      "dynamic",
				UniqueKey: currentMetricsKey,
				Hits:      int64(hits),
				Behavior:  r.behavior,
				Algorithm: gubernator.Algorithm_TOKEN_BUCKET,
				Limit:     maxDynamicLimit,
				Burst:     maxDynamicLimit,
				Duration:  cfg.EWMAWindow.Milliseconds() * 2,
				CreatedAt: &createdAt,
			},
		},
	})
	if err != nil {
		r.set.Logger.Error("dynamic: error executing gubernator dynamic rate limit request",
			zap.Error(err),
			zap.String("unique_key", currentMetricsKey),
			zap.Strings("metadata_keys", r.cfg.MetadataKeys),
		)
		return 0, errRateLimitInternalError
	}

	responses := gubResp.GetResponses()
	if err := validateGubernatorResponse(r.set.Logger, responses, 1, "dynamic rate limit request",
		zap.String("unique_key", currentMetricsKey),
		zap.Strings("metadata_keys", r.cfg.MetadataKeys),
	); err != nil {
		return 0, err
	}
	resp := responses[0]

	// We calculate the new dynamic limit *after* recording the accepted hits.
	_, currentRate = bytesAndRateFromResponse(resp, cfg.EWMAWindow)
	return computeDynamicLimit(currentRate, previousRate, rate, cfg), nil
}

func computeDynamicLimit(currentRate, previousRate float64, min float64, cfg DynamicRateLimiting) float64 {
	// Only allow the dynamic limit to be at most the static threshold when
	// there's no previous rate.
	if previousRate <= 0 {
		return min
	}

	// If there is no current traffic, we use the previous rate to set the limit.
	if currentRate <= 0 {
		return previousRate * cfg.EWMAMultiplier
	}

	// If both rates are available, calculate the new dynamic limit using
	// an EWMA formula. The current rate is weighted more heavily than the
	// previous rate.
	previousWeight := 1 - cfg.RecentWindowWeight
	ewma := cfg.RecentWindowWeight*currentRate + previousWeight*previousRate

	// Cap the new dynamic limit to be at most the previous limit * multiplier.
	// This is to avoid sudden spikes in the rate. We also ensure that the
	// limit is at least the static threshold.
	// A high EWMAMultiplier can lead to a very reactive and potentially unstable
	// rate limit, while a low value will result in a more conservative and stable limit.
	return math.Max(min,
		math.Min(ewma, previousRate)*cfg.EWMAMultiplier,
	)
}

func validateGubernatorResponse(logger *zap.Logger, res []*gubernator.RateLimitResp,
	n int, msg string, fields ...zap.Field,
) error {
	if len(res) != n {
		fields = append(fields, zap.Int("responses", len(res)))
		logger.Error(fmt.Sprintf("unexpected number of responses from gubernator %s", msg), fields...)
		return errRateLimitInternalError
	}
	for _, r := range res {
		if errStr := r.GetError(); errStr != "" {
			fields = append(fields, zap.Error(errors.New(errStr)))
			logger.Error(fmt.Sprintf("error in gubernator %s response", msg), fields...)
			return errRateLimitInternalError
		}
	}
	return nil
}

func bytesAndRateFromResponse(
	resp *gubernator.RateLimitResp, window time.Duration,
) (bytes, rate float64) {
	b := float64(resp.GetLimit() - resp.GetRemaining())
	return b, b / window.Seconds()
}
