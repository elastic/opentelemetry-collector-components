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

func (r *gubernatorRateLimiter) Shutdown(context.Context) error {
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

	rate, burst := cfg.Rate, cfg.Burst
	if r.cfg.DynamicRateLimiting.Enabled && !cfg.disableDynamic {
		rate, burst = r.calculateRateAndBurst(ctx, cfg, uniqueKey, hits, now)
		if rate < 0 {
			return errRateLimitInternalError
		}
	}
	// Execute rate limit check
	return r.executeRateLimit(ctx, cfg, uniqueKey, hits, rate, burst, now)
}

func (r *gubernatorRateLimiter) calculateRateAndBurst(ctx context.Context,
	cfg RateLimitSettings, uniqueKey string, hits int, now time.Time,
) (int, int) {
	limit, err := r.getDynamicLimit(ctx, cfg, uniqueKey, hits, now)
	if err != nil {
		r.set.Logger.Error("failed to get dynamic limit from gubernator",
			zap.Error(err),
			zap.String("unique_key", uniqueKey),
		)
		return -1, -1 // Signal error condition
	}

	result := int(math.Round(limit))
	return result, result
}

func (r *gubernatorRateLimiter) executeRateLimit(ctx context.Context, cfg RateLimitSettings, uniqueKey string, hits, rate, burst int, now time.Time) error {
	createdAt := now.UnixMilli()
	rateLimitResp, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
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

	resps := rateLimitResp.GetResponses()
	if err := validateGubernatorResponse(r.set.Logger, resps, 1, "rate limit request",
		zap.String("unique_key", uniqueKey),
		zap.Strings("metadata_keys", r.cfg.MetadataKeys),
	); err != nil {
		return err
	}

	if resp := resps[0]; resp.GetStatus() == gubernator.Status_OVER_LIMIT {
		return r.handleOverLimit(ctx, resp, createdAt)
	}
	return nil
}

func (r *gubernatorRateLimiter) handleOverLimit(ctx context.Context, resp *gubernator.RateLimitResp, createdAt int64) error {
	switch r.cfg.ThrottleBehavior {
	case ThrottleBehaviorError:
		r.set.Logger.Error("request is over the limits defined by the rate limiter",
			zap.Error(errTooManyRequests),
			zap.String("processor_id", r.set.ID.String()),
			zap.Strings("metadata_keys", r.cfg.MetadataKeys),
		)
		return status.Error(codes.ResourceExhausted, errTooManyRequests.Error())
	case ThrottleBehaviorDelay:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Until(time.UnixMilli(resp.GetResetTime()))):
		}
	}
	return nil
}

// dynamicRateContext holds the context for dynamic rate calculation
type dynamicRateContext struct {
	cfg                DynamicRateLimiting
	staticRate         float64
	currentWindow      time.Time
	previousWindow     time.Time
	currentMetricsKey  string
	previousMetricsKey string
	elapsed            time.Duration
	createdAt          int64
}

func newDynamicRateContext(uniqueKey string, now time.Time, cfg DynamicRateLimiting, staticRate float64) dynamicRateContext {
	currentWindow := now.Truncate(cfg.EWMAWindow)
	previousWindow := currentWindow.Add(-cfg.EWMAWindow)
	elapsed := now.Sub(currentWindow)
	if elapsed.Milliseconds() < 0 {
		elapsed = time.Millisecond
	}

	return dynamicRateContext{
		cfg:                cfg,
		staticRate:         staticRate,
		currentWindow:      currentWindow,
		previousWindow:     previousWindow,
		currentMetricsKey:  fmt.Sprintf("%s-%d", uniqueKey, currentWindow.UnixMilli()),
		previousMetricsKey: fmt.Sprintf("%s-%d", uniqueKey, previousWindow.UnixMilli()),
		elapsed:            elapsed,
		createdAt:          now.UnixMilli(),
	}
}

func (r *gubernatorRateLimiter) getDynamicLimit(ctx context.Context,
	cfg RateLimitSettings, uniqueKey string, hits int, now time.Time,
) (float64, error) {
	drc := newDynamicRateContext(uniqueKey, now, r.cfg.DynamicRateLimiting, float64(cfg.Rate))

	// Get current and previous window rates
	_, previousRate, err := r.getCurrentAndPreviousRates(ctx, drc)
	if err != nil {
		return 0, err
	}

	// Record the hits and recalculate the limit
	lim, err := r.getRateLimits(ctx, drc, hits, previousRate)
	return lim, err
}

// maxDynamicLimit is a sufficiently large number to capture all traffic for dynamic sampling.
// We set the limit to a very high number so that we always get a response from gubernator.
// We are only interested in the number of hits in the current window, not in actually limiting the traffic.
const maxDynamicLimit int64 = 1 << 53

// createDynamicRateLimitRequest creates a gubernator rate limit request for
// dynamic rate limiting
func createDynamicRateLimitRequest(uniqueKey string, hits, createdAt int64,
	windowDuration time.Duration,
) *gubernator.RateLimitReq {
	return &gubernator.RateLimitReq{
		Name:      "dynamic",
		UniqueKey: uniqueKey,
		Hits:      hits,
		Behavior:  gubernator.Behavior_BATCHING,
		Algorithm: gubernator.Algorithm_TOKEN_BUCKET,
		Limit:     maxDynamicLimit,
		Burst:     maxDynamicLimit,
		// Gubernator automatically expires the limit after the duration, so we double it to ensure
		// that we have enough time to process the request and recalculate the limit.
		Duration:  windowDuration.Milliseconds() * 2,
		CreatedAt: &createdAt,
	}
}

func (r *gubernatorRateLimiter) getCurrentAndPreviousRates(ctx context.Context, drc dynamicRateContext) (float64, float64, error) {
	// ----------------------- PEEK PHASE -----------------------
	peekResp, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{
			createDynamicRateLimitRequest(drc.currentMetricsKey, 0, drc.createdAt, drc.cfg.EWMAWindow),
			createDynamicRateLimitRequest(drc.previousMetricsKey, 0, drc.createdAt, drc.cfg.EWMAWindow),
		},
	})
	if err != nil {
		r.set.Logger.Error("dynamic: error executing gubernator dynamic peek request",
			zap.Error(err),
		)
		return 0, 0, errRateLimitInternalError
	}

	peekResponses := peekResp.GetResponses()
	if err := validateGubernatorResponse(r.set.Logger, peekResponses, 2, "dynamic peek request",
		zap.String("current_unique_key", drc.currentMetricsKey),
		zap.String("previous_unique_key", drc.previousMetricsKey),
	); err != nil {
		return 0, 0, err
	}

	// For the current rate, we want to calculate the rate based on the time
	// elapsed to prevent the limit from dropping at the start of a new window.
	currentRate := rateFromResponse(peekResponses[0], drc.elapsed)
	// We want to calculate the previous rate based on the EWMA window since
	// entire window has elapsed.
	previousRate := rateFromResponse(peekResponses[1], drc.cfg.EWMAWindow)
	return currentRate, previousRate, nil
}

func (r *gubernatorRateLimiter) getRateLimits(ctx context.Context,
	drc dynamicRateContext, hits int, previousRate float64,
) (float64, error) {
	// ----------------------- ACT PHASE -----------------------
	gubResp, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{
			createDynamicRateLimitRequest(drc.currentMetricsKey,
				int64(hits), drc.createdAt, drc.cfg.EWMAWindow,
			),
		},
	})
	if err != nil {
		r.set.Logger.Error("dynamic: error executing gubernator dynamic rate limit request",
			zap.Error(err),
			zap.String("unique_key", drc.currentMetricsKey),
			zap.Strings("metadata_keys", r.cfg.MetadataKeys),
		)
		return 0, errRateLimitInternalError
	}

	responses := gubResp.GetResponses()
	if err := validateGubernatorResponse(r.set.Logger, responses, 1, "dynamic rate limit request",
		zap.String("unique_key", drc.currentMetricsKey),
		zap.Strings("metadata_keys", r.cfg.MetadataKeys),
	); err != nil {
		return 0, err
	}
	// We calculate the new dynamic limit *after* recording the accepted hits.
	currentRate := rateFromResponse(responses[0], drc.elapsed)
	return computeDynamicLimit(currentRate, previousRate, drc.staticRate, drc.cfg), nil
}

func computeDynamicLimit(currentRate, previousRate float64,
	staticThreshold float64, cfg DynamicRateLimiting,
) float64 {
	// Use static threshold when no previous rate data is available
	if previousRate <= 0 {
		return staticThreshold
	}

	// Use previous rate with multiplier when no current traffic
	if currentRate <= 0 {
		return previousRate * cfg.EWMAMultiplier
	}

	// Calculate EWMA when both rates are available
	recentWeight := cfg.RecentWindowWeight
	historicalWeight := 1 - recentWeight
	ewma := recentWeight*currentRate + historicalWeight*previousRate

	// Apply multiplier with bounds checking
	dynamicLimit := ewma * cfg.EWMAMultiplier
	maxAllowedLimit := previousRate * cfg.EWMAMultiplier

	// Return the minimum of calculated limit and max allowed, but at least the static threshold
	return math.Max(staticThreshold, math.Min(dynamicLimit, maxAllowedLimit))
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

func rateFromResponse(resp *gubernator.RateLimitResp, window time.Duration) float64 {
	return float64(resp.GetLimit()-resp.GetRemaining()) / window.Seconds()
}
