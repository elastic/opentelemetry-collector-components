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

func (r *gubernatorRateLimiter) Start(ctx context.Context, _ component.Host) (err error) {
	r.daemon, err = gubernator.SpawnDaemon(ctx, r.daemonCfg)
	if err != nil {
		return fmt.Errorf("failed to spawn gubernator daemon: %w", err)
	}

	r.clientConn, err = grpc.NewClient(r.daemonCfg.GRPCListenAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return fmt.Errorf("failed to create gRPC client connection: %w", err)
	}
	r.client = gubernator.NewV1Client(r.clientConn)
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
			return fmt.Errorf("error calculating dynamic rate limit for unique key %s", uniqueKey)
		}
	}
	// Execute rate actual limit check / recording.
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
	// The limit is in <unit> per second, multiply it by throttle interval.
	// Burst is measured per second, so we divide it by the throttle interval.
	limit = math.Round(limit * r.cfg.ThrottleInterval.Seconds())
	// NOTE(marclop) we could potentially set the burst based on a multiplier.
	burst := math.Round(limit / cfg.ThrottleInterval.Seconds())
	return int(limit), int(burst)
}

// executeRateLimit sends the current hits to the unique key in Gubernator.
// If the request is over the defined limits, it will either return an error
// or delay the request based on the throttle behavior.
// The rate is expected to be normalized by the ThrottleInterval (since that
// determines the duration of the rate limit in Gubernator).
// However, burst is expected to be set in a per-second manner.
func (r *gubernatorRateLimiter) executeRateLimit(ctx context.Context,
	cfg RateLimitSettings, uniqueKey string, hits, rate, burst int, now time.Time,
) error {
	createdAt := now.UnixMilli()
	rateLimitResp, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{
			{
				Name:      cfg.Strategy.String(),
				UniqueKey: uniqueKey,
				Hits:      int64(hits),
				Behavior:  r.behavior,
				Algorithm: gubernator.Algorithm_LEAKY_BUCKET,
				Limit:     int64(rate), // rate is per ThrottleInterval, not per second.
				Burst:     int64(burst),
				Duration:  cfg.ThrottleInterval.Milliseconds(),
				CreatedAt: &createdAt,
			},
		},
	})
	if err != nil {
		r.set.Logger.Error("error executing gubernator rate limit request",
			zap.Error(err),
			zap.String("name", cfg.Strategy.String()),
			zap.String("unique_key", uniqueKey),
		)
		return err
	}

	// Inside the gRPC response, we should have a single-item list of responses.
	responses := rateLimitResp.GetResponses()
	if n := len(responses); n != 1 {
		return fmt.Errorf("expected 1 response from gubernator, got %d", n)
	}
	resp := responses[0]
	if resp.GetError() != "" {
		r.set.Logger.Error("failed to get response from gubernator", zap.Error(errors.New(resp.GetError())))
		return errors.New(resp.GetError())
	}

	if resp.GetStatus() == gubernator.Status_OVER_LIMIT {
		// Same logic as local
		switch r.cfg.ThrottleBehavior {
		case ThrottleBehaviorError:
			return status.Error(codes.ResourceExhausted, errTooManyRequests.Error())
		case ThrottleBehaviorDelay:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Until(time.UnixMilli(resp.GetResetTime()))):
			}
		}
	}
	return nil
}

// dynamicRateContext holds the context for dynamic rate calculation
type dynamicRateContext struct {
	cfg            DynamicRateLimiting
	staticRate     float64
	currentWindow  time.Time
	previousWindow time.Time
	currentKey     string
	previousKey    string
	elapsed        time.Duration
	createdAt      int64
}

func newDynamicRateContext(uniqueKey string, now time.Time,
	cfg DynamicRateLimiting, staticRate float64,
) dynamicRateContext {
	currentWindow := now.Truncate(cfg.WindowDuration)
	previousWindow := currentWindow.Add(-cfg.WindowDuration)
	elapsed := now.Sub(currentWindow)
	if elapsed.Milliseconds() < 0 {
		elapsed = time.Millisecond
	}
	return dynamicRateContext{
		cfg:            cfg,
		staticRate:     staticRate,
		currentWindow:  currentWindow,
		previousWindow: previousWindow,
		currentKey:     fmt.Sprintf("%s-%d", uniqueKey, currentWindow.UnixMilli()),
		previousKey:    fmt.Sprintf("%s-%d", uniqueKey, previousWindow.UnixMilli()),
		elapsed:        elapsed,
		createdAt:      now.UnixMilli(),
	}
}

// getDynamicLimit retrieves the dynamic limit from Gubernator for the given
// unique key. The dynamic rate limit is derived from the previous rate with
// the configured multiplier applied to it.
// The returned rates are always normalized per second.
func (r *gubernatorRateLimiter) getDynamicLimit(ctx context.Context,
	cfg RateLimitSettings, uniqueKey string, hits int, now time.Time,
) (float64, error) {
	drc := newDynamicRateContext(uniqueKey, now, r.cfg.DynamicRateLimiting,
		// This is crucial for dynamic rate limiting, calculate the rate based
		// on the throttle interval, not the rate itself which is set as the
		// total reqs/events/bytes per Throttle interval, which may be > 1s.
		float64(cfg.Rate)/r.cfg.ThrottleInterval.Seconds(),
	)
	// Get current and previous window rates
	current, previous, err := r.peekRates(ctx, drc)
	if err != nil {
		return -1, err
	}
	// Convert the current rate to per second.
	current += float64(hits) / float64(drc.elapsed.Seconds())
	// If the current rate is lower than the previous, record it in the current
	// metrics key, otherwise, just return the calculated rate.
	// This is to ensure that we do not record hits in the current metrics key
	// if the current rate is higher than the previous, as we want to use the
	// previous rate to calculate the dynamic limit.
	if current <= math.Max(drc.staticRate, previous*drc.cfg.WindowMultiplier) {
		if err := r.recordHits(ctx, drc, hits); err != nil {
			return -1, err
		}
	}
	return dynamicLimit(previous, drc.staticRate, drc.cfg.WindowMultiplier), nil
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
		// Since Gubernator expires the unique key after the duration, double
		// it to ensure the key survives until the end of the next window.
		Duration:  windowDuration.Milliseconds()*2 + 1,
		CreatedAt: &createdAt,
	}
}

// peekRates retrieves the current and previous rates from Gubernator. Rates
// are normalized per second.
func (r *gubernatorRateLimiter) peekRates(ctx context.Context,
	drc dynamicRateContext,
) (float64, float64, error) {
	// ----------------------- PEEK PHASE -----------------------
	peekResp, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{
			createDynamicRateLimitRequest(drc.currentKey, 0, drc.createdAt, drc.cfg.WindowDuration),
			createDynamicRateLimitRequest(drc.previousKey, 0, drc.createdAt, drc.cfg.WindowDuration),
		},
	})
	if err != nil {
		r.set.Logger.Error("dynamic: error executing gubernator dynamic peek request",
			zap.Error(err),
		)
		return -1, -1, fmt.Errorf("error executing gubernator dynamic peek request: %w", err)
	}

	peekResponses := peekResp.GetResponses()
	if err := validateGubernatorResponse(peekResponses, 2, "dynamic peek request"); err != nil {
		return -1, -1, fmt.Errorf("error validating gubernator dynamic peek response: %w", err)
	}

	// For the current rate, we want to calculate the rate based on the time
	// elapsed to prevent the limit from dropping at the start of a new window.
	currentRate := rateFromResponse(peekResponses[0], drc.elapsed)
	// We want to calculate the previous rate based on the window since the
	// entire window has elapsed.
	previousRate := rateFromResponse(peekResponses[1], drc.cfg.WindowDuration)
	return currentRate, previousRate, nil
}

func (r *gubernatorRateLimiter) recordHits(ctx context.Context, drc dynamicRateContext, hits int) error {
	// ----------------------- RECORD PHASE -----------------------
	if _, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{createDynamicRateLimitRequest(
			drc.currentKey, int64(hits), drc.createdAt, drc.cfg.WindowDuration,
		)},
	}); err != nil {
		r.set.Logger.Error("dynamic: error executing gubernator dynamic rate limit request",
			zap.Error(err),
			zap.String("unique_key", drc.currentKey),
			zap.Strings("metadata_keys", r.cfg.MetadataKeys),
		)
		return fmt.Errorf("error recording hits in gubernator: %w", err)
	}
	return nil
}

// dynamicLimit calculates the dynamic limit based on the previous rate.
func dynamicLimit(previous float64, initial float64, multiplier float64) float64 {
	// Use static threshold when no previous rate data is available
	if previous <= 0 {
		return initial
	}
	// Return the minimum of calculated limit and max allowed, but at least the static threshold
	return math.Max(initial, previous*multiplier)
}

func validateGubernatorResponse(res []*gubernator.RateLimitResp,
	n int, msg string,
) error {
	if len(res) != n {
		return fmt.Errorf(
			"unexpected number of responses from gubernator %s: got %d, want %d",
			msg, len(res), n,
		)
	}
	for _, r := range res {
		if errStr := r.GetError(); errStr != "" {
			return fmt.Errorf("error in gubernator %s response: %w", msg, errors.New(errStr))
		}
	}
	return nil
}

func rateFromResponse(resp *gubernator.RateLimitResp, window time.Duration) float64 {
	return float64(resp.GetLimit()-resp.GetRemaining()) / window.Seconds()
}
