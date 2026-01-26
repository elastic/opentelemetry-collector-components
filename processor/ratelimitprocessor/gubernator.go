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
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/gubernator-io/gubernator/v2"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"
)

var _ RateLimiter = (*gubernatorRateLimiter)(nil)

// ClassResolver resolves the class for a given key. Since the resolution
// takes place in the hot path, it MUST be fast and concurrently safe.
// Implementations may implement caching to speed up resolution.
type ClassResolver interface {
	// ResolveClass resolves the class for a given key.
	ResolveClass(ctx context.Context, key string) (string, error)
}

// WindowConfigurator allows adjusting the rates dynamically by configuring
// the multiplier for the next calculation window.
//
// NOTE(lahsivjar): We may want to make the duration configurable too.
type WindowConfigurator interface {
	// Multiplier returns the calculated multiplier for the next window.
	Multiplier(ctx context.Context, window time.Duration, key string) float64
}

type noopResolver struct{}

func (noopResolver) ResolveClass(context.Context, string) (string, error) {
	return "", nil
}

type defaultWindowConfigurator struct {
	multiplier float64
}

func (d defaultWindowConfigurator) Multiplier(context.Context, time.Duration, string) float64 {
	return d.multiplier
}

type gubernatorRateLimiter struct {
	cfg      *Config
	logger   *zap.Logger
	behavior gubernator.Behavior

	daemonCfg  gubernator.DaemonConfig
	daemon     *gubernator.Daemon
	client     gubernator.V1Client
	clientConn *grpc.ClientConn
	// Class resolver for class-based rate limiting
	classResolver      ClassResolver
	windowConfigurator WindowConfigurator
	telemetryBuilder   *metadata.TelemetryBuilder
	tracerProvider     trace.TracerProvider
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

func newGubernatorRateLimiter(cfg *Config, logger *zap.Logger, telemetryBuilder *metadata.TelemetryBuilder, tracerProvider trace.TracerProvider) (*gubernatorRateLimiter, error) {
	daemonCfg, err := newGubernatorDaemonConfig(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create gubernator daemon config: %w", err)
	}

	return &gubernatorRateLimiter{
		cfg:                cfg,
		logger:             logger,
		behavior:           gubernator.Behavior_BATCHING,
		daemonCfg:          daemonCfg,
		telemetryBuilder:   telemetryBuilder,
		tracerProvider:     tracerProvider,
		classResolver:      noopResolver{},
		windowConfigurator: defaultWindowConfigurator{multiplier: cfg.DefaultWindowMultiplier},
	}, nil
}

func (r *gubernatorRateLimiter) Start(ctx context.Context, host component.Host) (err error) {
	if res := r.cfg.ClassResolver; res.String() != "" {
		cr, ok := host.GetExtensions()[res]
		if !ok {
			return fmt.Errorf("class resolver %s not found", res)
		}
		if err := cr.Start(ctx, host); err != nil {
			return fmt.Errorf("failed to start class resolver %s: %w", res, err)
		}
		r.classResolver = cr.(ClassResolver)
	}

	if wCon := r.cfg.WindowConfigurator; wCon.String() != "" {
		wc, ok := host.GetExtensions()[wCon]
		if !ok {
			return fmt.Errorf("window configurator %s not found", wCon)
		}
		if err := wc.Start(ctx, host); err != nil {
			return fmt.Errorf("failed to start window configurator %s: %w", wCon, err)
		}
		r.windowConfigurator = wc.(WindowConfigurator)
	}

	if r.daemon == nil {
		r.daemon, err = gubernator.SpawnDaemon(ctx, r.daemonCfg)
		if err != nil {
			return fmt.Errorf("failed to spawn gubernator daemon: %w", err)
		}
	}

	if r.clientConn == nil {
		r.clientConn, err = grpc.NewClient(r.daemonCfg.GRPCListenAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(r.tracerProvider))),
		)
		if err != nil {
			return fmt.Errorf("failed to create gRPC client connection: %w", err)
		}
	}

	if r.client == nil {
		r.client = gubernator.NewV1Client(r.clientConn)
	}

	return nil
}

func (r *gubernatorRateLimiter) Shutdown(ctx context.Context) error {
	if r.daemon != nil {
		r.daemon.Close()
		r.daemon = nil
	}
	if r.clientConn != nil {
		_ = r.clientConn.Close()
		r.clientConn = nil
	}
	r.client = nil
	if c, ok := r.classResolver.(component.Component); ok {
		if err := c.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown class resolver: %w", err)
		}
	}
	if w, ok := r.windowConfigurator.(component.Component); ok {
		if err := w.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown window configurator: %w", err)
		}
	}
	return nil
}

func (r *gubernatorRateLimiter) RateLimit(ctx context.Context, hits int) error {
	metadata := client.FromContext(ctx).Metadata
	uniqueKey := getUniqueKey(metadata, r.cfg.MetadataKeys)
	// First resolve the class if classes are set.
	class, err := r.classResolver.ResolveClass(ctx, uniqueKey)
	if err != nil {
		r.telemetryBuilder.RatelimitResolverFailures.Add(ctx, 1,
			metric.WithAttributeSet(
				attribute.NewSet(attribute.String("unique_key", uniqueKey)),
			),
		)
		r.logger.Warn("class resolver failed, falling back",
			zap.Error(err),
			zap.String("unique_key", uniqueKey),
			zap.String("default_class", r.cfg.DefaultClass),
		)
	}
	// Resolve rate limit precedence:
	// override -> class -> default_class -> fallback.
	cfg, sourceKind, className := resolveRateLimit(r.cfg, class, metadata)
	rate, burst := cfg.Rate, cfg.Burst
	now := time.Now()
	// If dynamic rate limiting is enabled and not disabled for this request,
	// calculate the dynamic rate and burst.
	if r.cfg.Enabled && !cfg.disableDynamic {
		attrs := make([]attribute.KeyValue, 0, 3)
		attrs = append(attrs,
			attribute.String("source_kind", string(sourceKind)),
			attribute.String("class", className),
		)
		rate, burst = r.calculateRateAndBurst(ctx, cfg, uniqueKey, hits, now)
		if rate < 0 { // Degraded mode - Gubernator unreachable. Fallback to static rate.
			r.telemetryBuilder.RatelimitDynamicEscalations.Add(ctx, 1,
				metric.WithAttributeSet(attribute.NewSet(append(attrs,
					attribute.String("reason", "gubernator_error"),
				)...)),
			)
			rate, burst = cfg.Rate, cfg.Burst
		} else if rate > cfg.Rate { // Dynamic escalation occurred
			r.telemetryBuilder.RatelimitDynamicEscalations.Add(ctx, 1,
				metric.WithAttributeSet(attribute.NewSet(append(attrs,
					attribute.String("reason", "success"),
				)...)),
			)
		} else { // Dynamic escalation was skipped (dynamic <= static)
			r.telemetryBuilder.RatelimitDynamicEscalations.Add(ctx, 1,
				metric.WithAttributeSet(attribute.NewSet(append(attrs,
					attribute.String("reason", "skipped"),
				)...)),
			)
		}

	}
	// Execute rate actual limit check / recording.
	return r.executeRateLimit(ctx, cfg, uniqueKey, hits, rate, burst, now)
}

func (r *gubernatorRateLimiter) calculateRateAndBurst(ctx context.Context,
	cfg RateLimitSettings, uniqueKey string, hits int, now time.Time,
) (int, int) {
	// limit is computed in requests-per-second units.
	limit, err := r.getDynamicLimit(ctx, cfg, uniqueKey, hits, now)
	if err != nil {
		r.logger.Error("failed to get dynamic limit from gubernator",
			zap.Error(err),
			zap.String("unique_key", uniqueKey),
		)
		return -1, -1 // Signal error condition
	}
	// The limit is in <unit> per second, multiply it by throttle interval.
	// Burst is measured per second, so we divide it by the throttle interval.
	throttleInterval := r.cfg.ThrottleInterval.Seconds()
	limit = math.Round(limit * throttleInterval)
	// NOTE(marclop) we could potentially set the burst based on a multiplier.
	burst := math.Round(limit / throttleInterval)
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
	makeRateLimitRequest := func(createdAt int64) (*gubernator.RateLimitResp, error) {
		getRateLimitsResp, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
			Requests: []*gubernator.RateLimitReq{
				{
					Name:      cfg.Strategy.String(),
					UniqueKey: uniqueKey,
					Hits:      int64(hits),
					Behavior:  r.behavior,
					Algorithm: gubernator.Algorithm_LEAKY_BUCKET,
					Limit:     int64(rate), // rate is per ThrottleInterval, not per second.
					Burst:     int64(burst),
					Duration:  cfg.ThrottleInterval.Milliseconds(), // duration is in milliseconds, i.e. 1s
					CreatedAt: &createdAt,
				},
			},
		})
		if err != nil {
			return nil, err
		}
		// Inside the gRPC response, we should have a single-item list of responses.
		responses := getRateLimitsResp.GetResponses()
		if n := len(responses); n != 1 {
			return nil, fmt.Errorf("expected 1 response from gubernator, got %d", n)
		}
		resp := responses[0]
		if resp.GetError() != "" {
			return nil, errors.New(resp.GetError())
		}
		return resp, nil
	}
	resp, err := makeRateLimitRequest(now.UnixMilli())
	if err != nil {
		// If fail_open is enabled, allow traffic to pass despite the error
		if r.cfg.FailOpen {
			r.logger.Info("fail_open enabled, allowing traffic despite gubernator error",
				zap.Error(err),
				zap.Dict("ratelimit",
					zap.String("strategy", cfg.Strategy.String()),
					zap.String("unique_key", uniqueKey),
					zap.String("behavior", "fail_open"),
				),
			)
			return nil
		}
		r.logger.Error("error executing gubernator rate limit request",
			zap.Error(err),
			zap.Dict("ratelimit",
				zap.String("strategy", cfg.Strategy.String()),
				zap.String("unique_key", uniqueKey),
				zap.String("behavior", "fail_closed"),
			),
		)
		// Make the error retryable as gubernator being down is treated as a transient error.
		msg := fmt.Sprintf("service unavailable, try again in %v seconds", cfg.RetryDelay.Seconds())
		return errorWithDetails(status.Error(codes.Unavailable, msg), cfg)
	}
	if resp.GetStatus() == gubernator.Status_OVER_LIMIT {
		// Same logic as local
		switch r.cfg.ThrottleBehavior {
		case ThrottleBehaviorError:
			return errorWithDetails(errTooManyRequests, cfg)
		case ThrottleBehaviorDelay:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Until(time.UnixMilli(resp.GetResetTime()))):
				delay := time.Duration(resp.GetResetTime()-time.Now().UnixMilli()) * time.Millisecond
				timer := time.NewTimer(delay)
				defer timer.Stop()
			retry:
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-timer.C:
						resp, err = makeRateLimitRequest(time.Now().UnixMilli())
						if err != nil {
							return err
						}
						if resp.GetStatus() == gubernator.Status_UNDER_LIMIT {
							break retry
						}
						delay = time.Duration(resp.GetResetTime()-time.Now().UnixMilli()) * time.Millisecond
						timer.Reset(delay)
					}
				}
			}
		}
		return nil
	}
	return nil
}

// dynamicRateContext holds the context for dynamic rate calculation
type dynamicRateContext struct {
	DynamicRateLimiting
	currentKey  string
	previousKey string
	elapsed     time.Duration
	createdAt   int64
}

func newDynamicRateContext(key string, now time.Time, cfg DynamicRateLimiting) dynamicRateContext {
	currentWindow := now.Truncate(cfg.WindowDuration)
	previousWindow := currentWindow.Add(-cfg.WindowDuration)
	elapsed := now.Sub(currentWindow)
	if elapsed.Milliseconds() < 0 {
		elapsed = time.Millisecond
	}
	return dynamicRateContext{
		DynamicRateLimiting: cfg,

		currentKey:  fmt.Sprintf("%s-%d", key, currentWindow.UnixMilli()),
		previousKey: fmt.Sprintf("%s-%d", key, previousWindow.UnixMilli()),
		elapsed:     elapsed,
		createdAt:   now.UnixMilli(),
	}
}

// getDynamicLimit retrieves the dynamic limit from Gubernator for the given
// unique key. The dynamic rate limit is derived from the previous rate with
// the configured multiplier applied to it. Rates are normalized per second.
func (r *gubernatorRateLimiter) getDynamicLimit(ctx context.Context,
	cfg RateLimitSettings, uniqueKey string, hits int, now time.Time,
) (float64, error) {
	// This is crucial for dynamic rate limiting, calculate the rate based on
	// the throttle interval, not the rate itself which is set as the total
	// reqs/events/bytes per Throttle interval may not be 1s.
	staticRate := float64(cfg.Rate) / r.cfg.ThrottleInterval.Seconds()
	drc := newDynamicRateContext(uniqueKey, now, r.cfg.DynamicRateLimiting)
	// Get current and previous window rates, the current rates are without
	// accounting for the new hits.
	current, previous, err := r.peekRates(ctx, drc)
	if err != nil {
		return -1, err
	}
	windowMultiplier := r.windowConfigurator.Multiplier(
		ctx,
		drc.WindowDuration,
		uniqueKey,
	)
	if windowMultiplier < 0 {
		windowMultiplier = drc.DefaultWindowMultiplier
	}
	// Only record the incoming hits when the current rate is within the allowed
	// range, otherwise, do not record the hits and return the calculated rate.
	// MaxAllowed sets a ceiling on the rate with the window duration. If the
	// the window multiplier is suggesting lowering the ingestion rate then the
	// MaxAllowed will be allowed to go below the static rate (to as low as `1`).
	// As soon as the window multiplier suggests increasing the ingestion rate,
	// the MaxAllowed will jump to a minimum of static rate.
	//
	// NOTE(marclop) We may want to add a follow-up static ceiling to avoid
	// unbounded growth.
	var maxAllowed float64
	if windowMultiplier <= 1 {
		// multiplier indicates scale down. If we have hits in the previous
		// multiplier then scale that down, otherwise scale the static rate
		// as per the multiplier instead of returning the default static rate.
		// This should protect against deployments with spiky load patterns.
		if previous > 0 {
			maxAllowed = max(1, previous*windowMultiplier)
		} else {
			maxAllowed = max(1, staticRate*windowMultiplier)
		}
	} else {
		maxAllowed = max(staticRate, previous*windowMultiplier)
	}
	// Normalise the current rate assuming no more events will occur during the
	// rest of the window. This will ensure that we record hits based on the
	// currently observed hits and NOT based on extrapolated data.
	current = current * drc.elapsed.Seconds() / drc.WindowDuration.Seconds()
	if current <= maxAllowed {
		// Deduce how many hits to record to reach to the max allowed number
		remainingHits := int((maxAllowed - current) * drc.WindowDuration.Seconds())
		if err := r.recordHits(ctx, drc, min(hits, remainingHits)); err != nil {
			return -1, err
		}
	}
	if r.logger.Level() == zap.DebugLevel {
		r.logger.Debug(
			"Dynamic rate limiting applied",
			zap.Dict(
				"ratelimit",
				zap.String("unique_key", uniqueKey),
				zap.Float64("multiplier", windowMultiplier),
				zap.Float64("static_rate", staticRate),
				zap.Float64("previous_rate", previous),
				zap.Float64("limit", maxAllowed),
			),
		)
	}
	return maxAllowed, nil
}

func (r *gubernatorRateLimiter) newDynamicRequest(
	uniqueKey string, hits int64, drc dynamicRateContext,
) *gubernator.RateLimitReq {
	const maxLimit int64 = 1 << 53 // High number so it can record all hits.
	return &gubernator.RateLimitReq{
		Name:      "dynamic",
		UniqueKey: uniqueKey,
		Hits:      hits,
		Behavior:  r.behavior,
		// Use the TOKEN_BUCKET algorithm for dynamic rate limiting, since we
		// want to keep all the recorded tokens in the bucket. Using leaky
		// bucket is undesirable since it would leak tokens over time.
		Algorithm: gubernator.Algorithm_TOKEN_BUCKET,
		Limit:     maxLimit,
		// Since Gubernator expires the unique key after the duration, double
		// it to ensure the key survives until the end of the next window.
		Duration:  drc.WindowDuration.Milliseconds()*2 + 1,
		CreatedAt: &drc.createdAt,
	}
}

// peekRates retrieves the current and previous rates from Gubernator. All
// Rates are normalized per second.
func (r *gubernatorRateLimiter) peekRates(
	ctx context.Context,
	drc dynamicRateContext,
) (float64, float64, error) {
	// ----------------------- PEEK PHASE -----------------------
	peekResp, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{
			r.newDynamicRequest(drc.currentKey, 0, drc),
			r.newDynamicRequest(drc.previousKey, 0, drc),
		},
	})
	if err != nil {
		return -1, -1, fmt.Errorf("error executing gubernator dynamic peek request: %w", err)
	}
	peekResponses := peekResp.GetResponses()
	if err := validateResp(peekResponses, 2, "dynamic peek request"); err != nil {
		return -1, -1, err
	}
	// Normalize the current rate based on the elapsed time (since the window
	// hasn't fully elapsed).
	currentRate := rateFromResponse(peekResponses[0], drc.elapsed)
	// Normalize the PREVIOUS rate based on the window duration.
	previousRate := rateFromResponse(peekResponses[1], drc.WindowDuration)
	return currentRate, previousRate, nil
}

func rateFromResponse(resp *gubernator.RateLimitResp, window time.Duration) float64 {
	return float64(resp.GetLimit()-resp.GetRemaining()) / window.Seconds()
}

func (r *gubernatorRateLimiter) recordHits(ctx context.Context, drc dynamicRateContext, hits int) error {
	// ----------------------- RECORD PHASE -----------------------
	res, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{
			r.newDynamicRequest(drc.currentKey, int64(hits), drc),
		},
	})
	if err != nil {
		return fmt.Errorf("error recording hits in gubernator: %w", err)
	}
	return validateResp(res.GetResponses(), 1, "dynamic record request")
}

func validateResp(res []*gubernator.RateLimitResp, n int, msg string) error {
	if len(res) != n {
		return fmt.Errorf(
			"unexpected gubernator %s response count: %d, expected %d",
			msg, len(res), n,
		)
	}
	var errs []error
	for _, r := range res {
		if errStr := r.GetError(); errStr != "" {
			errs = append(errs, fmt.Errorf(
				"error in gubernator %s response: %w", msg, errors.New(errStr)),
			)
		}
	}
	return errors.Join(errs...)
}
