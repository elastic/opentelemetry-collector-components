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
	"sync"
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

	currentRequests   map[string]int
	currentRequestsMx sync.RWMutex
}

func newGubernatorDaemonConfig(logger *zap.Logger) (gubernator.DaemonConfig, error) {
	l, err := logrus.ParseLevel(logger.Level().String())
	if err != nil {
		return gubernator.DaemonConfig{}, err
	}
	log := logrus.New()
	log.SetLevel(l)
	log.SetFormatter(&logrus.JSONFormatter{})
	log.AddHook(otellogrus.NewHook(otellogrus.WithLevels(
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
		cfg:             cfg,
		set:             set,
		behavior:        gubernator.Behavior(0), // BATCHING behavior
		daemonCfg:       daemonCfg,
		currentRequests: make(map[string]int),
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

func (r *gubernatorRateLimiter) addRequests(uniqueKey string, hits int) int {
	r.currentRequestsMx.Lock()
	current, exists := r.currentRequests[uniqueKey]
	if !exists {
		r.currentRequests[uniqueKey] = hits
	} else {
		r.currentRequests[uniqueKey] = current + hits
	}
	current = r.currentRequests[uniqueKey]
	r.currentRequestsMx.Unlock()
	return current
}

func (r *gubernatorRateLimiter) deleteRequests(uniqueKey string, hits int) error {
	r.currentRequestsMx.Lock()
	current, exists := r.currentRequests[uniqueKey]
	if !exists {
		return fmt.Errorf("unexpected: current requests entry does not exist for this unique key %s", uniqueKey)
	} else {
		r.currentRequests[uniqueKey] = current - 1
		if r.currentRequests[uniqueKey] < 0 {
			return fmt.Errorf("unexpected: current request for unique key %s reached a negative value", uniqueKey)
		}
	}
	current = r.currentRequests[uniqueKey]
	r.currentRequestsMx.Unlock()
	return nil
}

func (r *gubernatorRateLimiter) RateLimit(ctx context.Context, hits int) error {
	uniqueKey := getUniqueKey(ctx, r.cfg.MetadataKeys)
	cfg := resolveRateLimitSettings(r.cfg, uniqueKey)

	createdAt := time.Now().UnixMilli()
	getRateLimitsResp, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{
			{
				Name:      r.set.ID.String(),
				UniqueKey: uniqueKey,
				Hits:      int64(hits),
				Behavior:  r.behavior,
				Algorithm: gubernator.Algorithm_LEAKY_BUCKET,
				Limit:     int64(cfg.Rate), // rate is per second
				Burst:     int64(cfg.Burst),
				Duration:  cfg.ThrottleInterval.Milliseconds(), // duration is in milliseconds, i.e. 1s
				CreatedAt: &createdAt,
			},
		},
	})
	if err != nil {
		r.set.Logger.Error("error executing gubernator rate limit request", zap.Error(err))
		return errRateLimitInternalError
	}

	// Inside the gRPC response, we should have a single-item list of responses.
	responses := getRateLimitsResp.GetResponses()
	if n := len(responses); n != 1 {
		return fmt.Errorf("expected 1 response from gubernator, got %d", n)
	}
	resp := responses[0]
	if resp.GetError() != "" {
		r.set.Logger.Error("failed to get response from gubernator", zap.Error(errors.New(resp.GetError())))
		return errRateLimitInternalError
	}

	if resp.GetStatus() == gubernator.Status_OVER_LIMIT {
		// Same logic as local
		switch r.cfg.ThrottleBehavior {
		case ThrottleBehaviorError:
			r.set.Logger.Error(
				"request is over the limits defined by the rate limiter",
				zap.Error(errTooManyRequests),
				zap.String("processor_id", r.set.ID.String()),
				zap.Strings("metadata_keys", r.cfg.MetadataKeys),
			)
			return status.Error(codes.ResourceExhausted, errTooManyRequests.Error())
		case ThrottleBehaviorDelay:
			current := r.addRequests(uniqueKey, hits)
			delay := time.Duration(resp.GetResetTime()-createdAt+int64(current)*cfg.ThrottleInterval.Milliseconds()) * time.Millisecond
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				_ = r.deleteRequests(uniqueKey, hits)
				return ctx.Err()
			case <-timer.C:
				return r.deleteRequests(uniqueKey, hits)
			}
		}
	}

	return nil
}
