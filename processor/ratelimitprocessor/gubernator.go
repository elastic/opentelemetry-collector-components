// ELASTICSEARCH CONFIDENTIAL
// __________________
//
//  Copyright Elasticsearch B.V. All rights reserved.
//
// NOTICE:  All information contained herein is, and remains
// the property of Elasticsearch B.V. and its suppliers, if any.
// The intellectual and technical concepts contained herein
// are proprietary to Elasticsearch B.V. and its suppliers and
// may be covered by U.S. and Foreign Patents, patents in
// process, and are protected by trade secret or copyright
// law.  Dissemination of this information or reproduction of
// this material is strictly forbidden unless prior written
// permission is obtained from Elasticsearch B.V.

package ratelimitprocessor

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/gubernator"
)

var _ RateLimiter = (*gubernatorRateLimiter)(nil)

type gubernatorRateLimiter struct {
	cfg      *Config
	set      processor.Settings
	behavior gubernator.Behavior

	conn   *grpc.ClientConn
	client gubernator.V1Client
}

func newGubernatorRateLimiter(cfg *Config, set processor.Settings) (*gubernatorRateLimiter, error) {
	var behavior int32
	for _, b := range cfg.Gubernator.Behavior {
		value, ok := gubernator.Behavior_value[strings.ToUpper(string(b))]
		if !ok {
			return nil, fmt.Errorf("invalid behavior %q", b)
		}
		behavior |= value
	}
	return &gubernatorRateLimiter{
		cfg:      cfg,
		set:      set,
		behavior: gubernator.Behavior(behavior),
	}, nil
}

func (r *gubernatorRateLimiter) Start(ctx context.Context, host component.Host) error {
	if r.cfg.Gubernator.Auth != nil && r.cfg.Gubernator.Auth.AuthenticatorID.String() == "" {
		// if we do not set this explicitly to nil, then it will fail when creating the connection with:
		// `failed to resolve authenticator "": authenticator not found`
		r.cfg.Gubernator.Auth = nil
	}
	conn, err := r.cfg.Gubernator.ToClientConn(ctx, host, r.set.TelemetrySettings)
	if err != nil {
		return fmt.Errorf("failed to connect to gubernator: %w", err)
	}
	r.conn = conn
	r.client = gubernator.NewV1Client(r.conn)
	return nil
}

func (r *gubernatorRateLimiter) Shutdown(ctx context.Context) error {
	if r.conn != nil {
		err := r.conn.Close()
		r.conn = nil
		r.client = nil
		return err
	}
	return nil
}

func (r *gubernatorRateLimiter) RateLimit(ctx context.Context, hits int) error {
	uniqueKey := getUniqueKey(ctx, r.cfg.MetadataKeys)
	getRateLimitsResp, err := r.client.GetRateLimits(ctx, &gubernator.GetRateLimitsReq{
		Requests: []*gubernator.RateLimitReq{{
			Name:      r.set.ID.String(),
			UniqueKey: uniqueKey,
			Hits:      int64(hits),
			Behavior:  r.behavior,
			Algorithm: gubernator.Algorithm_LEAKY_BUCKET,
			Limit:     int64(r.cfg.Rate), // rate is per second
			Burst:     int64(r.cfg.Burst),
			Duration:  1000, // duration is in milliseconds, i.e. 1s

			// TODO specify CreatedAt, so resulting reset time is
			// relative to the relative clock.
		}},
	})
	if err != nil {
		return fmt.Errorf("error executing gubernator rate limit request: %w", err)
	}

	// Inside the gRPC response, we should have a single-item list of responses.
	responses := getRateLimitsResp.GetResponses()
	if n := len(responses); n != 1 {
		return fmt.Errorf("expected 1 response from gubernator, got %d", n)
	}
	resp := responses[0]
	if resp.GetError() != "" {
		return errors.New(resp.GetError())
	}

	if isUnderLimit := resp.GetStatus() == gubernator.Status_UNDER_LIMIT; !isUnderLimit {
		// TODO add configurable behaviour for returning an error vs. delaying processing.
		r.set.Logger.Error(
			"request is over the limits defined by the rate limiter",
			zap.Error(errTooManyRequests),
			zap.String("processor_id", r.set.ID.String()),
			zap.Strings("metadata_keys", r.cfg.MetadataKeys),
		)
		return errTooManyRequests
	}
	return nil
}
