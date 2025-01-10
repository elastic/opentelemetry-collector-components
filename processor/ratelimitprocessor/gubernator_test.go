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

package ratelimitprocessor

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/processor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/gubernator"
	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"
)

func setupGubernatorServer(t *testing.T) (*testServer, string) {
	s := grpc.NewServer()
	server := &testServer{}
	gubernator.RegisterV1Server(s, server)
	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	go func() {
		err := s.Serve(lis)
		require.NoError(t, err)
	}()
	t.Cleanup(s.GracefulStop)
	return server, lis.Addr().String()
}

// newTestGubernatorRateLimiter creates a new gubernatorRateLimiter
// with a connection to a fake gubernator server.
func newTestGubernatorRateLimiter(t *testing.T, cfg *Config) (*testServer, *gubernatorRateLimiter) {
	server, address := setupGubernatorServer(t)
	if cfg == nil {
		cfg = createDefaultConfig().(*Config)
	}
	if cfg.Gubernator == nil {
		cfg.Gubernator = &GubernatorConfig{}
	}
	grpcClientConfig := configgrpc.NewDefaultClientConfig()
	grpcClientConfig.Auth = nil
	grpcClientConfig.Endpoint = address
	grpcClientConfig.TLSSetting.Insecure = true
	cfg.Gubernator.ClientConfig = *grpcClientConfig

	rl, err := newGubernatorRateLimiter(cfg, processor.Settings{
		ID:                component.NewIDWithName(metadata.Type, "abc123"),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err := rl.Shutdown(context.Background())
		assert.NoError(t, err)
	})
	return server, rl
}

func TestGubernatorRateLimiter_StartStop(t *testing.T) {
	_, rateLimiter := newTestGubernatorRateLimiter(t, nil)

	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	err = rateLimiter.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestGubernatorRateLimiter_RateLimit(t *testing.T) {
	for _, behavior := range []ThrottleBehavior{ThrottleBehaviorError, ThrottleBehaviorDelay} {
		t.Run(string(behavior), func(t *testing.T) {
			server, rateLimiter := newTestGubernatorRateLimiter(t, &Config{Rate: 1, Burst: 2, ThrottleBehavior: behavior})
			err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
			require.NoError(t, err)

			var resp gubernator.GetRateLimitsResp
			var req *gubernator.GetRateLimitsReq
			var respErr error
			server.getRateLimits = func(ctx context.Context, reqIn *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error) {
				req = reqIn
				return &resp, respErr
			}

			resp.Responses = []*gubernator.RateLimitResp{{Status: gubernator.Status_UNDER_LIMIT}}
			err = rateLimiter.RateLimit(context.Background(), 1)
			assert.NoError(t, err)
			require.NotNil(t, req)
			require.Len(t, req.Requests, 1)
			// CreatedAt is based on time.Now(). Check it is not nil, then nil it out for the next assertion.
			assert.NotNil(t, req.Requests[0].CreatedAt)
			req.Requests[0].CreatedAt = nil
			assert.Equal(t, &gubernator.RateLimitReq{
				Name:      "ratelimit/abc123",
				UniqueKey: "default",
				Hits:      1,
				Limit:     1,
				Burst:     2,
				Duration:  1000,
				Algorithm: gubernator.Algorithm_LEAKY_BUCKET,
			}, req.Requests[0])

			resp.Responses = []*gubernator.RateLimitResp{{Error: "yeah, nah"}}
			err = rateLimiter.RateLimit(context.Background(), 1)
			assert.EqualError(t, err, "yeah, nah")

			resp.Responses = []*gubernator.RateLimitResp{}
			err = rateLimiter.RateLimit(context.Background(), 1)
			assert.EqualError(t, err, "expected 1 response from gubernator, got 0")

			resp.Responses = []*gubernator.RateLimitResp{{Status: gubernator.Status_OVER_LIMIT}}
			err = rateLimiter.RateLimit(context.Background(), 1)
			assert.EqualError(t, err, "too many requests")

			resp.Responses = []*gubernator.RateLimitResp{{Status: -1}} // handled like OVER_LIMIT
			err = rateLimiter.RateLimit(context.Background(), 1)
			assert.EqualError(t, err, "too many requests")

			respErr = errors.New("nope")
			err = rateLimiter.RateLimit(context.Background(), 1)
			assert.EqualError(t, err, "error executing gubernator rate limit request: rpc error: code = Unknown desc = nope")
		})
	}
}

func TestGubernatorRateLimiter_RateLimit_MetadataKeys(t *testing.T) {
	server, rateLimiter := newTestGubernatorRateLimiter(t, &Config{
		Rate:         1,
		Burst:        2,
		MetadataKeys: []string{"metadata_key"},
	})
	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	clientContext1 := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"metadata_key": {"value1"},
		}),
	})
	clientContext2 := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"metadata_key": {"value2"},
		}),
	})

	var req *gubernator.GetRateLimitsReq
	server.getRateLimits = func(ctx context.Context, reqIn *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error) {
		req = reqIn
		return &gubernator.GetRateLimitsResp{
			Responses: []*gubernator.RateLimitResp{{Status: gubernator.Status_UNDER_LIMIT}},
		}, nil
	}

	// Each unique combination of metadata keys should get its own rate limit.
	err = rateLimiter.RateLimit(clientContext1, 1)
	assert.NoError(t, err)
	assert.Equal(t, "ratelimit/abc123", req.Requests[0].Name)
	assert.Equal(t, "metadata_key:value1", req.Requests[0].UniqueKey)

	err = rateLimiter.RateLimit(clientContext2, 1)
	assert.NoError(t, err)
	assert.Equal(t, "ratelimit/abc123", req.Requests[0].Name)
	assert.Equal(t, "metadata_key:value2", req.Requests[0].UniqueKey)
}

type testServer struct {
	gubernator.UnimplementedV1Server
	getRateLimits func(context.Context, *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error)
}

func (s *testServer) GetRateLimits(ctx context.Context, req *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error) {
	if s.getRateLimits != nil {
		return s.getRateLimits(ctx, req)
	}
	return nil, status.Errorf(codes.Unimplemented, "method GetRateLimits not implemented")
}
