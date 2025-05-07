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
	"reflect"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/processor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/gubernator"
	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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
func newTestGubernatorRateLimiter(t *testing.T, cfg *Config, mp *metric.MeterProvider) (*testServer, *gubernatorRateLimiter) {
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

	var rl *gubernatorRateLimiter
	var err error
	// Initiating the newMeterProvider that will simulate the global meterprovider
	if mp == nil {
		mp = metric.NewMeterProvider()
	}
	rl, err = newGubernatorRateLimiter(cfg, processor.Settings{
		ID:                component.NewIDWithName(metadata.Type, "abc123"),
		TelemetrySettings: component.TelemetrySettings{MeterProvider: mp, Logger: zap.NewNop()},
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
	_, rateLimiter := newTestGubernatorRateLimiter(t, nil, nil)

	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	err = rateLimiter.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestGubernatorRateLimiter_RateLimit(t *testing.T) {
	for _, behavior := range []ThrottleBehavior{ThrottleBehaviorError, ThrottleBehaviorDelay} {
		t.Run(string(behavior), func(t *testing.T) {
			server, rateLimiter := newTestGubernatorRateLimiter(t, &Config{Rate: 1, Burst: 2, ThrottleBehavior: behavior}, nil)
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
			assert.EqualError(t, err, errRateLimitInternalError.Error())

			resp.Responses = []*gubernator.RateLimitResp{}
			err = rateLimiter.RateLimit(context.Background(), 1)
			assert.EqualError(t, err, "expected 1 response from gubernator, got 0")

			reqTime := time.Now()
			resp.Responses = []*gubernator.RateLimitResp{{Status: gubernator.Status_OVER_LIMIT, ResetTime: reqTime.Add(100 * time.Millisecond).UnixMilli()}}
			err = rateLimiter.RateLimit(context.Background(), 1)
			switch behavior {
			case ThrottleBehaviorError:
				assert.NoError(t, err)
			case ThrottleBehaviorDelay:
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, time.Now(), reqTime.Add(100*time.Millisecond))
			}

			respErr = errors.New("nope")
			err = rateLimiter.RateLimit(context.Background(), 1)
			assert.EqualError(t, err, errRateLimitInternalError.Error())
		})
	}
}

func TestGubernatorRateLimiter_RateLimit_MetadataKeys(t *testing.T) {
	server, rateLimiter := newTestGubernatorRateLimiter(t, &Config{
		Rate:             1,
		Burst:            2,
		MetadataKeys:     []string{"metadata_key"},
		ThrottleBehavior: ThrottleBehaviorError,
	}, nil)
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

// Ensures that specific metrics are returned in case of relevant responses of GetRateLimits
func TestGubernatorRateLimiter_RateLimit_Meterprovider(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)

	cfg := &Config{
		Rate:             1,
		Burst:            2,
		MetadataKeys:     []string{"x-elastic-project-id"},
		ThrottleBehavior: ThrottleBehaviorError,
	}
	server, rl := newTestGubernatorRateLimiter(t, cfg, mp)

	// We provide the x-elastic-project-id in order to set the ProjectID
	clientContext1 := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"x-elastic-project-id": {"TestProjectID"},
		}),
	})

	err := rl.Start(clientContext1, componenttest.NewNopHost())
	require.NoError(t, err)

	getProjectIDFromAttrs := func(attrs []attribute.KeyValue) string {
		for _, attr := range attrs {
			if string(attr.Key) == "project_id" || string(attr.Key) == "x-elastic-project-id" {
				return attr.Value.AsString()
			}
		}
		return ""
	}

	getRatelimitRequests := func() int64 {
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name == "otelcol_ratelimit.requests" {
					sum := m.Data.(metricdata.Sum[int64])
					var total int64
					for _, dp := range sum.DataPoints {
						total += dp.Value
						projectID := getProjectIDFromAttrs(dp.Attributes.ToSlice())
						assert.Equal(t, "TestProjectID", projectID)
					}
					return total
				}
			}
		}
		return 0
	}

	// Under limit case
	server.getRateLimits = func(ctx context.Context, req *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error) {
		return &gubernator.GetRateLimitsResp{
			Responses: []*gubernator.RateLimitResp{{Status: gubernator.Status_UNDER_LIMIT}},
		}, nil
	}
	err = rl.RateLimit(clientContext1, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), getRatelimitRequests())

	// Over limit case
	server.getRateLimits = func(ctx context.Context, req *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error) {
		return &gubernator.GetRateLimitsResp{
			Responses: []*gubernator.RateLimitResp{{Status: gubernator.Status_OVER_LIMIT, ResetTime: time.Now().Add(100 * time.Millisecond).UnixMilli()}},
		}, nil
	}
	err = rl.RateLimit(clientContext1, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), getRatelimitRequests())

	// Error from server
	server.getRateLimits = func(ctx context.Context, req *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error) {
		return nil, errors.New("server error")
	}
	err = rl.RateLimit(clientContext1, 1)
	assert.Error(t, err)
	assert.Equal(t, int64(3), getRatelimitRequests())

	// Custom error in response
	server.getRateLimits = func(ctx context.Context, req *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error) {
		return &gubernator.GetRateLimitsResp{
			Responses: []*gubernator.RateLimitResp{{Error: "custom error"}},
		}, nil
	}
	err = rl.RateLimit(clientContext1, 1)
	assert.Error(t, err)
	assert.Equal(t, int64(4), getRatelimitRequests())
}

func TestRateLimitThresholdAttribute(t *testing.T) {
	limitUsedPercent := []int64{20, 30, 60, 80, 90, 95, 100}
	projectID := "TestProjectID"
	processorID := "ratelimit/abc123"
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)

	metadataKey := "project_id"
	cfg := &Config{
		Rate:             1,
		Burst:            2,
		MetadataKeys:     []string{metadataKey},
		ThrottleBehavior: ThrottleBehaviorDelay,
	}

	// We provide the project_id in order to set the ProjectID
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"project_id": {projectID},
		}),
	})

	for _, pctUsed := range limitUsedPercent {
		t.Run(strconv.Itoa(int(pctUsed)), func(t *testing.T) {
			server, rl := newTestGubernatorRateLimiter(t, cfg, mp)
			server.getRateLimits = func(ctx context.Context, req *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error) {
				return &gubernator.GetRateLimitsResp{
					Responses: []*gubernator.RateLimitResp{{
						Status:    gubernator.Status_UNDER_LIMIT,
						Limit:     100,
						Remaining: 100 - pctUsed}},
				}, nil
			}

			err := rl.Start(ctx, componenttest.NewNopHost())
			require.NoError(t, err)

			err = rl.RateLimit(ctx, 1)
			assert.NoError(t, err)

			var attrs []attribute.KeyValue
			if pctUsed == 100 {
				attrs = []attribute.KeyValue{
					attribute.Float64("limit_threshold", 0.95),
					telemetry.WithProcessorID(processorID),
					attribute.String(metadataKey, projectID),
					attribute.String("ratelimit_decision", "throttled"),
					attribute.String("reason", "over_limit"),
				}
			} else {
				attrs = []attribute.KeyValue{
					attribute.Float64("limit_threshold", getLimitThresholdBucket(float64(pctUsed)/100)),
					telemetry.WithProcessorID(processorID),
					attribute.String(metadataKey, projectID),
					attribute.String("ratelimit_decision", "accepted"),
					attribute.String("reason", "under_limit"),
				}
			}

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(context.Background(), &rm))
			assertMetrics(t, rm, "otelcol_ratelimit.requests", 1, map[string]struct{}{}, attrs...)
		})
	}
}

func assertMetrics(t *testing.T, rm metricdata.ResourceMetrics, name string, v int64, ignoredDimensions map[string]struct{}, expectedAttrs ...attribute.KeyValue) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				break
			}

			dp := m.Data.(metricdata.Sum[int64]).DataPoints
			for i := range dp {
				actualAttrs := dp[i].Attributes.ToSlice()

				// Removed ignored dimensions from actualAttrs
				filteredAttrs := make([]attribute.KeyValue, 0, len(actualAttrs))
				for i := range actualAttrs {
					if _, found := ignoredDimensions[string(actualAttrs[i].Key)]; !found {
						filteredAttrs = append(filteredAttrs, actualAttrs[i])
					}
				}

				if !reflect.DeepEqual(expectedAttrs, filteredAttrs) {
					continue
				}

				require.Equal(t, v, dp[i].Value)
				return
			}
			require.Fail(t, "metric with specified attributes could not be found", name)
		}
	}
	assert.Fail(t, "metric could not be found", name)
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
