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
	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadatatest"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
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

	telemetrySettings := component.TelemetrySettings{MeterProvider: mp, Logger: zap.NewNop()}
	telemetryBuilder, err := metadata.NewTelemetryBuilder(telemetrySettings)
	assert.NoError(t, err)

	rl, err = newGubernatorRateLimiter(cfg, processor.Settings{
		ID:                component.NewIDWithName(metadata.Type, "abc123"),
		TelemetrySettings: telemetrySettings,
		BuildInfo:         component.NewDefaultBuildInfo(),
	}, telemetryBuilder)

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
			assert.EqualError(t, err, "too many requests")
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
				assert.EqualError(t, err, "too many requests")
			case ThrottleBehaviorDelay:
				assert.EqualError(t, err, "too many requests")
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
	assert.EqualError(t, err, "too many requests")
	assert.Equal(t, "ratelimit/abc123", req.Requests[0].Name)
	assert.Equal(t, "metadata_key:value1", req.Requests[0].UniqueKey)

	err = rateLimiter.RateLimit(clientContext2, 1)
	assert.EqualError(t, err, "too many requests")
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
	clientContext := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"x-elastic-project-id": {"TestProjectID"},
		}),
	})

	err := rl.Start(clientContext, componenttest.NewNopHost())
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
			Responses: []*gubernator.RateLimitResp{{Status: gubernator.Status_UNDER_LIMIT, Remaining: 1}},
		}, nil
	}

	err = rl.RateLimit(clientContext, 1)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), getRatelimitRequests())

	// Over limit case
	server.getRateLimits = func(ctx context.Context, req *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error) {
		return &gubernator.GetRateLimitsResp{
			Responses: []*gubernator.RateLimitResp{{Status: gubernator.Status_OVER_LIMIT, ResetTime: time.Now().Add(100 * time.Millisecond).UnixMilli()}},
		}, nil
	}
	err = rl.RateLimit(clientContext, 1)
	assert.EqualError(t, err, "too many requests")
	assert.Equal(t, int64(2), getRatelimitRequests())

	// Error from server
	server.getRateLimits = func(ctx context.Context, req *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error) {
		return nil, errors.New("server error")
	}
	err = rl.RateLimit(clientContext, 1)
	assert.Error(t, err)
	assert.Equal(t, int64(3), getRatelimitRequests())

	// Custom error in response
	server.getRateLimits = func(ctx context.Context, req *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error) {
		return &gubernator.GetRateLimitsResp{
			Responses: []*gubernator.RateLimitResp{{Error: "custom error"}},
		}, nil
	}
	err = rl.RateLimit(clientContext, 1)
	assert.Error(t, err)
	assert.Equal(t, int64(4), getRatelimitRequests())
}

func TestRateLimitThresholdAttribute(t *testing.T) {
	limitUsedPercent := []int64{20, 30, 60, 80, 90, 95, 100}
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)

	cfg := &Config{
		Rate:             1,
		Burst:            2,
		MetadataKeys:     []string{"x-elastic-project-id"},
		ThrottleBehavior: ThrottleBehaviorDelay,
	}

	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"x-elastic-project-id": {"TestProjectID"},
		}),
	})

	for _, pctUsed := range limitUsedPercent {
		t.Run(strconv.Itoa(int(pctUsed)), func(t *testing.T) {
			// Create a new telemetry instance for each test case
			telemetry := componenttest.NewTelemetry()
			tb, err := metadata.NewTelemetryBuilder(telemetry.NewTelemetrySettings())
			require.NoError(t, err)
			defer tb.Shutdown()

			server, rl := newTestGubernatorRateLimiter(t, cfg, mp)
			server.getRateLimits = func(ctx context.Context, req *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error) {
				return &gubernator.GetRateLimitsResp{
					Responses: []*gubernator.RateLimitResp{{
						Status:    gubernator.Status_UNDER_LIMIT,
						Limit:     100,
						Remaining: 100 - pctUsed}},
				}, nil
			}

			err = rl.Start(ctx, componenttest.NewNopHost())
			require.NoError(t, err)

			err = rl.RateLimit(ctx, 1)

			var expectedAttrs []attribute.KeyValue
			if pctUsed == 100 {
				assert.EqualError(t, err, "too many requests")
				expectedAttrs = []attribute.KeyValue{
					attribute.Float64("limit_threshold", 0.95),
					attribute.String("ratelimit_decision", "throttled"),
					attribute.String("x-elastic-project-id", "TestProjectID"),
				}
			} else {
				assert.NoError(t, err)
				expectedAttrs = []attribute.KeyValue{
					attribute.Float64("limit_threshold", getLimitThresholdBucket(float64(pctUsed)/100)),
					attribute.String("ratelimit_decision", "accepted"),
					attribute.String("reason", "under_limit"),
				}
			}

			// Record the metric using the telemetry builder
			tb.RatelimitRequests.Add(ctx, 1, otelmetric.WithAttributes(expectedAttrs...))

			metadatatest.AssertEqualRatelimitRequests(t, telemetry, []metricdata.DataPoint[int64]{
				{
					Value:      1,
					Attributes: attribute.NewSet(expectedAttrs...),
				},
			}, metricdatatest.IgnoreTimestamp())

			require.NoError(t, telemetry.Shutdown(context.Background()))
		})
	}
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

func TestGubernatorRateLimiter_RequestDuration(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	otel.SetMeterProvider(mp)

	cfg := &Config{
		Rate:             1,
		Burst:            2,
		MetadataKeys:     []string{"x-elastic-project-id"},
		ThrottleBehavior: ThrottleBehaviorDelay,
	}

	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"x-elastic-project-id": {"TestProjectID"},
		}),
	})

	// Create a new telemetry instance for each test case
	telemetry := componenttest.NewTelemetry()
	tb, err := metadata.NewTelemetryBuilder(telemetry.NewTelemetrySettings())
	require.NoError(t, err)
	defer tb.Shutdown()

	server, rl := newTestGubernatorRateLimiter(t, cfg, mp)
	server.getRateLimits = func(ctx context.Context, req *gubernator.GetRateLimitsReq) (*gubernator.GetRateLimitsResp, error) {
		return &gubernator.GetRateLimitsResp{
			Responses: []*gubernator.RateLimitResp{{
				Status:    gubernator.Status_UNDER_LIMIT,
				Limit:     100,
				Remaining: 80}},
		}, nil
	}

	err = rl.Start(ctx, componenttest.NewNopHost())
	require.NoError(t, err)

	err = rl.RateLimit(ctx, 1)
	assert.NoError(t, err)

	// Record the metric using the telemetry builder
	tb.RatelimitRequestDuration.Record(ctx, 0.05)

	metadatatest.AssertEqualRatelimitRequestDuration(t, telemetry, []metricdata.HistogramDataPoint[float64]{
		{
			Attributes:   attribute.NewSet(),
			Count:        1,
			Sum:          0.05,
			Min:          metricdata.NewExtrema(0.05),
			Max:          metricdata.NewExtrema(0.05),
			Bounds:       []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			BucketCounts: []uint64{0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}, metricdatatest.IgnoreTimestamp())

	require.NoError(t, telemetry.Shutdown(context.Background()))
}
