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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadatatest"
	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/telemetry"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

var (
	inflight      int64
	clientContext = client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"x-tenant-id": {"TestProjectID"},
		}),
	})
)

func TestGetCountFunc_Logs(t *testing.T) {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	resourceLogs.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	resourceLogs.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	f := getLogsCountFunc(StrategyRateLimitRequests)
	assert.Equal(t, 1, f(logs))

	f = getLogsCountFunc(StrategyRateLimitRecords)
	assert.Equal(t, 2, f(logs))

	f = getLogsCountFunc(StrategyRateLimitBytes)
	assert.Greater(t, f(logs), 2)

	assert.Nil(t, getLogsCountFunc(""))
}

func TestGetCountFunc_Metrics(t *testing.T) {
	metrics := pmetric.NewMetrics()
	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	resourceMetrics.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptySum().DataPoints().AppendEmpty()
	resourceMetrics.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptySummary().DataPoints().AppendEmpty()

	f := getMetricsCountFunc(StrategyRateLimitRequests)
	assert.Equal(t, 1, f(metrics))

	f = getMetricsCountFunc(StrategyRateLimitRecords)
	assert.Equal(t, 2, f(metrics))

	f = getMetricsCountFunc(StrategyRateLimitBytes)
	assert.Greater(t, f(metrics), 2)

	assert.Nil(t, getMetricsCountFunc(""))
}

func TestGetCountFunc_Traces(t *testing.T) {
	traces := ptrace.NewTraces()
	resourceTraces := traces.ResourceSpans().AppendEmpty()
	resourceTraces.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	resourceTraces.ScopeSpans().AppendEmpty().Spans().AppendEmpty()

	f := getTracesCountFunc(StrategyRateLimitRequests)
	assert.Equal(t, 1, f(traces))

	f = getTracesCountFunc(StrategyRateLimitRecords)
	assert.Equal(t, 2, f(traces))

	f = getTracesCountFunc(StrategyRateLimitBytes)
	assert.Greater(t, f(traces), 2)

	assert.Nil(t, getTracesCountFunc(""))
}

func TestGetCountFunc_Profiles(t *testing.T) {
	profiles := pprofile.NewProfiles()
	resourceProfiles := profiles.ResourceProfiles().AppendEmpty()
	resourceProfiles.ScopeProfiles().AppendEmpty().Profiles().AppendEmpty().Sample().AppendEmpty()
	resourceProfiles.ScopeProfiles().AppendEmpty().Profiles().AppendEmpty().Sample().AppendEmpty()
	resourceProfiles.ScopeProfiles().AppendEmpty().Profiles().AppendEmpty().Sample().AppendEmpty()

	f := getProfilesCountFunc(StrategyRateLimitRequests)
	assert.Equal(t, 1, f(profiles))

	f = getProfilesCountFunc(StrategyRateLimitRecords)
	assert.Equal(t, 3, f(profiles))

	f = getProfilesCountFunc(StrategyRateLimitBytes)
	assert.Greater(t, f(profiles), 2)

	assert.Nil(t, getProfilesCountFunc(""))
}

func TestConsume_Logs(t *testing.T) {
	rateLimiter := newTestLocalRateLimiter(t, &Config{
		Type: LocalRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Rate:             1,
			Burst:            1,
			ThrottleBehavior: ThrottleBehaviorError,
			RetryDelay:       1 * time.Second,
			ThrottleInterval: 1 * time.Second,
		},
	})
	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	observedZapCore, observedLogs := observer.New(zapcore.ErrorLevel)
	tt := componenttest.NewTelemetry()
	telemetryBuilder, err := metadata.NewTelemetryBuilder(tt.NewTelemetrySettings())
	require.NoError(t, err)

	consumed := false
	rl := rateLimiterProcessor{
		rl:               rateLimiter,
		telemetryBuilder: telemetryBuilder,
		logger:           zap.New(observedZapCore),
		inflight:         &inflight,
		metadataKeys:     []string{"x-tenant-id"},
		strategy:         StrategyRateLimitBytes,
	}
	processor := &LogsRateLimiterProcessor{
		rateLimiterProcessor: rl,
		count: func(plog.Logs) int {
			return 1
		},
		next: func(context.Context, plog.Logs) error {
			consumed = true
			return nil
		},
	}

	logs := plog.NewLogs()
	err = processor.ConsumeLogs(clientContext, logs)
	assert.True(t, consumed)
	assert.NoError(t, err)
	testRequestSize(t, tt, 1, 1)

	consumed = false
	err = processor.ConsumeLogs(clientContext, logs)
	assert.False(t, consumed)
	testError(t, err)

	testRatelimitLogMetadata(t, observedLogs.TakeAll())
	testRateLimitTelemetry(t, tt)
	testRequestSize(t, tt, 2, 2)
}

func TestConsume_Metrics(t *testing.T) {
	rateLimiter := newTestLocalRateLimiter(t, &Config{
		Type: LocalRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Rate:             1,
			Burst:            1,
			ThrottleBehavior: ThrottleBehaviorError,
			RetryDelay:       1 * time.Second,
			ThrottleInterval: 1 * time.Second,
		},
	})
	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	observedZapCore, observedLogs := observer.New(zapcore.ErrorLevel)
	tt := componenttest.NewTelemetry()
	telemetryBuilder, err := metadata.NewTelemetryBuilder(tt.NewTelemetrySettings())
	require.NoError(t, err)

	consumed := false
	rl := rateLimiterProcessor{
		rl:               rateLimiter,
		telemetryBuilder: telemetryBuilder,
		logger:           zap.New(observedZapCore),
		inflight:         &inflight,
		metadataKeys:     []string{"x-tenant-id"},
		strategy:         StrategyRateLimitBytes,
	}
	processor := &MetricsRateLimiterProcessor{
		rateLimiterProcessor: rl,
		count: func(pmetric.Metrics) int {
			return 1
		},
		next: func(context.Context, pmetric.Metrics) error {
			consumed = true
			return nil
		},
	}

	metrics := pmetric.NewMetrics()
	err = processor.ConsumeMetrics(clientContext, metrics)
	assert.True(t, consumed)
	assert.NoError(t, err)
	testRequestSize(t, tt, 1, 1)

	consumed = false
	err = processor.ConsumeMetrics(clientContext, metrics)
	assert.False(t, consumed)
	testError(t, err)

	testRatelimitLogMetadata(t, observedLogs.TakeAll())
	testRateLimitTelemetry(t, tt)
	testRequestSize(t, tt, 2, 2)
}

func TestConsume_Traces(t *testing.T) {
	rateLimiter := newTestLocalRateLimiter(t, &Config{
		Type: LocalRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Rate:             1,
			Burst:            1,
			ThrottleBehavior: ThrottleBehaviorError,
			RetryDelay:       1 * time.Second,
			ThrottleInterval: 1 * time.Second,
		},
	})
	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	observedZapCore, observedLogs := observer.New(zapcore.ErrorLevel)
	tt := componenttest.NewTelemetry()
	telemetryBuilder, err := metadata.NewTelemetryBuilder(tt.NewTelemetrySettings())
	require.NoError(t, err)

	consumed := false
	rl := rateLimiterProcessor{
		rl:               rateLimiter,
		telemetryBuilder: telemetryBuilder,
		logger:           zap.New(observedZapCore),
		inflight:         &inflight,
		metadataKeys:     []string{"x-tenant-id"},
		strategy:         StrategyRateLimitBytes,
	}
	processor := &TracesRateLimiterProcessor{
		rateLimiterProcessor: rl,
		count: func(traces ptrace.Traces) int {
			return 1
		},
		next: func(context.Context, ptrace.Traces) error {
			consumed = true
			return nil
		},
	}

	traces := ptrace.NewTraces()
	err = processor.ConsumeTraces(clientContext, traces)
	assert.True(t, consumed)
	assert.NoError(t, err)
	testRequestSize(t, tt, 1, 1)

	consumed = false
	err = processor.ConsumeTraces(clientContext, traces)
	assert.False(t, consumed)
	testError(t, err)

	testRatelimitLogMetadata(t, observedLogs.TakeAll())
	testRateLimitTelemetry(t, tt)
	testRequestSize(t, tt, 2, 2)
}

func TestConsume_Profiles(t *testing.T) {
	rateLimiter := newTestLocalRateLimiter(t, &Config{
		Type: LocalRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Rate:             1,
			Burst:            1,
			ThrottleBehavior: ThrottleBehaviorError,
			RetryDelay:       1 * time.Second,
			ThrottleInterval: 1 * time.Second,
		},
	})
	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	observedZapCore, observedLogs := observer.New(zapcore.ErrorLevel)
	tt := componenttest.NewTelemetry()
	telemetryBuilder, err := metadata.NewTelemetryBuilder(tt.NewTelemetrySettings())
	require.NoError(t, err)
	defer telemetryBuilder.Shutdown()

	consumed := false
	rl := rateLimiterProcessor{
		rl:               rateLimiter,
		telemetryBuilder: telemetryBuilder,
		logger:           zap.New(observedZapCore),
		inflight:         &inflight,
		metadataKeys:     []string{"x-tenant-id"},
		strategy:         StrategyRateLimitBytes,
	}
	processor := &ProfilesRateLimiterProcessor{
		rateLimiterProcessor: rl,
		count: func(profiles pprofile.Profiles) int {
			return 1
		},
		next: func(context.Context, pprofile.Profiles) error {
			consumed = true
			return nil
		},
	}

	profiles := pprofile.NewProfiles()
	err = processor.ConsumeProfiles(clientContext, profiles)
	assert.True(t, consumed)
	assert.NoError(t, err)
	testRequestSize(t, tt, 1, 1)

	consumed = false
	err = processor.ConsumeProfiles(clientContext, profiles)
	assert.False(t, consumed)
	testError(t, err)

	testRatelimitLogMetadata(t, observedLogs.TakeAll())
	testRateLimitTelemetry(t, tt)
	testRequestSize(t, tt, 2, 2)
}

func TestConcurrentRequestsTelemetry(t *testing.T) {
	rateLimiter := newTestLocalRateLimiter(t, &Config{
		Type: LocalRateLimiter,
		RateLimitSettings: RateLimitSettings{
			Rate:             10,
			Burst:            10,
			ThrottleBehavior: ThrottleBehaviorError,
			RetryDelay:       1 * time.Second,
			ThrottleInterval: 1 * time.Second,
		},
	})
	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	tt := componenttest.NewTelemetry()
	telemetryBuilder, err := metadata.NewTelemetryBuilder(tt.NewTelemetrySettings())
	require.NoError(t, err)

	var (
		consumedCount int32
		wg            sync.WaitGroup
		startCh       = make(chan struct{})
		numWorkers    = 2
		blockCh       = make(chan struct{})

		readyWg sync.WaitGroup // readyWg to wait until all workers are inflight
	)

	readyWg.Add(numWorkers)
	rl := rateLimiterProcessor{
		rl:               rateLimiter,
		telemetryBuilder: telemetryBuilder,
		inflight:         &inflight,
		metadataKeys:     []string{"x-tenant-id"},
	}
	processor := &MetricsRateLimiterProcessor{
		rateLimiterProcessor: rl,
		count: func(pmetric.Metrics) int {
			return 1
		},
		next: func(ctx context.Context, md pmetric.Metrics) error {
			atomic.AddInt32(&consumedCount, 1)
			readyWg.Done()
			<-blockCh
			return nil
		},
	}

	metrics := pmetric.NewMetrics()
	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh
			_ = processor.ConsumeMetrics(clientContext, metrics)
		}()
	}

	close(startCh)
	readyWg.Wait()

	m, err := tt.GetMetric("otelcol_ratelimit.concurrent_requests")
	require.NoError(t, err, "expected to observe otelcol_ratelimit.concurrent_requests")
	for _, dp := range m.Data.(metricdata.Gauge[int64]).DataPoints {
		if assert.Equal(t, int64(2), dp.Value, "expected to observe otelcol_ratelimit.concurrent_requests == 2") {
			break
		}
	}

	// Release both goroutines
	close(blockCh)
	wg.Wait()
}

func testRequestSize(t *testing.T, tt *componenttest.Telemetry, count int, sum int) {
	metadatatest.AssertEqualRatelimitRequestSize(t, tt, []metricdata.HistogramDataPoint[int64]{
		{
			Count: uint64(count),
			Sum:   int64(sum),
			Attributes: attribute.NewSet(
				[]attribute.KeyValue{
					attribute.String("x-tenant-id", "TestProjectID"),
				}...),
		},
	}, metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreValue())
}

func testRateLimitTelemetry(t *testing.T, tel *componenttest.Telemetry) {
	metadatatest.AssertEqualRatelimitRequests(t, tel, []metricdata.DataPoint[int64]{
		{
			Value: 1,
			Attributes: attribute.NewSet(
				[]attribute.KeyValue{
					telemetry.WithDecision("accepted"),
					telemetry.WithReason(telemetry.StatusUnderLimit),
					attribute.String("x-tenant-id", "TestProjectID"),
				}...),
		},
		{
			Value: 1,
			Attributes: attribute.NewSet(
				[]attribute.KeyValue{
					telemetry.WithDecision("throttled"),
					attribute.String("x-tenant-id", "TestProjectID"),
				}...),
		},
	}, metricdatatest.IgnoreTimestamp())

	metadatatest.AssertEqualRatelimitRequestDuration(t, tel, []metricdata.HistogramDataPoint[float64]{
		{
			Attributes: attribute.NewSet(
				attribute.String("x-tenant-id", "TestProjectID"),
			),
		},
	}, metricdatatest.IgnoreValue(), metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualRatelimitConcurrentRequests(t, tel, []metricdata.DataPoint[int64]{
		{
			Value: 1,
			Attributes: attribute.NewSet(
				attribute.String("x-tenant-id", "TestProjectID"),
			),
		},
	}, metricdatatest.IgnoreValue(), metricdatatest.IgnoreTimestamp())
}

func testRatelimitLogMetadata(t *testing.T, logEntries []observer.LoggedEntry) {
	require.Len(t, logEntries, 1, "Expected exactly one error log entry")
	logEntry := logEntries[0]
	assert.Equal(t, zapcore.ErrorLevel, logEntry.Level)

	fields := make(map[string]any)
	for _, field := range logEntry.Context {
		switch field.Type {
		case zapcore.StringType:
			fields[field.Key] = field.String
		case zapcore.Int64Type:
			fields[field.Key] = field.Integer
		}
	}

	assert.Equal(t, "TestProjectID", fields["x-tenant-id"])
	assert.Equal(t, int64(1), fields["hits"])
}

func testError(t *testing.T, err error) {
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "expected gRPC status error")
	assert.Equal(t, codes.ResourceExhausted, st.Code())
	assert.Equal(t, "rpc error: code = ResourceExhausted desc = too many requests", st.Err().Error())
	details := st.Details()
	require.Len(t, details, 2, "expected 2 details")
	errorInfo, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok, "expected errorinfo detail")
	assert.Equal(t, "ingest.elastic.co", errorInfo.Domain)
	assert.Equal(t, map[string]string{
		"component":         "ratelimitprocessor",
		"limit":             "1",
		"throttle_interval": "1s",
	}, errorInfo.Metadata)
	retryInfo, ok := details[1].(*errdetails.RetryInfo)
	require.True(t, ok, "expected retryinfo detail")
	assert.Equal(t, "seconds:1", retryInfo.RetryDelay.String())
}
