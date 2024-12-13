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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
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
	rateLimiter := newTestLocalRateLimiter(t, &Config{Rate: 1, Burst: 1})
	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	consumed := false
	rl := rateLimiterProcessor{
		rl: rateLimiter,
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
	err = processor.ConsumeLogs(context.Background(), logs)
	assert.True(t, consumed)
	assert.NoError(t, err)

	consumed = false
	err = processor.ConsumeLogs(context.Background(), logs)
	assert.False(t, consumed)
	assert.EqualError(t, err, "too many requests")
}

func TestConsume_Metrics(t *testing.T) {
	rateLimiter := newTestLocalRateLimiter(t, &Config{Rate: 1, Burst: 1})
	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	consumed := false
	rl := rateLimiterProcessor{
		rl: rateLimiter,
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
	err = processor.ConsumeMetrics(context.Background(), metrics)
	assert.True(t, consumed)
	assert.NoError(t, err)

	consumed = false
	err = processor.ConsumeMetrics(context.Background(), metrics)
	assert.False(t, consumed)
	assert.EqualError(t, err, "too many requests")
}

func TestConsume_Traces(t *testing.T) {
	rateLimiter := newTestLocalRateLimiter(t, &Config{Rate: 1, Burst: 1})
	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	consumed := false
	rl := rateLimiterProcessor{
		rl: rateLimiter,
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
	err = processor.ConsumeTraces(context.Background(), traces)
	assert.True(t, consumed)
	assert.NoError(t, err)

	consumed = false
	err = processor.ConsumeTraces(context.Background(), traces)
	assert.False(t, consumed)
	assert.EqualError(t, err, "too many requests")
}

func TestConsume_Profiles(t *testing.T) {
	rateLimiter := newTestLocalRateLimiter(t, &Config{Rate: 1, Burst: 1})
	err := rateLimiter.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	consumed := false
	rl := rateLimiterProcessor{
		rl: rateLimiter,
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
	err = processor.ConsumeProfiles(context.Background(), profiles)
	assert.True(t, consumed)
	assert.NoError(t, err)

	consumed = false
	err = processor.ConsumeProfiles(context.Background(), profiles)
	assert.False(t, consumed)
	assert.EqualError(t, err, "too many requests")
}
