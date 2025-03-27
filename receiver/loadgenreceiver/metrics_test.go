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

package loadgenreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver"

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

func TestMetricsGenerator_doneCh(t *testing.T) {
	const maxReplay = 2
	for _, concurrency := range []int{1, 2} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			doneCh := make(chan Stats)
			sink := &consumertest.MetricsSink{}
			cfg := createDefaultReceiverConfig(nil, doneCh, nil)
			cfg.(*Config).Metrics.MaxReplay = maxReplay
			cfg.(*Config).Concurrency = concurrency
			r, _ := createMetricsReceiver(context.Background(), receiver.Settings{
				ID: component.ID{},
				TelemetrySettings: component.TelemetrySettings{
					Logger: zap.NewNop(),
				},
				BuildInfo: component.BuildInfo{},
			}, cfg, sink)
			err := r.Start(context.Background(), componenttest.NewNopHost())
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, r.Shutdown(context.Background()))
			}()
			stats := <-doneCh
			want := maxReplay * bytes.Count(demoMetrics, []byte("\n"))
			assert.Equal(t, want, stats.Requests)
			assert.Equal(t, want, len(sink.AllMetrics()))
			assert.Equal(t, sink.DataPointCount(), stats.MetricDataPoints)
		})
	}
}

func TestMetricsGenerator_TSDBWorkaround(t *testing.T) {
	now := time.Now()
	originalTimestamp := time.Unix(1000, 0)
	filename := "metrics.jsonl"
	originalMetrics := pmetric.NewMetrics()
	originalDp := originalMetrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetEmptySum().DataPoints().AppendEmpty()
	originalDp.SetTimestamp(pcommon.NewTimestampFromTime(originalTimestamp))
	marshaler := pmetric.JSONMarshaler{}
	b, err := marshaler.MarshalMetrics(originalMetrics)
	require.NoError(t, err)
	tmpdir := t.TempDir()
	tmpfile := filepath.Join(tmpdir, filename)
	err = os.WriteFile(tmpfile, b, 0600)
	require.NoError(t, err)

	doneCh := make(chan Stats)
	sink := &consumertest.MetricsSink{}
	cfg := createDefaultReceiverConfig(nil, doneCh, nil)
	cfg.(*Config).Metrics.MaxReplay = 1
	cfg.(*Config).Concurrency = 1
	cfg.(*Config).Metrics.JsonlFile = JsonlFile(tmpfile)
	r, err := createMetricsReceiver(context.Background(), receiver.Settings{
		ID: component.ID{},
		TelemetrySettings: component.TelemetrySettings{
			Logger: zap.NewNop(),
		},
		BuildInfo: component.BuildInfo{},
	}, cfg, sink)
	require.NoError(t, err)
	err = r.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, r.Shutdown(context.Background()))
	}()
	<-doneCh
	metrics := sink.AllMetrics()
	dp := metrics[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0)
	assert.GreaterOrEqual(t, dp.Timestamp().AsTime(), now)
	ts, ok := dp.Attributes().Get("original_timestamp")
	require.True(t, ok)
	assert.Equal(t, originalTimestamp.UnixNano(), ts.Int())
}
