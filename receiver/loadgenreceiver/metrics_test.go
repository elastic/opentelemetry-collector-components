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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
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

func TestMetricsGenerator_addCounterAttr(t *testing.T) {
	for _, addCounterAttr := range []bool{false, true} {
		t.Run(fmt.Sprintf("addCounterAttr=%v", addCounterAttr), func(t *testing.T) {
			doneCh := make(chan Stats)
			var last int64
			consume := consumer.ConsumeMetricsFunc(func(ctx context.Context, md pmetric.Metrics) error {
				// Assertions need to be done in ConsumeMetrics, not in MetricsSink after consuming,
				// as loadgenreceiver perf optimization may reuse and mutate pmetric structs
				rm := md.ResourceMetrics()
				for i := 0; i < rm.Len(); i++ {
					counter, ok := rm.At(i).Resource().Attributes().Get(counterAttr)
					if addCounterAttr {
						require.True(t, ok)
						assert.Equal(t, last+1, counter.Int())
					} else {
						require.False(t, ok)
					}
				}
				last++
				return nil
			})
			sink, err := consumer.NewMetrics(consume)
			require.NoError(t, err)
			cfg := createDefaultReceiverConfig(nil, doneCh, nil)
			cfg.(*Config).Metrics.MaxReplay = 1
			cfg.(*Config).Concurrency = 1
			cfg.(*Config).Metrics.AddCounterAttr = addCounterAttr
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
		})
	}
}
