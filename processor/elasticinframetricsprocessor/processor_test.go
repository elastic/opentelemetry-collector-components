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

package elasticinframetricsprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

func TestProcessMetrics(t *testing.T) {
	testCases := []struct {
		name               string
		cfg                *Config
		createMetrics      func() pmetric.Metrics
		expectedHostname   string
		expectedMetricName string
		expetedMetricValue int64
	}{
		{
			name: "ProcessMetrics when AddSystemMetrics is enabled",
			cfg:  &Config{AddSystemMetrics: true},
			createMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr("host.name", "test-host")
				sm := rm.ScopeMetrics().AppendEmpty()
				metric := sm.Metrics().AppendEmpty()
				metric.SetName("test.metric")
				dp := metric.SetEmptySum().DataPoints().AppendEmpty()
				dp.SetIntValue(10)
				return md
			},
			expectedHostname:   "test-host",
			expectedMetricName: "test.metric",
			expetedMetricValue: 10,
		},
		{
			name: "ProcessMetrics when AddSystemMetrics is disabled",
			cfg:  &Config{AddSystemMetrics: false},
			createMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr("host.name", "test-host")
				sm := rm.ScopeMetrics().AppendEmpty()
				metric := sm.Metrics().AppendEmpty()
				metric.SetName("test.metric")
				return md
			},
			expectedHostname:   "",
			expectedMetricName: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			set := processor.Settings{
				TelemetrySettings: component.TelemetrySettings{
					Logger: zap.NewNop(),
				},
			}

			p := newProcessor(set, tc.cfg)

			md := tc.createMetrics()
			_, err := p.processMetrics(context.Background(), md)

			assert.NoError(t, err)

			// Check if remapper was initialized based on the config
			if tc.cfg.AddSystemMetrics {
				assert.NotEmpty(t, p.remappers, "expected remapper to be initialized")
				// Check remapping results
				if len(p.remappers) > 0 {
					rm := md.ResourceMetrics().At(0)
					resource := rm.Resource()
					scopeMetric := rm.ScopeMetrics().At(0)
					metric := scopeMetric.Metrics().At(0)
					dpvalue := metric.Sum().DataPoints().At(0).IntValue()
					hostname, ok := resource.Attributes().Get("host.name")
					assert.True(t, ok, "expected attribute 'host.name'")
					assert.Equal(t, tc.expectedHostname, hostname.Str(), "expected resource attribute to be 'test-host'")
					assert.Equal(t, tc.expectedMetricName, metric.Name(), "expected metric name to be 'test.metric'")
					assert.Equal(t, tc.expetedMetricValue, dpvalue, "expected metric value to be 10")

				}
			} else {
				assert.Empty(t, p.remappers, "expected no remapper to be initialized")
			}
		})
	}
}

func TestDropOriginalMetrics(t *testing.T) {
	testCases := []struct {
		name                   string
		cfg                    *Config
		createMetrics          func() pmetric.Metrics
		expectedPodname        string
		expectedMetricName     string
		expetedMetricValue     float64
		expetedLengthOfMetrics int
	}{
		// {
		// 	name: "ProcessMetrics when AddK8sMetrics is enabled and DropOriginal is enabled",
		// 	cfg:  &Config{AddK8sMetrics: true, DropOriginal: true},
		// 	createMetrics: func() pmetric.Metrics {
		// 		md := pmetric.NewMetrics()
		// 		rm := md.ResourceMetrics().AppendEmpty()
		// 		rm.Resource().Attributes().PutStr("k8s.pod.name", "test-pod")
		// 		sm := rm.ScopeMetrics().AppendEmpty()
		// 		sm.Scope().SetName("github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kubeletstatsreceiver")
		// 		// Creating first metric
		// 		metric1 := sm.Metrics().AppendEmpty()
		// 		metric1.SetName("k8s.pod.cpu_limit_utilization")
		// 		t1 := metric1.SetEmptyGauge()
		// 		t1DP := t1.DataPoints().AppendEmpty()
		// 		t1DP.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		// 		t1DP.SetDoubleValue(0.5)
		// 		t1DP.Attributes().PutBool("test", true)

		// 		//dp1.Attributes().PutBool("otel_remapped", true)
		// 		//Creating second metric
		// 		// This metric does not have the otel_remapped:true attribute set, so it should be dropped
		// 		metric2 := sm.Metrics().AppendEmpty()
		// 		metric2.SetName("k8s.volume.capacity")
		// 		t2 := metric2.SetEmptyGauge()
		// 		t2DP := t2.DataPoints().AppendEmpty()
		// 		t2DP.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		// 		t2DP.SetIntValue(50)
		// 		t2DP.Attributes().PutBool("test", true)
		// 		return md
		// 	},
		// 	expectedPodname:        "test-pod",
		// 	expectedMetricName:     "kubernetes.pod.cpu.usage.limit.pct",
		// 	expetedMetricValue:     0.5,
		// 	expetedLengthOfMetrics: 6,
		// },
		{
			name: "ProcessMetrics when AddK8sMetrics is enabled and DropOriginal is disabled",
			cfg:  &Config{AddK8sMetrics: true, DropOriginal: false},
			createMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr("k8s.pod.name", "test-pod")
				sm := rm.ScopeMetrics().AppendEmpty()
				sm.Scope().SetName("github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kubeletstatsreceiver")
				// Creating first metric
				metric1 := sm.Metrics().AppendEmpty()
				metric1.SetName("k8s.pod.cpu_limit_utilization")
				t1 := metric1.SetEmptyGauge()
				t1DP := t1.DataPoints().AppendEmpty()
				t1DP.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				t1DP.SetDoubleValue(0.5)
				t1DP.Attributes().PutBool("test", true)

				//dp1.Attributes().PutBool("otel_remapped", true)
				//Creating second metric
				// This metric does not have the otel_remapped:true attribute set, so it should be dropped
				metric2 := sm.Metrics().AppendEmpty()
				metric2.SetName("k8s.volume.capacity")
				t2 := metric2.SetEmptyGauge()
				t2DP := t2.DataPoints().AppendEmpty()
				t2DP.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
				t2DP.SetIntValue(50)
				t2DP.Attributes().PutBool("test", true)
				return md
			},
			expectedPodname:        "test-pod",
			expectedMetricName:     "k8s.pod.cpu_limit_utilization",
			expetedMetricValue:     0.5,
			expetedLengthOfMetrics: 8,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			set := processor.Settings{
				TelemetrySettings: component.TelemetrySettings{
					Logger: zap.NewNop(),
				},
			}

			p := newProcessor(set, tc.cfg)

			md := tc.createMetrics()
			md1, err := p.processMetrics(context.Background(), md)

			assert.NoError(t, err)

			// Check if remapper was initialized based on the config
			if tc.cfg.AddK8sMetrics {
				assert.NotEmpty(t, p.remappers, "expected remapper to be initialized")
				// Check remapping results
				if len(p.remappers) > 0 {
					rm := md1.ResourceMetrics().At(0)
					resource := rm.Resource()
					scopeMetric := rm.ScopeMetrics().At(0)
					metric := scopeMetric.Metrics().At(0)
					dpvalue := metric.Gauge().DataPoints().At(0).DoubleValue()
					podname, ok := resource.Attributes().Get("k8s.pod.name")
					assert.True(t, ok, "expected attribute 'k8s.pod.name'")
					assert.Equal(t, tc.expectedPodname, podname.Str(), "expected resource attribute to be 'test-pod'")
					assert.Equal(t, tc.expectedMetricName, metric.Name(), "expected metric name to be 'kubernetes.pod.cpu.usage.limit.pct'")
					assert.Equal(t, tc.expetedMetricValue, dpvalue, "expected metric value to be 0.5")
					assert.Equal(t, tc.expetedLengthOfMetrics, scopeMetric.Metrics().Len(), "expected metrics returned to be 8")

				}
			} else {
				assert.Empty(t, p.remappers, "expected no remapper to be initialized")
			}
		})
	}
}

func TestRemappers(t *testing.T) {
	testCases := []struct {
		name              string
		cfg               *Config
		expectedRemappers int
	}{
		{
			name:              "AddSystemMetrics and AddK8sMetrics are disabled",
			cfg:               &Config{AddSystemMetrics: false, AddK8sMetrics: false},
			expectedRemappers: 0,
		},
		{
			name:              "AddSystemMetrics and AddK8sMetrics are enabled",
			cfg:               &Config{AddSystemMetrics: true, AddK8sMetrics: true},
			expectedRemappers: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			set := processor.Settings{
				TelemetrySettings: component.TelemetrySettings{
					Logger: zap.NewNop(),
				},
			}

			p := newProcessor(set, tc.cfg)
			assert.Equal(t, len(p.remappers), tc.expectedRemappers)
		})
	}
}
