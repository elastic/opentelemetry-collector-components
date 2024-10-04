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

package config

// import (
// 	"path/filepath"
// 	"testing"
//
// 	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/metadata"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/require"
// 	"go.opentelemetry.io/collector/component"
// 	"go.opentelemetry.io/collector/confmap/confmaptest"
// )
//
// func TestConfig(t *testing.T) {
// 	for _, tc := range []struct {
// 		path     string // relative to testdata directory
// 		expected *Config
// 		errorMsg string
// 	}{
// 		{
// 			path:     "with_empty",
// 			expected: &Config{},
// 			errorMsg: "no configuration provided",
// 		},
// 		{
// 			path: "with_attributes",
// 			expected: &Config{
// 				Spans: []SpanMetricInfo{
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "http.trace.span.duration",
// 							Description: "Span duration for HTTP spans",
// 							Attributes:  []Attribute{{Key: "http.response.status_code"}},
// 						},
// 						Unit: MetricUnitMs,
// 						Histogram: Histogram{
// 							Explicit: &ExplicitHistogram{
// 								Buckets: defaultHistogramBuckets[:],
// 							},
// 							Exponential: &ExponentialHistogram{
// 								MaxSize: defaultExponentialHistogramMaxSize,
// 							},
// 						},
// 					},
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "db.trace.span.duration",
// 							Description: "Span duration for DB spans",
// 							Attributes:  []Attribute{{Key: "db.system"}},
// 						},
// 						Unit: MetricUnitMs,
// 						Histogram: Histogram{
// 							Explicit: &ExplicitHistogram{
// 								Buckets: defaultHistogramBuckets[:],
// 							},
// 							Exponential: &ExponentialHistogram{
// 								MaxSize: defaultExponentialHistogramMaxSize,
// 							},
// 						},
// 					},
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "msg.trace.span.duration",
// 							Description: "Span duration for messaging spans",
// 							Attributes:  []Attribute{{Key: "messaging.system"}},
// 						},
// 						Unit: MetricUnitMs,
// 						Histogram: Histogram{
// 							Explicit: &ExplicitHistogram{
// 								Buckets: defaultHistogramBuckets[:],
// 							},
// 							Exponential: &ExponentialHistogram{
// 								MaxSize: defaultExponentialHistogramMaxSize,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			path: "with_custom_histogram_configs",
// 			expected: &Config{
// 				Spans: []SpanMetricInfo{
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "trace.span.duration",
// 							Description: "Span duration with custom histogram buckets",
// 						},
// 						Unit: MetricUnitS,
// 						Histogram: Histogram{
// 							Explicit: &ExplicitHistogram{
// 								Buckets: []float64{0.001, 0.1, 1, 10},
// 							},
// 							Exponential: &ExponentialHistogram{
// 								MaxSize: 2,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			path:     "with_identical_metric_name_identical_attrs",
// 			errorMsg: "duplicate configuration found",
// 		},
// 		{
// 			path: "with_identical_metric_name_different_attrs",
// 			expected: &Config{
// 				Spans: []SpanMetricInfo{
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "identical.name",
// 							Description: "Identical description",
// 							Attributes:  []Attribute{{Key: "key.1"}},
// 						},
// 						Unit: MetricUnitMs,
// 						Histogram: Histogram{
// 							Explicit: &ExplicitHistogram{
// 								Buckets: defaultHistogramBuckets[:],
// 							},
// 							Exponential: &ExponentialHistogram{
// 								MaxSize: defaultExponentialHistogramMaxSize,
// 							},
// 						},
// 					},
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "identical.name",
// 							Description: "Different description",
// 							Attributes:  []Attribute{{Key: "key.2"}},
// 						},
// 						Unit: MetricUnitMs,
// 						Histogram: Histogram{
// 							Explicit: &ExplicitHistogram{
// 								Buckets: defaultHistogramBuckets[:],
// 							},
// 							Exponential: &ExponentialHistogram{
// 								MaxSize: defaultExponentialHistogramMaxSize,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			path: "with_identical_metric_name_desc_different_attrs",
// 			expected: &Config{
// 				Spans: []SpanMetricInfo{
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "identical.name",
// 							Description: "Identical description",
// 							Attributes:  []Attribute{{Key: "key.1"}},
// 						},
// 						Unit: MetricUnitMs,
// 						Histogram: Histogram{
// 							Explicit: &ExplicitHistogram{
// 								Buckets: defaultHistogramBuckets[:],
// 							},
// 							Exponential: &ExponentialHistogram{
// 								MaxSize: defaultExponentialHistogramMaxSize,
// 							},
// 						},
// 					},
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "identical.name",
// 							Description: "Identical description",
// 							Attributes:  []Attribute{{Key: "key.2"}},
// 						},
// 						Unit: MetricUnitMs,
// 						Histogram: Histogram{
// 							Explicit: &ExplicitHistogram{
// 								Buckets: defaultHistogramBuckets[:],
// 							},
// 							Exponential: &ExponentialHistogram{
// 								MaxSize: defaultExponentialHistogramMaxSize,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			path: "with_summary",
// 			expected: &Config{
// 				Spans: []SpanMetricInfo{
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "http.trace.span.summary",
// 							Description: "Summary for HTTP spans",
// 							Attributes:  []Attribute{{Key: "http.response.status_code"}},
// 						},
// 						Unit:    MetricUnitMs,
// 						Summary: &Summary{},
// 					},
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "db.trace.span.summary",
// 							Description: "Summary for DB spans",
// 							Attributes:  []Attribute{{Key: "db.system"}},
// 						},
// 						Unit:    MetricUnitMs,
// 						Summary: &Summary{},
// 					},
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "msg.trace.span.summary",
// 							Description: "Summary for messaging spans",
// 							Attributes:  []Attribute{{Key: "messaging.system"}},
// 						},
// 						Unit:    MetricUnitMs,
// 						Summary: &Summary{},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			path: "with_include_resource_attributes",
// 			expected: &Config{
// 				Spans: []SpanMetricInfo{
// 					{
//
// 						MetricInfo: MetricInfo{
// 							Name:                       "with_resource_attribute_filtering_foo",
// 							Description:                "Output with resource attribute filtering on foo",
// 							EphemeralResourceAttribute: true,
// 							IncludeResourceAttributes:  []Attribute{{Key: "resource.foo"}},
// 						},
// 						Unit:    MetricUnitMs,
// 						Summary: &Summary{},
// 					},
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:                       "with_resource_attribute_filtering_bar",
// 							Description:                "Output with resource attribute filtering on bar",
// 							EphemeralResourceAttribute: true,
// 							IncludeResourceAttributes:  []Attribute{{Key: "resource.bar"}},
// 						},
// 						Unit:    MetricUnitMs,
// 						Summary: &Summary{},
// 					},
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "without_resource_attribute_filtering",
// 							Description: "Output with no resource attribute filtering",
// 						},
// 						Unit:    MetricUnitMs,
// 						Summary: &Summary{},
// 					},
// 				},
// 			},
// 		},
// 		{
// 			path: "with_sum_and_count",
// 			expected: &Config{
// 				Spans: []SpanMetricInfo{
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "http.trace.span.sumcount",
// 							Description: "Sum and count for HTTP spans with default metric suffixes",
// 							Attributes:  []Attribute{{Key: "http.response.status_code"}},
// 						},
// 						Unit: MetricUnitMs,
// 						SumAndCount: &SumAndCount{
// 							SumSuffix:   ".sum",
// 							CountSuffix: ".count",
// 						},
// 					},
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "db.trace.span.sumcount",
// 							Description: "Sum and count for DB spans with default metric suffixes",
// 							Attributes:  []Attribute{{Key: "db.system"}},
// 						},
// 						Unit: MetricUnitMs,
// 						SumAndCount: &SumAndCount{
// 							SumSuffix:   ".sum",
// 							CountSuffix: ".count",
// 						},
// 					},
// 					{
// 						MetricInfo: MetricInfo{
// 							Name:        "msg.trace.span.sumcount",
// 							Description: "Sum and count for messaging spans with custom metric suffixes",
// 							Attributes:  []Attribute{{Key: "messaging.system"}},
// 						},
// 						Unit: MetricUnitUs,
// 						SumAndCount: &SumAndCount{
// 							SumSuffix:   ".sum.us",
// 							CountSuffix: ".count",
// 						},
// 					},
// 				},
// 			},
// 		},
// 	} {
// 		t.Run(tc.path, func(t *testing.T) {
// 			dir := filepath.Join("../testdata", tc.path)
// 			cfg := &Config{}
// 			cm, err := confmaptest.LoadConf(filepath.Join(dir, "config.yaml"))
// 			require.NoError(t, err)
//
// 			sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
// 			require.NoError(t, err)
// 			require.NoError(t, sub.Unmarshal(&cfg))
//
// 			err = component.ValidateConfig(cfg)
// 			if tc.errorMsg != "" {
// 				assert.ErrorContains(t, err, tc.errorMsg)
// 				return
// 			}
//
// 			assert.NoError(t, err)
// 			assert.Equal(t, tc.expected, cfg)
// 		})
// 	}
// }
