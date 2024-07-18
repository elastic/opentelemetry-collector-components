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

package spanmetricsconnectorv2

import (
	"path/filepath"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestConfig(t *testing.T) {
	for _, tc := range []struct {
		path     string // relative to testdata directory
		expected *Config
	}{
		{
			path: "with_default",
			expected: &Config{
				Spans: defaultSpansConfig(),
			},
		},
		{
			path: "with_attributes",
			expected: &Config{
				Spans: map[string]MetricInfo{
					"http.trace.span.duration": {
						Description: "Span duration for HTTP spans",
						Unit:        MetricUnitMs,
						Attributes:  []AttributeConfig{{Key: "http.response.status_code"}},
						Histogram: HistogramConfig{
							Explicit: &ExplicitHistogramConfig{
								Buckets: defaultExplicitHistogramBuckets(MetricUnitMs),
							},
						},
					},
					"db.trace.span.duration": {
						Description: "Span duration for DB spans",
						Unit:        MetricUnitMs,
						Attributes:  []AttributeConfig{{Key: "db.system"}},
						Histogram: HistogramConfig{
							Explicit: &ExplicitHistogramConfig{
								Buckets: defaultExplicitHistogramBuckets(MetricUnitMs),
							},
						},
					},
					"msg.trace.span.duration": {
						Description: "Span duration for messaging spans",
						Unit:        MetricUnitMs,
						Attributes:  []AttributeConfig{{Key: "messaging.system"}},
						Histogram: HistogramConfig{
							Explicit: &ExplicitHistogramConfig{
								Buckets: defaultExplicitHistogramBuckets(MetricUnitMs),
							},
						},
					},
				},
			},
		},
		{
			path: "with_custom_histogram_buckets",
			expected: &Config{
				Spans: map[string]MetricInfo{
					"trace.span.duration": {
						Description: "Span duration with custom histogram buckets",
						Unit:        MetricUnitS,
						Histogram: HistogramConfig{
							Explicit: &ExplicitHistogramConfig{
								Buckets: []float64{0.001, 0.1, 10, 100},
							},
						},
					},
				},
			},
		},
	} {
		t.Run(tc.path, func(t *testing.T) {
			dir := filepath.Join("testdata", tc.path)
			cfg := createDefaultConfig()
			cm, err := confmaptest.LoadConf(filepath.Join(dir, "config.yaml"))
			require.NoError(t, err)

			sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(&cfg))

			assert.NoError(t, component.ValidateConfig(cfg))
			assert.Equal(t, tc.expected, cfg)
		})
	}
}
