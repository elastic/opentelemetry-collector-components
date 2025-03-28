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

package merger

import (
	"math"
	"path/filepath"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const (
	defaultMaxBuckets = 160
)

func TestMergeMetric(t *testing.T) {
	for _, tc := range []string{
		"empty",
		"single_metric",
		"all_overflow",
	} {
		t.Run(tc, func(t *testing.T) {
			v := getTestValue(t)
			dir := filepath.Join("../../testdata/merger", tc)
			md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)
			mergeMetrics(t, v, md)

			// Do a binary<>merger#Value conversion to assert the binary marshaling logic
			vb, err := v.AppendBinary(nil)
			require.NoError(t, err)
			v = getTestValue(t)
			require.NoError(t, v.Unmarshal(vb))

			// Compare the final metric with the expected metric
			expected, err := golden.ReadMetrics(filepath.Join(dir, "output.yaml"))
			require.NoError(t, err)
			actual, err := v.Finalize()
			require.NoError(t, err)
			assert.NoError(t, pmetrictest.CompareMetrics(expected, actual))
		})
	}
}

func getTestValue(t *testing.T) *Value {
	t.Helper()

	return NewValue(
		config.LimitConfig{
			MaxCardinality: 1,
			Overflow: config.OverflowConfig{
				Attributes: []config.Attribute{{Key: "resource_overflow", Value: true}},
			},
		},
		config.LimitConfig{
			MaxCardinality: 1,
			Overflow: config.OverflowConfig{
				Attributes: []config.Attribute{{Key: "scope_overflow", Value: true}},
			},
		},
		config.LimitConfig{
			MaxCardinality: 1,
			Overflow: config.OverflowConfig{
				Attributes: []config.Attribute{{Key: "metric_overflow", Value: true}},
			},
		},
		config.LimitConfig{
			MaxCardinality: 1,
			Overflow: config.OverflowConfig{
				Attributes: []config.Attribute{{Key: "dp_overflow", Value: true}},
			},
		},
		defaultMaxBuckets,
	)
}

func mergeMetrics(t *testing.T, v *Value, md pmetric.Metrics) {
	t.Helper()

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				require.NoError(t, v.MergeMetric(rm, sm, ms.At(k)))
			}
		}
	}
}

// we use the overflow test files but overflow behaviour will be governed
// by the limit configurations used in the tests/benchmarks.
var benchTestCases = []string{
	"sum_cumulative_overflow",
	"sum_delta_overflow",
	"histogram_cumulative_overflow",
	"histogram_delta_overflow",
	"exphistogram_cumulative_overflow",
	"exphistogram_delta_overflow",
}

func BenchmarkMerge(b *testing.B) {
	for _, tc := range []struct {
		name        string
		resLimit    config.LimitConfig
		scopeLimit  config.LimitConfig
		metricLimit config.LimitConfig
		dpLimit     config.LimitConfig
	}{
		{
			name:        "without_overflow",
			resLimit:    config.LimitConfig{MaxCardinality: math.MaxInt64},
			scopeLimit:  config.LimitConfig{MaxCardinality: math.MaxInt64},
			metricLimit: config.LimitConfig{MaxCardinality: math.MaxInt64},
			dpLimit:     config.LimitConfig{MaxCardinality: math.MaxInt64},
		},
		{
			name:        "with_overflow",
			resLimit:    config.LimitConfig{MaxCardinality: 1},
			scopeLimit:  config.LimitConfig{MaxCardinality: 1},
			metricLimit: config.LimitConfig{MaxCardinality: 1},
			dpLimit:     config.LimitConfig{MaxCardinality: 1},
		},
	} {
		benchmarkMergeWithTestdata(b, tc.name, tc.resLimit, tc.scopeLimit, tc.metricLimit, tc.dpLimit)
	}
}

// TODO rename this to BenchmarkAppendBinary once we have compared
// the old and AppendBinary implementations.
// TODO use subtests for with/without buffer reuse
func BenchmarkMarshal(b *testing.B) {
	benchmarkAppendBinary(b, func(b *testing.B, v *Value) {
		for i := 0; i < b.N; i++ {
			if _, err := v.AppendBinary(nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkAppendBinaryReuseBuffer(b *testing.B) {
	benchmarkAppendBinary(b, func(b *testing.B, v *Value) {
		var buf []byte
		var err error
		for i := 0; i < b.N; i++ {
			buf, err = v.AppendBinary(buf[:0])
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkAppendBinary(b *testing.B, f func(*testing.B, *Value)) {
	b.Helper()
	for _, tc := range benchTestCases {
		dir := filepath.Join("../../testdata", tc)
		md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
		require.NoError(b, err)
		md.MarkReadOnly()
		b.Run(tc, func(b *testing.B) {
			maxLimit := config.LimitConfig{MaxCardinality: math.MaxInt64}
			v := NewValue(maxLimit, maxLimit, maxLimit, maxLimit, defaultMaxBuckets)
			require.NoError(b, updateValueWithPMetrics(md, v))
			f(b, v)
		})
	}
}

func benchmarkMergeWithTestdata(
	b *testing.B,
	name string,
	resLimit, scopeLimit, metricLimit, dpLimit config.LimitConfig,
) {
	b.Helper()

	for _, tc := range benchTestCases {
		dir := filepath.Join("../../testdata", tc)
		md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
		require.NoError(b, err)
		md.MarkReadOnly()
		b.Run(name+"/"+tc, func(b *testing.B) {
			to := NewValue(resLimit, scopeLimit, metricLimit, dpLimit, defaultMaxBuckets)
			from := NewValue(resLimit, scopeLimit, metricLimit, dpLimit, defaultMaxBuckets)
			require.NoError(b, updateValueWithPMetrics(md, from))

			for i := 0; i < b.N; i++ {
				if err := to.Merge(from); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func updateValueWithPMetrics(m pmetric.Metrics, v *Value) error {
	rms := m.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			ms := sm.Metrics()
			for k := 0; k < ms.Len(); k++ {
				if err := v.MergeMetric(rm, sm, ms.At(k)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
