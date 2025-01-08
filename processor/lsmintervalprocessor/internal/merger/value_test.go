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
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// we use the overflow test files but overflow behaviour will be governed
// by the limit configurations used in the tests/benchmarks.
var testCases = []string{
	"sum_cumulative_overflow",
	"sum_delta_overflow",
	"histogram_cumulative_overflow",
	"histogram_delta_overflow",
	"exphistogram_cumulative_overflow",
	"exphistogram_delta_overflow",
}

func BenchmarkMerge(b *testing.B) {
	for _, tc := range []struct {
		name          string
		resLimit      config.LimitConfig
		scopeLimit    config.LimitConfig
		scopeDPsLimit config.LimitConfig
	}{
		{
			name:          "without_overflow",
			resLimit:      config.LimitConfig{MaxCardinality: math.MaxInt64},
			scopeLimit:    config.LimitConfig{MaxCardinality: math.MaxInt64},
			scopeDPsLimit: config.LimitConfig{MaxCardinality: math.MaxInt64},
		},
		{
			name:          "with_overflow",
			resLimit:      config.LimitConfig{MaxCardinality: 1},
			scopeLimit:    config.LimitConfig{MaxCardinality: 1},
			scopeDPsLimit: config.LimitConfig{MaxCardinality: 1},
		},
	} {
		benchmarkWithTestdata(b, tc.name, tc.resLimit, tc.scopeLimit, tc.scopeDPsLimit)
	}
}

func benchmarkWithTestdata(
	b *testing.B,
	name string,
	resLimit, scopeLimit, scopeDPsLimit config.LimitConfig,
) {
	b.Helper()

	for _, tc := range testCases {
		dir := filepath.Join("../../testdata", tc)
		md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
		require.NoError(b, err)
		md.MarkReadOnly()
		b.ResetTimer()
		b.Run(name+"/"+tc, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				to := NewValue(resLimit, scopeLimit, scopeDPsLimit)
				from := NewValue(resLimit, scopeLimit, scopeDPsLimit)
				// Update from to have the pmetric structure
				mdCopy := pmetric.NewMetrics()
				md.CopyTo(mdCopy)
				require.NoError(b, updateValueWithPMetrics(mdCopy, from))
				b.StartTimer()
				err = to.Merge(from)
				require.NoError(b, err)
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
