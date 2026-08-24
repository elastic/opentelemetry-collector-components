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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestMergeFinishLone(t *testing.T) {
	op := mustFatLSMOperand(t, fatLSMShape{resources: 4, dps: 2})
	out, _ := compactFinish(t, op)
	require.Equal(t, op, out)
}

func TestMergeFinishCombine(t *testing.T) {
	baseShape := fatLSMShape{resources: 4, dps: 2}
	sibShape := fatLSMShape{resources: 2, dps: 2}
	base := mustFatLSMOperand(t, baseShape)
	sib := mustFatLSMOperand(t, sibShape)
	out, _ := compactFinish(t, base, sib)
	require.NotEqual(t, base, out)

	v := peakValue()
	require.NoError(t, v.Unmarshal(out))
	md, _, err := v.Finalize()
	require.NoError(t, err)

	// peakCompactMetrics sets each expo datapoint count to 1000.
	// Overlapping identities add those counts.
	want := uint64((baseShape.resources*baseShape.dps + sibShape.resources*sibShape.dps) * 1000)
	require.Equal(t, want, expoHistogramCount(md))
}

func TestMergeFinishCopiesOperand(t *testing.T) {
	op := mustFatLSMOperand(t, fatLSMShape{resources: 2, dps: 1})
	orig := slices.Clone(op)

	vm, err := peakPebbleMerger().Merge(nil, op)
	require.NoError(t, err)
	op[0] ^= 0xff

	out, _, err := vm.Finish(true)
	require.NoError(t, err)
	require.Equal(t, orig, out)
}

func expoHistogramCount(md pmetric.Metrics) uint64 {
	var n uint64
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			for _, m := range sms.At(j).Metrics().All() {
				if m.Type() != pmetric.MetricTypeExponentialHistogram {
					continue
				}
				for _, dp := range m.ExponentialHistogram().DataPoints().All() {
					n += dp.Count()
				}
			}
		}
	}
	return n
}
