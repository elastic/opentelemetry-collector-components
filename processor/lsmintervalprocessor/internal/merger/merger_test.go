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
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Finish must not Unmarshal and re-encode a single MERGE operand.
func TestLoneMergeFinishRoundTrip(t *testing.T) {
	operand := mustFatLSMOperand(t, fatLSMShape{
		resources: 4,
		dps:       2,
	})

	out, closer := compactFinish(t, operand)
	t.Cleanup(func() {
		if closer != nil {
			require.NoError(t, closer.Close())
		}
	})

	require.Equal(t, operand, out)
}

// TestLoneMergeFinishCopiesOperand checks that Finish does not alias
// Pebble's input buffer. Pebble may reuse that buffer after Merge.
func TestLoneMergeFinishCopiesOperand(t *testing.T) {
	operand := mustFatLSMOperand(t, fatLSMShape{resources: 2, dps: 1})
	orig := append([]byte(nil), operand...)

	vm, err := peakPebbleMerger().Merge(nil, operand)
	require.NoError(t, err)
	operand[0] ^= 0xff

	out, closer, err := vm.Finish(true)
	require.NoError(t, err)
	t.Cleanup(func() {
		if closer != nil {
			require.NoError(t, closer.Close())
		}
	})
	require.Equal(t, orig, out)
}

// A sibling operand forces Unmarshal and a real merge.
func TestCombineMergeFinishMergesSibling(t *testing.T) {
	base := mustFatLSMOperand(t, fatLSMShape{resources: 4, dps: 2})
	sibling := mustFatLSMOperand(t, fatLSMShape{resources: 2, dps: 2})

	out, closer := compactFinish(t, base, sibling)
	t.Cleanup(func() {
		if closer != nil {
			require.NoError(t, closer.Close())
		}
	})

	require.NotEqual(t, base, out)

	merged := peakValue()
	require.NoError(t, merged.Unmarshal(out))
	md, _, err := merged.Finalize()
	require.NoError(t, err)

	// Base is 4 resources × 2 expo datapoints × count 1000 = 8000.
	// Sibling overlaps the first 2 resources (4 expo datapoints × 1000).
	// Merge must add those counts, not append rows.
	var expoCount uint64
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				metric := ms.At(k)
				if metric.Type() != pmetric.MetricTypeExponentialHistogram {
					continue
				}
				dps := metric.ExponentialHistogram().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					expoCount += dps.At(l).Count()
				}
			}
		}
	}
	require.Equal(t, uint64(12000), expoCount)
}
