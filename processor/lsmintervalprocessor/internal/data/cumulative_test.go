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

package data

import (
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"testing"
)

func TestCumulativeTieIsOrderIndependent(t *testing.T) {
	number := func(n int64) any { dp := pmetric.NewNumberDataPoint(); dp.SetIntValue(n); return dp }
	histogram := func(n int64) any { dp := pmetric.NewHistogramDataPoint(); dp.SetCount(uint64(n)); return dp }
	exponential := func(n int64) any { dp := pmetric.NewExponentialHistogramDataPoint(); dp.SetCount(uint64(n)); return dp }
	summary := func(n int64) any { dp := pmetric.NewSummaryDataPoint(); dp.SetCount(uint64(n)); return dp }
	for _, makeDP := range []func(int64) any{number, histogram, exponential, summary} {
		a, b := makeDP(1), makeDP(2)
		require.NotEqual(t, PreferCumulative(a, b), PreferCumulative(b, a))
		require.False(t, PreferCumulative(a, a))
	}
}

func TestCumulativeTieCanonicalizesExemplarMapsWithoutMutation(t *testing.T) {
	a, b := pmetric.NewNumberDataPoint(), pmetric.NewNumberDataPoint()
	a.SetIntValue(1)
	b.SetIntValue(1)
	a.Attributes().PutStr("stream", "a")
	b.Attributes().PutStr("stream", "b")
	aa := a.Exemplars().AppendEmpty().FilteredAttributes()
	ba := b.Exemplars().AppendEmpty().FilteredAttributes()
	aa.PutStr("first", "a")
	aa.PutEmptyMap("nested").PutStr("key", "v")
	ba.PutEmptyMap("nested").PutStr("key", "v")
	ba.PutStr("first", "a")
	require.False(t, PreferCumulative(a, b))
	require.False(t, PreferCumulative(b, a))
	require.Equal(t, 1, a.Attributes().Len())
	require.Equal(t, 1, b.Attributes().Len())
}
