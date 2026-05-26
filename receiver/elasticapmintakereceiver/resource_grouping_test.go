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

package elasticapmintakereceiver

import (
	"testing"

	xxhashv2 "github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/elastic/apm-data/model/modelpb"
)

func TestGetOrCreateTraceScope_NumericLabelGrouping(t *testing.T) {
	tests := []struct {
		name              string
		values            []float64
		expectedResources int
	}{
		{
			name:              "distinct close numeric labels do not merge",
			values:            []float64{1.0000000001, 1.0000000002},
			expectedResources: 2,
		},
		{
			name:              "identical numeric labels merge",
			values:            []float64{1.0000000001, 1.0000000001},
			expectedResources: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &elasticAPMIntakeReceiver{}
			var td *ptrace.Traces
			var groups signalGroups
			h := xxhashv2.New()

			for _, value := range tt.values {
				event := &modelpb.APMEvent{
					NumericLabels: map[string]*modelpb.NumericLabelValue{
						"close": {Value: value},
					},
				}
				_ = r.getOrCreateTraceScope(event, &td, &groups, h)
			}

			require.NotNil(t, td)
			require.Equal(t, tt.expectedResources, td.ResourceSpans().Len())
		})
	}
}
