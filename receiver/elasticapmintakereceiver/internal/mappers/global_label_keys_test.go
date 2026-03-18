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

package mappers

import (
	"slices"
	"testing"

	"github.com/elastic/apm-data/model/modelpb"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestSetLabelsCollectsGlobalKeys(t *testing.T) {
	cases := []struct {
		name               string
		event              *modelpb.APMEvent
		expectedGlobalKeys []string
		expectedAttrs      map[string]any
	}{
		{
			name:               "nil labels",
			event:              &modelpb.APMEvent{},
			expectedGlobalKeys: nil,
			expectedAttrs:      map[string]any{},
		},
		{
			name: "no global labels",
			event: &modelpb.APMEvent{
				Labels: map[string]*modelpb.LabelValue{
					"local_tag": {Value: "v1", Global: false},
				},
				NumericLabels: map[string]*modelpb.NumericLabelValue{
					"local_num": {Value: 42, Global: false},
				},
			},
			expectedGlobalKeys: nil,
			expectedAttrs: map[string]any{
				"labels.local_tag":        "v1",
				"numeric_labels.local_num": float64(42),
			},
		},
		{
			name: "only global string labels",
			event: &modelpb.APMEvent{
				Labels: map[string]*modelpb.LabelValue{
					"team.name": {Value: "platform", Global: true},
					"env":       {Value: "prod", Global: true},
					"local":     {Value: "skip", Global: false},
				},
			},
			expectedGlobalKeys: []string{"env", "team.name"},
			expectedAttrs: map[string]any{
				"labels.team.name": "platform",
				"labels.env":      "prod",
				"labels.local":    "skip",
			},
		},
		{
			name: "only global numeric labels",
			event: &modelpb.APMEvent{
				NumericLabels: map[string]*modelpb.NumericLabelValue{
					"cost_center": {Value: 100, Global: true},
					"local_num":   {Value: 1, Global: false},
				},
			},
			expectedGlobalKeys: []string{"cost_center"},
			expectedAttrs: map[string]any{
				"numeric_labels.cost_center": float64(100),
				"numeric_labels.local_num":   float64(1),
			},
		},
		{
			name: "mixed global labels and numeric labels",
			event: &modelpb.APMEvent{
				Labels: map[string]*modelpb.LabelValue{
					"team.name": {Value: "platform", Global: true},
					"local":     {Value: "skip", Global: false},
				},
				NumericLabels: map[string]*modelpb.NumericLabelValue{
					"cost_center": {Value: 100, Global: true},
				},
			},
			expectedGlobalKeys: []string{"cost_center", "team.name"},
			expectedAttrs: map[string]any{
				"labels.team.name":           "platform",
				"labels.local":              "skip",
				"numeric_labels.cost_center": float64(100),
			},
		},
		{
			name: "duplicate key across labels and numeric labels",
			event: &modelpb.APMEvent{
				Labels: map[string]*modelpb.LabelValue{
					"shared_key": {Value: "str", Global: true},
				},
				NumericLabels: map[string]*modelpb.NumericLabelValue{
					"shared_key": {Value: 1, Global: true},
				},
			},
			expectedGlobalKeys: []string{"shared_key"},
			expectedAttrs: map[string]any{
				"labels.shared_key":         "str",
				"numeric_labels.shared_key": float64(1),
			},
		},
		{
			name: "nil label value skipped",
			event: &modelpb.APMEvent{
				Labels: map[string]*modelpb.LabelValue{
					"good": {Value: "v", Global: true},
					"nil":  nil,
				},
				NumericLabels: map[string]*modelpb.NumericLabelValue{
					"nil_num": nil,
				},
			},
			expectedGlobalKeys: []string{"good"},
			expectedAttrs: map[string]any{
				"labels.good": "v",
			},
		},
		{
			name: "empty key skipped",
			event: &modelpb.APMEvent{
				Labels: map[string]*modelpb.LabelValue{
					"":     {Value: "v", Global: true},
					"real": {Value: "v", Global: true},
				},
			},
			expectedGlobalKeys: []string{"real"},
			expectedAttrs: map[string]any{
				"labels.real": "v",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			attrs := pcommon.NewMap()
			globalKeys := make([]string, 0)

			setLabels(tc.event, attrs, &globalKeys)

			slices.Sort(globalKeys)
			globalKeys = slices.Compact(globalKeys)
			if len(globalKeys) == 0 {
				globalKeys = nil
			}
			require.Equal(t, tc.expectedGlobalKeys, globalKeys)

			got := attrs.AsRaw()
			require.Equal(t, tc.expectedAttrs, got)
		})
	}
}
