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

package elasticapmconnector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestParseDynamicAttributeKeys(t *testing.T) {
	cases := []struct {
		name        string
		metadataKey string
		metadata    map[string][]string
		expected    map[string]struct{}
	}{
		{
			name:     "empty_key",
			expected: nil,
		},
		{
			name:        "key_not_in_metadata",
			metadataKey: "x-dynamic-resource-attributes",
			metadata:    map[string][]string{"other": {"val"}},
			expected:    nil,
		},
		{
			name:        "empty_value",
			metadataKey: "x-dynamic-resource-attributes",
			metadata:    map[string][]string{"x-dynamic-resource-attributes": {""}},
			expected:    nil,
		},
		{
			name:        "single_key",
			metadataKey: "x-dynamic-resource-attributes",
			metadata:    map[string][]string{"x-dynamic-resource-attributes": {"team.name"}},
			expected:    map[string]struct{}{"team.name": {}},
		},
		{
			name:        "comma_separated",
			metadataKey: "x-dynamic-resource-attributes",
			metadata:    map[string][]string{"x-dynamic-resource-attributes": {"team.name,cost_center"}},
			expected:    map[string]struct{}{"team.name": {}, "cost_center": {}},
		},
		{
			name:        "whitespace_trimmed",
			metadataKey: "x-dynamic-resource-attributes",
			metadata:    map[string][]string{"x-dynamic-resource-attributes": {" team.name , cost_center "}},
			expected:    map[string]struct{}{"team.name": {}, "cost_center": {}},
		},
		{
			name:        "trailing_comma_ignored",
			metadataKey: "x-dynamic-resource-attributes",
			metadata:    map[string][]string{"x-dynamic-resource-attributes": {"team.name,"}},
			expected:    map[string]struct{}{"team.name": {}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.metadata != nil {
				info := client.Info{Metadata: client.NewMetadata(tc.metadata)}
				ctx = client.NewContext(ctx, info)
			}
			got := parseDynamicAttributeKeys(ctx, tc.metadataKey)
			if tc.expected == nil {
				assert.Nil(t, got)
			} else {
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}

var defaultPrefixes = []string{"labels.", "numeric_labels."}

func TestFilterDynamicAttributes(t *testing.T) {
	cases := []struct {
		name      string
		initial   map[string]any
		allowKeys map[string]struct{}
		expected  map[string]any
	}{
		{
			name:     "nil_allow_keys_strips_all_labels",
			initial:  map[string]any{"service.name": "foo", "labels.team": "platform", "numeric_labels.cost": float64(42)},
			expected: map[string]any{"service.name": "foo"},
		},
		{
			name:      "preserves_allowed_strips_others",
			initial:   map[string]any{"service.name": "foo", "labels.team": "platform", "labels.non_global": "stale", "numeric_labels.cost": float64(42), "numeric_labels.non_global": float64(1)},
			allowKeys: map[string]struct{}{"team": {}, "cost": {}},
			expected:  map[string]any{"service.name": "foo", "labels.team": "platform", "numeric_labels.cost": float64(42)},
		},
		{
			name:      "empty_allow_keys_strips_all_labels",
			initial:   map[string]any{"labels.old": "stale", "service.name": "foo"},
			allowKeys: map[string]struct{}{},
			expected:  map[string]any{"service.name": "foo"},
		},
		{
			name:      "non_label_attrs_untouched",
			initial:   map[string]any{"service.name": "foo", "host.name": "bar"},
			allowKeys: map[string]struct{}{"team": {}},
			expected:  map[string]any{"service.name": "foo", "host.name": "bar"},
		},
		{
			name:      "no_labels_present",
			initial:   map[string]any{"service.name": "foo"},
			allowKeys: map[string]struct{}{"team": {}},
			expected:  map[string]any{"service.name": "foo"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			attrs := pcommon.NewMap()
			require.NoError(t, attrs.FromRaw(tc.initial))
			filterDynamicAttributes(attrs, defaultPrefixes, tc.allowKeys)
			assert.Equal(t, tc.expected, attrs.AsRaw())
		})
	}
}
