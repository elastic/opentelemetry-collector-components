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

package identity

import (
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"testing"
)

// Persisted v2 hashes cannot silently change with a pdata dependency bump.
// Updating these fixtures requires a new format version and migration of
// old identities, not simply accepting the new hash outputs.
func TestPersistedHashContract(t *testing.T) {
	cases := []struct {
		attrs map[string]any
		hash  string
	}{
		{map[string]any{}, "00000000000000000000000000000000"},
		{map[string]any{"service.name": "checkout", "shard": int64(7), "enabled": true}, "1cfda8d83746d0e74b0efe7298c5b038"},
		{map[string]any{"nested": map[string]any{"n": 1.5, "bytes": []byte{0, 1, 255}}, "list": []any{"x", int64(-3)}}, "a3795c18f151e50f432b2c7489a3f5e5"},
	}
	for _, tc := range cases {
		r := pcommon.NewResource()
		if err := r.Attributes().FromRaw(tc.attrs); err != nil {
			t.Fatal(err)
		}
		got := fmt.Sprintf("%x", OfResource(r).attrs)
		if got != tc.hash {
			t.Errorf("hash contract: got %s, want %s", got, tc.hash)
		}
	}
}
