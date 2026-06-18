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

package entityanalyticsreceiver

import (
	"reflect"
	"testing"

	"go.opentelemetry.io/collector/confmap"

	ecokta "github.com/elastic/entcollect/provider/okta"
)

// TestOktaProviderConfigRoundTrip verifies that every provider-owned field of
// ecokta.Config is represented in the oktaLocalConf mirror inside
// oktaProvider.
//
//  1. Field-count assertion — ecokta.Config currently has wantTotal fields.
//     Two of them (SyncInterval, UpdateInterval) are owned by the receiver's
//     top-level Config and are intentionally absent from oktaLocalConf.
//     If the total changes, update oktaLocalConf and this test.
//  2. Functional round-trip — all oktaLocalConf fields are set to non-default
//     sentinel values via a confmap.Conf; oktaProvider must return no error,
//     confirming that credentials and optional fields are wired through.
func TestOktaProviderConfigRoundTrip(t *testing.T) {
	const wantTotal = 12
	if got := reflect.TypeOf(ecokta.Config{}).NumField(); got != wantTotal {
		t.Fatalf("ecokta.Config has %d exported fields, want %d; "+
			"update oktaLocalConf inside oktaProvider and this test", got, wantTotal)
	}

	cfg := confmap.NewFromStringMap(map[string]any{
		"okta_domain":  "test.okta.com",
		"okta_token":   "test-token",
		"dataset":      "users",
		"enrich_with":  []string{"groups", "factors"},
		"batch_size":   100,
		"idset_shards": 32,
		"limit_window": "2m",
		"limit_fixed":  50,
		"scratch_dir":  "/scratch",
	})

	_, err := oktaProvider(cfg)
	if err != nil {
		t.Fatalf("oktaProvider returned unexpected error: %v", err)
	}
}
