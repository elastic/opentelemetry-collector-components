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

	ecjamf "github.com/elastic/entcollect/provider/jamf"
)

// TestJamfProviderConfigRoundTrip verifies that every provider-owned field of
// ecjamf.Config is represented in the jamfLocalConf mirror inside
// jamfProvider. It does this in two ways:
//
//  1. Field-count assertion — ecjamf.Config currently has wantTotal fields.
//     Two of them (SyncInterval, UpdateInterval) are owned by the receiver's
//     top-level Config and are intentionally absent from jamfLocalConf.
//     If the total changes, update jamfLocalConf and this test.
//  2. Functional round-trip — all jamfLocalConf fields are set to non-default
//     sentinel values via a confmap.Conf; jamfProvider must return no error,
//     confirming that credentials and optional fields are wired through.
func TestJamfProviderConfigRoundTrip(t *testing.T) {
	const wantTotal = 8
	if got := reflect.TypeOf(ecjamf.Config{}).NumField(); got != wantTotal {
		t.Fatalf("ecjamf.Config has %d exported fields, want %d; "+
			"update jamfLocalConf inside jamfProvider and this test", got, wantTotal)
	}

	cfg := confmap.NewFromStringMap(map[string]any{
		"jamf_tenant":        "tenant.jamfcloud.com",
		"jamf_username":      "user",
		"jamf_password":      "pass",
		"page_size":          50,
		"idset_shards":       32,
		"token_grace_period": "2m",
	})

	_, err := jamfProvider(cfg)
	if err != nil {
		t.Fatalf("jamfProvider returned unexpected error: %v", err)
	}
}
