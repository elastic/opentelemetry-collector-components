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

	ecad "github.com/elastic/entcollect/provider/ad"
)

// TestADProviderConfigRoundTrip verifies that every provider-owned field of
// ecad.Config is represented in the adLocalConf mirror inside adProvider.
//
//  1. Field-count assertion — ecad.Config currently has wantTotal fields.
//     Three of them (SyncInterval, UpdateInterval, TLS) are owned by the
//     receiver's top-level Config or built separately, and are intentionally
//     absent from adLocalConf. If the total changes, update adLocalConf and
//     this test.
//  2. Functional round-trip — all adLocalConf fields are set to non-default
//     sentinel values via a confmap.Conf; adProvider must return no error,
//     confirming that credentials and optional fields are wired through.
func TestADProviderConfigRoundTrip(t *testing.T) {
	const wantTotal = 15
	if got := reflect.TypeOf(ecad.Config{}).NumField(); got != wantTotal {
		t.Fatalf("ecad.Config has %d exported fields, want %d; "+
			"update adLocalConf inside adProvider and this test", got, wantTotal)
	}

	cfg := confmap.NewFromStringMap(map[string]any{
		"ad_url":               "ldap://dc.example.com",
		"ad_base_dn":           "DC=example,DC=com",
		"ad_user":              "cn=admin,dc=example,dc=com",
		"ad_password":          "secret",
		"dataset":              "users",
		"user_query":           "(&(objectCategory=person))",
		"device_query":         "(&(objectClass=computer))",
		"include_empty_groups": true,
		"user_attributes":      []string{"cn", "mail"},
		"group_attributes":     []string{"cn"},
		"ad_paging_size":       500,
		"idset_shards":         32,
	})

	_, err := adProvider(cfg)
	if err != nil {
		t.Fatalf("adProvider returned unexpected error: %v", err)
	}
}
