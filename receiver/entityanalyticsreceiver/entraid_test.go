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

	ecentraid "github.com/elastic/entcollect/provider/entraid"
)

// TestEntraidProviderConfigRoundTrip verifies that every provider-owned field
// of ecentraid.Config is represented in the entraidLocalConf mirror inside
// entraidProvider.
//
//  1. Field-count assertion — ecentraid.Config currently has wantTotal
//     fields. Two of them (SyncInterval, UpdateInterval) are owned by the
//     receiver's top-level Config and are intentionally absent from
//     entraidLocalConf. If the total changes, update entraidLocalConf
//     and this test.
//  2. Functional round-trip — all entraidLocalConf fields are set to
//     non-default sentinel values via a confmap.Conf; entraidProvider
//     must return no error, confirming that credentials and optional
//     fields are wired through.
func TestEntraidProviderConfigRoundTrip(t *testing.T) {
	const wantTotal = 13
	if got := reflect.TypeOf(ecentraid.Config{}).NumField(); got != wantTotal {
		t.Fatalf("ecentraid.Config has %d exported fields, want %d; "+
			"update entraidLocalConf inside entraidProvider and this test", got, wantTotal)
	}

	cfg := confmap.NewFromStringMap(map[string]any{
		"tenant_id":      "test-tenant",
		"client_id":      "test-client",
		"client_secret":  "test-secret",
		"login_endpoint": "https://login.example.com",
		"login_scopes":   []string{"https://graph.example.com/.default"},
		"api_endpoint":   "https://graph.example.com/v1.0",
		"dataset":        "users",
		"enrich_with":    []string{"mfa"},
		"select_users":   []string{"displayName", "mail"},
		"select_groups":  []string{"displayName"},
		"select_devices": []string{"displayName", "operatingSystem"},
	})

	_, err := entraidProvider(cfg)
	if err != nil {
		t.Fatalf("entraidProvider returned unexpected error: %v", err)
	}
}
