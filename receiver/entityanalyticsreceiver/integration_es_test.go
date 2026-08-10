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

//go:build integration

package entityanalyticsreceiver

import (
	"encoding/json"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"

	ecokta "github.com/elastic/entcollect/provider/okta"
)

// TestADReceiverLifecycle_ES exercises the full AD receiver lifecycle
// with a real Elasticsearch store instead of newMemoryStore. This
// validates that the esStore implementation satisfies the
// entcollect.Store interface under production-like conditions.
func TestADReceiverLifecycle_ES(t *testing.T) {
	client := esClient(t)
	store := newESStore(t, client, "test-ad-lifecycle-es")

	fix := &adFixture{
		users: []adEntry{
			{dn: "cn=alice,dc=example,dc=com", attrs: map[string][]string{
				"cn": {"alice"}, "distinguishedName": {"cn=alice,dc=example,dc=com"}, "whenChanged": {"20260101120000.0Z"},
			}},
			{dn: "cn=bob,dc=example,dc=com", attrs: map[string][]string{
				"cn": {"bob"}, "distinguishedName": {"cn=bob,dc=example,dc=com"}, "whenChanged": {"20260101130000.0Z"},
			}},
		},
		devices: []adEntry{
			{dn: "cn=workstation1,dc=example,dc=com", attrs: map[string][]string{
				"cn": {"workstation1"}, "distinguishedName": {"cn=workstation1,dc=example,dc=com"}, "whenChanged": {"20260101140000.0Z"},
			}},
		},
	}

	ldapURL := startADIntegLDAPServer(t, fix)
	const provName = "test_ad_lifecycle_es"
	registerTestADFactory(t, provName, ldapURL)

	// Phase 1: first sync discovers all.
	sink1 := &consumertest.LogsSink{}
	rcvr1 := newReceiver(nopSettings(), adIntegConfig(provName, ldapURL), sink1)
	err := rcvr1.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("receiver start failed: %v", err)
	}
	waitForLogs(t, sink1, 3, 30*time.Second)
	waitForStoreKey(t, store, "ad.users.idset.meta", 10*time.Second)
	err = rcvr1.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink1.AllLogs(), map[string]int{"discovered": 3})

	// Phase 2: remove bob, restart with same store.
	fix.users = fix.users[:1]
	sink2 := &consumertest.LogsSink{}
	rcvr2 := newReceiver(nopSettings(), adIntegConfig(provName, ldapURL), sink2)
	err = rcvr2.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("second receiver start failed: %v", err)
	}
	waitForLogs(t, sink2, 3, 30*time.Second)
	err = rcvr2.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("second receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink2.AllLogs(), map[string]int{
		"modified": 2,
		"deleted":  1,
	})
}

// TestOktaReceiverLifecycle_ES exercises the Okta receiver with a real ES store.
func TestOktaReceiverLifecycle_ES(t *testing.T) {
	client := esClient(t)
	store := newESStore(t, client, "test-okta-lifecycle-es")

	now := time.Now()
	fix := &oktaFixture{
		users: []ecokta.User{
			{ID: "u1", Status: "ACTIVE", LastUpdated: now, Profile: map[string]any{"login": "alice@example.com"}},
			{ID: "u2", Status: "ACTIVE", LastUpdated: now, Profile: map[string]any{"login": "bob@example.com"}},
		},
	}

	srv := startOktaIntegServer(t, fix)
	host := hostFromServerURL(t, srv.URL)
	httpClient := srv.Client()

	const provName = "test_okta_lifecycle_es"
	registerTestOktaFactory(t, provName, httpClient)

	// Phase 1: discover all.
	sink1 := &consumertest.LogsSink{}
	rcvr1 := newReceiver(nopSettings(), oktaIntegConfig(provName, host), sink1)
	err := rcvr1.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("receiver start failed: %v", err)
	}
	waitForLogs(t, sink1, 2, 30*time.Second)
	waitForStoreKey(t, store, "okta.users.idset.meta", 10*time.Second)
	err = rcvr1.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink1.AllLogs(), map[string]int{"discovered": 2})

	// Phase 2: remove u2, restart with same store.
	fix.users = fix.users[:1]
	sink2 := &consumertest.LogsSink{}
	rcvr2 := newReceiver(nopSettings(), oktaIntegConfig(provName, host), sink2)
	err = rcvr2.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("second receiver start failed: %v", err)
	}
	waitForLogs(t, sink2, 2, 30*time.Second)
	err = rcvr2.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("second receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink2.AllLogs(), map[string]int{
		"modified": 1,
		"deleted":  1,
	})
}

// TestEntraidReceiverLifecycle_ES exercises the EntraID receiver with a real ES store.
func TestEntraidReceiverLifecycle_ES(t *testing.T) {
	client := esClient(t)
	store := newESStore(t, client, "test-entraid-lifecycle-es")

	fix := &entraidFixture{
		fullUsers: []json.RawMessage{
			entraidRawUser(t, "u1", map[string]any{"displayName": "Alice"}),
			entraidRawUser(t, "u2", map[string]any{"displayName": "Bob"}),
		},
		fullDevices: []json.RawMessage{
			entraidRawDevice(t, "d1", map[string]any{"displayName": "Laptop"}),
		},
	}

	srv := startEntraidIntegServer(t, fix)
	httpClient := srv.Client()

	const provName = "test_entraid_lifecycle_es"
	registerTestEntraidFactory(t, provName, httpClient)

	// Phase 1: discover all — 2 users + 1 device = 3 events.
	sink1 := &consumertest.LogsSink{}
	rcvr1 := newReceiver(nopSettings(), entraidIntegConfig(provName, srv.URL), sink1)
	err := rcvr1.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("receiver start failed: %v", err)
	}
	waitForLogs(t, sink1, 3, 30*time.Second)
	err = rcvr1.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink1.AllLogs(), map[string]int{"discovered": 3})

	// Phase 2: @removed u2, restart with same store.
	fix.fullUsers = []json.RawMessage{
		entraidRawUser(t, "u1", map[string]any{"displayName": "Alice"}),
		entraidRawRemovedUser(t, "u2"),
	}

	sink2 := &consumertest.LogsSink{}
	rcvr2 := newReceiver(nopSettings(), entraidIntegConfig(provName, srv.URL), sink2)
	err = rcvr2.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("second receiver start failed: %v", err)
	}
	waitForLogs(t, sink2, 3, 30*time.Second)
	err = rcvr2.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("second receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink2.AllLogs(), map[string]int{
		"discovered": 2,
		"deleted":    1,
	})
}

// waitForStoreKey polls the store until the given key exists or the
// timeout expires. This is needed because the receiver publishes
// events during the sync but commits state (including idset shards)
// only after all events have been emitted. Without this wait, a
// Shutdown issued right after waitForLogs may cancel the context
// before the commit completes.
func waitForStoreKey(t *testing.T, store *esStore, key string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		var discard json.RawMessage
		err := store.Get(key, &discard)
		if err == nil {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for store key %q", key)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
