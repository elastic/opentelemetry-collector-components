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
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jimlambrt/gldap"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"

	"github.com/elastic/entcollect"
	ecad "github.com/elastic/entcollect/provider/ad"
)

func TestADReceiverLifecycle(t *testing.T) {
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
	const provName = "test_ad_lifecycle"
	registerTestADFactory(t, provName, ldapURL)

	store := newMemoryStore()

	// Phase 1: first sync discovers all entities.
	sink1 := &consumertest.LogsSink{}
	rcvr1 := newReceiver(nopSettings(), adIntegConfig(provName, ldapURL), sink1)
	err := rcvr1.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("receiver start failed: %v", err)
	}
	waitForLogs(t, sink1, 3, 10*time.Second)
	err = rcvr1.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink1.AllLogs(), map[string]int{"discovered": 3})

	// Phase 2: remove bob, restart with same store.
	fix.users = fix.users[:1] // keep only alice
	sink2 := &consumertest.LogsSink{}
	rcvr2 := newReceiver(nopSettings(), adIntegConfig(provName, ldapURL), sink2)
	err = rcvr2.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("second receiver start failed: %v", err)
	}
	waitForLogs(t, sink2, 3, 10*time.Second)
	err = rcvr2.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("second receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink2.AllLogs(), map[string]int{
		"modified": 2, // alice + workstation1
		"deleted":  1, // bob
	})
}

func TestADReceiverStatePersistence(t *testing.T) {
	fix := &adFixture{
		users: []adEntry{
			{dn: "cn=alice,dc=example,dc=com", attrs: map[string][]string{
				"cn": {"alice"}, "distinguishedName": {"cn=alice,dc=example,dc=com"}, "whenChanged": {"20260101120000.0Z"},
			}},
		},
	}

	ldapURL := startADIntegLDAPServer(t, fix)
	const provName = "test_ad_persist"
	registerTestADFactory(t, provName, ldapURL)

	store := newMemoryStore()

	// Phase 1: discover alice.
	sink1 := &consumertest.LogsSink{}
	rcvr1 := newReceiver(nopSettings(), adIntegConfig(provName, ldapURL), sink1)
	err := rcvr1.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("first receiver start failed: %v", err)
	}
	waitForLogs(t, sink1, 1, 10*time.Second)
	err = rcvr1.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("first receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink1.AllLogs(), map[string]int{"discovered": 1})

	var cursor time.Time
	err = store.Get("ad.cursor.when_changed", &cursor)
	if err != nil {
		t.Fatalf("cursor should be persisted after sync: %v", err)
	}
	if cursor.IsZero() {
		t.Error("persisted cursor is zero")
	}

	// Phase 2: same fixture, same store -> alice should be modified.
	sink2 := &consumertest.LogsSink{}
	rcvr2 := newReceiver(nopSettings(), adIntegConfig(provName, ldapURL), sink2)
	err = rcvr2.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("second receiver start failed: %v", err)
	}
	waitForLogs(t, sink2, 1, 10*time.Second)
	err = rcvr2.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("second receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink2.AllLogs(), map[string]int{"modified": 1})
}

// registerTestADFactory registers a ProviderFactory that creates a real
// ecad.Provider backed by the given gldap server URL.
func registerTestADFactory(t *testing.T, name, ldapURL string) {
	t.Helper()
	Register(name, func(cfg *confmap.Conf) (entcollect.Provider, error) {
		ec := ecad.DefaultConfig()
		if cfg != nil {
			type lc struct {
				URL      string `mapstructure:"ad_url"`
				BaseDN   string `mapstructure:"ad_base_dn"`
				User     string `mapstructure:"ad_user"`
				Password string `mapstructure:"ad_password"`
			}
			var c lc
			err := cfg.Unmarshal(&c)
			if err != nil {
				return nil, fmt.Errorf("unmarshal config: %w", err)
			}
			ec.URL = c.URL
			ec.BaseDN = c.BaseDN
			ec.User = c.User
			ec.Password = c.Password
		}
		ec.URL = ldapURL
		err := ec.Validate()
		if err != nil {
			return nil, err
		}
		return ecad.New(ec)
	})
	t.Cleanup(func() { unregister(name) })
}

// adIntegConfig returns a receiver Config wired to the named AD
// provider with long sync/update intervals so only the initial
// full sync fires during a test.
func adIntegConfig(provider, ldapURL string) *Config {
	return &Config{
		Provider:       provider,
		StorageID:      "elasticsearch_storage",
		SyncInterval:   24 * time.Hour,
		UpdateInterval: time.Hour,
		ProviderConfig: map[string]any{
			"ad_url":      ldapURL,
			"ad_base_dn":  "DC=example,DC=com",
			"ad_user":     "cn=admin,dc=example,dc=com",
			"ad_password": "pass",
		},
	}
}

// AD mock server, ported from entcollect/provider/ad/provider_test.go.

// adEntry represents a single LDAP directory entry with a DN and
// attribute map.
type adEntry struct {
	dn    string
	attrs map[string][]string
}

// adFixture holds the directory entries returned by the mock LDAP
// server. Slices are mutable between test phases to simulate
// entity additions and removals.
type adFixture struct {
	users   []adEntry
	devices []adEntry
	groups  []adEntry
}

// startADIntegLDAPServer starts a gldap server that responds to
// bind and search requests using the entries in fix. It returns the
// LDAP URL (ldap://host:port) to connect to.
func startADIntegLDAPServer(t *testing.T, fix *adFixture) string {
	t.Helper()

	s, err := gldap.NewServer()
	if err != nil {
		t.Fatalf("gldap new server: %v", err)
	}
	t.Cleanup(func() {
		err := s.Stop()
		if err != nil {
			t.Errorf("stopping gldap server: %v", err)
		}
	})

	mux, err := gldap.NewMux()
	if err != nil {
		t.Fatalf("gldap new mux: %v", err)
	}

	err = mux.Bind(func(w *gldap.ResponseWriter, r *gldap.Request) {
		resp := r.NewBindResponse()
		resp.SetResultCode(gldap.ResultSuccess)
		_ = w.Write(resp)
	})
	if err != nil {
		t.Fatalf("mux bind: %v", err)
	}

	err = mux.Search(adSearchHandler(t, fix))
	if err != nil {
		t.Fatalf("mux search: %v", err)
	}

	err = mux.Unbind(func(w *gldap.ResponseWriter, r *gldap.Request) {})
	if err != nil {
		t.Fatalf("mux unbind: %v", err)
	}

	err = s.Router(mux)
	if err != nil {
		t.Fatalf("gldap router: %v", err)
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	go func() { _ = s.Run(addr) }()

	for i := 0; i < 100; i++ {
		if s.Ready() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !s.Ready() {
		t.Fatal("gldap server not ready")
	}

	return "ldap://" + addr
}

// adSearchHandler returns a gldap handler that dispatches search
// results from fix based on the LDAP filter: objectCategory=person
// returns users, objectClass=computer returns devices, and
// objectClass=group returns groups.
func adSearchHandler(t *testing.T, fix *adFixture) gldap.HandlerFunc {
	t.Helper()
	return func(w *gldap.ResponseWriter, r *gldap.Request) {
		msg, err := r.GetSearchMessage()
		if err != nil {
			t.Errorf("get search message: %v", err)
			return
		}

		filter := msg.Filter
		var results []adEntry
		switch {
		case strings.Contains(filter, "objectClass=group") && strings.Contains(filter, "!(member="):
			results = fix.groups
		case strings.Contains(filter, "objectClass=group"):
			results = fix.groups
		case strings.Contains(filter, "objectClass=computer"):
			results = fix.devices
		case strings.Contains(filter, "objectCategory=person"):
			results = fix.users
		default:
			t.Logf("unhandled filter: %s", filter)
		}

		for _, entry := range results {
			e := r.NewSearchResponseEntry(entry.dn)
			for name, vals := range entry.attrs {
				e.AddAttribute(name, vals)
			}
			_ = w.Write(e)
		}

		done := r.NewSearchDoneResponse()
		done.SetResultCode(gldap.ResultSuccess)
		_ = w.Write(done)
	}
}
