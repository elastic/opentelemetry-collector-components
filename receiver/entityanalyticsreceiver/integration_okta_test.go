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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"

	"github.com/elastic/entcollect"
	ecokta "github.com/elastic/entcollect/provider/okta"
)

func TestOktaReceiverLifecycle(t *testing.T) {
	now := time.Now()
	fix := &oktaFixture{
		users: []ecokta.User{
			{ID: "u1", Status: "ACTIVE", LastUpdated: now, Profile: map[string]any{"login": "alice@example.com"}},
			{ID: "u2", Status: "ACTIVE", LastUpdated: now, Profile: map[string]any{"login": "bob@example.com"}},
			{ID: "u3", Status: "ACTIVE", LastUpdated: now, Profile: map[string]any{"login": "charlie@example.com"}},
		},
		devices:  []ecokta.Device{{ID: "d1", Status: "ACTIVE"}},
		devUsers: map[string][]ecokta.User{"d1": {{ID: "u1"}}},
	}

	srv := startOktaIntegServer(t, fix)
	host := hostFromServerURL(t, srv.URL)
	httpClient := srv.Client()

	const provName = "test_okta_lifecycle"
	registerTestOktaFactory(t, provName, httpClient)

	store := newMemoryStore()

	// Phase 1: discover all — 3 users + 1 device = 4 events.
	sink1 := &consumertest.LogsSink{}
	rcvr1 := newReceiver(nopSettings(), oktaIntegConfig(provName, host), sink1)
	err := rcvr1.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("receiver start failed: %v", err)
	}
	waitForLogs(t, sink1, 4, 10*time.Second)
	err = rcvr1.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink1.AllLogs(), map[string]int{"discovered": 4})

	// Phase 2: remove u3, restart with same store.
	fix.users = fix.users[:2]
	sink2 := &consumertest.LogsSink{}
	rcvr2 := newReceiver(nopSettings(), oktaIntegConfig(provName, host), sink2)
	err = rcvr2.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("second receiver start failed: %v", err)
	}
	waitForLogs(t, sink2, 4, 10*time.Second)
	err = rcvr2.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("second receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink2.AllLogs(), map[string]int{
		"modified": 3, // u1, u2, d1
		"deleted":  1, // u3
	})
}

func TestOktaReceiverStatePersistence(t *testing.T) {
	now := time.Now()
	fix := &oktaFixture{
		users: []ecokta.User{
			{ID: "u1", Status: "ACTIVE", LastUpdated: now, Profile: map[string]any{"login": "alice@example.com"}},
		},
	}

	srv := startOktaIntegServer(t, fix)
	host := hostFromServerURL(t, srv.URL)
	httpClient := srv.Client()

	const provName = "test_okta_persist"
	registerTestOktaFactory(t, provName, httpClient)

	store := newMemoryStore()

	// Phase 1: discover.
	sink1 := &consumertest.LogsSink{}
	rcvr1 := newReceiver(nopSettings(), oktaIntegConfig(provName, host), sink1)
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
	err = store.Get("okta.cursor.user.last_sync", &cursor)
	if err != nil {
		t.Fatalf("cursor should be persisted after sync: %v", err)
	}
	if cursor.IsZero() {
		t.Error("persisted cursor is zero")
	}

	// Phase 2: same fixture, same store -> modified.
	sink2 := &consumertest.LogsSink{}
	rcvr2 := newReceiver(nopSettings(), oktaIntegConfig(provName, host), sink2)
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

// registerTestOktaFactory registers a ProviderFactory that creates a
// real ecokta.Provider using the given TLS-capable HTTP client (from
// httptest.Server.Client). The factory is unregistered on test cleanup.
func registerTestOktaFactory(t *testing.T, name string, httpClient *http.Client) {
	t.Helper()
	Register(name, func(cfg *confmap.Conf) (entcollect.Provider, error) {
		ec := ecokta.DefaultConfig()
		if cfg != nil {
			type lc struct {
				Domain     string `mapstructure:"okta_domain"`
				Token      string `mapstructure:"okta_token"`
				LimitFixed *int   `mapstructure:"limit_fixed"`
			}
			var c lc
			err := cfg.Unmarshal(&c)
			if err != nil {
				return nil, fmt.Errorf("unmarshal config: %w", err)
			}
			ec.Domain = c.Domain
			ec.Token = c.Token
			if c.LimitFixed != nil {
				ec.LimitFixed = c.LimitFixed
			}
		}
		err := ec.Validate()
		if err != nil {
			return nil, err
		}
		return ecokta.NewWithClient(ec, httpClient), nil
	})
	t.Cleanup(func() { unregister(name) })
}

// oktaIntegConfig returns a receiver Config wired to the named Okta
// provider with long sync/update intervals so only the initial full
// sync fires during a test.
func oktaIntegConfig(provider, host string) *Config {
	limit := 1000
	return &Config{
		Provider:       provider,
		StorageID:      "elasticsearch_storage",
		SyncInterval:   24 * time.Hour,
		UpdateInterval: time.Hour,
		ProviderConfig: map[string]any{
			"okta_domain": host,
			"okta_token":  "test-token",
			"limit_fixed": limit,
		},
	}
}

// Okta mock server, ported from entcollect/provider/okta/provider_test.go.

// oktaFixture holds the entities returned by the mock Okta API server.
// Slices are mutable between test phases to simulate entity changes.
type oktaFixture struct {
	users    []ecokta.User
	groups   []ecokta.Group
	members  map[string][]string // group ID -> user IDs
	devices  []ecokta.Device
	devUsers map[string][]ecokta.User // device ID -> users
}

// startOktaIntegServer starts an httptest TLS server that serves the
// Okta REST API endpoints (users, groups, devices, factors, roles)
// using the data in fix.
func startOktaIntegServer(t *testing.T, fix *oktaFixture) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/users", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(fix.users)
		if err != nil {
			t.Errorf("encoding users response: %v", err)
		}
	})

	mux.HandleFunc("GET /api/v1/groups", func(w http.ResponseWriter, r *http.Request) {
		var groups []ecokta.Group
		if fix.groups != nil {
			groups = fix.groups
		}
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(groups)
		if err != nil {
			t.Errorf("encoding groups response: %v", err)
		}
	})

	mux.HandleFunc("GET /api/v1/groups/{groupId}/users", func(w http.ResponseWriter, r *http.Request) {
		gid := r.PathValue("groupId")
		memberIDs := fix.members[gid]
		var result []ecokta.User
		for _, uid := range memberIDs {
			for _, u := range fix.users {
				if u.ID == uid {
					result = append(result, u)
					break
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(result)
		if err != nil {
			t.Errorf("encoding group %s users response: %v", gid, err)
		}
	})

	mux.HandleFunc("GET /api/v1/users/{userId}/groups", func(w http.ResponseWriter, r *http.Request) {
		uid := r.PathValue("userId")
		var result []ecokta.Group
		for _, g := range fix.groups {
			for _, mid := range fix.members[g.ID] {
				if mid == uid {
					result = append(result, g)
					break
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(result)
		if err != nil {
			t.Errorf("encoding user %s groups response: %v", uid, err)
		}
	})

	mux.HandleFunc("GET /api/v1/devices", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(fix.devices)
		if err != nil {
			t.Errorf("encoding devices response: %v", err)
		}
	})

	mux.HandleFunc("GET /api/v1/devices/{deviceId}/users", func(w http.ResponseWriter, r *http.Request) {
		did := r.PathValue("deviceId")
		users := fix.devUsers[did]
		type wrapped struct {
			User ecokta.User `json:"user"`
		}
		var result []wrapped
		for _, u := range users {
			result = append(result, wrapped{User: u})
		}
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(result)
		if err != nil {
			t.Errorf("encoding device %s users response: %v", did, err)
		}
	})

	mux.HandleFunc("GET /api/v1/users/{userId}/factors", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode([]ecokta.Factor{})
		if err != nil {
			t.Errorf("encoding user %s factors response: %v", r.PathValue("userId"), err)
		}
	})

	mux.HandleFunc("GET /api/v1/users/{userId}/roles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode([]ecokta.Role{})
		if err != nil {
			t.Errorf("encoding user %s roles response: %v", r.PathValue("userId"), err)
		}
	})

	mux.HandleFunc("GET /api/v1/users/{userId}/devices", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode([]ecokta.Device{})
		if err != nil {
			t.Errorf("encoding user %s devices response: %v", r.PathValue("userId"), err)
		}
	})

	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return srv
}
