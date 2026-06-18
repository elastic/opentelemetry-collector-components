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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"

	"github.com/elastic/entcollect"
	ecentraid "github.com/elastic/entcollect/provider/entraid"
)

func TestEntraidReceiverLifecycle(t *testing.T) {
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

	const provName = "test_entraid_lifecycle"
	registerTestEntraidFactory(t, provName, httpClient)

	store := newMemoryStore()

	// Phase 1: discover all — 2 users + 1 device = 3 events.
	sink1 := &consumertest.LogsSink{}
	rcvr1 := newReceiver(nopSettings(), entraidIntegConfig(provName, srv.URL), sink1)
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
	waitForLogs(t, sink2, 3, 10*time.Second)
	err = rcvr2.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("second receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink2.AllLogs(), map[string]int{
		"discovered": 2, // u1 + d1 (full sync always emits discovered for non-removed)
		"deleted":    1, // u2
	})

	var dl string
	err = store.Get("entraid.cursor.users_delta", &dl)
	if err != nil {
		t.Fatalf("delta link should be persisted: %v", err)
	}
	if dl == "" {
		t.Error("persisted delta link is empty")
	}
}

// TestEntraidReceiverIncrementalDeltaLink verifies that the receiver's
// ticker-driven incremental sync produces ActionModified entities using
// delta link state from the initial full sync.
//
// Fixture call sequence:
//   - Call 1 (full sync, no $deltatoken): all users/devices + delta link.
//   - Call 2+ (incremental, has $deltatoken): modified user + new delta link.
//
// Tests bypass Config.Validate() via newReceiver (SyncInterval > UpdateInterval
// check would reject these intervals).
func TestEntraidReceiverIncrementalDeltaLink(t *testing.T) {
	fix := &entraidFixture{
		fullUsers: []json.RawMessage{
			entraidRawUser(t, "u1", map[string]any{"displayName": "Alice"}),
		},
		deltaUsers: []json.RawMessage{
			entraidRawUser(t, "u1", map[string]any{"displayName": "Alice Updated"}),
		},
		fullDevices: []json.RawMessage{},
	}

	srv := startEntraidIntegServer(t, fix)
	httpClient := srv.Client()

	const provName = "test_entraid_incr"
	registerTestEntraidFactory(t, provName, httpClient)

	store := newMemoryStore()
	sink := &consumertest.LogsSink{}

	cfg := entraidIntegConfig(provName, srv.URL)
	cfg.SyncInterval = 24 * time.Hour
	cfg.UpdateInterval = 100 * time.Millisecond

	rcvr := newReceiver(nopSettings(), cfg, sink)
	err := rcvr.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("receiver start failed: %v", err)
	}

	// Wait for full sync (1 user) + at least one incremental tick (1 modified user).
	waitForLogs(t, sink, 2, 10*time.Second)

	err = rcvr.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("receiver shutdown failed: %v", err)
	}

	records := collectLogRecords(sink.AllLogs())
	if len(records) < 2 {
		t.Fatalf("got %d records; want at least 2 (1 full + 1 incremental)", len(records))
	}

	// The first record from full sync is "discovered"; subsequent records
	// from incremental ticks are "modified".
	firstAction := records[0].Body().Map().AsRaw()["event.action"]
	if firstAction != "discovered" {
		t.Errorf("first record action = %v; want discovered", firstAction)
	}

	hasModified := false
	for _, lr := range records[1:] {
		action := lr.Body().Map().AsRaw()["event.action"]
		if action == "modified" {
			hasModified = true
			break
		}
	}
	if !hasModified {
		t.Error("expected at least one modified record from incremental sync")
	}
}

// TestEntraidReceiverDeltaRecovery verifies that when the delta link
// expires (410 Gone), the provider recovers by deleting the stored
// cursor and retrying a full fetch.
//
// Fixture call sequence:
//   - Call 1 (full sync): entities + delta link.
//   - Call 2 (incremental): mock returns 410 -> provider retries without delta.
//   - Call 3 (retry full): entities returned as modified.
func TestEntraidReceiverDeltaRecovery(t *testing.T) {
	fix := &entraidFixture{
		fullUsers: []json.RawMessage{
			entraidRawUser(t, "u1", map[string]any{"displayName": "Alice"}),
		},
		fullDevices: []json.RawMessage{},
	}

	srv := startEntraidIntegServer(t, fix)
	httpClient := srv.Client()

	const provName = "test_entraid_recovery"
	registerTestEntraidFactory(t, provName, httpClient)

	store := newMemoryStore()
	sink := &consumertest.LogsSink{}

	cfg := entraidIntegConfig(provName, srv.URL)
	cfg.SyncInterval = 24 * time.Hour
	cfg.UpdateInterval = 100 * time.Millisecond

	rcvr := newReceiver(nopSettings(), cfg, sink)
	err := rcvr.Start(t.Context(), testHost(provName, store))
	if err != nil {
		t.Fatalf("receiver start failed: %v", err)
	}

	// Wait for full sync (1 user).
	waitForLogs(t, sink, 1, 10*time.Second)

	// Set delta status to 410 before the incremental tick fires.
	fix.deltaStatus.Store(http.StatusGone)

	// Wait for recovery: the provider should detect 410, delete the
	// cursor, and retry a full fetch producing another record.
	waitForLogs(t, sink, 2, 10*time.Second)

	err = rcvr.Shutdown(t.Context())
	if err != nil {
		t.Fatalf("receiver shutdown failed: %v", err)
	}

	records := collectLogRecords(sink.AllLogs())
	if len(records) < 2 {
		t.Fatalf("got %d records; want at least 2 (1 full + 1 recovered)", len(records))
	}
}

// registerTestEntraidFactory registers a ProviderFactory that creates
// a real ecentraid.Provider using the given HTTP client (from
// httptest.Server.Client). The factory is unregistered on test cleanup.
func registerTestEntraidFactory(t *testing.T, name string, httpClient *http.Client) {
	t.Helper()
	Register(name, func(cfg *confmap.Conf) (entcollect.Provider, error) {
		ec := ecentraid.DefaultConfig()
		if cfg != nil {
			type lc struct {
				TenantID     string `mapstructure:"tenant_id"`
				ClientID     string `mapstructure:"client_id"`
				ClientSecret string `mapstructure:"client_secret"`
				LoginEndpt   string `mapstructure:"login_endpoint"`
				APIEndpt     string `mapstructure:"api_endpoint"`
				Dataset      string `mapstructure:"dataset"`
			}
			var c lc
			err := cfg.Unmarshal(&c)
			if err != nil {
				return nil, fmt.Errorf("unmarshal config: %w", err)
			}
			ec.TenantID = c.TenantID
			ec.ClientID = c.ClientID
			ec.ClientSecret = c.ClientSecret
			ec.LoginEndpoint = c.LoginEndpt
			ec.APIEndpoint = c.APIEndpt
			if c.Dataset != "" {
				ec.Dataset = c.Dataset
			}
		}
		err := ec.Validate()
		if err != nil {
			return nil, err
		}
		return ecentraid.NewWithClient(ec, httpClient), nil
	})
	t.Cleanup(func() { unregister(name) })
}

// entraidIntegConfig returns a receiver Config wired to the named
// EntraID provider, pointing login and API endpoints at srvURL.
func entraidIntegConfig(provider, srvURL string) *Config {
	return &Config{
		Provider:       provider,
		StorageID:      "elasticsearch_storage",
		SyncInterval:   24 * time.Hour,
		UpdateInterval: time.Hour,
		ProviderConfig: map[string]any{
			"tenant_id":      "tenant-1",
			"client_id":      "client-1",
			"client_secret":  "secret-1",
			"login_endpoint": srvURL,
			"api_endpoint":   srvURL + "/v1.0",
		},
	}
}

// EntraID mock server, ported from entcollect/provider/entraid/provider_test.go.

// entraidFixture holds the entities returned by the mock Microsoft
// Graph delta API. fullUsers/fullDevices are returned on initial
// requests (no $deltatoken); deltaUsers/deltaDevices are returned
// on incremental requests (with $deltatoken). deltaStatus, if
// non-zero, causes incremental requests to return that HTTP status
// code (e.g. 410 Gone for delta expiry).
type entraidFixture struct {
	fullUsers    []json.RawMessage
	deltaUsers   []json.RawMessage
	fullDevices  []json.RawMessage
	deltaDevices []json.RawMessage
	deltaStatus  atomic.Int32 // non-zero -> return this HTTP status on delta requests
}

type entraidDeltaResponse struct {
	Value     []json.RawMessage `json:"value,omitempty"`
	DeltaLink string            `json:"@odata.deltaLink,omitempty"`
	NextLink  string            `json:"@odata.nextLink,omitempty"`
}

// startEntraidIntegServer starts an httptest server that serves the
// Microsoft Graph delta API endpoints (users/delta, devices/delta,
// groups, OAuth token) and an OAuth2 token endpoint. Full vs
// incremental responses are selected by the presence of $deltatoken
// in the query string.
func startEntraidIntegServer(t *testing.T, fix *entraidFixture) *httptest.Server {
	t.Helper()
	var srvURL string

	mux := http.NewServeMux()

	mux.HandleFunc("POST /tenant-1/oauth2/v2.0/token", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(map[string]any{
			"access_token": "test-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
		if err != nil {
			t.Errorf("encoding token response: %v", err)
		}
	})

	mux.HandleFunc("GET /v1.0/users/delta", func(w http.ResponseWriter, r *http.Request) {
		isDelta := r.URL.Query().Has("$deltatoken")
		if status := fix.deltaStatus.Load(); status != 0 && isDelta {
			w.WriteHeader(int(status))
			return
		}

		var resp entraidDeltaResponse
		if isDelta {
			resp.Value = fix.deltaUsers
			resp.DeltaLink = srvURL + "/v1.0/users/delta?$deltatoken=incr"
		} else {
			resp.Value = fix.fullUsers
			resp.DeltaLink = srvURL + "/v1.0/users/delta?$deltatoken=full"
		}
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(resp)
		if err != nil {
			t.Errorf("encoding users/delta response: %v", err)
		}
	})

	mux.HandleFunc("GET /v1.0/devices/delta", func(w http.ResponseWriter, r *http.Request) {
		isDelta := r.URL.Query().Has("$deltatoken")
		if status := fix.deltaStatus.Load(); status != 0 && isDelta {
			w.WriteHeader(int(status))
			return
		}

		var resp entraidDeltaResponse
		if isDelta {
			resp.Value = fix.deltaDevices
			resp.DeltaLink = srvURL + "/v1.0/devices/delta?$deltatoken=incr"
		} else {
			resp.Value = fix.fullDevices
			resp.DeltaLink = srvURL + "/v1.0/devices/delta?$deltatoken=full"
		}
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(resp)
		if err != nil {
			t.Errorf("encoding devices/delta response: %v", err)
		}
	})

	mux.HandleFunc("GET /v1.0/groups", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(entraidDeltaResponse{})
		if err != nil {
			t.Errorf("encoding groups response: %v", err)
		}
	})

	mux.HandleFunc("GET /v1.0/groups/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 5 || parts[4] != "members" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(entraidDeltaResponse{})
		if err != nil {
			t.Errorf("encoding group members response: %v", err)
		}
	})

	mux.HandleFunc("GET /v1.0/devices/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 5 {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(entraidDeltaResponse{})
		if err != nil {
			t.Errorf("encoding device detail response: %v", err)
		}
	})

	srv := httptest.NewServer(mux)
	srvURL = srv.URL
	t.Cleanup(srv.Close)
	return srv
}

// entraidRawUser builds a JSON object representing a Graph API user
// with the given id and additional fields.
func entraidRawUser(t *testing.T, id string, fields map[string]any) json.RawMessage {
	t.Helper()
	m := map[string]any{"id": id}
	for k, v := range fields {
		m[k] = v
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal user %s: %v", id, err)
	}
	return b
}

// entraidRawRemovedUser builds a JSON object representing a user
// marked as removed via the @removed annotation in a delta response.
func entraidRawRemovedUser(t *testing.T, id string) json.RawMessage {
	t.Helper()
	m := map[string]any{
		"id":       id,
		"@removed": map[string]any{"reason": "deleted"},
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal removed user %s: %v", id, err)
	}
	return b
}

// entraidRawDevice builds a JSON object representing a Graph API
// device with the given id and additional fields.
func entraidRawDevice(t *testing.T, id string, fields map[string]any) json.RawMessage {
	t.Helper()
	m := map[string]any{"id": id}
	for k, v := range fields {
		m[k] = v
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal device %s: %v", id, err)
	}
	return b
}
