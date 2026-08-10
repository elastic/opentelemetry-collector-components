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
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"reflect"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/elastic/entcollect"
	ecjamf "github.com/elastic/entcollect/provider/jamf"
)

// TestJamfReceiverLifecycle exercises the full receiver lifecycle with a real
// Jamf entcollect provider against an httptest server, using an in-memory
// registry (real ES deferred to session 6b).
//
// Phase 1: 3 computers → 3 discovered events in the consumertest sink.
// Phase 2: 1 computer removed → restart with same store → 2 modified + 1 deleted.
func TestJamfReceiverLifecycle(t *testing.T) {
	fixture := newIntegFixture(makeIntegComputersJSON(t,
		integComputer{Name: "dev-laptop", UDID: "AAA-111", Managed: true},
		integComputer{Name: "staging-server", UDID: "BBB-222", Managed: true},
		integComputer{Name: "to-remove", UDID: "CCC-333", Managed: true},
	))
	srv := startJamfIntegServer(t, fixture)
	host := hostFromServerURL(t, srv.URL)
	httpClient := srv.Client()

	const provName = "test_jamf_lifecycle"
	registerTestJamfFactory(t, provName, httpClient)

	store := newMemoryStore()

	// Phase 1: first sync discovers all computers.

	sink1 := &consumertest.LogsSink{}
	rcvr1 := newReceiver(nopSettings(), integConfig(provName, host), sink1)
	if err := rcvr1.Start(context.Background(), testHost(provName, store)); err != nil {
		t.Fatalf("receiver start failed: %v", err)
	}

	waitForLogs(t, sink1, 3, 10*time.Second)

	if err := rcvr1.Shutdown(context.Background()); err != nil {
		t.Fatalf("receiver shutdown failed: %v", err)
	}

	if got := sink1.LogRecordCount(); got != 3 {
		t.Errorf("first sync: got %d documents, want 3", got)
	}
	checkActionCounts(t, sink1.AllLogs(), map[string]int{
		"discovered": 3,
	})

	// Phase 2: remove one computer, restart with same store.

	fixture.set(makeIntegComputersJSON(t,
		integComputer{Name: "dev-laptop", UDID: "AAA-111", Managed: true},
		integComputer{Name: "staging-server", UDID: "BBB-222", Managed: true},
	))

	sink2 := &consumertest.LogsSink{}
	rcvr2 := newReceiver(nopSettings(), integConfig(provName, host), sink2)
	if err := rcvr2.Start(context.Background(), testHost(provName, store)); err != nil {
		t.Fatalf("second receiver start failed: %v", err)
	}

	waitForLogs(t, sink2, 3, 10*time.Second)

	if err := rcvr2.Shutdown(context.Background()); err != nil {
		t.Fatalf("second receiver shutdown failed: %v", err)
	}

	if got := sink2.LogRecordCount(); got != 3 {
		t.Errorf("second sync: got %d documents, want 3 (2 modified + 1 deleted)", got)
	}
	checkActionCounts(t, sink2.AllLogs(), map[string]int{
		"modified": 2,
		"deleted":  1,
	})
}

// TestJamfReceiverConfigThroughReceiver verifies that provider-specific
// config (jamf_tenant etc.) flows from the receiver Config through to the
// factory, creating a working provider without environment variables.
func TestJamfReceiverConfigThroughReceiver(t *testing.T) {
	fixture := newIntegFixture(makeIntegComputersJSON(t,
		integComputer{Name: "cfg-test", UDID: "CFG-001", Managed: true},
	))
	srv := startJamfIntegServer(t, fixture)
	host := hostFromServerURL(t, srv.URL)
	httpClient := srv.Client()

	const provName = "test_jamf_cfgpath"
	registerTestJamfFactory(t, provName, httpClient)

	store := newMemoryStore()
	sink := &consumertest.LogsSink{}

	rcvr := newReceiver(nopSettings(), integConfig(provName, host), sink)
	if err := rcvr.Start(context.Background(), testHost(provName, store)); err != nil {
		t.Fatalf("receiver start failed: %v", err)
	}

	waitForLogs(t, sink, 1, 10*time.Second)

	if err := rcvr.Shutdown(context.Background()); err != nil {
		t.Fatalf("receiver shutdown failed: %v", err)
	}

	records := collectLogRecords(sink.AllLogs())
	if len(records) != 1 {
		t.Fatalf("got %d documents, want 1", len(records))
	}

	body := records[0].Body().Map().AsRaw()
	if got := body["event.kind"]; got != "asset" {
		t.Errorf("event.kind = %v, want %q", got, "asset")
	}
	if got := body["event.action"]; got != "discovered" {
		t.Errorf("event.action = %v, want %q", got, "discovered")
	}
	if got := body["device.id"]; got != "CFG-001" {
		t.Errorf("device.id = %v, want %q", got, "CFG-001")
	}
}

// TestJamfReceiverStatePersistence verifies that cursor/idset state written
// by the provider survives in the in-memory store across receiver restarts.
func TestJamfReceiverStatePersistence(t *testing.T) {
	fixture := newIntegFixture(makeIntegComputersJSON(t,
		integComputer{Name: "persist-test", UDID: "PERSIST-001", Managed: true},
	))
	srv := startJamfIntegServer(t, fixture)
	host := hostFromServerURL(t, srv.URL)
	httpClient := srv.Client()

	const provName = "test_jamf_persist"
	registerTestJamfFactory(t, provName, httpClient)

	store := newMemoryStore()

	sink1 := &consumertest.LogsSink{}
	rcvr1 := newReceiver(nopSettings(), integConfig(provName, host), sink1)
	if err := rcvr1.Start(context.Background(), testHost(provName, store)); err != nil {
		t.Fatalf("first receiver start failed: %v", err)
	}

	waitForLogs(t, sink1, 1, 10*time.Second)

	if err := rcvr1.Shutdown(context.Background()); err != nil {
		t.Fatalf("first receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink1.AllLogs(), map[string]int{"discovered": 1})

	var syncTime time.Time
	if err := store.Get("jamf.cursor.last_sync", &syncTime); err != nil {
		t.Fatalf("cursor should be persisted after sync: %v", err)
	}
	if syncTime.IsZero() {
		t.Error("persisted sync time is zero")
	}

	// Same device on second run → should be modified (not re-discovered).
	sink2 := &consumertest.LogsSink{}
	rcvr2 := newReceiver(nopSettings(), integConfig(provName, host), sink2)
	if err := rcvr2.Start(context.Background(), testHost(provName, store)); err != nil {
		t.Fatalf("second receiver start failed: %v", err)
	}

	waitForLogs(t, sink2, 1, 10*time.Second)

	if err := rcvr2.Shutdown(context.Background()); err != nil {
		t.Fatalf("second receiver shutdown failed: %v", err)
	}
	checkActionCounts(t, sink2.AllLogs(), map[string]int{"modified": 1})
}

// registerTestJamfFactory registers a ProviderFactory that creates a real
// ecjamf.Provider backed by the given httptest client. Config is parsed
// from the confmap.Conf passed by the receiver (exercising the wiring
// path), but the HTTP client is injected for TLS cert trust.
func registerTestJamfFactory(t *testing.T, name string, httpClient *http.Client) {
	t.Helper()
	Register(name, func(cfg *confmap.Conf) (entcollect.Provider, error) {
		ec := ecjamf.DefaultConfig()
		if cfg != nil {
			type lc struct {
				TenantID string `mapstructure:"jamf_tenant"`
				Username string `mapstructure:"jamf_username"`
				Password string `mapstructure:"jamf_password"`
			}
			var c lc
			if err := cfg.Unmarshal(&c); err != nil {
				return nil, fmt.Errorf("unmarshal config: %w", err)
			}
			ec.TenantID = c.TenantID
			ec.Username = c.Username
			ec.Password = c.Password
		}
		if err := ec.Validate(); err != nil {
			return nil, err
		}
		return ecjamf.NewWithClient(ec, httpClient), nil
	})
	t.Cleanup(func() { unregister(name) })
}

// integConfig returns a receiver Config wired to the given provider name
// with Jamf credentials pointing at the httptest server tenant.
func integConfig(provider, jamfTenant string) *Config {
	return &Config{
		Provider:       provider,
		StorageID:      "elasticsearch_storage",
		SyncInterval:   24 * time.Hour,
		UpdateInterval: time.Hour,
		ProviderConfig: map[string]any{
			"jamf_tenant":   jamfTenant,
			"jamf_username": "testuser",
			"jamf_password": "testpass",
		},
	}
}

// integFixture holds a mutable API response body that can be swapped
// between test phases while the httptest server is running.
type integFixture struct {
	mu   sync.Mutex
	data []byte
}

// newIntegFixture returns an integFixture initialised with data.
func newIntegFixture(data []byte) *integFixture {
	return &integFixture{data: data}
}

func (f *integFixture) get() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data
}

func (f *integFixture) set(data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = data
}

// startJamfIntegServer returns a TLS httptest server that serves Jamf
// token and computer-list endpoints backed by the given fixture.
func startJamfIntegServer(t *testing.T, fixture *integFixture) *httptest.Server {
	t.Helper()

	var tokenMu sync.Mutex
	var currentToken string

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth/token", func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "testuser" || pass != "testpass" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		tokenMu.Lock()
		currentToken = randomHex(16)
		tok := currentToken
		tokenMu.Unlock()

		expires := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
		_, _ = fmt.Fprintf(w, `{"token":%q,"expires":%q}`, tok, expires)
	})
	mux.HandleFunc("/api/preview/computers", func(w http.ResponseWriter, r *http.Request) {
		tokenMu.Lock()
		tok := currentToken
		tokenMu.Unlock()

		if tok == "" || r.Header.Get("Authorization") != "Bearer "+tok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(fixture.get())
	})

	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// integComputer describes a minimal Jamf computer for fixture generation.
type integComputer struct {
	Name    string
	UDID    string
	Managed bool
}

// makeIntegComputersJSON marshals computers into a Jamf API response body.
func makeIntegComputersJSON(t *testing.T, computers ...integComputer) []byte {
	t.Helper()

	type result struct {
		Location         struct{} `json:"location"`
		Name             *string  `json:"name"`
		UDID             *string  `json:"udid"`
		IsManaged        *bool    `json:"isManaged"`
		LastContactDate  string   `json:"lastContactDate"`
		LastReportDate   string   `json:"lastReportDate"`
		LastEnrolledDate string   `json:"lastEnrolledDate"`
	}

	results := make([]result, len(computers))
	for i, c := range computers {
		name := c.Name
		udid := c.UDID
		managed := c.Managed
		results[i] = result{
			Name:             &name,
			UDID:             &udid,
			IsManaged:        &managed,
			LastContactDate:  "2024-01-01T00:00:00Z",
			LastReportDate:   "2024-01-01T00:00:00Z",
			LastEnrolledDate: "2024-01-01T00:00:00Z",
		}
	}

	body := struct {
		TotalCount int      `json:"totalCount"`
		Results    []result `json:"results"`
	}{
		TotalCount: len(results),
		Results:    results,
	}

	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshalling fixture: %v", err)
	}
	return b
}

// hostFromServerURL extracts the host:port from a URL string.
func hostFromServerURL(t *testing.T, rawURL string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parsing server URL: %v", err)
	}
	return u.Host
}

// waitForLogs polls the sink until it contains at least n log records or
// the timeout expires.
func waitForLogs(t *testing.T, sink *consumertest.LogsSink, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if sink.LogRecordCount() >= n {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d log records (got %d)", n, sink.LogRecordCount())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// checkActionCounts extracts event.action values from all log records and
// reports mismatches against the expected counts.
func checkActionCounts(t *testing.T, allLogs []plog.Logs, want map[string]int) {
	t.Helper()
	got := map[string]int{}
	for _, lr := range collectLogRecords(allLogs) {
		body := lr.Body().Map().AsRaw()
		if action, ok := body["event.action"].(string); ok {
			got[action]++
		}
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("event.action counts = %v, want %v", got, want)
	}
}

// randomHex returns a hex-encoded string of n random bytes.
func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// collectLogRecords flattens all plog.Logs into a slice of individual LogRecords.
func collectLogRecords(allLogs []plog.Logs) []plog.LogRecord {
	var out []plog.LogRecord
	for _, logs := range allLogs {
		for i := range logs.ResourceLogs().Len() {
			rl := logs.ResourceLogs().At(i)
			for j := range rl.ScopeLogs().Len() {
				sl := rl.ScopeLogs().At(j)
				for k := range sl.LogRecords().Len() {
					out = append(out, sl.LogRecords().At(k))
				}
			}
		}
	}
	return out
}
