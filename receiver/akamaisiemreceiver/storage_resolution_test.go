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

package akamaisiemreceiver

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// Tests in this file cover storage-extension resolution (explicit `storage:`
// references with Fleet-suffix fallback) and instance-name auto-bind (no
// `storage:` configured). Fleet-managed configurations rename every component
// declared in a policy stream to <type>/<stream-suffix> without rewriting the
// receiver's storage reference; the resolver reconstructs that binding.

// mockHostWithExtensions builds a component.Host exposing an arbitrary
// extension map, unlike mockHost which hardcodes an unnamed file_storage.
func mockHostWithExtensions(exts map[component.ID]component.Component) component.Host {
	return &mockHostImpl{extensions: exts}
}

// nonStorageExtension is a component that does NOT implement storage.Extension.
type nonStorageExtension struct {
	component.StartFunc
	component.ShutdownFunc
}

// storageTestServer returns a mock Akamai SIEM endpoint serving one event so
// a poll completes and the cursor is persisted.
func storageTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, `{"httpMessage":{"start":"1000","host":"test.com","status":"200"}}`)
		_, _ = fmt.Fprintln(w, `{"offset":"resolution-test-offset","total":1,"limit":10000}`)
	}))
	t.Cleanup(server.Close)
	return server
}

func storageTestConfig(endpoint string) *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.HTTP.Endpoint = endpoint
	cfg.ConfigIDs = "1"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	cfg.PollInterval = 24 * time.Hour
	return cfg
}

// namedSettings returns receiver settings with a deterministic instance name.
// NewNopSettings assigns a random UUID name, so tests must override it: an
// empty name yields the standalone shape, a non-empty one the Fleet shape.
func namedSettings(name string) receiver.Settings {
	set := receivertest.NewNopSettings(NewFactory().Type())
	if name == "" {
		set.ID = component.MustNewID("akamai_siem")
	} else {
		set.ID = component.MustNewIDWithName("akamai_siem", name)
	}
	return set
}

// runUntilFirstPoll starts the receiver, waits for one event, and shuts it
// down. Shutdown waits for the poll goroutine, so cursor persistence (or its
// absence) is deterministic afterwards.
func runUntilFirstPoll(t *testing.T, cfg *Config, set receiver.Settings, host component.Host) {
	t.Helper()
	sink := &consumertest.LogsSink{}
	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), host))
	require.Eventually(t, func() bool { return sink.LogRecordCount() >= 1 }, 5*time.Second, 50*time.Millisecond)
	require.NoError(t, rcv.Shutdown(context.Background()))
}

// startExpectingError asserts that Start fails and returns the error.
func startExpectingError(t *testing.T, cfg *Config, set receiver.Settings, host component.Host) error {
	t.Helper()
	rcv, err := NewFactory().CreateLogs(context.Background(), set, cfg, &consumertest.LogsSink{})
	require.NoError(t, err)
	err = rcv.Start(context.Background(), host)
	require.Error(t, err)
	return err
}

func cursorBytes(t *testing.T, client *memStorageClient) []byte {
	t.Helper()
	data, err := client.Get(context.Background(), "akamai_siem_cursor")
	require.NoError(t, err)
	return data
}

// Bare reference, host has only the suffixed instance whose name matches the
// receiver's own (the Fleet-managed shape) → resolves via instance-name match.
func TestStorageResolution_BareRef_InstanceNameMatch(t *testing.T) {
	server := storageTestServer(t)
	memClient := newMemStorageClient()
	host := mockHostWithExtensions(map[component.ID]component.Component{
		component.MustNewIDWithName("file_storage", "policy-suffix"): &mockStorageExtension{client: memClient},
	})

	cfg := storageTestConfig(server.URL)
	storageID := component.MustNewID("file_storage")
	cfg.StorageID = &storageID

	runUntilFirstPoll(t, cfg, namedSettings("policy-suffix"), host)
	require.NotNil(t, cursorBytes(t, memClient), "cursor should persist via instance-name-matched extension")
}

// Bare reference, single same-type extension with a different instance name →
// resolves via unique type match.
func TestStorageResolution_BareRef_UniqueTypeMatch(t *testing.T) {
	server := storageTestServer(t)
	memClient := newMemStorageClient()
	host := mockHostWithExtensions(map[component.ID]component.Component{
		component.MustNewIDWithName("file_storage", "other"): &mockStorageExtension{client: memClient},
	})

	cfg := storageTestConfig(server.URL)
	storageID := component.MustNewID("file_storage")
	cfg.StorageID = &storageID

	runUntilFirstPoll(t, cfg, namedSettings("policy-suffix"), host)
	require.NotNil(t, cursorBytes(t, memClient), "cursor should persist via unique-type-matched extension")
}

// Bare reference, two same-type extensions, one matching the receiver's
// instance name → instance-name match beats ambiguity.
func TestStorageResolution_BareRef_NameBeatsAmbiguity(t *testing.T) {
	server := storageTestServer(t)
	memA := newMemStorageClient()
	memB := newMemStorageClient()
	host := mockHostWithExtensions(map[component.ID]component.Component{
		component.MustNewIDWithName("file_storage", "a"): &mockStorageExtension{client: memA},
		component.MustNewIDWithName("file_storage", "b"): &mockStorageExtension{client: memB},
	})

	cfg := storageTestConfig(server.URL)
	storageID := component.MustNewID("file_storage")
	cfg.StorageID = &storageID

	runUntilFirstPoll(t, cfg, namedSettings("b"), host)
	require.NotNil(t, cursorBytes(t, memB), "cursor should persist to the name-matched extension")
	assert.Nil(t, cursorBytes(t, memA), "non-matching extension must stay untouched")
}

// Bare reference, two same-type extensions, neither matching the receiver's
// instance name → Start fails listing both candidates.
func TestStorageResolution_BareRef_Ambiguous(t *testing.T) {
	host := mockHostWithExtensions(map[component.ID]component.Component{
		component.MustNewIDWithName("file_storage", "a"): &mockStorageExtension{client: newMemStorageClient()},
		component.MustNewIDWithName("file_storage", "b"): &mockStorageExtension{client: newMemStorageClient()},
	})

	cfg := storageTestConfig("http://localhost:0")
	storageID := component.MustNewID("file_storage")
	cfg.StorageID = &storageID

	err := startExpectingError(t, cfg, namedSettings("c"), host)
	assert.Contains(t, err.Error(), "ambiguous")
	assert.Contains(t, err.Error(), "file_storage/a")
	assert.Contains(t, err.Error(), "file_storage/b")
}

// Explicitly named reference that is missing → hard error, no fallback to
// other instances of the same type.
func TestStorageResolution_NamedRef_NoFallback(t *testing.T) {
	host := mockHostWithExtensions(map[component.ID]component.Component{
		component.MustNewIDWithName("file_storage", "bar"): &mockStorageExtension{client: newMemStorageClient()},
	})

	cfg := storageTestConfig("http://localhost:0")
	storageID := component.MustNewIDWithName("file_storage", "foo")
	cfg.StorageID = &storageID

	err := startExpectingError(t, cfg, namedSettings("bar"), host)
	assert.Contains(t, err.Error(), `storage extension "file_storage/foo" not found`)
	assert.Contains(t, err.Error(), "file_storage/bar", "error should list available storage extensions")
}

// Bare reference with no storage extensions on the host → Start fails.
func TestStorageResolution_BareRef_NoStorageExtensions(t *testing.T) {
	host := mockHostWithExtensions(map[component.ID]component.Component{})

	cfg := storageTestConfig("http://localhost:0")
	storageID := component.MustNewID("file_storage")
	cfg.StorageID = &storageID

	err := startExpectingError(t, cfg, namedSettings("policy-suffix"), host)
	assert.Contains(t, err.Error(), `storage extension "file_storage" not found`)
	assert.Contains(t, err.Error(), "no storage extensions are available")
}

// A same-type component that does not implement storage.Extension keeps
// today's "not a storage extension" error on every explicit-ref path.
func TestStorageResolution_NotAStorageExtension(t *testing.T) {
	cfg := storageTestConfig("http://localhost:0")
	storageID := component.MustNewID("file_storage")
	cfg.StorageID = &storageID

	t.Run("exact match", func(t *testing.T) {
		host := mockHostWithExtensions(map[component.ID]component.Component{
			component.MustNewID("file_storage"): &nonStorageExtension{},
		})
		err := startExpectingError(t, cfg, namedSettings("policy-suffix"), host)
		assert.Contains(t, err.Error(), `extension "file_storage" is not a storage extension`)
	})

	t.Run("instance-name match", func(t *testing.T) {
		host := mockHostWithExtensions(map[component.ID]component.Component{
			component.MustNewIDWithName("file_storage", "policy-suffix"): &nonStorageExtension{},
		})
		err := startExpectingError(t, cfg, namedSettings("policy-suffix"), host)
		assert.Contains(t, err.Error(), `extension "file_storage/policy-suffix" is not a storage extension`)
	})
}

// No storage configured, named receiver, one storage-capable extension with
// the same instance name → auto-binds and the cursor survives a stop/start
// cycle through that extension.
func TestAutoBind_InstanceNameMatch(t *testing.T) {
	server := storageTestServer(t)
	memClient := newMemStorageClient()
	host := mockHostWithExtensions(map[component.ID]component.Component{
		component.MustNewIDWithName("file_storage", "sfx"): &mockStorageExtension{client: memClient},
	})

	cfg := storageTestConfig(server.URL)
	require.Nil(t, cfg.StorageID)

	set := namedSettings("sfx")
	runUntilFirstPoll(t, cfg, set, host)
	first := cursorBytes(t, memClient)
	require.NotNil(t, first, "cursor should persist via auto-bound extension")

	// Second instance binds to the same store and starts from the persisted
	// cursor rather than a fresh initial_lookback window.
	runUntilFirstPoll(t, cfg, set, host)
	require.NotNil(t, cursorBytes(t, memClient), "cursor should survive the stop/start cycle")
}

// No storage configured, named receiver, the only storage extension has a
// different instance name → starts without persistence.
func TestAutoBind_NoMatch_RunsUnpersisted(t *testing.T) {
	server := storageTestServer(t)
	memClient := newMemStorageClient()
	host := mockHostWithExtensions(map[component.ID]component.Component{
		component.MustNewIDWithName("file_storage", "other"): &mockStorageExtension{client: memClient},
	})

	cfg := storageTestConfig(server.URL)
	runUntilFirstPoll(t, cfg, namedSettings("sfx"), host)
	assert.Nil(t, cursorBytes(t, memClient), "non-matching extension must not be auto-bound")
}

// Regression guard: an UNNAMED receiver next to an unnamed file_storage must
// NOT auto-bind — standalone behavior is unchanged and an explicit storage
// reference stays required.
func TestAutoBind_UnnamedReceiver_NoBind(t *testing.T) {
	server := storageTestServer(t)
	memClient := newMemStorageClient()
	host := mockHostWithExtensions(map[component.ID]component.Component{
		component.MustNewID("file_storage"): &mockStorageExtension{client: memClient},
	})

	cfg := storageTestConfig(server.URL)
	runUntilFirstPoll(t, cfg, namedSettings(""), host)
	assert.Nil(t, cursorBytes(t, memClient), "unnamed receivers must never auto-bind")
}

// Two storage-capable extensions share the receiver's instance name → ambiguous,
// run without persistence (a config without `storage:` must never fail).
func TestAutoBind_MultipleCandidates_RunsUnpersisted(t *testing.T) {
	server := storageTestServer(t)
	memA := newMemStorageClient()
	memB := newMemStorageClient()
	host := mockHostWithExtensions(map[component.ID]component.Component{
		component.MustNewIDWithName("file_storage", "sfx"):  &mockStorageExtension{client: memA},
		component.MustNewIDWithName("other_storage", "sfx"): &mockStorageExtension{client: memB},
	})

	cfg := storageTestConfig(server.URL)
	runUntilFirstPoll(t, cfg, namedSettings("sfx"), host)
	assert.Nil(t, cursorBytes(t, memA), "ambiguous auto-bind must not persist")
	assert.Nil(t, cursorBytes(t, memB), "ambiguous auto-bind must not persist")
}

// A non-storage extension sharing the receiver's instance name is ignored by
// auto-bind.
func TestAutoBind_NonStorageExtensionIgnored(t *testing.T) {
	server := storageTestServer(t)
	host := mockHostWithExtensions(map[component.ID]component.Component{
		component.MustNewIDWithName("some_ext", "sfx"): &nonStorageExtension{},
	})

	cfg := storageTestConfig(server.URL)
	runUntilFirstPoll(t, cfg, namedSettings("sfx"), host)
}
