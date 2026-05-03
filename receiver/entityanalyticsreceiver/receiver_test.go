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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/elastic/entcollect"
)

func TestFullSyncCycle(t *testing.T) {
	const name = "test_fullsync"
	docs := testDocs()

	Register(name, fakeFactory(docs, nil, 0))
	t.Cleanup(func() { unregister(name) })

	store := newMemoryStore()
	sink := &consumertest.LogsSink{}
	rcvr := newReceiver(nopSettings(), testConfig(name), sink)
	require.NoError(t, rcvr.Start(context.Background(), testHost(name, store)))
	t.Cleanup(func() { require.NoError(t, rcvr.Shutdown(context.Background())) })

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 3
	}, 5*time.Second, 10*time.Millisecond)
	assert.Equal(t, 3, sink.LogRecordCount())
}

func TestPublisherMapping(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	doc := entcollect.Document{
		ID:        "u-42",
		Kind:      entcollect.KindUser,
		Action:    entcollect.ActionDiscovered,
		Timestamp: now,
		Fields:    map[string]any{"user.name": "alice", "user.email": "alice@example.com"},
	}

	sink := &consumertest.LogsSink{}
	pub := newPublisher(sink, "testprov", "test.scope", "v0.0.0-test")
	require.NoError(t, pub(context.Background(), doc))

	require.Equal(t, 1, sink.LogRecordCount())

	logs := sink.AllLogs()[0]
	rl := logs.ResourceLogs().At(0)

	sl := rl.ScopeLogs().At(0)
	assert.Equal(t, "test.scope", sl.Scope().Name())
	assert.Equal(t, "v0.0.0-test", sl.Scope().Version())
	mode, ok := sl.Scope().Attributes().Get("elastic.mapping.mode")
	require.True(t, ok, "scope must have elastic.mapping.mode")
	assert.Equal(t, "bodymap", mode.Str())

	lr := sl.LogRecords().At(0)
	assert.Equal(t, now.UnixNano(), lr.Timestamp().AsTime().UnixNano())

	docID, ok := lr.Attributes().Get("elasticsearch.document_id")
	require.True(t, ok, "log record must have elasticsearch.document_id")
	assert.Equal(t, "u-42", docID.Str())

	body := lr.Body().Map().AsRaw()
	assert.Equal(t, "alice", body["user.name"])
	assert.Equal(t, "alice@example.com", body["user.email"])
	assert.Equal(t, "discovered", body["event.action"])
	assert.Equal(t, "asset", body["event.kind"])
	assert.Equal(t, "user", body["asset.type"])
	assert.Equal(t, "u-42", body["asset.id"])
	assert.Equal(t, "testprov", body["labels.identity_source"])
}

func TestPublisherDeletedEventKind(t *testing.T) {
	doc := entcollect.Document{
		ID:        "u-99",
		Action:    entcollect.ActionDeleted,
		Timestamp: time.Now(),
	}
	sink := &consumertest.LogsSink{}
	pub := newPublisher(sink, "testprov", "test.scope", "")
	require.NoError(t, pub(context.Background(), doc))

	lr := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	body := lr.Body().Map().AsRaw()
	assert.Equal(t, "event", body["event.kind"])
}

func TestSyncErrorDiscardsBuffer(t *testing.T) {
	const name = "test_syncerr"
	docs := testDocs()
	syncErr := errors.New("provider failure")

	Register(name, fakeFactory(docs, syncErr, 1))
	t.Cleanup(func() { unregister(name) })

	store := newMemoryStore()
	require.NoError(t, store.Set("pre_existing", "value"))
	sink := &consumertest.LogsSink{}
	rcvr := newReceiver(nopSettings(), testConfig(name), sink)
	require.NoError(t, rcvr.Start(context.Background(), testHost(name, store)))

	// Wait for the sync attempt to complete (it will fail).
	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 1
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, rcvr.Shutdown(context.Background()))

	// Store should be unchanged — the buffer was discarded.
	var v string
	require.NoError(t, store.Get("pre_existing", &v))
	assert.Equal(t, "value", v)
	assert.Equal(t, 1, len(store.data), "store should only have pre_existing key")
}

func TestCommitFailureAfterSuccessfulSync(t *testing.T) {
	const name = "test_commitfail"
	docs := testDocs()

	Register(name, func(_ *confmap.Conf) (entcollect.Provider, error) {
		return &checkpointingProvider{docs: docs}, nil
	})
	t.Cleanup(func() { unregister(name) })

	base := &failingSetStore{memoryStore: newMemoryStore()}
	sink := &consumertest.LogsSink{}
	rcvr := newReceiver(nopSettings(), testConfig(name), sink)
	require.NoError(t, rcvr.Start(context.Background(), testHost(name, base)))

	// Docs are published during sync before Commit, so they reach the sink
	// even though the subsequent state commit will fail.
	require.Eventually(t, func() bool {
		return sink.LogRecordCount() >= 3
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, rcvr.Shutdown(context.Background()))

	assert.Equal(t, 3, sink.LogRecordCount(), "docs should have been consumed despite commit failure")
	assert.Empty(t, base.data, "store should be empty — commit could not persist state")
}

// TestSelfHealingAfterFailedFullSync verifies that when the initial
// full sync fails, the next tick retries a full sync rather than
// silently calling IncrementalSync against unestablished state.
func TestSelfHealingAfterFailedFullSync(t *testing.T) {
	const name = "test_selfheal"

	prov := &recordingProvider{failFullN: 1}
	Register(name, func(_ *confmap.Conf) (entcollect.Provider, error) { return prov, nil })
	t.Cleanup(func() { unregister(name) })

	cfg := &Config{
		Provider:       name,
		StorageID:      "elasticsearch_storage",
		SyncInterval:   10 * time.Millisecond,
		UpdateInterval: 10 * time.Millisecond,
	}
	rcvr := newReceiver(nopSettings(), cfg, &consumertest.LogsSink{})
	require.NoError(t, rcvr.Start(context.Background(), testHost(name, newMemoryStore())))
	t.Cleanup(func() { require.NoError(t, rcvr.Shutdown(context.Background())) })

	require.Eventually(t, func() bool {
		return prov.callCount() >= 2
	}, 5*time.Second, 5*time.Millisecond)

	calls := prov.calls()
	require.GreaterOrEqual(t, len(calls), 2)
	assert.Equal(t, "full", calls[0], "first call must be full")
	assert.Equal(t, "full", calls[1], "second call must also be full because the first full sync failed")
}

func TestShutdownCancelsSync(t *testing.T) {
	const name = "test_shutdown"
	Register(name, fakeFactory(nil, nil, 0))
	t.Cleanup(func() { unregister(name) })

	store := newMemoryStore()
	sink := &consumertest.LogsSink{}
	rcvr := newReceiver(nopSettings(), testConfig(name), sink)
	require.NoError(t, rcvr.Start(context.Background(), testHost(name, store)))
	require.NoError(t, rcvr.Shutdown(context.Background()))
}

func TestMissingStorageExtension(t *testing.T) {
	const name = "test_noext"
	Register(name, fakeFactory(nil, nil, 0))
	t.Cleanup(func() { unregister(name) })

	host := &testComponentHost{
		extensions: map[component.ID]component.Component{},
	}
	rcvr := newReceiver(nopSettings(), testConfig(name), &consumertest.LogsSink{})
	err := rcvr.Start(context.Background(), host)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestExtensionNotRegistry(t *testing.T) {
	const name = "test_badext"
	Register(name, fakeFactory(nil, nil, 0))
	t.Cleanup(func() { unregister(name) })

	extID := component.MustNewID("elasticsearch_storage")
	host := &testComponentHost{
		extensions: map[component.ID]component.Component{
			extID: &notRegistryExtension{},
		},
	}
	rcvr := newReceiver(nopSettings(), testConfig(name), &consumertest.LogsSink{})
	err := rcvr.Start(context.Background(), host)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not implement")
}

// Helpers

func nopSettings() receiver.Settings {
	return receivertest.NewNopSettings(component.MustNewType("entity_analytics"))
}

func testConfig(provider string) *Config {
	return &Config{
		Provider:       provider,
		StorageID:      "elasticsearch_storage",
		SyncInterval:   time.Hour,
		UpdateInterval: time.Hour,
	}
}

func testDocs() []entcollect.Document {
	now := time.Now()
	return []entcollect.Document{
		{ID: "1", Kind: entcollect.KindUser, Action: entcollect.ActionDiscovered, Timestamp: now, Fields: map[string]any{"user.name": "alice"}},
		{ID: "2", Kind: entcollect.KindDevice, Action: entcollect.ActionModified, Timestamp: now, Fields: map[string]any{"device.id": "dev1"}},
		{ID: "3", Kind: entcollect.KindUser, Action: entcollect.ActionDeleted, Timestamp: now, Fields: map[string]any{"user.name": "bob"}},
	}
}

func testHost(provider string, store entcollect.Store) component.Host {
	extID := component.MustNewID("elasticsearch_storage")
	return &testComponentHost{
		extensions: map[component.ID]component.Component{
			extID: &testExtension{stores: map[string]entcollect.Store{
				"entity_analytics." + provider: store,
			}},
		},
	}
}

func fakeFactory(docs []entcollect.Document, syncErr error, errAfter int) ProviderFactory {
	return func(_ *confmap.Conf) (entcollect.Provider, error) {
		return &fakeProvider{docs: docs, syncErr: syncErr, errAfter: errAfter}, nil
	}
}

// fakeProvider implements entcollect.Provider for testing.
type fakeProvider struct {
	docs     []entcollect.Document
	syncErr  error
	errAfter int
}

func (f *fakeProvider) FullSync(ctx context.Context, _ entcollect.Store, pub entcollect.Publisher, _ *slog.Logger) error {
	for i, doc := range f.docs {
		if f.syncErr != nil && i >= f.errAfter {
			return f.syncErr
		}
		if err := pub(ctx, doc); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeProvider) IncrementalSync(ctx context.Context, store entcollect.Store, pub entcollect.Publisher, log *slog.Logger) error {
	return f.FullSync(ctx, store, pub, log)
}

// memoryStore implements entcollect.Store backed by an in-memory map.
type memoryStore struct {
	data map[string]json.RawMessage
}

func newMemoryStore() *memoryStore {
	return &memoryStore{data: make(map[string]json.RawMessage)}
}

func (m *memoryStore) Get(key string, dst any) error {
	raw, ok := m.data[key]
	if !ok {
		return entcollect.ErrKeyNotFound
	}
	return json.Unmarshal(raw, dst)
}

func (m *memoryStore) Set(key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	m.data[key] = raw
	return nil
}

func (m *memoryStore) Delete(key string) error {
	delete(m.data, key)
	return nil
}

func (m *memoryStore) Each(fn func(string, func(any) error) (bool, error)) error {
	for key, raw := range m.data {
		cont, err := fn(key, func(dst any) error {
			return json.Unmarshal(raw, dst)
		})
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return nil
}

// testComponentHost implements component.Host for testing.
type testComponentHost struct {
	extensions map[component.ID]component.Component
}

func (h *testComponentHost) GetExtensions() map[component.ID]component.Component {
	return h.extensions
}

func (h *testComponentHost) GetFactory(_ component.Kind, _ component.Type) component.Factory {
	return nil
}

// testExtension implements both component.Component and
// entcollect.Registry for testing.
type testExtension struct {
	stores map[string]entcollect.Store
}

func (f *testExtension) Start(context.Context, component.Host) error { return nil }
func (f *testExtension) Shutdown(context.Context) error              { return nil }
func (f *testExtension) Store(name string) (entcollect.Store, error) {
	s, ok := f.stores[name]
	if !ok {
		return nil, fmt.Errorf("store %q not found", name)
	}
	return s, nil
}

// checkpointingProvider publishes documents and writes a state
// checkpoint, modelling the pattern of a real identity provider.
type checkpointingProvider struct {
	docs []entcollect.Document
}

func (p *checkpointingProvider) FullSync(ctx context.Context, store entcollect.Store, pub entcollect.Publisher, _ *slog.Logger) error {
	for _, doc := range p.docs {
		if err := pub(ctx, doc); err != nil {
			return err
		}
	}
	return store.Set("cursor", "checkpoint-1")
}

func (p *checkpointingProvider) IncrementalSync(ctx context.Context, store entcollect.Store, pub entcollect.Publisher, log *slog.Logger) error {
	return p.FullSync(ctx, store, pub, log)
}

// failingSetStore wraps a memoryStore but always returns an error
// from Set, simulating a storage backend failure during commit.
type failingSetStore struct {
	*memoryStore
}

func (f *failingSetStore) Set(string, any) error {
	return errors.New("storage write failed")
}

// notRegistryExtension implements component.Component but not
// entcollect.Registry.
type notRegistryExtension struct{}

func (n *notRegistryExtension) Start(context.Context, component.Host) error { return nil }
func (n *notRegistryExtension) Shutdown(context.Context) error              { return nil }

// recordingProvider records the kind of each sync call ("full" or
// "incremental") and fails the first failFullN full sync calls.
type recordingProvider struct {
	mu         sync.Mutex
	callsLog   []string
	fullErrors int
	failFullN  int
}

func (p *recordingProvider) FullSync(context.Context, entcollect.Store, entcollect.Publisher, *slog.Logger) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.callsLog = append(p.callsLog, "full")
	if p.fullErrors < p.failFullN {
		p.fullErrors++
		return errors.New("full sync failed")
	}
	return nil
}

func (p *recordingProvider) IncrementalSync(context.Context, entcollect.Store, entcollect.Publisher, *slog.Logger) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.callsLog = append(p.callsLog, "incremental")
	return nil
}

func (p *recordingProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.callsLog)
}

func (p *recordingProvider) calls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.callsLog))
	copy(out, p.callsLog)
	return out
}
