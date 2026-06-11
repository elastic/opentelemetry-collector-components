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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"reflect"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/elastic/entcollect"
	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func TestESStore_BufferCommitRoundTrip(t *testing.T) {
	client := esClient(t)
	store := newESStore(t, client, "test-buffer-roundtrip")

	buf := entcollect.NewBuffer(store)

	type payload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	err := buf.Set("key1", payload{Name: "alice", Count: 1})
	if err != nil {
		t.Fatalf("buf.Set key1: %v", err)
	}
	err = buf.Set("key2", []string{"id-a", "id-b"})
	if err != nil {
		t.Fatalf("buf.Set key2: %v", err)
	}
	err = buf.Commit()
	if err != nil {
		t.Fatalf("buf.Commit: %v", err)
	}

	var got payload
	err = store.Get("key1", &got)
	if err != nil {
		t.Fatalf("Get key1 after Commit: %v", err)
	}
	want := payload{Name: "alice", Count: 1}
	if got != want {
		t.Errorf("key1: got %+v, want %+v", got, want)
	}

	var ids []string
	err = store.Get("key2", &ids)
	if err != nil {
		t.Fatalf("Get key2 after Commit: %v", err)
	}
	if !reflect.DeepEqual(ids, []string{"id-a", "id-b"}) {
		t.Errorf("key2: got %v, want [id-a id-b]", ids)
	}
}

func TestESStore_SetGetRoundTrip(t *testing.T) {
	client := esClient(t)
	store := newESStore(t, client, "test-roundtrip")

	type payload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	want := payload{Name: "test", Count: 42}
	err := store.Set("key1", want)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	var got payload
	err = store.Get("key1", &got)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != want {
		t.Errorf("round-trip: got %+v, want %+v", got, want)
	}
}

func TestESStore_LargeShardDocument(t *testing.T) {
	client := esClient(t)
	store := newESStore(t, client, "test-large-shard")

	uuids := make([]string, 2000)
	for i := range uuids {
		uuids[i] = fmt.Sprintf("uuid-%06d", i)
	}

	err := store.Set("shard.0", uuids)
	if err != nil {
		t.Fatalf("Set large shard: %v", err)
	}

	var got []string
	err = store.Get("shard.0", &got)
	if err != nil {
		t.Fatalf("Get large shard: %v", err)
	}
	if len(got) != len(uuids) {
		t.Fatalf("shard element count: got %d, want %d", len(got), len(uuids))
	}
	if got[0] != uuids[0] {
		t.Errorf("first element: got %s, want %s", got[0], uuids[0])
	}
	if got[len(got)-1] != uuids[len(uuids)-1] {
		t.Errorf("last element: got %s, want %s", got[len(got)-1], uuids[len(uuids)-1])
	}
}

func TestESStore_MissingIndex(t *testing.T) {
	client := esClient(t)
	store := newESStore(t, client, "test-missing-index")

	// First Set should succeed — ES auto-creates the index.
	err := store.Set("first", "value")
	if err != nil {
		t.Fatalf("Set on non-existent index: %v", err)
	}

	var got string
	err = store.Get("first", &got)
	if err != nil {
		t.Fatalf("Get after auto-create: %v", err)
	}
	if got != "value" {
		t.Errorf("got %q, want %q", got, "value")
	}

	// Get on missing key returns ErrKeyNotFound.
	err = store.Get("nonexistent", &got)
	if !errors.Is(err, entcollect.ErrKeyNotFound) {
		t.Fatalf("Get missing key: got %v, want ErrKeyNotFound", err)
	}
}

func TestESStore_DeletedIndex(t *testing.T) {
	client := esClient(t)
	store := newESStore(t, client, "test-deleted-index")

	err := store.Set("key1", "value1")
	if err != nil {
		t.Fatalf("Set before delete: %v", err)
	}

	deleteESIndex(t, client, "test-deleted-index")

	var got string
	err = store.Get("key1", &got)
	if err == nil {
		t.Fatal("Get after index deletion should error")
	}
}

// esClient returns an Elasticsearch client for integration tests.
// If ES_URL is set, it connects to that endpoint directly (useful
// when running ES outside the test, e.g. via podman). Otherwise it
// starts a container via testcontainers-go (requires Docker).
func esClient(t *testing.T) *elasticsearch.Client {
	t.Helper()

	endpoint := os.Getenv("ES_URL")
	if endpoint == "" {
		endpoint = startESContainer(t)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(func() { transport.CloseIdleConnections() })

	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{endpoint},
		Transport: transport,
	})
	if err != nil {
		t.Fatalf("creating ES client: %v", err)
	}

	deadline := time.Now().Add(2 * time.Minute)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Info()
		if err != nil {
			lastErr = err
		} else {
			_ = resp.Body.Close()
			if resp.IsError() {
				lastErr = fmt.Errorf("ES info: %s", resp.Status())
			} else {
				lastErr = nil
				break
			}
		}
		time.Sleep(time.Second)
	}
	if lastErr != nil {
		t.Fatalf("ES not ready after 2m: %v", lastErr)
	}

	return client
}

const esPort = "9200"

// startESContainer starts an Elasticsearch container via
// testcontainers-go and returns its HTTP endpoint. The container is
// terminated on test cleanup.
func startESContainer(t *testing.T) string {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:        "docker.io/library/elasticsearch:8.18.1",
		ExposedPorts: []string{esPort},
		Env: map[string]string{
			"discovery.type":         "single-node",
			"ES_JAVA_OPTS":           "-Xms512m -Xmx512m",
			"xpack.security.enabled": "false",
		},
		WaitingFor: wait.ForListeningPort(esPort).WithStartupTimeout(2 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		_ = testcontainers.TerminateContainer(container)
		t.Fatalf("starting ES container: %v", err)
	}
	t.Cleanup(func() {
		err := testcontainers.TerminateContainer(container)
		if err != nil {
			t.Errorf("terminating ES container: %v", err)
		}
	})

	host, err := container.Host(context.Background())
	if err != nil {
		t.Fatalf("getting container host: %v", err)
	}
	port, err := container.MappedPort(context.Background(), esPort)
	if err != nil {
		t.Fatalf("getting container port: %v", err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

// esStore is a test-only entcollect.Store backed by go-elasticsearch/v8.
// Values are JSON-encoded and stored as a string in a {"v":"..."} wrapper
// document, so all keys share a single string-typed mapping regardless
// of the Go type being stored. refresh=true is used on writes for
// immediate test visibility.
type esStore struct {
	client *elasticsearch.Client
	index  string
}

// newESStore returns an esStore backed by the given index. Any
// existing index with the same name is deleted first so the test
// starts with a clean slate.
func newESStore(t *testing.T, client *elasticsearch.Client, index string) *esStore {
	t.Helper()
	resp, err := client.Indices.Delete([]string{index})
	if err == nil {
		_ = resp.Body.Close()
	}
	return &esStore{client: client, index: index}
}

func (s *esStore) Get(key string, dst any) error {
	docID := url.QueryEscape(key)
	resp, err := s.client.Get(s.index, docID)
	if err != nil {
		return fmt.Errorf("es get %q: %w", key, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return entcollect.ErrKeyNotFound
	}
	if resp.IsError() {
		return fmt.Errorf("es get %q: %s", key, resp.Status())
	}

	var result struct {
		Source struct {
			V string `json:"v"`
		} `json:"_source"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return fmt.Errorf("es get %q decode: %w", key, err)
	}
	return json.Unmarshal([]byte(result.Source.V), dst)
}

func (s *esStore) Set(key string, value any) error {
	docID := url.QueryEscape(key)
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("es set %q marshal value: %w", key, err)
	}
	// Store as {"v":"<json>"} so the v field is always a string,
	// avoiding ES mapping conflicts between heterogeneous value types.
	doc := struct {
		V string `json:"v"`
	}{V: string(raw)}
	body, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("es set %q marshal doc: %w", key, err)
	}

	req := esapi.IndexRequest{
		Index:      s.index,
		DocumentID: docID,
		Body:       bytes.NewReader(body),
		Refresh:    "true",
	}
	resp, err := req.Do(context.Background(), s.client)
	if err != nil {
		return fmt.Errorf("es set %q: %w", key, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.IsError() {
		return fmt.Errorf("es set %q: %s", key, resp.Status())
	}
	return nil
}

func (s *esStore) Delete(key string) error {
	docID := url.QueryEscape(key)
	req := esapi.DeleteRequest{
		Index:      s.index,
		DocumentID: docID,
		Refresh:    "true",
	}
	resp, err := req.Do(context.Background(), s.client)
	if err != nil {
		return fmt.Errorf("es delete %q: %w", key, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.IsError() {
		return fmt.Errorf("es delete %q: %s", key, resp.Status())
	}
	return nil
}

func (s *esStore) Each(fn func(string, func(any) error) (bool, error)) error {
	body := `{"query":{"match_all":{}},"size":1000}`
	resp, err := s.client.Search(
		s.client.Search.WithContext(context.Background()),
		s.client.Search.WithIndex(s.index),
		s.client.Search.WithBody(bytes.NewReader([]byte(body))),
	)
	if err != nil {
		return fmt.Errorf("es each search: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.IsError() {
		return fmt.Errorf("es each search: %s", resp.Status())
	}

	var result struct {
		Hits struct {
			Hits []struct {
				ID     string `json:"_id"`
				Source struct {
					V string `json:"v"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return fmt.Errorf("es each decode: %w", err)
	}

	for _, hit := range result.Hits.Hits {
		key, err := url.QueryUnescape(hit.ID)
		if err != nil {
			key = hit.ID
		}
		v := hit.Source.V
		cont, err := fn(key, func(dst any) error {
			return json.Unmarshal([]byte(v), dst)
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

// deleteESIndex deletes an ES index. Used by tests to simulate index loss.
func deleteESIndex(t *testing.T, client *elasticsearch.Client, index string) {
	t.Helper()
	resp, err := client.Indices.Delete([]string{index})
	if err != nil {
		t.Fatalf("delete index %s: %v", index, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.IsError() {
		t.Fatalf("delete index %s: %s", index, resp.Status())
	}
}
