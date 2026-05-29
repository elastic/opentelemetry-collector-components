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

package kubetunnel

import (
	"testing"

	"github.com/elastic/opentelemetry-collector-components/internal/kubetunnel/tunnelpb"
)

// fakeSender records frames pushed down the tunnel.
type fakeSender struct{ sent []*tunnelpb.ServerFrame }

func (f *fakeSender) Send(frame *tunnelpb.ServerFrame) error {
	f.sent = append(f.sent, frame)
	return nil
}

func newTestTunnel(id string) (*Tunnel, *fakeSender) {
	s := &fakeSender{}
	return NewTunnel(s, ProbeInfo{ProbeID: id, ClusterID: "c-" + id}), s
}

func TestRegistryAddListGetRemove(t *testing.T) {
	r := NewRegistry()
	t1, _ := newTestTunnel("b")
	t2, _ := newTestTunnel("a")
	r.Add(t1)
	r.Add(t2)

	got := r.List()
	if len(got) != 2 {
		t.Fatalf("expected 2 probes, got %d", len(got))
	}
	// List is sorted by probe id.
	if got[0].ProbeID != "a" || got[1].ProbeID != "b" {
		t.Fatalf("expected sorted [a b], got [%s %s]", got[0].ProbeID, got[1].ProbeID)
	}

	if _, ok := r.Get("a"); !ok {
		t.Fatal("expected to find probe a")
	}
	if _, ok := r.Get("missing"); ok {
		t.Fatal("did not expect to find missing probe")
	}

	r.Remove("a", t2)
	if _, ok := r.Get("a"); ok {
		t.Fatal("probe a should have been removed")
	}
}

// TestRemoveOnlyEvictsCurrent ensures a stale tunnel's cleanup does not evict a
// freshly reconnected tunnel registered under the same id.
func TestRemoveOnlyEvictsCurrent(t *testing.T) {
	r := NewRegistry()
	stale, _ := newTestTunnel("x")
	fresh, _ := newTestTunnel("x")
	r.Add(stale)
	r.Add(fresh) // reconnect replaces stale

	r.Remove("x", stale) // stale cleanup must be a no-op
	if cur, ok := r.Get("x"); !ok || cur != fresh {
		t.Fatal("fresh tunnel should still be registered after stale cleanup")
	}
}

func TestTunnelSendCallDeliver(t *testing.T) {
	tn, sender := newTestTunnel("a")

	ch := tn.Call("req-1")
	if err := tn.Send("req-1", []byte(`{"operation":"list"}`)); err != nil {
		t.Fatalf("send: %v", err)
	}
	if len(sender.sent) != 1 || sender.sent[0].RequestId != "req-1" {
		t.Fatalf("expected one frame for req-1, got %+v", sender.sent)
	}

	tn.Deliver("req-1", &ReadResult{JSON: []byte(`[]`)})
	select {
	case res := <-ch:
		if string(res.JSON) != "[]" {
			t.Fatalf("unexpected result: %s", res.JSON)
		}
	default:
		t.Fatal("expected a delivered result")
	}

	// Delivering to an unknown id must not panic or block.
	tn.Deliver("unknown", &ReadResult{})
	tn.EndCall("req-1")
}
