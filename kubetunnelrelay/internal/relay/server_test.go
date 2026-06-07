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

package relay

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	kubetunnel "github.com/elastic/opentelemetry-collector-components/internal/kubetunnel"
	"github.com/elastic/opentelemetry-collector-components/internal/kubetunnel/tunnelpb"
)

func TestListTargetsEmpty(t *testing.T) {
	srv := New(nil, time.Second, nil)
	rec := httptest.NewRecorder()
	srv.HTTPHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/targets", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var got []kubetunnel.ProbeInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no targets, got %d", len(got))
	}
}

func TestReadUnknownProbeReturns404(t *testing.T) {
	srv := New(nil, time.Second, nil)
	rec := httptest.NewRecorder()
	body := strings.NewReader(`{"operation":"list","resource":"pods"}`)
	srv.HTTPHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/targets/nope", body))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown probe, got %d", rec.Code)
	}
}

func TestReadTimesOutWhenProbeSilent(t *testing.T) {
	srv := New(nil, 50*time.Millisecond, nil)
	// Register a tunnel whose sender swallows frames (probe never answers).
	srv.registry.Add(kubetunnel.NewTunnel(swallowSender{}, kubetunnel.ProbeInfo{ProbeID: "p1"}))

	rec := httptest.NewRecorder()
	body := strings.NewReader(`{"operation":"list","resource":"pods"}`)
	srv.HTTPHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/targets/p1", body))

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504 timeout, got %d", rec.Code)
	}
}

func TestHTTPAuthEnforced(t *testing.T) {
	srv := New([]string{"secret"}, time.Second, nil)
	rec := httptest.NewRecorder()
	srv.HTTPHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/targets", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/targets", nil)
	req.Header.Set("Authorization", "Bearer secret")
	srv.HTTPHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid token, got %d", rec.Code)
	}
}

type swallowSender struct{}

func (swallowSender) Send(*tunnelpb.ServerFrame) error { return nil }
