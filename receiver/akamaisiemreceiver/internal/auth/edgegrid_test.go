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

package auth

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEdgeGridSigner_Sign(t *testing.T) {
	signer := NewEdgeGridSigner("client-token", "client-secret", "access-token")

	req, err := http.NewRequest(http.MethodGet, "https://test.luna.akamaiapis.net/siem/v1/configs/12345?limit=10000", nil)
	require.NoError(t, err)

	signer.Sign(req)

	authHeader := req.Header.Get("Authorization")
	assert.Contains(t, authHeader, "EG1-HMAC-SHA256")
	assert.Contains(t, authHeader, "client_token=client-token")
	assert.Contains(t, authHeader, "access_token=access-token")
	assert.Contains(t, authHeader, "timestamp=")
	assert.Contains(t, authHeader, "nonce=")
	assert.Contains(t, authHeader, "signature=")
}

func TestEdgeGridSigner_SignDeterministic(t *testing.T) {
	signer := NewEdgeGridSigner("ct", "cs", "at")

	req1, _ := http.NewRequest(http.MethodGet, "https://test.example.com/path", nil)
	req2, _ := http.NewRequest(http.MethodGet, "https://test.example.com/path", nil)

	signer.Sign(req1)
	signer.Sign(req2)

	// Each signature should be unique (different nonce/timestamp).
	assert.NotEqual(t, req1.Header.Get("Authorization"), req2.Header.Get("Authorization"))
}

func TestTransport_RoundTrip(t *testing.T) {
	signer := NewEdgeGridSigner("ct", "cs", "at")

	var capturedAuth string
	transport := &Transport{
		Base: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			capturedAuth = req.Header.Get("Authorization")
			return &http.Response{StatusCode: 200}, nil
		}),
		Signer: signer,
	}

	req, _ := http.NewRequest(http.MethodGet, "https://test.example.com/path", nil)
	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Contains(t, capturedAuth, "EG1-HMAC-SHA256")

	// Original request should NOT have auth header (transport clones it).
	assert.Empty(t, req.Header.Get("Authorization"))
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
