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

// Package auth implements Akamai EdgeGrid HMAC-SHA256 request signing.
package auth // import "github.com/elastic/opentelemetry-collector-components/receiver/akamaisiemreceiver/internal/auth"

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// EdgeGridSigner signs HTTP requests using Akamai EdgeGrid authentication.
type EdgeGridSigner struct {
	clientToken  string
	clientSecret string
	accessToken  string
}

// NewEdgeGridSigner creates a new signer with the provided credentials.
func NewEdgeGridSigner(clientToken, clientSecret, accessToken string) *EdgeGridSigner {
	return &EdgeGridSigner{
		clientToken:  clientToken,
		clientSecret: clientSecret,
		accessToken:  accessToken,
	}
}

// Sign adds the EdgeGrid authorization header to the request.
func (s *EdgeGridSigner) Sign(req *http.Request) {
	timestamp := time.Now().UTC().Format("20060102T15:04:05-0700")
	nonce := uuid.New().String()

	authBase := fmt.Sprintf(
		"EG1-HMAC-SHA256 client_token=%s;access_token=%s;timestamp=%s;nonce=%s;",
		s.clientToken, s.accessToken, timestamp, nonce,
	)

	signingKey := createSigningKey(timestamp, s.clientSecret)
	dataToSign := buildDataToSign(req, authBase)
	signature := computeSignature(dataToSign, signingKey)

	req.Header.Set("Authorization", authBase+"signature="+signature)
}

func createSigningKey(timestamp, clientSecret string) string {
	mac := hmac.New(sha256.New, []byte(clientSecret))
	mac.Write([]byte(timestamp))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func buildDataToSign(req *http.Request, authBase string) string {
	scheme := strings.ToLower(req.URL.Scheme)
	if scheme == "" {
		scheme = "https"
	}
	host := strings.ToLower(req.URL.Host)
	path := req.URL.Path
	if path == "" {
		path = "/"
	}

	var sb strings.Builder
	sb.WriteString(req.Method)
	sb.WriteString("\t")
	sb.WriteString(scheme)
	sb.WriteString("\t")
	sb.WriteString(host)
	sb.WriteString("\t")
	sb.WriteString(path)
	if req.URL.RawQuery != "" {
		sb.WriteString("?")
		sb.WriteString(req.URL.RawQuery)
	}
	sb.WriteString("\t\t\t")
	sb.WriteString(authBase)

	return sb.String()
}

func computeSignature(data, key string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(data))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// Transport wraps an http.RoundTripper to add EdgeGrid authentication.
type Transport struct {
	Base   http.RoundTripper
	Signer *EdgeGridSigner
}

// RoundTrip signs the request and delegates to the base transport.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	t.Signer.Sign(clone)
	return t.Base.RoundTrip(clone)
}
