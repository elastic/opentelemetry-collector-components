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

package apikeyauthextension // import "github.com/elastic/opentelemetry-collector-components/extension/apikeyauthextension"

import (
	"context"
	"crypto/rand"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/extensionauth"
	"golang.org/x/crypto/pbkdf2"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/security/hasprivileges"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-freelru"
)

const (
	// authorizationHeader is the name of the header that contains the API Key Authorization.
	authorizationHeader = "Authorization"

	// lowerAuthorizationHeader is the lowercase version of the authorization header.
	lowerAuthorizationHeader = "authorization"
)

var (
	errAuthorizationHeaderWrongScheme = errors.New("ApiKey prefix not found")
	errAuthorizationHeaderInvalid     = errors.New("invalid API Key")

	_ client.AuthData      = (*authData)(nil)
	_ extensionauth.Server = (*authenticator)(nil)
)

type cacheEntry struct {
	// key holds the PBKDF2 hashed value of the API Key.
	// This is used to ensure that the key is not stored in
	// plaintext while protecting against API Key collisions.
	key []byte

	err  error
	data *authData
}

type authData struct {
	username string
	apiKeyID string
}

func (a *authData) GetAttribute(name string) any {
	switch name {
	case "api_key":
		return a.apiKeyID
	case "username":
		return a.username
	}
	return nil
}

func (a *authData) GetAttributeNames() []string {
	return []string{"username", "api_key"}
}

type authenticator struct {
	config            *Config
	telemetrySettings component.TelemetrySettings

	esClient *elasticsearch.TypedClient
	cache    freelru.Cache[string, *cacheEntry]
	salt     [16]byte // used for deriving keys from API Keys
}

func newAuthenticator(cfg *Config, set component.TelemetrySettings) (*authenticator, error) {
	cache, err := freelru.NewSharded[string, *cacheEntry](cfg.Cache.Capacity, func(key string) uint32 {
		h := fnv.New32a()
		h.Write([]byte(key))
		return h.Sum32()
	})
	if err != nil {
		return nil, err
	}
	cache.SetLifetime(cfg.Cache.TTL)

	authenticator := &authenticator{
		config:            cfg,
		telemetrySettings: set,
		cache:             cache,
	}
	if _, err := rand.Read(authenticator.salt[:]); err != nil {
		return nil, err
	}
	return authenticator, nil
}

func (a *authenticator) Start(ctx context.Context, host component.Host) error {
	httpClient, err := a.config.ToClient(ctx, host, a.telemetrySettings)
	if err != nil {
		return err
	}
	esClient, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: []string{a.config.Endpoint},
		Transport: httpClient.Transport,
		Instrumentation: elasticsearch.NewOpenTelemetryInstrumentation(
			a.telemetrySettings.TracerProvider, false,
		),
	})
	if err != nil {
		return err
	}
	a.esClient = esClient
	return nil
}

func (a *authenticator) Shutdown(ctx context.Context) error {
	a.cache.Purge()
	return nil
}

// parseAuthorizationHeader checks that the Authorization header follows the expected format,
// and returns the full header value, and the API Key ID.
func (a *authenticator) parseAuthorizationHeader(headers map[string][]string) (string, string, error) {
	orig, ok := getHeader(headers, authorizationHeader, lowerAuthorizationHeader)
	if !ok {
		return "", "", fmt.Errorf("missing header %q", authorizationHeader)
	}

	// The expected format of the Authorization header is:
	//
	//     Authorization: ApiKey base64(ID:APIKey)
	//
	encoded, found := strings.CutPrefix(orig, "ApiKey ")
	if !found {
		return "", "", errAuthorizationHeaderWrongScheme
	}

	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", "", err
	}
	id, _, ok := strings.Cut(string(decoded), ":")
	if !ok {
		return "", "", errAuthorizationHeaderInvalid
	}

	return orig, id, nil
}

func getHeader(headers map[string][]string, titlecase, lowercase string) (string, bool) {
	values, ok := headers[titlecase]
	if !ok {
		values, ok = headers[lowercase]
		if !ok {
			for k, v := range headers {
				if strings.EqualFold(k, lowercase) {
					values = v
					break
				}
			}
		}
	}
	if len(values) != 0 {
		return values[0], true
	}
	return "", false
}

// hasPrivileges checks if the API Key is valid and has the required privileges.
func (a *authenticator) hasPrivileges(ctx context.Context, authHeaderValue string) (bool, string, error) {
	applications := make([]types.ApplicationPrivilegesCheck, len(a.config.ApplicationPrivileges))
	for i, app := range a.config.ApplicationPrivileges {
		applications[i] = types.ApplicationPrivilegesCheck{
			Application: app.Application,
			Privileges:  app.Privileges,
			Resources:   app.Resources,
		}
	}
	req := a.esClient.Security.HasPrivileges()
	req.Header(authorizationHeader, authHeaderValue)
	req.Request(&hasprivileges.Request{Application: applications})
	resp, err := req.Do(ctx)
	if err != nil {
		return false, "", err
	}
	return resp.HasAllRequested, resp.Username, nil
}

// getCacheKey computes a cache key for the given API Key ID and headers.
func (a *authenticator) getCacheKey(id string, headers map[string][]string) (string, error) {
	key := id
	for _, header := range a.config.Cache.KeyHeaders {
		value, ok := getHeader(headers, header, strings.ToLower(header))
		if !ok {
			return "", fmt.Errorf("error computing cache key: missing header %q", header)
		}
		key += " " + value
	}
	return key, nil
}

// Authenticate validates an ApiKey scheme Authorization header,
// passing it to Elasticsearch for checking privileges.
func (a *authenticator) Authenticate(ctx context.Context, headers map[string][]string) (context.Context, error) {
	authHeaderValue, id, err := a.parseAuthorizationHeader(headers)
	if err != nil {
		return ctx, err
	}

	cacheKey, err := a.getCacheKey(id, headers)
	if err != nil {
		return ctx, err
	}

	derivedKey := pbkdf2.Key(
		[]byte(authHeaderValue),
		a.salt[:],
		a.config.Cache.PBKDF2Iterations,
		32, // key length
		sha512.New,
	)
	if cacheEntry, ok := a.cache.Get(cacheKey); ok {
		if subtle.ConstantTimeCompare(cacheEntry.key, derivedKey) == 0 {
			// Client has specified an API Key with a colliding ID,
			// but whose secret component does not match the one in
			// the cache.
			return ctx, fmt.Errorf("API Key %q unauthorized", id)
		}
		if cacheEntry.err != nil {
			return ctx, cacheEntry.err
		}
		clientInfo := client.FromContext(ctx)
		clientInfo.Auth = cacheEntry.data
		return client.NewContext(ctx, clientInfo), nil
	}

	hasPrivileges, username, err := a.hasPrivileges(ctx, authHeaderValue)
	if err != nil {
		return ctx, err
	}
	if !hasPrivileges {
		cacheEntry := &cacheEntry{
			key: derivedKey,
			err: fmt.Errorf("API Key %q unauthorized", id),
		}
		a.cache.Add(cacheKey, cacheEntry)
		return ctx, cacheEntry.err
	}
	cacheEntry := &cacheEntry{
		key: derivedKey,
		data: &authData{
			username: username,
			apiKeyID: id,
		},
	}
	a.cache.Add(cacheKey, cacheEntry)
	clientInfo := client.FromContext(ctx)
	clientInfo.Auth = cacheEntry.data
	return client.NewContext(ctx, clientInfo), nil
}
