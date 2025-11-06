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

package apikeyauthextension

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/security/hasprivileges"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const user = "test"

var successfulResponse = hasprivileges.Response{
	Application: map[string]types.ResourcePrivileges{
		"my_app": map[string]types.Privileges{
			"the_resource": {
				"write": true,
			},
		},
	},
	Username:        user,
	HasAllRequested: true,
}

func TestAuthenticator(t *testing.T) {
	for name, testcase := range map[string]struct {
		handler http.HandlerFunc
		header  map[string][]string

		expectedUsername string
		expectedAPIKeyID string
		expectedErr      string
	}{
		"success": {
			handler:          newCannedHasPrivilegesHandler(successfulResponse),
			expectedUsername: "test",
			expectedAPIKeyID: "id",
		},
		"error": {
			handler: newCannedErrorHandler(types.ElasticsearchError{
				ErrorCause: types.ErrorCause{
					Type: "a_type",
					Reason: func() *string {
						reason := "a_reason"
						return &reason
					}(),
				},
				Status: 400,
			}),
			expectedErr: `rpc error: code = Internal desc = error checking privileges for API Key "id": status: 400, failed: [a_type], reason: a_reason`,
		},
		"auth_error": {
			handler: newCannedErrorHandler(types.ElasticsearchError{
				ErrorCause: types.ErrorCause{
					Type: "auth_reason",
					Reason: func() *string {
						reason := "auth_reason"
						return &reason
					}(),
				},
				Status: 401,
			}),
			expectedErr: `rpc error: code = Unauthenticated desc = status: 401, failed: [auth_reason], reason: auth_reason`,
		},
		"proxy_502_error": {
			handler: func(w http.ResponseWriter, r *http.Request) {
				// Simulate proxy returning 502 when ES is unreachable - empty response body
				w.WriteHeader(http.StatusBadGateway)
			},
			expectedErr: `rpc error: code = Unavailable desc = retryable server error for API Key "id": EOF`,
		},
		"missing_privileges": {
			handler:     newCannedHasPrivilegesHandler(hasprivileges.Response{HasAllRequested: false}),
			expectedErr: `rpc error: code = PermissionDenied desc = API Key "id" unauthorized`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			srv := newMockElasticsearch(t, testcase.handler)
			authenticator := newTestAuthenticator(t, srv, createDefaultConfig().(*Config))

			ctx, err := authenticator.Authenticate(context.Background(), map[string][]string{
				"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id:secret"))},
			})
			if testcase.expectedErr != "" {
				assert.EqualError(t, err, testcase.expectedErr)
				return
			}
			assert.NoError(t, err)

			clientInfo := client.FromContext(ctx)
			require.NotNil(t, clientInfo.Auth)
			assert.Equal(t, []string{"username", "api_key"}, clientInfo.Auth.GetAttributeNames())
			assert.Equal(t, testcase.expectedUsername, clientInfo.Auth.GetAttribute("username"))
			assert.Equal(t, testcase.expectedAPIKeyID, clientInfo.Auth.GetAttribute("api_key"))
		})
	}
}

func TestAuthenticator_ApplicationPrivileges(t *testing.T) {
	srv := newMockElasticsearch(t, func(w http.ResponseWriter, r *http.Request) {
		var body hasprivileges.Request
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, hasprivileges.Request{
			Application: []types.ApplicationPrivilegesCheck{{
				Application: "my_app",
				Resources:   []string{"the_resource"},
				Privileges:  []string{"read", "write"},
			}},
		}, body)
		assert.NoError(t, json.NewEncoder(w).Encode(successfulResponse))
	})

	config := createDefaultConfig().(*Config)
	config.ApplicationPrivileges = []ApplicationPrivilegesConfig{{
		Application: "my_app",
		Resources:   []string{"the_resource"},
		Privileges:  []string{"read", "write"},
	}}
	authenticator := newTestAuthenticator(t, srv, config)
	_, err := authenticator.Authenticate(context.Background(), map[string][]string{
		"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id:secret"))},
	})
	assert.NoError(t, err)
}

func TestAuthenticator_Caching(t *testing.T) {
	var calls int
	srv := newMockElasticsearch(t, func(w http.ResponseWriter, r *http.Request) {
		h := newCannedHasPrivilegesHandler(successfulResponse)
		h.ServeHTTP(w, r)
		calls++
	})
	authenticator := newTestAuthenticator(t, srv, createDefaultConfig().(*Config))

	ctx, err := authenticator.Authenticate(context.Background(), map[string][]string{
		"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id1:secret1"))},
	})
	require.NoError(t, err)
	clientInfo := client.FromContext(ctx)
	assert.Equal(t, user, clientInfo.Auth.GetAttribute("username"))
	assert.Equal(t, "id1", clientInfo.Auth.GetAttribute("api_key"))
	assert.Equal(t, 1, calls)

	ctx, err = authenticator.Authenticate(context.Background(), map[string][]string{
		"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id1:secret1"))},
	})
	require.NoError(t, err)
	clientInfo = client.FromContext(ctx)
	assert.Equal(t, user, clientInfo.Auth.GetAttribute("username"))
	assert.Equal(t, "id1", clientInfo.Auth.GetAttribute("api_key"))
	assert.Equal(t, 1, calls) // cache hit, no additional calls

	ctx, err = authenticator.Authenticate(context.Background(), map[string][]string{
		"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id2:secret1"))},
	})
	require.NoError(t, err)
	clientInfo = client.FromContext(ctx)
	assert.Equal(t, user, clientInfo.Auth.GetAttribute("username"))
	assert.Equal(t, "id2", clientInfo.Auth.GetAttribute("api_key"))
	assert.Equal(t, 2, calls) // cache miss

	// Cache hit on ID, different secret
	_, err = authenticator.Authenticate(context.Background(), map[string][]string{
		"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id2:secret2"))},
	})
	assert.EqualError(t, err, `rpc error: code = Unauthenticated desc = API Key "id2" unauthorized`)
}

func TestAuthenticator_UserAgent(t *testing.T) {
	srv := newMockElasticsearch(t, func(w http.ResponseWriter, r *http.Request) {
		// Verify that the User-Agent header is set to "foobar"
		assert.Equal(t, "foobar", r.Header.Get("User-Agent"))
		assert.NoError(t, json.NewEncoder(w).Encode(successfulResponse))
	})

	authenticator := newTestAuthenticator(t, srv, createDefaultConfig().(*Config))
	_, err := authenticator.Authenticate(context.Background(), map[string][]string{
		"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id:secret"))},
	})
	assert.NoError(t, err)
}

func TestAuthenticator_ErrorWithDetails(t *testing.T) {
	for name, testcase := range map[string]struct {
		setup            func(*testing.T) (*authenticator, map[string][]string)
		expectedCode     codes.Code
		expectedMsg      string
		expectedMetadata map[string]string
	}{
		"invalid_header": {
			setup: func(t *testing.T) (*authenticator, map[string][]string) {
				srv := newMockElasticsearch(t, newCannedHasPrivilegesHandler(successfulResponse))
				authenticator := newTestAuthenticator(t, srv, createDefaultConfig().(*Config))
				headers := map[string][]string{
					"Authorization": {"Bearer invalid"},
				}
				return authenticator, headers
			},
			expectedCode: codes.Unauthenticated,
			expectedMsg:  "ApiKey prefix not found, expected ApiKey <value>",
			expectedMetadata: map[string]string{
				"component": "apikeyauthextension",
			},
		},
		"api_key_collision": {
			setup: func(t *testing.T) (*authenticator, map[string][]string) {
				srv := newMockElasticsearch(t, newCannedHasPrivilegesHandler(successfulResponse))
				authenticator := newTestAuthenticator(t, srv, createDefaultConfig().(*Config))
				_, err := authenticator.Authenticate(context.Background(), map[string][]string{
					"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("collision_id:secret1"))},
				})
				require.NoError(t, err)
				// cause collision case
				headers := map[string][]string{
					"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("collision_id:secret2"))},
				}
				return authenticator, headers
			},
			expectedCode: codes.Unauthenticated,
			expectedMsg:  `API Key "collision_id" unauthorized`,
			expectedMetadata: map[string]string{
				"component": "apikeyauthextension",
				"api_key":   "collision_id",
			},
		},
		"missing_privileges": {
			setup: func(t *testing.T) (*authenticator, map[string][]string) {
				srv := newMockElasticsearch(t, newCannedHasPrivilegesHandler(hasprivileges.Response{
					Username:        user,
					HasAllRequested: false,
				}))
				authenticator := newTestAuthenticator(t, srv, createDefaultConfig().(*Config))
				headers := map[string][]string{
					"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("no_privs_id:secret"))},
				}
				return authenticator, headers
			},
			expectedCode: codes.PermissionDenied,
			expectedMsg:  `API Key "no_privs_id" unauthorized`,
			expectedMetadata: map[string]string{
				"component": "apikeyauthextension",
				"api_key":   "no_privs_id",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			authenticator, headers := testcase.setup(t)
			_, err := authenticator.Authenticate(context.Background(), headers)
			require.Error(t, err)

			st, ok := status.FromError(err)
			require.True(t, ok, "Expected gRPC status error")
			assert.Equal(t, testcase.expectedCode, st.Code())
			assert.Equal(t, testcase.expectedMsg, st.Message())

			details := st.Details()
			require.Len(t, details, 1, "expected 1 errorinfo detail")

			errorInfo, ok := details[0].(*errdetails.ErrorInfo)
			require.True(t, ok, "expected errorinfo detail")
			assert.Equal(t, "ingest.elastic.co", errorInfo.Domain)
			assert.Equal(t, testcase.expectedMetadata, errorInfo.Metadata)
		})
	}
}

func TestAuthenticator_CacheKeyHeaders(t *testing.T) {
	var calls int
	srv := newMockElasticsearch(t, func(w http.ResponseWriter, r *http.Request) {
		h := newCannedHasPrivilegesHandler(successfulResponse)
		h.ServeHTTP(w, r)
		calls++
	})
	config := createDefaultConfig().(*Config)
	config.Cache.KeyHeaders = []string{"X-Tenant-Id"}
	authenticator := newTestAuthenticator(t, srv, config)

	// Missing X-Tenant-Id header should result in an error.
	_, err := authenticator.Authenticate(context.Background(), map[string][]string{
		"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id1:secret1"))},
	})
	require.EqualError(t, err, `error computing cache key: missing header "X-Tenant-Id"`)

	ctx, err := authenticator.Authenticate(context.Background(), map[string][]string{
		"X-Tenant-Id":   {"tenant1"},
		"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id1:secret1"))},
	})
	require.NoError(t, err)
	clientInfo := client.FromContext(ctx)
	assert.Equal(t, user, clientInfo.Auth.GetAttribute("username"))
	assert.Equal(t, "id1", clientInfo.Auth.GetAttribute("api_key"))
	assert.Equal(t, 1, calls)

	// Different x-tenant-id header value should result in a cache miss,
	// despite the API Key ID being the same.
	ctx, err = authenticator.Authenticate(context.Background(), map[string][]string{
		"x-tenant-id":   {"tenant2"},
		"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id1:secret2"))},
	})
	require.NoError(t, err)
	clientInfo = client.FromContext(ctx)
	assert.Equal(t, user, clientInfo.Auth.GetAttribute("username"))
	assert.Equal(t, "id1", clientInfo.Auth.GetAttribute("api_key"))
	assert.Equal(t, 2, calls)
}

func TestAuthenticator_CacheKeyMetadata(t *testing.T) {
	var calls int
	srv := newMockElasticsearch(t, func(w http.ResponseWriter, r *http.Request) {
		h := newCannedHasPrivilegesHandler(successfulResponse)
		h.ServeHTTP(w, r)
		calls++
	})
	config := createDefaultConfig().(*Config)
	config.Cache.KeyMetadata = []string{"X-Tenant-Id"}
	authenticator := newTestAuthenticator(t, srv, config)

	// Missing X-Tenant-Id header should result in an error.
	_, err := authenticator.Authenticate(context.Background(), map[string][]string{
		"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id1:secret1"))},
	})
	require.EqualError(t, err, `error computing cache key: missing client metadata "X-Tenant-Id"`)

	withMetadata := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"X-Tenant-Id": {"tenant1"},
		}),
	})
	ctx, err := authenticator.Authenticate(withMetadata, map[string][]string{
		"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id1:secret1"))},
	})
	require.NoError(t, err)
	clientInfo := client.FromContext(ctx)
	assert.Equal(t, user, clientInfo.Auth.GetAttribute("username"))
	assert.Equal(t, "id1", clientInfo.Auth.GetAttribute("api_key"))
	assert.Equal(t, 1, calls)

	// Different x-tenant-id header value should result in a cache miss,
	// despite the API Key ID being the same.
	withMetadata2 := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"X-Tenant-Id": {"tenant2"},
		}),
	})
	ctx, err = authenticator.Authenticate(withMetadata2, map[string][]string{
		"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id1:secret2"))},
	})
	require.NoError(t, err)
	clientInfo = client.FromContext(ctx)
	assert.Equal(t, user, clientInfo.Auth.GetAttribute("username"))
	assert.Equal(t, "id1", clientInfo.Auth.GetAttribute("api_key"))
	assert.Equal(t, 2, calls)
}

func TestAuthenticator_CacheTTL(t *testing.T) {
	var calls int
	srv := newMockElasticsearch(t, func(w http.ResponseWriter, r *http.Request) {
		h := newCannedHasPrivilegesHandler(successfulResponse)
		h.ServeHTTP(w, r)
		calls++
	})
	config := createDefaultConfig().(*Config)
	config.Cache.TTL = time.Nanosecond
	authenticator := newTestAuthenticator(t, srv, config)

	for i := 0; i < 10; i++ {
		ctx, err := authenticator.Authenticate(context.Background(), map[string][]string{
			"Authorization": {"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id1:secret1"))},
		})
		require.NoError(t, err)
		clientInfo := client.FromContext(ctx)
		assert.Equal(t, user, clientInfo.Auth.GetAttribute("username"))
		assert.Equal(t, "id1", clientInfo.Auth.GetAttribute("api_key"))
		assert.Equal(t, i+1, calls) // every attempt should be a cache miss due to the short TTL
		time.Sleep(time.Millisecond)
	}
}

func TestAuthenticator_AuthorizationHeader(t *testing.T) {
	srv := newMockElasticsearch(t, newCannedHasPrivilegesHandler(successfulResponse))
	for name, testcase := range map[string]struct {
		headers     map[string][]string
		expectedErr string
	}{
		"uppercase_header": {
			headers: map[string][]string{
				"Authorization": {
					"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id:secret")),
				},
			},
		},
		"lowercase_header": {
			headers: map[string][]string{
				"authorization": {
					"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id:secret")),
				},
			},
		},
		"mixedcase_header": {
			headers: map[string][]string{
				"aUthOrizAtiOn": {
					"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id:secret")),
				},
			},
		},
		"missing_header": {
			headers:     map[string][]string{},
			expectedErr: `rpc error: code = Unauthenticated desc = missing header "Authorization", expected "ApiKey <value>"`,
		},
		"invalid_scheme": {
			headers: map[string][]string{
				"Authorization": {
					"Bearer " + base64.StdEncoding.EncodeToString([]byte("id:secret")),
				},
			},
			expectedErr: `rpc error: code = Unauthenticated desc = ApiKey prefix not found, expected ApiKey <value>`,
		},
		"invalid_base64": {
			headers: map[string][]string{
				"Authorization": {"ApiKey not_base64"},
			},
			expectedErr: "rpc error: code = Unauthenticated desc = illegal base64 data at input byte 3",
		},
		"invalid_encoded_apikey": {
			headers: map[string][]string{
				"Authorization": {
					"ApiKey " + base64.StdEncoding.EncodeToString([]byte("junk")),
				},
			},
			expectedErr: "rpc error: code = Unauthenticated desc = invalid API Key",
		},
	} {
		t.Run(name, func(t *testing.T) {
			authenticator := newTestAuthenticator(t, srv, createDefaultConfig().(*Config))
			_, err := authenticator.Authenticate(context.Background(), testcase.headers)
			if testcase.expectedErr != "" {
				assert.EqualError(t, err, testcase.expectedErr)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func BenchmarkAuthenticator(b *testing.B) {
	for _, iters := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("iters_%d", iters), func(b *testing.B) {
			srv := newMockElasticsearch(b, newCannedHasPrivilegesHandler(successfulResponse))
			config := createDefaultConfig().(*Config)
			config.Cache.PBKDF2Iterations = iters
			authenticator := newTestAuthenticator(b, srv, config)

			ctx := context.Background()
			headers := map[string][]string{
				"Authorization": {
					"ApiKey " + base64.StdEncoding.EncodeToString([]byte("id:secret")),
				},
			}
			for range b.N {
				if _, err := authenticator.Authenticate(ctx, headers); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func newTestAuthenticator(t testing.TB, srv *httptest.Server, config *Config) *authenticator {
	config.Endpoint = srv.URL
	auth, err := newAuthenticator(config, componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)

	err = auth.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, auth.Shutdown(context.Background()))
	})
	return auth
}

func newMockElasticsearch(t testing.TB, handleHasPrivileges http.HandlerFunc) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/_security/user/_has_privileges", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		handleHasPrivileges.ServeHTTP(w, r)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func newCannedHasPrivilegesHandler(response hasprivileges.Response) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewEncoder(w).Encode(response); err != nil {
			panic(err)
		}
	})
}

func newCannedErrorHandler(response types.ElasticsearchError) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(response.Status)
		if err := json.NewEncoder(w).Encode(response); err != nil {
			panic(err)
		}
	})
}
