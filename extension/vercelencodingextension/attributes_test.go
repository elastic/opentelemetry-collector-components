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

package vercelencodingextension

import (
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestPutURLAttrs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		origin string
		path   string
		query  string
		want   map[string]any
	}{
		{
			name:   "absolute url reconstructs full with path and query",
			origin: "https://example.com",
			path:   "/dashboard",
			query:  "utm_source=newsletter",
			want: map[string]any{
				"url.scheme": "https",
				"url.domain": "example.com",
				"url.path":   "/dashboard",
				"url.query":  "utm_source=newsletter",
				"url.full":   "https://example.com/dashboard?utm_source=newsletter",
			},
		},
		{
			name:   "absolute url without path or query",
			origin: "https://example.com",
			path:   "",
			query:  "",
			want: map[string]any{
				"url.scheme": "https",
				"url.domain": "example.com",
				"url.full":   "https://example.com",
			},
		},
		{
			name:   "schemeless origin falls back to raw url.full",
			origin: "example.com",
			path:   "/foo",
			query:  "",
			want: map[string]any{
				"url.path": "/foo",
				"url.full": "example.com",
			},
		},
		{
			name:   "empty origin drops url.full",
			origin: "",
			path:   "/foo",
			query:  "",
			want: map[string]any{
				"url.path": "/foo",
			},
		},
		{
			name:   "path and query override those already on the origin",
			origin: "https://example.com/old?x=1",
			path:   "/new",
			query:  "y=2",
			want: map[string]any{
				"url.scheme": "https",
				"url.domain": "example.com",
				"url.path":   "/new",
				"url.query":  "y=2",
				"url.full":   "https://example.com/new?y=2",
			},
		},
		{
			name:   "already-escaped path is not double-encoded in url.full",
			origin: "https://example.com",
			path:   "/products/foo%2Fbar",
			query:  "",
			want: map[string]any{
				"url.scheme": "https",
				"url.domain": "example.com",
				"url.path":   "/products/foo%2Fbar",
				"url.full":   "https://example.com/products/foo%2Fbar",
			},
		},
		{
			name:   "malformed percent-encoding falls back to raw path",
			origin: "https://example.com",
			path:   "/foo%zz",
			query:  "",
			want: map[string]any{
				"url.scheme": "https",
				"url.domain": "example.com",
				"url.path":   "/foo%zz",
				"url.full":   "https://example.com/foo%zz",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			attrs := pcommon.NewMap()
			putURLAttrs(attrs, tc.origin, tc.path, tc.query)
			require.Equal(t, tc.want, attrs.AsRaw())
		})
	}
}

func TestPutURLPathAttrs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		path string
		want map[string]any
	}{
		{
			name: "path with query splits into semconv attributes",
			path: "/dashboard?tab=metrics",
			want: map[string]any{
				"url.path":  "/dashboard",
				"url.query": "tab=metrics",
			},
		},
		{
			name: "path without query only emits path",
			path: "/dashboard",
			want: map[string]any{
				"url.path": "/dashboard",
			},
		},
		{
			name: "empty path emits nothing",
			path: "",
			want: map[string]any{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			attrs := pcommon.NewMap()
			putURLPathAttrs(attrs, tc.path)
			require.Equal(t, tc.want, attrs.AsRaw())
		})
	}
}

func TestPutSnakeCaseMap(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		src  map[string]any
		want map[string]any
	}{
		{
			name: "camelcase key becomes snake_case",
			src:  map[string]any{"drainUrl": "https://example.com"},
			want: map[string]any{"drain_url": "https://example.com"},
		},
		{
			name: "acronym run collapses to single word",
			src:  map[string]any{"drainURL": "https://example.com"},
			want: map[string]any{"drain_url": "https://example.com"},
		},
		{
			name: "acronym followed by word splits",
			src:  map[string]any{"HTTPStatusCode": float64(200)},
			want: map[string]any{"http_status_code": float64(200)},
		},
		{
			name: "digit boundary keeps word together",
			src:  map[string]any{"s3Bucket": "logs"},
			want: map[string]any{"s3_bucket": "logs"},
		},
		{
			name: "already snake_case is unchanged",
			src:  map[string]any{"already_snake_case": "value"},
			want: map[string]any{"already_snake_case": "value"},
		},
		{
			name: "nested map recurses into keys",
			src: map[string]any{
				"filterConfig": map[string]any{"maxRetryCount": float64(5)},
			},
			want: map[string]any{
				"filter_config": map[string]any{"max_retry_count": float64(5)},
			},
		},
		{
			name: "array of objects recurses into element keys",
			src: map[string]any{
				"headerList": []any{
					map[string]any{"headerName": "X-Signature"},
				},
			},
			want: map[string]any{
				"header_list": []any{
					map[string]any{"header_name": "X-Signature"},
				},
			},
		},
		{
			name: "scalar types are preserved",
			src: map[string]any{
				"isEnabled":    true,
				"samplingRate": 0.25,
				"missingValue": nil,
			},
			want: map[string]any{
				"is_enabled":    true,
				"sampling_rate": 0.25,
				"missing_value": nil,
			},
		},
		{
			name: "json.Number decodes to int and float",
			src: map[string]any{
				"retryCount": json.Number("5"),
				"errorRate":  json.Number("2.5"),
			},
			want: map[string]any{
				"retry_count": int64(5),
				"error_rate":  2.5,
			},
		},
		{
			name: "unhandled type falls back to formatted string",
			src:  map[string]any{"rawCount": 42},
			want: map[string]any{"raw_count": "42"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			attrs := pcommon.NewMap()
			putSnakeCaseMap(attrs, tc.src)
			require.Equal(t, tc.want, attrs.AsRaw())
		})
	}
}
