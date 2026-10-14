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

package beatsencodingextension

import (
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectElements drains an iterator into the raw records it produced.
func collectElements(input string, unwrap []string) ([]string, error) {
	it := newJSONElements(strings.NewReader(input), unwrap)
	var out []string
	for {
		raw, err := it.next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, string(raw))
	}
}

func TestJSONElements(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		unwrap  []string
		want    []string // compared with JSONEq, so spacing does not matter
		wantErr string
	}{
		{
			name:  "no unwrap, one value",
			input: `{"a": 1}`,
			want:  []string{`{"a":1}`},
		},
		{
			name:  "no unwrap, concatenated values on lines",
			input: "{\"a\":1}\n{\"a\":2}\n{\"a\":3}",
			want:  []string{`{"a":1}`, `{"a":2}`, `{"a":3}`},
		},
		{
			name:  "no unwrap, trailing whitespace",
			input: "{\"a\":1}\n\n   \n",
			want:  []string{`{"a":1}`},
		},
		{
			name:   "unwrap, one value",
			input:  `{"Records": [{"a":1}, {"a":2}]}`,
			unwrap: []string{"Records"},
			want:   []string{`{"a":1}`, `{"a":2}`},
		},
		{
			name:   "unwrap, keys after the array",
			input:  `{"Records": [{"a":1}], "meta": {"x": [1, 2]}, "n": 5}`,
			unwrap: []string{"Records"},
			want:   []string{`{"a":1}`},
		},
		{
			name:   "unwrap, concatenated values",
			input:  "{\"Records\":[{\"a\":1}]}\n{\"Records\":[{\"a\":2},{\"a\":3}]}",
			unwrap: []string{"Records"},
			want:   []string{`{"a":1}`, `{"a":2}`, `{"a":3}`},
		},
		{
			name:   "unwrap, empty array then values",
			input:  `{"Records": []}{"Records": [{"a":1}]}`,
			unwrap: []string{"Records"},
			want:   []string{`{"a":1}`},
		},
		{
			name:   "nested unwrap, concatenated",
			input:  `{"data": {"items": [1], "count": 1}, "meta": 0}{"data": {"items": [2, 3]}}`,
			unwrap: []string{"data", "items"},
			want:   []string{`1`, `2`, `3`},
		},
		{
			name:   "unwrap, duplicate key, first one is used",
			input:  `{"Records": [1], "Records": [2]}`,
			unwrap: []string{"Records"},
			want:   []string{`1`},
		},
		{
			name:   "root array",
			input:  `[{"a":1}, {"a":2}]`,
			unwrap: []string{".[]"},
			want:   []string{`{"a":1}`, `{"a":2}`},
		},
		{
			name:   "root array, concatenated arrays",
			input:  "[1]\n[2, 3]",
			unwrap: []string{".[]"},
			want:   []string{`1`, `2`, `3`},
		},
		{
			name:    "root array, root is object",
			input:   `{"a": 1}`,
			unwrap:  []string{".[]"},
			wantErr: `unwrapping root array: expected '['`,
		},
		{
			name:    "unwrap key missing",
			input:   `{"other": 1}`,
			unwrap:  []string{"Records"},
			wantErr: `key "Records" not found in object`,
		},
		{
			name:    "unwrap, value is not an object",
			input:   `[1, 2]`,
			unwrap:  []string{"Records"},
			wantErr: `expected '{' before key "Records"`,
		},
		{
			name:  "empty input",
			input: ``,
		},
		{
			name:  "whitespace only",
			input: "  \n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := collectElements(tt.input, tt.unwrap)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, len(tt.want))
			for i := range tt.want {
				assert.JSONEq(t, tt.want[i], got[i])
			}
		})
	}
}

// TestJSONElements_IncompleteInput checks that only a clean end between
// top-level values ends the stream. A read error, a truncated value or a
// stray closing delimiter must not pass as io.EOF, or the rest of the
// object would be dropped without an error.
func TestJSONElements_IncompleteInput(t *testing.T) {
	readErr := errors.New("connection reset")

	tests := []struct {
		name    string
		reader  io.Reader
		unwrap  []string
		wantErr error  // checked with errors.Is when set
		wantMsg string // checked with Contains when set
	}{
		{
			name:    "read error between values",
			reader:  io.MultiReader(strings.NewReader("{\"a\":1}\n{\"a\":2}\n"), iotest.ErrReader(readErr)),
			wantErr: readErr,
		},
		{
			name:    "read error between unwrapped values",
			reader:  io.MultiReader(strings.NewReader(`{"Records":[{"a":1}]}`), iotest.ErrReader(readErr)),
			unwrap:  []string{"Records"},
			wantErr: readErr,
		},
		{
			name:    "truncated after the unwrap key",
			reader:  strings.NewReader(`{"x":1,"data":`),
			unwrap:  []string{"data", "items"},
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "truncated second value",
			reader:  strings.NewReader(`{"Records":[{"a":1}]}{"Records":`),
			unwrap:  []string{"Records"},
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "stray closing brace between values",
			reader:  strings.NewReader(`{"a":1}}{"a":2}`),
			wantMsg: "reading next value",
		},
		{
			name:    "stray closing bracket with root array",
			reader:  strings.NewReader(`]`),
			unwrap:  []string{".[]"},
			wantMsg: "reading next value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := newJSONElements(tt.reader, tt.unwrap)
			var err error
			for err == nil {
				_, err = it.next()
			}
			require.NotErrorIs(t, err, io.EOF, "got a clean end of input: %v", err)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			}
			if tt.wantMsg != "" {
				assert.Contains(t, err.Error(), tt.wantMsg)
			}
		})
	}
}
