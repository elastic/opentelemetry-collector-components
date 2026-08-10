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
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseVercelTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   string
		want    time.Time
		wantErr bool
	}{
		{
			name:  "documented RFC3339 UTC",
			value: "2023-09-14T15:30:00.000Z",
			want:  time.Date(2023, time.September, 14, 15, 30, 0, 0, time.UTC),
		},
		{
			name:  "RFC3339 offset",
			value: "2023-09-14T17:30:00.123456789+02:00",
			want:  time.Date(2023, time.September, 14, 15, 30, 0, 123456789, time.UTC),
		},
		{
			name:  "ISO local date-time with T",
			value: "2026-07-29T11:07:47.884",
			want:  time.Date(2026, time.July, 29, 11, 7, 47, 884000000, time.UTC),
		},
		{
			name:  "ISO local date-time with space",
			value: "2026-07-29 11:07:47.884",
			want:  time.Date(2026, time.July, 29, 11, 7, 47, 884000000, time.UTC),
		},
		{
			name:  "ISO date-time with space and offset",
			value: "2023-09-14 17:30:00+02:00",
			want:  time.Date(2023, time.September, 14, 15, 30, 0, 0, time.UTC),
		},
		{
			name:    "date only",
			value:   "2026-07-29",
			wantErr: true,
		},
		{
			name:    "invalid",
			value:   "2026/07/29 11:07:47.884",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseVercelTimestamp(test.value)
			if test.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.True(t, test.want.Equal(got), "expected %s, got %s", test.want, got)
		})
	}
}
