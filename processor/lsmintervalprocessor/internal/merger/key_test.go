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

package merger // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/lsmintervalprocessor/internal/merger"

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKey(t *testing.T) {
	for _, tc := range []struct {
		name           string
		ivl            time.Duration
		processingTime time.Time
	}{
		{
			name:           "zero",
			ivl:            0,
			processingTime: time.Unix(0, 0),
		},
		{
			name:           "non_zero",
			ivl:            time.Minute,
			processingTime: time.Unix(time.Now().Unix(), 0),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := NewKey(tc.ivl, tc.processingTime)
			assert.Equal(t, 10, key.SizeBinary())
			b, err := key.Marshal()
			assert.NoError(t, err)
			var newKey Key
			assert.NoError(t, newKey.Unmarshal(b))
			assert.Equal(t, newKey, key)
		})
	}
}

func TestKeyOrdered(t *testing.T) {
	// For querying purposes the key should be ordered and comparable
	ts := time.Unix(0, 0)
	ivl := time.Minute

	before := NewKey(ivl, ts)
	for i := 0; i < 10; i++ {
		beforeBytes, err := before.Marshal()
		require.NoError(t, err)

		ts = ts.Add(time.Minute)
		after := NewKey(ivl, ts)
		afterBytes, err := after.Marshal()
		require.NoError(t, err)

		// before should always come first
		assert.Equal(t, -1, pebble.DefaultComparer.Compare(beforeBytes, afterBytes))
		before = after
	}
}
