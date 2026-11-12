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

package compressedotlpencodingextension

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMagic(t *testing.T) {
	for p := protocolMin; p < protocolMax; p++ {
		for c := compressionMin; c < compressionMax; c++ {
			t.Run(fmt.Sprintf("%s-%s", p.String(), c.String()), func(t *testing.T) {
				m := Magic{protocol: p, compression: c}
				magicBytes := m.ToBytes()
				b := append(magicBytes, []byte("foobar")...)
				parsed, payload, err := parseMagic(b)
				require.NoError(t, err)
				assert.Equal(t, m, parsed)
				assert.Equal(t, string(payload), "foobar")
			})
		}
	}
}
