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

package lsmintervalprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor"

import (
	"encoding/binary"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger"
)

// metadataHash returns the hash of the client metadata of an aggregation
// key and true, or false if there is none.
//
// The value is part of the identity of the exported streams, so the
// algorithm and the encoding must not change. Keys are lower-cased, and
// keys and values are length-prefixed so that no two different combinations
// share an encoding. The entries are hashed in the given order, which the
// processor keeps sorted by key.
func metadataHash(md []merger.KeyValues) (uint64, bool) {
	if len(md) == 0 {
		return 0, false
	}
	var (
		d       xxhash.Digest
		scratch [binary.MaxVarintLen64]byte
	)
	d.Reset()
	for _, kvs := range md {
		k := strings.ToLower(kvs.Key)
		_, _ = d.Write(binary.AppendUvarint(scratch[:0], uint64(len(k))))
		_, _ = d.WriteString(k)
		_, _ = d.Write(binary.AppendUvarint(scratch[:0], uint64(len(kvs.Values))))
		for _, v := range kvs.Values {
			_, _ = d.Write(binary.AppendUvarint(scratch[:0], uint64(len(v))))
			_, _ = d.WriteString(v)
		}
	}
	return d.Sum64(), true
}
