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

package lsmintervalprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger"
)

func TestMetadataHash(t *testing.T) {
	hash := func(md ...merger.KeyValues) uint64 { return metadataHash(md) }
	kv := func(k string, vs ...string) merger.KeyValues {
		return merger.KeyValues{Key: k, Values: vs}
	}

	t.Run("golden", func(t *testing.T) {
		// The hash is part of the identity of exported streams: if these
		// values have to change then existing series are split on upgrade.
		assert.Equal(t, uint64(0xdc13440679d9d55c), hash(kv("kafka.partition", "3")))
		assert.Equal(t, uint64(0x7f5482ade9b5cfd2), hash(kv("kafka.partition", "3"), kv("x-elastic-target-id", "t")))
	})

	t.Run("distinct_combinations", func(t *testing.T) {
		seen := map[uint64]string{}
		for name, md := range map[string][]merger.KeyValues{
			"p1":          {kv("kafka.partition", "1")},
			"p1p2":        {kv("kafka.partition", "1", "2")},
			"p12":         {kv("kafka.partition", "12")},
			"p1,2":        {kv("kafka.partition", "1,2")},
			"p1_t":        {kv("kafka.partition", "1"), kv("kafka.topic", "t")},
			"t1_p":        {kv("kafka.partition", "t"), kv("kafka.topic", "1")},
			"empty_value": {kv("kafka.partition", "")},
			"no_value":    {kv("kafka.partition")},
		} {
			h := hash(md...)
			if other, dup := seen[h]; dup {
				t.Errorf("%s and %s share a hash", name, other)
			}
			seen[h] = name
		}
	})
}
