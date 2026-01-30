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

package mappers // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/mappers"

import (
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// putNonEmptyStr puts a string attribute in the given map
// only if the provided value is not empty.
func putNonEmptyStr(attributes pcommon.Map, key, value string) {
	if value != "" {
		attributes.PutStr(key, value)
	}
}

// putPtrInt puts an int attribute in the given map
// only if the provided value is not nil.
// Supports signed and unsigned integer pointer types.
// For uint64 values exceeding int64 max, the value is clamped
// to math.MaxInt64 to prevent overflow.
func putPtrInt[T ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~int8 | ~int16 | ~int32 | ~int64](attributes pcommon.Map, key string, value *T) {
	if value != nil {
		val := int64(*value)
		// Check for uint64 overflow: if the original unsigned value was > int64 max,
		// the cast will produce a negative number. Clamp to max int64.
		if val < 0 && *value > 0 {
			val = math.MaxInt64
		}
		attributes.PutInt(key, val)
	}
}

// putPtrBool puts a bool attribute in the given map
// only if the provided value is not nil.
func putPtrBool(attributes pcommon.Map, key string, value *bool) {
	if value != nil {
		attributes.PutBool(key, *value)
	}
}
