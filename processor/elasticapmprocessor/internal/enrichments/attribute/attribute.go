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

package attribute // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/attribute"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// PutStr wrapper around the attribute map `PutStr` method
// that only inserts the entry if no key-value exists.
func PutStr(attrs pcommon.Map, key string, value string) {
	if _, ok := attrs.Get(key); !ok {
		attrs.PutStr(key, value)
	}
}

// PutInt wrapper around the attribute map `PutInt` method
// that only inserts the entry if no key-value exists
func PutInt(attrs pcommon.Map, key string, value int64) {
	if _, ok := attrs.Get(key); !ok {
		attrs.PutInt(key, value)
	}
}

// PutDouble wrapper around the attribute map `PutDouble` method
// that only inserts the entry if no key-value exists
func PutDouble(attrs pcommon.Map, key string, value float64) {
	if _, ok := attrs.Get(key); !ok {
		attrs.PutDouble(key, value)
	}
}

// PutBool wrapper around the attribute map `PutBool` method
// that only inserts the entry if no key-value exists
func PutBool(attrs pcommon.Map, key string, value bool) {
	if _, ok := attrs.Get(key); !ok {
		attrs.PutBool(key, value)
	}
}
