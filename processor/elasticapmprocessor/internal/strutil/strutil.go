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

package strutil // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/strutil"

// StandardKeyWordLength is the maximum number of runes for ECS keyword fields.
const StandardKeyWordLength = 1024

// Truncate returns s truncated to StandardKeyWordLength runes.
func Truncate(s string) string {
	return TruncateTo(s, StandardKeyWordLength)
}

// TruncateTo returns s truncated to n runes.
func TruncateTo(s string, n uint) string {
	var j uint
	for i := range s {
		if j == n {
			return s[:i]
		}
		j++
	}
	return s
}
