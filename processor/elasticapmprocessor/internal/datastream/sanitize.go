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

package datastream // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/datastream"

import (
	"strings"
	"unicode"
)

const (
	MaxDataStreamBytes = 100
)

func SanitizeDataset(field string) string {
	return sanitize(field, replaceDisallowedDatasetRune)
}

func SanitizeNamespace(field string) string {
	return sanitize(field, replaceDisallowedNamespaceRune)
}

func sanitize(field string, replaceFn func(r rune) rune) string {
	field = strings.Map(replaceFn, field)
	if len(field) > MaxDataStreamBytes {
		return field[:MaxDataStreamBytes]
	}
	return field
}

func replaceDisallowedDatasetRune(r rune) rune {
	switch r {
	case '-', '\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',', '#', ':':
		return '_'
	}
	return unicode.ToLower(r)
}

func replaceDisallowedNamespaceRune(r rune) rune {
	switch r {
	case '\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',', '#', ':':
		return '_'
	}
	return unicode.ToLower(r)
}
