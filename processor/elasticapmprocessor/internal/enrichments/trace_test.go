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

package enrichments

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func BenchmarkEnrich(b *testing.B) {
	traceFile := filepath.Join("testdata", "trace.yaml")
	traces, err := golden.ReadTraces(traceFile)
	require.NoError(b, err)
	enricher := NewEnricher(config.Config{})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enricher.EnrichTraces(traces)
	}
}

// BenchmarkBaseline benchmarks the baseline of adding a given number of
// attributes a specific trace data. It can be used as an baseline to
// reason about the performance of BenchmarkEnrich.
func BenchmarkBaseline(b *testing.B) {
	attrsToAdd := 3
	var attrKeys []string
	for i := 0; i < attrsToAdd; i++ {
		attrKeys = append(attrKeys, fmt.Sprintf("teststr%d", i))
	}

	traceFile := filepath.Join("testdata", "trace.yaml")
	traces, err := golden.ReadTraces(traceFile)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rspans := traces.ResourceSpans()
		for j := 0; j < rspans.Len(); j++ {
			rspan := rspans.At(j)
			sspan := rspan.ScopeSpans()
			for k := 0; k < sspan.Len(); k++ {
				scope := sspan.At(k)
				spans := scope.Spans()
				for l := 0; l < spans.Len(); l++ {
					span := spans.At(l)
					span.Attributes().Range(func(k string, v pcommon.Value) bool {
						// no-op range
						return true
					})
					span.Attributes().EnsureCapacity(attrsToAdd)
					for _, key := range attrKeys {
						span.Attributes().PutStr(key, "random")
					}
				}
			}
		}
	}
}
