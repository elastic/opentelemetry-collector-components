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
	"github.com/ua-parser/uap-go/uaparser"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/internal/elastic"
)

// Enricher enriches the OTel traces with attributes required to power
// functionalities in the Elastic UI.
type Enricher struct {
	// If there are more than one parser in the future we should consider
	// abstracting the parsers in a separate internal package.
	userAgentParser *uaparser.Parser

	Config config.Config
}

// EnrichTraces enriches the OTel traces with attributes required to power
// functionalities in the Elastic UI. The traces are processed as per the
// Elastic's definition of transactions and spans. The traces passed to
// this function are mutated.
// Any existing attributes will not be enriched or modified.
func (e *Enricher) EnrichTraces(pt ptrace.Traces) {
	resSpans := pt.ResourceSpans()
	for i := 0; i < resSpans.Len(); i++ {
		resSpan := resSpans.At(i)
		elastic.EnrichResource(resSpan.Resource(), e.Config.Resource)
		scopeSpans := resSpan.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			scopeSpan := scopeSpans.At(j)
			elastic.EnrichScope(scopeSpan.Scope(), e.Config)
			spans := scopeSpan.Spans()
			for k := 0; k < spans.Len(); k++ {
				elastic.EnrichSpan(spans.At(k), e.Config, e.userAgentParser)
			}
		}
	}
}

// EnrichLogs enriches the OTel logs with attributes required to power
// functionalities in the Elastic UI. The logs passed to this function are mutated.
// Any existing attributes will not be enriched or modified.
func (e *Enricher) EnrichLogs(pl plog.Logs) {
	resLogs := pl.ResourceLogs()
	for i := 0; i < resLogs.Len(); i++ {
		resLog := resLogs.At(i)
		resource := resLog.Resource()
		elastic.EnrichResource(resource, e.Config.Resource)
		resourceAttrs := resource.Attributes().AsRaw()
		scopeLogs := resLog.ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			scopeSpan := scopeLogs.At(j)
			elastic.EnrichScope(scopeSpan.Scope(), e.Config)
			logRecords := scopeSpan.LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				elastic.EnrichLog(resourceAttrs, logRecords.At(k), e.Config)
			}
		}
	}
}

// EnrichMetrics enriches the OTel metrics with attributes required to power
// functionalities in the Elastic UI. The metrics passed to this function are mutated.
// Any existing attributes will not be enriched or modified.
func (e *Enricher) EnrichMetrics(pl pmetric.Metrics) {
	resMetrics := pl.ResourceMetrics()
	for i := 0; i < resMetrics.Len(); i++ {
		resMetric := resMetrics.At(i)
		elastic.EnrichMetric(resMetric, e.Config)
		elastic.EnrichResource(resMetric.Resource(), e.Config.Resource)
		scopeMetics := resMetric.ScopeMetrics()
		for j := 0; j < scopeMetics.Len(); j++ {
			scopeMetric := scopeMetics.At(j)
			elastic.EnrichScope(scopeMetric.Scope(), e.Config)
		}
	}
}

// NewEnricher creates a new instance of Enricher.
func NewEnricher(cfg config.Config) *Enricher {
	return &Enricher{
		Config:          cfg,
		userAgentParser: uaparser.NewFromSaved(),
	}
}
