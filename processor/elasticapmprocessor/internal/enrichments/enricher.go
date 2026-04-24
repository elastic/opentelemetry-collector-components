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

package enrichments // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments"

import (
	"context"
	"sync"

	"github.com/ua-parser/uap-go/uaparser"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/ecs"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"
)

// sharedUserAgentParser compiles all ~500 UA/OS/Device regexes exactly once
// and shares the result across all Enricher instances. uaparser.NewFromSaved()
// recompiles all patterns on every call; using sync.OnceValue avoids redundant
// compilations when multiple processor instances are created (e.g. one per signal type).
var sharedUserAgentParser = sync.OnceValue(uaparser.NewFromSaved)

// Enricher enriches the OTel traces with attributes required to power
// functionalities in the Elastic UI.
type Enricher struct {
	// If there are more than one parser in the future we should consider
	// abstracting the parsers in a separate internal package.
	userAgentParser *uaparser.Parser

	Config config.Config

	// remapToECSLabels controls whether unsupported attributes are
	// translated to labels.*/numeric_labels.* for ECS compatibility.
	// This is set by the enricher type (true for OTel, false for APM/Default)
	// and is not user-configurable.
	remapToECSLabels bool
}

// EnrichResourceSpans enriches a single ResourceSpans with the current enricher
// configuration. This is used when ECS batches need per-resource enrichment
// policies based on the origin of each ResourceSpans.
func (e *Enricher) EnrichResourceSpans(resSpan ptrace.ResourceSpans) {
	EnrichResource(resSpan.Resource(), e.Config.Resource)
	scopeSpans := resSpan.ScopeSpans()
	for j := 0; j < scopeSpans.Len(); j++ {
		scopeSpan := scopeSpans.At(j)
		EnrichScope(scopeSpan.Scope(), e.Config)
		spans := scopeSpan.Spans()
		for k := 0; k < spans.Len(); k++ {
			EnrichSpan(spans.At(k), e.Config, e.userAgentParser, e.remapToECSLabels)
		}
	}
}

// EnrichResourceLogs enriches a single ResourceLogs with the current enricher
// configuration.
func (e *Enricher) EnrichResourceLogs(resLog plog.ResourceLogs) {
	resource := resLog.Resource()
	EnrichResource(resource, e.Config.Resource)
	resourceAttrs := resource.Attributes().AsRaw()
	scopeLogs := resLog.ScopeLogs()
	for j := 0; j < scopeLogs.Len(); j++ {
		scopeLog := scopeLogs.At(j)
		EnrichScope(scopeLog.Scope(), e.Config)
		logRecords := scopeLog.LogRecords()
		for k := 0; k < logRecords.Len(); k++ {
			EnrichLog(resourceAttrs, logRecords.At(k), e.Config, e.remapToECSLabels)
		}
	}
}

// EnrichResourceMetrics enriches a single ResourceMetrics with the current
// enricher configuration.
func (e *Enricher) EnrichResourceMetrics(resMetric pmetric.ResourceMetrics) {
	EnrichMetric(resMetric, e.Config)
	EnrichResource(resMetric.Resource(), e.Config.Resource)
	scopeMetrics := resMetric.ScopeMetrics()
	for j := 0; j < scopeMetrics.Len(); j++ {
		scopeMetric := scopeMetrics.At(j)
		EnrichScope(scopeMetric.Scope(), e.Config)
		scopeAttrs := scopeMetric.Scope().Attributes()
		metrics := scopeMetric.Metrics()
		for k := 0; k < metrics.Len(); k++ {
			EnrichMetricDataPoints(metrics.At(k), scopeAttrs, e.Config, e.remapToECSLabels)
		}
	}
}

// ecsOTelConfig returns a config with the common ECS resource enrichments
// enabled for elastic OTel events. Signal-specific flags (e.g.
// Span/Log/Metric.TranslateUnsupportedAttributes) must be set by the caller.
func ecsOTelConfig(cfg config.Config) config.Config {
	cfg.Resource.HostOSType.Enabled = true
	cfg.Resource.ServiceName.Enabled = true
	cfg.Resource.DefaultDeploymentEnvironment.Enabled = true
	cfg.Resource.DefaultServiceLanguage.Enabled = true
	return cfg
}

// ecsAPMConfig returns a config with the common ECS resource enrichments
// enabled for elastic APM intake events. The intake receiver already
// provides host.os.type and default service language, so these are left
// disabled. Signal-specific flags must be set by the caller.
func ecsAPMConfig(cfg config.Config) config.Config {
	cfg.Resource.ServiceName.Enabled = true
	cfg.Resource.DefaultDeploymentEnvironment.Enabled = true
	return cfg
}

// ecsPreProcessResource runs the shared ECS pre-processing pipeline on a
// resource. This is common across all signal types (traces, logs, metrics)
// in ECS mode.
func ecsPreProcessResource(ctx context.Context, resource pcommon.Resource, dataStreamType string, serviceNameInDataStreamDataset bool, hostIPEnabled bool) {
	ecs.TranslateResourceMetadata(resource)
	ecs.ApplyResourceConventions(resource)
	routing.EncodeDataStream(resource, dataStreamType, serviceNameInDataStreamDataset)
	if hostIPEnabled {
		ecs.SetHostIP(ctx, resource.Attributes())
	}
}

// NewEnricher creates a new instance of Enricher. remapToECSLabels
// controls whether unsupported attributes are moved to labels.* /
// numeric_labels.* for ECS compatibility (true for OTel enrichers).
func NewEnricher(cfg config.Config, remapToECSLabels bool) *Enricher {
	return &Enricher{
		Config:           cfg,
		remapToECSLabels: remapToECSLabels,
		userAgentParser:  sharedUserAgentParser(),
	}
}
