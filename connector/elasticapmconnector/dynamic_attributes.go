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

package elasticapmconnector // import "github.com/elastic/opentelemetry-collector-components/connector/elasticapmconnector"

import (
	"context"
	"strings"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// dynamicAttributeTraceFilter wraps a traces consumer to strip
// prefixed resource attributes not in the allow-list provided
// via client.Metadata.
type dynamicAttributeTraceFilter struct {
	metadataKey string
	prefixes    []string
	next        consumer.Traces
}

func (f *dynamicAttributeTraceFilter) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	allowKeys := parseDynamicAttributeKeys(ctx, f.metadataKey)
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		filterDynamicAttributes(td.ResourceSpans().At(i).Resource().Attributes(), f.prefixes, allowKeys)
	}
	return f.next.ConsumeTraces(ctx, td)
}

func (f *dynamicAttributeTraceFilter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// dynamicAttributeLogFilter wraps a logs consumer to strip
// prefixed resource attributes not in the allow-list provided
// via client.Metadata.
type dynamicAttributeLogFilter struct {
	metadataKey string
	prefixes    []string
	next        consumer.Logs
}

func (f *dynamicAttributeLogFilter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	allowKeys := parseDynamicAttributeKeys(ctx, f.metadataKey)
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		filterDynamicAttributes(ld.ResourceLogs().At(i).Resource().Attributes(), f.prefixes, allowKeys)
	}
	return f.next.ConsumeLogs(ctx, ld)
}

func (f *dynamicAttributeLogFilter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// dynamicAttributeMetricFilter wraps a metrics consumer to strip
// prefixed resource attributes not in the allow-list provided
// via client.Metadata.
type dynamicAttributeMetricFilter struct {
	metadataKey string
	prefixes    []string
	next        consumer.Metrics
}

func (f *dynamicAttributeMetricFilter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	allowKeys := parseDynamicAttributeKeys(ctx, f.metadataKey)
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		filterDynamicAttributes(md.ResourceMetrics().At(i).Resource().Attributes(), f.prefixes, allowKeys)
	}
	return f.next.ConsumeMetrics(ctx, md)
}

func (f *dynamicAttributeMetricFilter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// filterDynamicAttributes removes resource attributes that match any
// of the given prefixes but whose base key (after stripping the prefix)
// is not in the allow-list. Non-matching attributes are never removed.
// A nil allowKeys strips all attributes matching the prefixes.
func filterDynamicAttributes(attrs pcommon.Map, prefixes []string, allowKeys map[string]struct{}) {
	attrs.RemoveIf(func(k string, _ pcommon.Value) bool {
		for _, prefix := range prefixes {
			if base, ok := strings.CutPrefix(k, prefix); ok {
				_, allowed := allowKeys[base]
				return !allowed
			}
		}
		return false
	})
}

// parseDynamicAttributeKeys reads a comma-separated list of attribute
// key names from the configured client.Metadata key. Returns nil if
// the key is absent or empty.
func parseDynamicAttributeKeys(ctx context.Context, metadataKey string) map[string]struct{} {
	if metadataKey == "" {
		return nil
	}
	info := client.FromContext(ctx)
	vals := info.Metadata.Get(metadataKey)
	if len(vals) == 0 || vals[0] == "" {
		return nil
	}
	parts := strings.Split(vals[0], ",")
	keys := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		if k := strings.TrimSpace(p); k != "" {
			keys[k] = struct{}{}
		}
	}
	return keys
}
