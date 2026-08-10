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

package routing // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/elastic/apm-data/model/modelprocessor"
	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/ecs"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
)

// DataStreamType tracks the text associated with a data stream type.
const (
	DataStreamTypeLogs    = "logs"
	DataStreamTypeMetrics = "metrics"
	DataStreamTypeTraces  = "traces"

	NamespaceDefault = "default"
)

func EncodeDataStream(resource pcommon.Resource, dataStreamType string, svcInDataset bool, resCtx ecs.ResourceAttrContext) {
	attributes := resource.Attributes()
	attributes.PutStr(elasticattr.DataStreamType, dataStreamType)
	if resCtx.DataStreamNamespace == "" {
		attributes.PutStr(elasticattr.DataStreamNamespace, NamespaceDefault)
	}
	if resCtx.DataStreamDataset == "" {
		dataset := "apm"
		if svcInDataset {
			// derive service name for encoding in dataset
			serviceName := "unknown"
			if resCtx.ServiceName != "" {
				serviceName = resCtx.ServiceName
			}
			dataset = "apm.app." + normalizeServiceName(serviceName)
		}
		attributes.PutStr(elasticattr.DataStreamDataset, dataset)
	}
}

// IsErrorEvent checks if a log record or span event represents an APM error event.
// The processor.event attribute is set by the elasticapmintakereceiver
// when converting APM error events to OTLP logs or span events.
// For OTLP logs and span events, error events are identified by the presence of exception.type
// and exception.message attributes, following OpenTelemetry semantic conventions.
func IsErrorEvent(attributes pcommon.Map) bool {
	// Check for APM-specific error events.
	// Fast path for log records where apm-data always sets processor.event.
	if processorEvent, ok := attributes.Get(elasticattr.ProcessorEvent); ok {
		return processorEvent.Str() == "error"
	}

	// Check for OTLP exception attributes
	_, hasExceptionType := attributes.Get(string(semconv.ExceptionTypeKey))
	_, hasExceptionMessage := attributes.Get(string(semconv.ExceptionMessageKey))
	if hasExceptionType || hasExceptionMessage {
		return true
	}

	return false
}

// EncodeErrorDataStream sets the data stream attributes for error logs and span events.
// Error logs should always be routed to the "apm.error" dataset regardless of service name.
// Error span events should also be routed to the "apm.error" dataset.
// namespace is used as the fallback when no namespace is already set on attributes;
// if empty, NamespaceDefault is used.
func EncodeErrorDataStream(attributes pcommon.Map, dataStreamType, namespace string) {
	attributes.PutStr(elasticattr.DataStreamType, dataStreamType)
	attributes.PutStr(elasticattr.DataStreamDataset, "apm.error")
	if isStrKeyMissingOrEmpty(attributes, elasticattr.DataStreamNamespace) {
		if namespace == "" {
			namespace = NamespaceDefault
		}
		attributes.PutStr(elasticattr.DataStreamNamespace, namespace)
	}
}

// EncodeDataStreamMetricDataPoint determines if the metric represents an internal metric
// and sets the appropriate data stream for a metric data point.
// This implements the routing logic from apm-data's metricsetDataset function.
// Returns true if the metric is routed to an internal data stream, false otherwise.
func EncodeDataStreamMetricDataPoint(attributes pcommon.Map, metricName string, hasServiceName bool) bool {
	// Check for special cases: transaction/span context, no service name, or service_summary
	if hasTransactionSpanContext(attributes) || !hasServiceName || isServiceSummary(attributes) {
		// Check if there's a metricset interval for interval-based routing
		interval := getMetricsetInterval(attributes)
		if interval != "" {
			metricsetName := getMetricsetName(attributes)
			if metricsetName == "" {
				// Fallback to a generic name if metricset.name is missing
				metricsetName = "metrics"
			}
			internalIntervalMetricDataStream(attributes, DataStreamTypeMetrics, metricsetName, interval)
		} else {
			internalMetricDataStream(attributes, DataStreamTypeMetrics)
		}
		return true
	}

	// Check if the metric name is recognized as an internal metric using apm-data conventions
	if modelprocessor.IsInternalMetricName(metricName) && !isOTelRemapped(attributes) {
		internalMetricDataStream(attributes, DataStreamTypeMetrics)
		return true
	}

	return false
}

// hasTransactionSpanContext checks if the attributes contain transaction or span context.
// This is used to identify metrics that include transaction/span fields, which should be
// routed to internal metrics data streams.
func hasTransactionSpanContext(attributes pcommon.Map) bool {
	// Check for transaction-related attributes
	if _, ok := attributes.Get(elasticattr.TransactionID); ok {
		return true
	}
	if _, ok := attributes.Get(elasticattr.TransactionName); ok {
		return true
	}
	if _, ok := attributes.Get(elasticattr.TransactionType); ok {
		return true
	}

	// Check for span-related attributes
	if _, ok := attributes.Get(elasticattr.SpanID); ok {
		return true
	}
	if _, ok := attributes.Get(elasticattr.SpanName); ok {
		return true
	}

	return false
}

// getMetricsetInterval extracts the metricset.interval attribute value,
// or empty string if not present.
func getMetricsetInterval(attributes pcommon.Map) string {
	if interval, ok := attributes.Get("metricset.interval"); ok {
		return interval.Str()
	}
	return ""
}

// getMetricsetName extracts the metricset.name attribute value,
// or empty string if not present.
func getMetricsetName(attributes pcommon.Map) string {
	if name, ok := attributes.Get("metricset.name"); ok {
		return name.Str()
	}
	return ""
}

func isServiceSummary(attributes pcommon.Map) bool {
	return getMetricsetName(attributes) == "service_summary"
}

// Using default "apm.internal" data stream
func internalMetricDataStream(attributes pcommon.Map, dataStreamType string) {
	attributes.PutStr(elasticattr.DataStreamType, dataStreamType)
	attributes.PutStr(elasticattr.DataStreamDataset, "apm.internal")
	if isStrKeyMissingOrEmpty(attributes, elasticattr.DataStreamNamespace) {
		attributes.PutStr(elasticattr.DataStreamNamespace, NamespaceDefault)
	}
}

// Data stream formatted as: apm.${metricset.name}.${metricset.interval}
func internalIntervalMetricDataStream(attributes pcommon.Map, dataStreamType, metricsetName, interval string) {
	attributes.PutStr(elasticattr.DataStreamType, dataStreamType)
	attributes.PutStr(elasticattr.DataStreamDataset, fmt.Sprintf("apm.%s.%s", metricsetName, interval))
	if isStrKeyMissingOrEmpty(attributes, elasticattr.DataStreamNamespace) {
		attributes.PutStr(elasticattr.DataStreamNamespace, NamespaceDefault)
	}
}

func normalizeServiceName(s string) string {
	s = strings.ToLower(s)
	return strings.Map(func(r rune) rune {
		switch r {
		case '\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',', '#', ':', '.', '-':
			return '_'
		}
		return r
	}, s)
}

func isOTelRemapped(attributes pcommon.Map) bool {
	for _, key := range []string{"otel_remapped", "labels.otel_remapped"} {
		if value, ok := attributes.Get(key); ok {
			switch value.Type() {
			case pcommon.ValueTypeBool:
				if value.Bool() {
					return true
				}
			case pcommon.ValueTypeStr:
				if remapped, err := strconv.ParseBool(value.Str()); err == nil && remapped {
					return true
				}
			}
		}
	}
	return false
}

func isStrKeyMissingOrEmpty(attributes pcommon.Map, key string) bool {
	value, exists := attributes.Get(key)
	return !exists || (value.Type() == pcommon.ValueTypeStr && value.Str() == "")
}
