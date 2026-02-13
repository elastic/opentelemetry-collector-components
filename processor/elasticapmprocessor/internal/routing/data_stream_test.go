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

package routing_test

import (
	"strings"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/datastream"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
)

func TestDataStremaEncoderDefault(t *testing.T) {
	resource := pcommon.NewResource()
	routing.EncodeDataStream(resource, "logs", false)

	attributes := resource.Attributes()

	dataStreamType, ok := attributes.Get("data_stream.type")
	assert.True(t, ok)
	assert.Equal(t, "logs", dataStreamType.Str())

	dataStreamDataset, ok := attributes.Get("data_stream.dataset")
	assert.True(t, ok)
	assert.Equal(t, "apm", dataStreamDataset.Str())

	dataStreamNamespace, ok := attributes.Get("data_stream.namespace")
	assert.True(t, ok)
	assert.Equal(t, "default", dataStreamNamespace.Str())
}

func TestDataStreamEncoderWithServiceName(t *testing.T) {
	resource := pcommon.NewResource()
	attributes := resource.Attributes()
	attributes.PutStr("service.name", "my-service")

	routing.EncodeDataStream(resource, "metrics", true)

	dataStreamType, ok := attributes.Get("data_stream.type")
	assert.True(t, ok)
	assert.Equal(t, "metrics", dataStreamType.Str())

	dataStreamDataset, ok := attributes.Get("data_stream.dataset")
	assert.True(t, ok)
	assert.Equal(t, "apm.app.my_service", dataStreamDataset.Str())

	dataStreamNamespace, ok := attributes.Get("data_stream.namespace")
	assert.True(t, ok)
	assert.Equal(t, "default", dataStreamNamespace.Str())
}

func TestDataStreamEncoderWithServiceNameSanitizesAndTruncates(t *testing.T) {
	resource := pcommon.NewResource()
	attributes := resource.Attributes()
	attributes.PutStr("service.name", strings.Repeat("A-", 80))

	routing.EncodeDataStream(resource, "logs", true)

	dataStreamDataset, ok := attributes.Get("data_stream.dataset")
	assert.True(t, ok)
	assert.Len(t, dataStreamDataset.Str(), datastream.MaxDataStreamBytes)
	assert.True(t, strings.HasPrefix(dataStreamDataset.Str(), "apm.app."))
	assert.False(t, strings.Contains(dataStreamDataset.Str(), "-"))
	assert.Equal(t, strings.ToLower(dataStreamDataset.Str()), dataStreamDataset.Str())
}

func TestIsErrorEvent(t *testing.T) {
	tests := []struct {
		name     string
		setupFn  func(pcommon.Map)
		expected bool
	}{
		{
			name: "has processor.event=error",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr("processor.event", "error")
			},
			expected: true,
		},
		{
			name: "has processor.event=transaction",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr("processor.event", "transaction")
			},
			expected: false,
		},
		{
			name: "has processor.event=span",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr("processor.event", "span")
			},
			expected: false,
		},
		{
			name: "no processor.event attribute",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr("some.other.attribute", "value")
			},
			expected: false,
		},
		{
			name: "empty attributes",
			setupFn: func(attrs pcommon.Map) {
				// no attributes set
			},
			expected: false,
		},
		{
			name: "has exception.type and exception.message",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr(string(semconv.ExceptionTypeKey), "java.lang.NullPointerException")
				attrs.PutStr(string(semconv.ExceptionMessageKey), "Cannot invoke method on null object")
			},
			expected: true,
		},
		{
			name: "has only exception.type",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr(string(semconv.ExceptionTypeKey), "java.lang.NullPointerException")
			},
			expected: true,
		},
		{
			name: "has only exception.message",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr(string(semconv.ExceptionMessageKey), "Cannot invoke method on null object")
			},
			expected: true,
		},
		{
			name: "has both processor.event and exception attributes",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr("processor.event", "error")
				attrs.PutStr(string(semconv.ExceptionTypeKey), "java.lang.NullPointerException")
				attrs.PutStr(string(semconv.ExceptionMessageKey), "Cannot invoke method on null object")
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := pcommon.NewMap()
			tt.setupFn(attrs)
			result := routing.IsErrorEvent(attrs)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEncodeErrorDataStream(t *testing.T) {
	attrs := pcommon.NewMap()
	routing.EncodeErrorDataStream(attrs, "logs")

	dataStreamType, ok := attrs.Get("data_stream.type")
	assert.True(t, ok)
	assert.Equal(t, "logs", dataStreamType.Str())

	dataStreamDataset, ok := attrs.Get("data_stream.dataset")
	assert.True(t, ok)
	assert.Equal(t, "apm.error", dataStreamDataset.Str())

	dataStreamNamespace, ok := attrs.Get("data_stream.namespace")
	assert.True(t, ok)
	assert.Equal(t, "default", dataStreamNamespace.Str())
}

func TestRouteMetricDataPoint(t *testing.T) {
	tests := []struct {
		name             string
		setupFn          func(pcommon.Map)
		metricName       string
		hasServiceName   bool
		expectedDataset  string
		expectedInternal bool
	}{
		{
			name: "internal metric with service name",
			setupFn: func(attrs pcommon.Map) {
				// No special attributes
			},
			metricName:       "golang.heap.allocations.active",
			hasServiceName:   true,
			expectedDataset:  "apm.internal",
			expectedInternal: true,
		},
		{
			name: "non-internal metric with service name",
			setupFn: func(attrs pcommon.Map) {
				// No special attributes
			},
			metricName:       "my.custom.metric",
			hasServiceName:   true,
			expectedDataset:  "",
			expectedInternal: false,
		},
		{
			name: "metric without service name",
			setupFn: func(attrs pcommon.Map) {
				// No special attributes
			},
			metricName:       "my.custom.metric",
			hasServiceName:   false,
			expectedDataset:  "apm.internal",
			expectedInternal: true,
		},
		{
			name: "service_summary metric",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr("metricset.name", "service_summary")
			},
			metricName:       "some.metric",
			hasServiceName:   true,
			expectedDataset:  "apm.internal",
			expectedInternal: true,
		},
		{
			name: "service_summary with interval",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr("metricset.name", "service_summary")
				attrs.PutStr("metricset.interval", "10s")
			},
			metricName:       "some.metric",
			hasServiceName:   true,
			expectedDataset:  "apm.service_summary.10s",
			expectedInternal: true,
		},
		{
			name: "transaction metric with interval",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr("transaction.id", "abc123")
				attrs.PutStr("metricset.name", "transaction")
				attrs.PutStr("metricset.interval", "1m")
			},
			metricName:       "transaction.duration",
			hasServiceName:   true,
			expectedDataset:  "apm.transaction.1m",
			expectedInternal: true,
		},
		{
			name: "span metric with interval",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr("span.id", "xyz789")
				attrs.PutStr("metricset.name", "themetricspan")
				attrs.PutStr("metricset.interval", "30s")
			},
			metricName:       "span.duration",
			hasServiceName:   true,
			expectedDataset:  "apm.themetricspan.30s",
			expectedInternal: true,
		},
		{
			name: "transaction metric without interval",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr("transaction.id", "abc123")
			},
			metricName:       "transaction.duration",
			hasServiceName:   true,
			expectedDataset:  "apm.internal",
			expectedInternal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := pcommon.NewMap()
			tt.setupFn(attrs)
			isInternal := routing.EncodeDataStreamMetricDataPoint(attrs, tt.metricName, tt.hasServiceName)

			assert.Equal(t, tt.expectedInternal, isInternal, "unexpected internal metric detection")

			if tt.expectedDataset == "" {
				// Should not have set data stream attributes
				_, ok := attrs.Get("data_stream.dataset")
				assert.False(t, ok, "data_stream.dataset should not be set for non-internal metrics with service name")
			} else {
				dataStreamDataset, ok := attrs.Get("data_stream.dataset")
				assert.True(t, ok)
				assert.Equal(t, tt.expectedDataset, dataStreamDataset.Str())
			}
		})
	}
}

func TestRouteMetricDataPointSanitizesAndTruncatesDataset(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("transaction.id", "abc123")
	attrs.PutStr("metricset.name", strings.Repeat("Very-Long-Metric-Set-", 20))
	attrs.PutStr("metricset.interval", "1m")

	isInternal := routing.EncodeDataStreamMetricDataPoint(attrs, "transaction.duration", true)
	assert.True(t, isInternal)

	dataStreamDataset, ok := attrs.Get("data_stream.dataset")
	assert.True(t, ok)
	assert.Len(t, dataStreamDataset.Str(), datastream.MaxDataStreamBytes)
	assert.False(t, strings.Contains(dataStreamDataset.Str(), "-"))
	assert.Equal(t, strings.ToLower(dataStreamDataset.Str()), dataStreamDataset.Str())
}
