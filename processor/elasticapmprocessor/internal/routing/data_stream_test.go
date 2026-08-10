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
	"testing"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/ecs"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
)

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

func TestDataStreamEncoders(t *testing.T) {
	tests := []struct {
		name                string
		attrs               map[string]any
		svcInDataset        bool
		isErrorEncode       bool
		expectedType        string
		expectedDataset     string
		expectedNamespace   string
		expectedServiceName string
	}{
		{
			name:              "EncodeDataStream default encoder behavior",
			expectedType:      "logs",
			expectedDataset:   "apm",
			expectedNamespace: "default",
		},
		{
			name:              "EncodeDataStream preserves namespace without service name dataset",
			attrs:             map[string]any{"data_stream.namespace": "custom-ns"},
			expectedType:      "traces",
			expectedDataset:   "apm",
			expectedNamespace: "custom-ns",
		},
		{
			name:                "EncodeDataStream preserves namespace with service name dataset",
			attrs:               map[string]any{"service.name": "my-svc", "data_stream.namespace": "prod"},
			svcInDataset:        true,
			expectedType:        "logs",
			expectedDataset:     "apm.app.my_svc",
			expectedNamespace:   "prod",
			expectedServiceName: "my-svc",
		},
		{
			name:              "EncodeErrorDataStream preserves namespace",
			attrs:             map[string]any{"data_stream.namespace": "staging"},
			isErrorEncode:     true,
			expectedType:      "logs",
			expectedDataset:   "apm.error",
			expectedNamespace: "staging",
		},
		{
			name:              "EncodeDataStream preserves existing type and dataset when both are already set",
			attrs:             map[string]any{"data_stream.type": "custom-type", "data_stream.dataset": "custom-dataset"},
			expectedType:      "custom-type",
			expectedDataset:   "custom-dataset",
			expectedNamespace: "default",
		},
		{
			name:              "EncodeDataStream overwrites existing type when dataset is missing",
			attrs:             map[string]any{"data_stream.type": "custom-type"},
			expectedType:      "traces",
			expectedDataset:   "apm",
			expectedNamespace: "default",
		},
		{
			name:              "EncodeDataStream preserves existing dataset without service name dataset",
			attrs:             map[string]any{"data_stream.dataset": "custom-dataset"},
			expectedType:      "traces",
			expectedDataset:   "custom-dataset",
			expectedNamespace: "default",
		},
		{
			name:                "EncodeDataStream preserves existing dataset with service name dataset",
			attrs:               map[string]any{"service.name": "my-svc", "data_stream.dataset": "custom-logs"},
			svcInDataset:        true,
			expectedType:        "logs",
			expectedDataset:     "custom-logs",
			expectedNamespace:   "default",
			expectedServiceName: "my-svc",
		},
		{
			name:              "EncodeErrorDataStream always overwrites existing dataset",
			attrs:             map[string]any{"data_stream.dataset": "something-else"},
			isErrorEncode:     true,
			expectedType:      "logs",
			expectedDataset:   "apm.error",
			expectedNamespace: "default",
		},
		{
			name:              "EncodeDataStream defaults namespace without service name dataset",
			expectedType:      "traces",
			expectedDataset:   "apm",
			expectedNamespace: "default",
		},
		{
			name:                "EncodeDataStream defaults namespace with service name dataset",
			attrs:               map[string]any{"service.name": "my-svc"},
			svcInDataset:        true,
			expectedType:        "logs",
			expectedDataset:     "apm.app.my_svc",
			expectedNamespace:   "default",
			expectedServiceName: "my-svc",
		},
		{
			name:              "EncodeErrorDataStream defaults namespace",
			isErrorEncode:     true,
			expectedType:      "logs",
			expectedDataset:   "apm.error",
			expectedNamespace: "default",
		},
		{
			name:              "EncodeDataStream defaults dataset without service name dataset when absent",
			expectedType:      "traces",
			expectedDataset:   "apm",
			expectedNamespace: "default",
		},
		{
			name:                "EncodeDataStream defaults dataset with service name dataset when absent",
			attrs:               map[string]any{"service.name": "my-svc"},
			svcInDataset:        true,
			expectedType:        "logs",
			expectedDataset:     "apm.app.my_svc",
			expectedNamespace:   "default",
			expectedServiceName: "my-svc",
		},
		{
			name:                "EncodeDataStream with service name simple alphanumeric",
			attrs:               map[string]any{"service.name": "myservice"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.myservice",
			expectedNamespace:   "default",
			expectedServiceName: "myservice",
		},
		{
			name:                "EncodeDataStream with service name hyphen replaced",
			attrs:               map[string]any{"service.name": "my-service"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: "my-service",
		},
		{
			name:                "EncodeDataStream with service name dot replaced",
			attrs:               map[string]any{"service.name": "my.service"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: "my.service",
		},
		{
			name:                "EncodeDataStream with service name uppercase lowercased",
			attrs:               map[string]any{"service.name": "My-Service"},
			svcInDataset:        true,
			expectedType:        "logs",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: "My-Service",
		},
		{
			name:                "EncodeDataStream with service name backslash replaced",
			attrs:               map[string]any{"service.name": `my\service`},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: `my\service`,
		},
		{
			name:                "EncodeDataStream with service name forward slash replaced",
			attrs:               map[string]any{"service.name": "my/service"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: "my/service",
		},
		{
			name:                "EncodeDataStream with service name asterisk replaced",
			attrs:               map[string]any{"service.name": "my*service"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: "my*service",
		},
		{
			name:                "EncodeDataStream with service name question mark replaced",
			attrs:               map[string]any{"service.name": "my?service"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: "my?service",
		},
		{
			name:                "EncodeDataStream with service name double quote replaced",
			attrs:               map[string]any{"service.name": `my"service`},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: `my"service`,
		},
		{
			name:                "EncodeDataStream with service name angle brackets replaced",
			attrs:               map[string]any{"service.name": "my<service>name"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service_name",
			expectedNamespace:   "default",
			expectedServiceName: "my<service>name",
		},
		{
			name:                "EncodeDataStream with service name pipe replaced",
			attrs:               map[string]any{"service.name": "my|service"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: "my|service",
		},
		{
			name:                "EncodeDataStream with service name space replaced",
			attrs:               map[string]any{"service.name": "my service"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: "my service",
		},
		{
			name:                "EncodeDataStream with service name comma replaced",
			attrs:               map[string]any{"service.name": "my,service"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: "my,service",
		},
		{
			name:                "EncodeDataStream with service name hash replaced",
			attrs:               map[string]any{"service.name": "my#service"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: "my#service",
		},
		{
			name:                "EncodeDataStream with service name colon replaced",
			attrs:               map[string]any{"service.name": "my:service"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: "my:service",
		},
		{
			name:                "EncodeDataStream with service name multiple special characters",
			attrs:               map[string]any{"service.name": "My Service.v2-beta/rc#1"},
			svcInDataset:        true,
			expectedType:        "logs",
			expectedDataset:     "apm.app.my_service_v2_beta_rc_1",
			expectedNamespace:   "default",
			expectedServiceName: "My Service.v2-beta/rc#1",
		},
		{
			name:                "EncodeDataStream with service name all disallowed characters replaced",
			attrs:               map[string]any{"service.name": `a\b/c*d?e"f<g>h|i,j#k:l.m`},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.a_b_c_d_e_f_g_h_i_j_k_l_m",
			expectedNamespace:   "default",
			expectedServiceName: `a\b/c*d?e"f<g>h|i,j#k:l.m`,
		},
		{
			name:                "EncodeDataStream with service name alphanumeric and underscore preserved",
			attrs:               map[string]any{"service.name": "a b-c_d0"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.a_b_c_d0",
			expectedNamespace:   "default",
			expectedServiceName: "a b-c_d0",
		},
		{
			name:              "EncodeDataStream with service name empty falls back to unknown",
			svcInDataset:      true,
			expectedType:      "metrics",
			expectedDataset:   "apm.app.unknown",
			expectedNamespace: "default",
		},
		{
			name:                "EncodeDataStream with service name underscores preserved",
			attrs:               map[string]any{"service.name": "my_service_name"},
			svcInDataset:        true,
			expectedType:        "metrics",
			expectedDataset:     "apm.app.my_service_name",
			expectedNamespace:   "default",
			expectedServiceName: "my_service_name",
		},
		{
			name:                "EncodeDataStream with service name traces type",
			attrs:               map[string]any{"service.name": "my-service"},
			svcInDataset:        true,
			expectedType:        "traces",
			expectedDataset:     "apm.app.my_service",
			expectedNamespace:   "default",
			expectedServiceName: "my-service",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testResource := pcommon.NewResource()
			if len(tt.attrs) > 0 {
				assert.NoError(t, testResource.Attributes().FromRaw(tt.attrs))
			}

			if tt.isErrorEncode {
				routing.EncodeErrorDataStream(testResource.Attributes(), tt.expectedType, "")
			} else {
				routing.EncodeDataStream(
					testResource,
					tt.expectedType,
					tt.svcInDataset,
					ecs.TranslateResourceMetadata(testResource, true),
				)
			}

			resultAttrs := testResource.Attributes()

			dataStreamType, ok := resultAttrs.Get("data_stream.type")
			assert.True(t, ok)
			assert.Equal(t, tt.expectedType, dataStreamType.Str())

			dataStreamDataset, ok := resultAttrs.Get("data_stream.dataset")
			assert.True(t, ok)
			assert.Equal(t, tt.expectedDataset, dataStreamDataset.Str())

			dataStreamNamespace, ok := resultAttrs.Get("data_stream.namespace")
			assert.True(t, ok)
			assert.Equal(t, tt.expectedNamespace, dataStreamNamespace.Str())

			if tt.expectedServiceName != "" {
				serviceName, ok := resultAttrs.Get("service.name")
				assert.True(t, ok)
				assert.Equal(t, tt.expectedServiceName, serviceName.Str())
			}
		})
	}
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
			name: "internal metric with otel_remapped is not routed to internal dataset",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr("otel_remapped", "true")
			},
			metricName:       "golang.heap.allocations.active",
			hasServiceName:   true,
			expectedDataset:  "",
			expectedInternal: false,
		},
		{
			name: "internal metric with labels.otel_remapped is not routed to internal dataset",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr("labels.otel_remapped", "true")
			},
			metricName:       "golang.heap.allocations.active",
			hasServiceName:   true,
			expectedDataset:  "",
			expectedInternal: false,
		},
		{
			name: "metric without service name still routes internal even when otel_remapped",
			setupFn: func(attrs pcommon.Map) {
				attrs.PutStr("otel_remapped", "true")
			},
			metricName:       "golang.heap.allocations.active",
			hasServiceName:   false,
			expectedDataset:  "apm.internal",
			expectedInternal: true,
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
