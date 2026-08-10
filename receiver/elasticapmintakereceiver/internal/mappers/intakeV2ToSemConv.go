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

// This file contains all the mapping from IntakeV2 fields to OTel Semantic Convention

package mappers // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/mappers"

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/elastic/apm-data/model/modelpb"
)

// TranslateIntakeV2TransactionToOTelAttributes translates transaction attributes from the Elastic APM model to SemConv attributes
func TranslateIntakeV2TransactionToOTelAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	translateHttpAttributes(event, attributes)
	translateUrlAttributes(event, attributes)

	if event.Transaction.Message != nil {
		attributes.PutStr(string(semconv.MessagingDestinationNameKey), event.Transaction.Message.QueueName)
	}
}

// TranslateIntakeV2SpanToOTelAttributes translates span attributes from the Elastic APM model to SemConv attributes
func TranslateIntakeV2SpanToOTelAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	translateHttpAttributes(event, attributes)
	translateUrlAttributes(event, attributes)

	if event.Span == nil {
		return
	}

	if event.Span.Db != nil {
		attributes.PutStr(string(semconv.DBSystemKey), event.Span.Db.Type)
		attributes.PutStr(string(semconv.DBNamespaceKey), event.Span.Db.Instance)
		attributes.PutStr(string(semconv.DBQueryTextKey), event.Span.Db.Statement)
	}
	if event.Span.Message != nil {
		// Elastic APM span.subtype does not 100% overlap with https://opentelemetry.io/docs/specs/semconv/attributes-registry/messaging/#messaging-system
		// E.g. azureservicebus in Elastic APM vs servicebus in SemConv
		attributes.PutStr(string(semconv.MessagingSystemKey), event.Span.Subtype)
		// No 100% overlap either
		attributes.PutStr(string(semconv.MessagingOperationNameKey), event.Span.Action)

		if event.Span.Message.QueueName != "" {
			attributes.PutStr(string(semconv.MessagingDestinationNameKey), event.Span.Message.QueueName)
		}
	}

	if event.Destination != nil {
		if event.Destination.Address != "" {
			attributes.PutStr(string(semconv.DestinationAddressKey), event.Destination.Address)
		}
		if event.Destination.Port != 0 {
			attributes.PutInt(string(semconv.DestinationPortKey), int64(event.Destination.Port))
		}
	}
}

// TranslateIntakeV2LogToOTelAttributes translates log/error attributes from the Elastic APM model to SemConv attributes
// Note: error events contain additional context that requires otel semconv attributes, logs are not expected to have
// this additional context. Both events are treated the same here for consistency.
func TranslateIntakeV2LogToOTelAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	translateHttpAttributes(event, attributes)
	translateUrlAttributes(event, attributes)
}

func translateHttpAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Http != nil {
		if event.Http.Request != nil {
			attributes.PutStr(string(semconv.HTTPRequestMethodKey), event.Http.Request.Method)
		}
		if event.Http.Response != nil {
			if event.Http.Response.StatusCode != 0 {
				attributes.PutInt(string(semconv.HTTPResponseStatusCodeKey), int64(event.Http.Response.StatusCode))
			}
			putPtrInt(attributes, string(semconv.HTTPResponseBodySizeKey), event.Http.Response.EncodedBodySize)
		}
	}
}

// translateUrlAttributes sets URL semconv attributes that are defined below:
// https://opentelemetry.io/docs/specs/semconv/registry/attributes/url/
func translateUrlAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Url == nil {
		return
	}
	putNonEmptyStr(attributes, string(semconv.URLOriginalKey), event.Url.Original)
	putNonEmptyStr(attributes, string(semconv.URLSchemeKey), event.Url.Scheme)
	putNonEmptyStr(attributes, string(semconv.URLFullKey), event.Url.Full)
	putNonEmptyStr(attributes, string(semconv.URLDomainKey), event.Url.Domain)
	putNonEmptyStr(attributes, string(semconv.URLPathKey), event.Url.Path)
	putNonEmptyStr(attributes, string(semconv.URLQueryKey), event.Url.Query)
	putNonEmptyStr(attributes, string(semconv.URLFragmentKey), event.Url.Fragment)
	if event.Url.Port != 0 {
		attributes.PutInt(string(semconv.URLPortKey), int64(event.Url.Port))
	}
}
