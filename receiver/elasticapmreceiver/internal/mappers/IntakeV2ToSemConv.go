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

package mappers

import (
	"github.com/elastic/apm-data/model/modelpb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/collector/semconv/v1.5.0"
)

// Translates resource attributes from the Elastic APM model to the OpenTelemetry resource attributes
func TranslateToOtelResourceAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutStr(semconv.AttributeServiceName, event.Service.Name)
	attributes.PutStr(semconv.AttributeServiceVersion, event.Service.Version)
	attributes.PutStr(semconv.AttributeTelemetrySDKLanguage, event.Service.Language.Name)
	attributes.PutStr(semconv.AttributeTelemetrySDKName, "ElasticAPM")
	attributes.PutStr(semconv.AttributeDeploymentEnvironment, event.Service.Environment)
}

func TranslateIntakeV2TransactionToOTelAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Http.Request != nil {
		attributes.PutStr(semconv.AttributeHTTPMethod, event.Http.Request.Method)
		attributes.PutInt(semconv.AttributeHTTPStatusCode, int64(event.Http.Response.StatusCode))

		if event.Url != nil {
			attributes.PutStr(semconv.AttributeHTTPURL, event.Url.Full)
		}
	} else if event.Span.Message != nil {
		attributes.PutStr(semconv.AttributeMessagingDestination, event.Transaction.Message.QueueName)
		attributes.PutStr(semconv.AttributeMessagingRabbitmqRoutingKey, event.Transaction.Message.RoutingKey)

		// This may need to be unified, see AttributeMessagingSystem for spans
		attributes.PutStr(semconv.AttributeMessagingSystem, event.Service.Framework.Name)
	}
}

func TranslateIntakeV2SpanToOTelAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {

	if event.Http != nil {
		attributes.PutStr(semconv.AttributeHTTPMethod, event.Http.Request.Method)
		attributes.PutInt(semconv.AttributeHTTPStatusCode, int64(event.Http.Response.StatusCode))

		if event.Url != nil {
			attributes.PutStr(semconv.AttributeHTTPURL, event.Url.Full)
		}
	}
	if event.Span.Db != nil {
		attributes.PutStr(semconv.AttributeDBSystem, event.Span.Db.Type)
		attributes.PutStr(semconv.AttributeDBName, event.Span.Db.Instance)
		attributes.PutStr(semconv.AttributeDBStatement, event.Span.Db.Statement)
	}
	if event.Span.Message != nil {
		// TODO - Elastic APM span.subtype does not 100% overlap with https://opentelemetry.io/docs/specs/semconv/attributes-registry/messaging/#messaging-system
		// E.g. azureservicebus in Elastic APM vs servicebus in SemConv
		attributes.PutStr(semconv.AttributeMessagingSystem, event.Span.Subtype)
		// No 100% overlap either
		attributes.PutStr(semconv.AttributeMessagingOperation, event.Span.Action)

		if event.Span.Message.QueueName != "" {
			attributes.PutStr(semconv.AttributeMessagingDestination, event.Span.Message.QueueName)
		}
		if event.Span.Message.RoutingKey != "" {
			attributes.PutStr(semconv.AttributeMessagingRabbitmqRoutingKey, event.Span.Message.RoutingKey)
		}
	}
}