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

// This file contains mappings where we move intakeV2 fields into Attributes and Resource attributes on OTel events
// These fields are not covered by SemConv and are specific to Elastic

package mappers // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/mappers"

import (
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	attr "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal"
)

// SetDerivedFieldsForTransaction sets fields that are NOT part of OTel for transactions. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForTransaction(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutStr(elasticattr.ProcessorEvent, "transaction")
	attributes.PutInt(elasticattr.TransactionDurationUs, int64(event.Event.Duration/1_000))

	setCommonDerivedRecordAttributes(event, attributes)

	if event.Transaction == nil {
		return
	}

	putNonEmptyStr(attributes, elasticattr.TransactionName, event.Transaction.Name)
	putNonEmptyStr(attributes, elasticattr.TransactionType, event.Transaction.Type)
	putNonEmptyStr(attributes, elasticattr.TransactionResult, event.Transaction.Result)
	attributes.PutBool(elasticattr.TransactionSampled, event.Transaction.Sampled)
}

// setCommonDerivedRecordAttributes sets common attributes which are shared at the record
// level for span, transaction, and error events.
func setCommonDerivedRecordAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Transaction != nil {
		putNonEmptyStr(attributes, elasticattr.TransactionID, event.Transaction.Id)
	}

	if event.Service != nil && event.Service.Target != nil {
		attributes.PutStr(elasticattr.ServiceTargetType, event.Service.Target.Type)
		attributes.PutStr(elasticattr.ServiceTargetName, event.Service.Target.Name)
	}
}

// SetDerivedFieldsForSpan sets fields that are NOT part of OTel for spans. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForSpan(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutStr(elasticattr.ProcessorEvent, "span")
	attributes.PutInt(elasticattr.SpanDurationUs, int64(event.Event.Duration/1_000))

	setCommonDerivedRecordAttributes(event, attributes)

	if event.Span == nil {
		return
	}

	attributes.PutStr("span.id", event.Span.Id)

	putNonEmptyStr(attributes, elasticattr.SpanName, event.Span.Name)
	putNonEmptyStr(attributes, elasticattr.SpanType, event.Span.Type)
	putNonEmptyStr(attributes, elasticattr.SpanSubtype, event.Span.Subtype)
	putNonEmptyStr(attributes, "span.action", event.Span.Action)

	putPtrBool(attributes, "span.sync", event.Span.Sync)

	if event.Span.DestinationService != nil {
		putNonEmptyStr(attributes, elasticattr.SpanDestinationServiceResource, event.Span.DestinationService.Resource)
	}
}

// SetDerivedResourceAttributes sets resource fields that are NOT part of OTel. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedResourceAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	if event.Agent != nil {
		attributes.PutStr(elasticattr.AgentName, event.Agent.Name)
		attributes.PutStr(elasticattr.AgentVersion, event.Agent.Version)
	}

	if event.Service != nil {
		if event.Service.Language != nil {
			putNonEmptyStr(attributes, attr.ServiceLanguageName, event.Service.Language.Name)
			putNonEmptyStr(attributes, attr.ServiceLanguageVersion, event.Service.Language.Version)
		}
	}
}

// SetDerivedFieldsForMetrics sets fields that are NOT part of OTel for metrics. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForMetrics(attributes pcommon.Map) {
	attributes.PutStr(elasticattr.ProcessorEvent, "metric")
}

// SetDerivedFieldsCommon sets shared fields that are NOT part of OTel for multipe signals. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsCommon(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutInt(elasticattr.TimestampUs, int64(event.Timestamp/1_000))

	outcome := event.GetEvent().GetOutcome()
	if strings.EqualFold(outcome, "success") {
		attributes.PutStr(elasticattr.EventOutcome, "success")
	} else if strings.EqualFold(outcome, "failure") {
		attributes.PutStr(elasticattr.EventOutcome, "failure")
	} else {
		attributes.PutStr(elasticattr.EventOutcome, "unknown")
	}
}

// SetDerivedFieldsForError sets fields that are NOT part of OTel for errors. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForError(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutStr(elasticattr.ProcessorEvent, "error")

	setCommonDerivedRecordAttributes(event, attributes)

	if event.Error == nil {
		return
	}

	putNonEmptyStr(attributes, elasticattr.ErrorID, event.Error.Id)
	putNonEmptyStr(attributes, elasticattr.ParentID, event.ParentId)

	putNonEmptyStr(attributes, "error.type", event.Error.Type)
	putNonEmptyStr(attributes, "message", event.Error.Message)
	attributes.PutStr(elasticattr.ErrorGroupingKey, event.Error.GroupingKey)
	attributes.PutInt(elasticattr.TimestampUs, int64(event.Timestamp/1_000))

	putNonEmptyStr(attributes, "error.culprit", event.Error.Culprit)

	if event.Error.Exception != nil {
		putNonEmptyStr(attributes, "exception.type", event.Error.Exception.Type)
		putNonEmptyStr(attributes, "exception.message", event.Error.Exception.Message)

		if event.Error.Exception.Stacktrace != nil {
			str := ""
			for _, frame := range event.Error.Exception.Stacktrace {
				f := frame.GetFunction()
				l := frame.GetLineno()
				if f != "" {
					str += fmt.Sprintf("%s:%d %s \n", frame.GetFilename(), l, f)
				} else {
					c := frame.GetClassname()
					if c != "" && l != 0 {
						str += fmt.Sprintf("%s:%d \n", c, l)
					}
				}
			}

			attributes.PutStr("exception.stacktrace", str)
		}

		putNonEmptyStr(attributes, "error.exception.module", event.Error.Exception.Module)
		putPtrBool(attributes, "error.exception.handled", event.Error.Exception.Handled)
		putNonEmptyStr(attributes, "error.exception.code", event.Error.Exception.Code)
	}
}

// SetDerivedFieldsForLog sets fields that are NOT part of OTel for logs. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForLog(event *modelpb.APMEvent, attributes pcommon.Map) {
	setCommonDerivedRecordAttributes(event, attributes)
}
