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

	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/opentelemetry-lib/elasticattr"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Sets fields that are NOT part of OTel for transactions. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForTransaction(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutStr(elasticattr.ProcessorEvent, "transaction")
	attributes.PutStr(elasticattr.TransactionID, event.Transaction.Id)
	attributes.PutStr(elasticattr.TransactionName, event.Transaction.Name)
	attributes.PutBool(elasticattr.TransactionSampled, event.Transaction.Sampled)
	// from whatever reason Transaction.Root is always false. That seems to be a derived field already - I don't see that fields directly on IntakeV2 - there is only ParentId
	attributes.PutBool(elasticattr.TransactionRoot, event.ParentId == "")
	attributes.PutStr(elasticattr.TransactionType, event.Transaction.Type)
	attributes.PutStr(elasticattr.TransactionResult, event.Transaction.Result)
	attributes.PutInt(elasticattr.TransactionDurationUs, int64(event.Event.Duration/1_000))
}

// Sets fields that are NOT part of OTel for spans. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForSpan(event *modelpb.APMEvent, attributes pcommon.Map) {

	attributes.PutStr(elasticattr.ProcessorEvent, "span")
	attributes.PutInt(elasticattr.SpanDurationUs, int64(event.Event.Duration/1_000))
	attributes.PutStr("span.id", event.Span.Id)
	attributes.PutStr(elasticattr.SpanName, event.Span.Name)
	attributes.PutStr(elasticattr.SpanType, event.Span.Type)
	attributes.PutStr(elasticattr.SpanSubtype, event.Span.Subtype)
	attributes.PutStr("span.action", event.Span.Action)

	if event.Span.Sync != nil {
		attributes.PutBool("span.sync", *event.Span.Sync)
	}

	if event.Span.DestinationService != nil {
		attributes.PutStr(elasticattr.ServiceTargetName, event.Span.DestinationService.Name)
		attributes.PutStr(elasticattr.ServiceTargetType, event.Span.DestinationService.Type)
		attributes.PutStr(elasticattr.SpanDestinationServiceResource, event.Span.DestinationService.Resource)
	}
}

// Sets resource fields that are NOT part of OTel. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedResourceAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutStr(elasticattr.AgentName, event.Agent.Name)
	attributes.PutStr(elasticattr.AgentVersion, event.Agent.Version)
	if event.Service != nil && event.Service.Language != nil {
		if event.Service.Language.Name != "" {
			attributes.PutStr("service.language.name", event.Service.Language.Name)
		}
		if event.Service.Language.Version != "" {
			attributes.PutStr("service.language.version", event.Service.Language.Version)
		}
	}
}

func SetDerivedFieldsForMetrics(attributes pcommon.Map) {
	attributes.PutStr(elasticattr.ProcessorEvent, "metric")
}

// Shared across spans and transactions
func SetDerivedFieldsCommon(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutInt(elasticattr.TimestampUs, int64(event.Timestamp/1_000))

	if strings.EqualFold(event.Event.Outcome, "success") {
		attributes.PutStr(elasticattr.EventOutcome, "success")
	} else if strings.EqualFold(event.Event.Outcome, "failure") {
		attributes.PutStr(elasticattr.EventOutcome, "failure")
	} else {
		attributes.PutStr(elasticattr.EventOutcome, "unknown")
	}
}

// Sets fields that are NOT part of OTel for errors. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForError(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutStr(elasticattr.ProcessorEvent, "error")

	if event.Error == nil {
		return
	}

	if event.Error.Id != "" {
		attributes.PutStr(elasticattr.ErrorID, event.Error.Id)
	}
	if event.Transaction != nil && event.Transaction.Id != "" {
		attributes.PutStr(elasticattr.TransactionID, event.Transaction.Id)
	}
	if event.ParentId != "" {
		attributes.PutStr(elasticattr.ParentID, event.ParentId)
	}

	if event.Error.Type != "" {
		attributes.PutStr("error.type", event.Error.Type)
	}
	if event.Error.Message != "" {
		attributes.PutStr("message", event.Error.Message)
	}
	attributes.PutStr(elasticattr.ErrorGroupingKey, event.Error.GroupingKey)
	attributes.PutInt(elasticattr.TimestampUs, int64(event.Timestamp/1_000))

	if event.Error.Culprit != "" {
		attributes.PutStr("error.culprit", event.Error.Culprit)
	}

	if event.Error.Exception != nil {
		if event.Error.Exception.Type != "" {
			attributes.PutStr("exception.type", event.Error.Exception.Type)
		}
		if event.Error.Exception.Message != "" {
			attributes.PutStr("exception.message", event.Error.Exception.Message)
		}

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

		if event.Error.Exception.Module != "" {
			attributes.PutStr("error.exception.module", event.Error.Exception.Module)
		}
		if event.Error.Exception.Handled != nil {
			attributes.PutBool("error.exception.handled", *event.Error.Exception.Handled)
		}
		if event.Error.Exception.Code != "" {
			attributes.PutStr("error.exception.code", event.Error.Exception.Code)
		}
	}
}
