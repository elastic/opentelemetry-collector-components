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

// TODO: attribute names should be pulled in from https://github.com/elastic/opentelemetry-lib/blob/main/enrichments/trace/internal/elastic/attributes.go

package mappers

import (
	"strings"

	"github.com/elastic/apm-data/model/modelpb"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Sets fields that are NOT part of OTel. These fields are derived by the Enrichment lib in case of OTLP input
func SetDerivedFieldsForTransaction(event *modelpb.APMEvent, attributes pcommon.Map) {

	attributes.PutStr("processor.event", "transaction")
	attributes.PutStr("transaction.id", event.Transaction.Id)
	attributes.PutStr("transaction.name", event.Transaction.Name)
	attributes.PutBool("transaction.sampled", event.Transaction.Sampled)
	// from whatever reason Transaction.Root is always false. That seems to be a derived field already - I don't see that fields directly on IntakeV2 - there is only ParentId
	attributes.PutBool("transaction.root", event.ParentId == "")
	attributes.PutStr("transaction.type", event.Transaction.Type)
	attributes.PutStr("transaction.result", event.Transaction.Result)
	attributes.PutInt("transaction.duration.us", int64(event.Event.Duration))
}

func SetDerivedFieldsForSpan(event *modelpb.APMEvent, attributes pcommon.Map) {

	attributes.PutStr("processor.event", "span")
	attributes.PutDouble("span.duration.us", float64(event.Event.Duration))
	attributes.PutStr("span.id", event.Span.Id)
	attributes.PutStr("span.name", event.Span.Name)
	attributes.PutStr("span.type", event.Span.Type)

	if event.Span.DestinationService != nil {
		attributes.PutStr("service.target.name", event.Span.DestinationService.Name)
		attributes.PutStr("service.target.type", event.Span.DestinationService.Type)
		attributes.PutStr("span.destination.service.resource", event.Span.DestinationService.Resource)
	}
}

func SetDerivedResourceAttributes(event *modelpb.APMEvent, attributes pcommon.Map) {
	attributes.PutStr("agent.name", event.Agent.Name)
	attributes.PutStr("agent.version", event.Agent.Version)
}

// Shared across spans and transactions
func SetDerivedFieldsCommon(event *modelpb.APMEvent, attributes pcommon.Map) {
	//TODO: that's not correct.
	attributes.PutInt("timestamp.us", int64(event.Timestamp))

	if strings.EqualFold(event.Event.Outcome, "success") {
		attributes.PutStr("event.outcome", "success")
	} else if strings.EqualFold(event.Event.Outcome, "failure") {
		attributes.PutStr("event.outcome", "failure")
	} else {
		attributes.PutStr("event.outcome", "unknown")
	}
}