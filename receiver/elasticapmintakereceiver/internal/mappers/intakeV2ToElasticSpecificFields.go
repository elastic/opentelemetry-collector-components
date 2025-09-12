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
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/elastic/apm-data/model/modelpb"
	attr "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal"
)

// Sets fields on spans that are not defined by OTel.
// Unlike fields from IntakeV2ToDerivedFields.go, these fields are not used by the UI
// and store information about a specific span type
func SetElasticSpecificFieldsForSpan(event *modelpb.APMEvent, attributesMap pcommon.Map) {
	if event.Span.Db != nil {
		attributesMap.PutStr(attr.SpanDBLink, event.Span.Db.Link)
		// SemConv db.response.returned_rows is similar, but not the same
		attributesMap.PutInt(attr.SpanDBRowsAffected, int64(*event.Span.Db.RowsAffected))
		attributesMap.PutStr(attr.SpanDBUserName, event.Span.Db.UserName)
	}

	if event.Http.Request != nil {
		attributesMap.PutStr(attr.HTTPRequestBody, event.Http.Request.Body.GetStringValue())
		attributesMap.PutStr(attr.HTTPRequestID, event.Http.Request.Id)
		attributesMap.PutStr(attr.HTTPRequestReferrer, event.Http.Request.Referrer)
	}

	if event.Http.Response != nil {
		// SemConv http.response.body.size may match one of these.
		attributesMap.PutInt(attr.HTTPResponseDecodedBodySize, int64(*event.Http.Response.DecodedBodySize))
		attributesMap.PutInt(attr.HTTPResponseEncodedBodySize, int64(*event.Http.Response.EncodedBodySize))
		attributesMap.PutInt(attr.HTTPResponseTransferSize, int64(*event.Http.Response.TransferSize))
	}

	if event.Span.Message != nil {
		attributesMap.PutStr(attr.SpanMessageBody, event.Span.Message.Body)
	}
}

func SetElasticSpecificMetadataFields(event *modelpb.APMEvent, attributesMap pcommon.Map) {
	if event.Cloud != nil {
		if event.Cloud.ProjectId != "" {
			attributesMap.PutStr(attr.CloudProjectID, event.Cloud.ProjectId)
		}
		if event.Cloud.ProjectName != "" {
			attributesMap.PutStr(attr.CloudProjectName, event.Cloud.ProjectName)
		}
	}
	if event.Faas != nil {
		if event.Faas.TriggerRequestId != "" {
			attributesMap.PutStr(attr.TriggerRequestId, event.Faas.TriggerRequestId)
		}
		if event.Faas.Execution != "" {
			attributesMap.PutStr(attr.FaaSExecution, event.Faas.Execution)
		}
	}
}
