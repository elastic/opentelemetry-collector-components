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

// SetElasticSpecificFieldsForSpan sets fields on spans that are not defined by OTel.
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

// SetElasticSpecificResourceAttributes maps APM event fields to OTel attributes at the resource level.
// The majority of the APM event fields are from the APM metadata model, so this mapping is applicable
// to all event types (OTel  signals).
// Some APM events may contain fields that are APM metadata e.g error.context.service.framework will override
// the framework provided in the metadata. The apm-data library handles the override, so this function simply
// sets the resource attribute.
// These fields that are not defined by OTel.
// Unlike fields from IntakeV2ToDerivedFields.go, these fields are not used by the UI.
func SetElasticSpecificResourceAttributes(event *modelpb.APMEvent, attributesMap pcommon.Map) {
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

	if event.Agent != nil {
		if event.Agent.EphemeralId != "" {
			attributesMap.PutStr(attr.AgentEphemeralId, event.Agent.EphemeralId)
		}
		if event.Agent.ActivationMethod != "" {
			attributesMap.PutStr(attr.AgentActivationMethod, event.Agent.ActivationMethod)
		}
	}

	if event.Service != nil {
		if event.Service.Language != nil {
			if event.Service.Language.Name != "" {
				attributesMap.PutStr(attr.ServiceLanguageName, event.Service.Language.Name)
			}
			if event.Service.Language.Version != "" {
				attributesMap.PutStr(attr.ServiceLanguageVersion, event.Service.Language.Version)
			}
		}
		if event.Service.Framework != nil {
			if event.Service.Framework.Name != "" {
				attributesMap.PutStr(attr.ServiceFrameworkName, event.Service.Framework.Name)
			}
			if event.Service.Framework.Version != "" {
				attributesMap.PutStr(attr.ServiceFrameworkVersion, event.Service.Framework.Version)
			}
		}
		if event.Service.Runtime != nil {
			if event.Service.Runtime.Name != "" {
				attributesMap.PutStr(attr.ServiceRuntimeName, event.Service.Runtime.Name)
			}
			if event.Service.Runtime.Version != "" {
				attributesMap.PutStr(attr.ServiceRuntimeVersion, event.Service.Runtime.Version)
			}
		}
		if event.Service.Origin != nil {
			if event.Service.Origin.Id != "" {
				attributesMap.PutStr(attr.ServiceOriginId, event.Service.Origin.Id)
			}
			if event.Service.Origin.Name != "" {
				attributesMap.PutStr(attr.ServiceOriginName, event.Service.Origin.Name)
			}
			if event.Service.Origin.Version != "" {
				attributesMap.PutStr(attr.ServiceOriginVersion, event.Service.Origin.Version)
			}
		}
		if event.Service.Target != nil {
			if event.Service.Target.Name != "" {
				attributesMap.PutStr(attr.ServiceTargetName, event.Service.Target.Name)
			}
			if event.Service.Target.Type != "" {
				attributesMap.PutStr(attr.ServiceTargetType, event.Service.Target.Type)
			}
		}
	}

	if event.Host != nil {
		if event.Host.Os != nil {
			if event.Host.Os.Platform != "" {
				attributesMap.PutStr(attr.HostOSPlatform, event.Host.Os.Platform)
			}
		}
	}
}

// SetElasticSpecificFieldsForLog sets fields that are not defined by OTel.
// Unlike fields from IntakeV2ToDerivedFields.go, these fields are not used by the UI.
func SetElasticSpecificFieldsForLog(event *modelpb.APMEvent, attributesMap pcommon.Map) {
	// processor.event is not set for logs
	if event.Log != nil {
		if event.Log.Logger != "" {
			attributesMap.PutStr(attr.LogLogger, event.Log.Logger)
		}

		if event.Log.Origin != nil {
			if event.Log.Origin.FunctionName != "" {
				attributesMap.PutStr(attr.LogOriginFunction, event.Log.Origin.FunctionName)
			}
			if event.Log.Origin.File != nil {
				if event.Log.Origin.File.Line != 0 {
					attributesMap.PutInt(attr.LogOriginFileLine, int64(event.Log.Origin.File.Line))
				}
				if event.Log.Origin.File.Name != "" {
					attributesMap.PutStr(attr.LogOriginFileName, event.Log.Origin.File.Name)
				}
			}
		}
	}

	if event.Event != nil {
		if event.Event.Action != "" {
			attributesMap.PutStr(attr.EventAction, event.Event.Action)
		}
		if event.Event.Dataset != "" {
			attributesMap.PutStr(attr.EventDataset, event.Event.Dataset)
		}
		if event.Event.Category != "" {
			attributesMap.PutStr(attr.EventCategory, event.Event.Category)
		}
		if event.Event.Type != "" {
			attributesMap.PutStr(attr.EventType, event.Event.Type)
		}
	}

	if event.Error == nil {
		attributesMap.PutStr(attr.EventKind, "event")
	}

	if event.Process != nil {
		if event.Process.Thread != nil {
			if event.Process.Thread.Name != "" {
				attributesMap.PutStr(attr.ProcessThreadName, event.Process.Thread.Name)
			}
		}
		if event.Process.Title != "" {
			attributesMap.PutStr(attr.ProcessTitle, event.Process.Title)
		}
	}

	if event.Session != nil {
		if event.Session.Id != "" {
			attributesMap.PutStr(attr.SessionID, event.Session.Id)
		}
	}
}
