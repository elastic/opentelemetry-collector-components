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

package elastic

import (
	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/internal/attribute"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/internal/elastic/mobile"
	"go.opentelemetry.io/collector/pdata/plog"
)

func EnrichLog(resourceAttrs map[string]any, log plog.LogRecord, cfg config.Config) {
	if cfg.Log.ProcessorEvent.Enabled {
		attribute.PutStr(log.Attributes(), elasticattr.ProcessorEvent, "log")
	}
	eventName, ok := getEventName(log)
	if ok {
		ctx := mobile.EventContext{
			ResourceAttributes: resourceAttrs,
			EventName:          eventName,
		}
		mobile.EnrichLogEvent(ctx, log)
	}
}

// getEventName returns the event name from the log record.
// If the event name is not set, it returns an empty string.
func getEventName(logRecord plog.LogRecord) (string, bool) {
	if logRecord.EventName() != "" {
		return logRecord.EventName(), true
	}
	attributeValue, ok := logRecord.Attributes().Get("event.name")
	if ok {
		return attributeValue.AsString(), true
	}
	return "", false
}
