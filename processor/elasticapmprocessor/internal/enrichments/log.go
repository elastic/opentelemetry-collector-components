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

package enrichments // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments"

import (
	"crypto/md5"
	"encoding/hex"
	"io"

	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/ecs"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/attribute"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/mobile"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"
	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	// emptyExceptionMsg is used to replace an empty exception message only in ecs mode.
	emptyExceptionMsg = "[EMPTY]"
)

func EnrichLog(resourceAttrs map[string]any, log plog.LogRecord, cfg config.Config) {
	if cfg.Log.TranslateUnsupportedAttributes.Enabled {
		ecs.TranslateLogRecordAttributes(log.Attributes())
	}
	ctx := mobile.NewEventContext(resourceAttrs, log)

	if ctx.EventName != "" {
		mobile.EnrichLogEvent(ctx, log, cfg)
	}

	if routing.IsErrorEvent(log.Attributes()) {
		EnrichLogError(log, cfg)
	}

	if ctx.Action == "crash" || routing.IsErrorEvent(log.Attributes()) {
		routing.EncodeErrorDataStream(log.Attributes(), routing.DataStreamTypeLogs)
	}
}

// EnrichLogError enriches the log record with error attributes.
// exists early if the are no exceptions or error messages.
func EnrichLogError(logRecord plog.LogRecord, cfg config.Config) {
	attributes := logRecord.Attributes()
	ec := getErrorEventContext(attributes)

	if ec.exceptionType == "" && ec.exceptionMessage == "" {
		return
	}

	if ec.exceptionMessage == "" {
		ec.exceptionMessage = emptyExceptionMsg
	}

	if cfg.Log.ErrorConfig.ErrorID.Enabled {
		if id, err := attribute.NewErrorID(); err == nil {
			attribute.PutStr(attributes, elasticattr.ErrorID, id)
		}
	}

	if cfg.Log.ErrorExceptionConfig.ErrorExceptionHandled.Enabled {
		attribute.PutBool(attributes, elasticattr.ErrorExceptionHandled, !ec.exceptionEscaped)
	}
	if cfg.Log.ErrorExceptionConfig.ErrorExceptionMessage.Enabled {
		attribute.PutStr(attributes, "error.exception.message", ec.exceptionMessage)
	}
	if cfg.Log.ErrorExceptionConfig.ErrorExceptionType.Enabled && ec.exceptionType != "" {
		attribute.PutStr(attributes, "error.exception.type", ec.exceptionType)
	}
	if cfg.Log.ErrorConfig.ErrorStackTrace.Enabled && ec.exceptionStacktrace != "" {
		attribute.PutStr(attributes, elasticattr.ErrorStackTrace, ec.exceptionStacktrace)
	}

	// Note: this is a fallback to set the error.grouping_key attribute.
	// For mobile crash events, the error.grouping_key is set by the mobile.EnrichLogEvent function.
	if cfg.Log.ErrorConfig.ErrorGroupingKey.Enabled {
		if key := getGenericErrorGroupingKey(ec); key != "" {
			attribute.PutStr(attributes, elasticattr.ErrorGroupingKey, key)
		}
	}

	if cfg.Log.ErrorConfig.TimestampUs.Enabled {
		ts := logRecord.Timestamp()
		if ts == 0 {
			ts = logRecord.ObservedTimestamp()
		}
		attribute.PutInt(attributes, elasticattr.TimestampUs, attribute.ToTimestampUS(ts))
	}

	if cfg.Log.EventConfig.EventKind.Enabled {
		attribute.PutStr(attributes, elasticattr.EventKind, "event")
	}

	if cfg.Log.EventConfig.EventType.Enabled {
		attribute.PutStr(attributes, elasticattr.EventType, "error")
	}
}

func getGenericErrorGroupingKey(ec errorEventContext) string {
	hash := md5.New()
	if ec.exceptionType != "" {
		_, _ = io.WriteString(hash, ec.exceptionType)
	}
	if ec.exceptionMessage != "" {
		_, _ = io.WriteString(hash, ec.exceptionMessage)
	}
	if ec.exceptionStacktrace != "" {
		_, _ = io.WriteString(hash, ec.exceptionStacktrace)
	}
	return hex.EncodeToString(hash.Sum(nil))
}
