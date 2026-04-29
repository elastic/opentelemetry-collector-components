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

package entityanalyticsreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/entityanalyticsreceiver"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/elastic/entcollect"
)

// newPublisher returns an entcollect.Publisher that converts each
// Document to a plog.Logs and pushes it to the consumer. Each
// document is published individually (not batched) so that memory
// use stays proportional to a single document during large syncs.
//
// The log record uses bodymap mapping mode: the entire ECS document
// is placed in the log record body, and the scope attribute
// "elastic.mapping.mode" is set to "bodymap". This tells the
// elasticsearch exporter to use the body as the ES document directly,
// stripping OTLP attributes from the final document. Only routing
// metadata (document ID) goes in attributes.
//
// See https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/35444
func newPublisher(cons consumer.Logs, provider, scopeName, scopeVersion string) entcollect.Publisher {
	return func(ctx context.Context, doc entcollect.Document) error {
		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()

		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName(scopeName)
		sl.Scope().SetVersion(scopeVersion)
		sl.Scope().Attributes().PutStr("elastic.mapping.mode", "bodymap")

		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(doc.Timestamp))

		if doc.ID != "" {
			lr.Attributes().PutStr("elasticsearch.document_id", doc.ID)
		}

		kind := "asset"
		if doc.Action == entcollect.ActionDeleted {
			kind = "event"
		}

		body := make(map[string]any)
		for k, v := range doc.Fields {
			body[k] = v
		}
		body["event.action"] = doc.Action.String()
		body["event.kind"] = kind
		body["asset.type"] = doc.Kind.String()
		body["asset.id"] = doc.ID
		body["labels.identity_source"] = provider

		if err := lr.Body().SetEmptyMap().FromRaw(body); err != nil {
			return fmt.Errorf("encoding document body: %w", err)
		}
		return cons.ConsumeLogs(ctx, logs)
	}
}
