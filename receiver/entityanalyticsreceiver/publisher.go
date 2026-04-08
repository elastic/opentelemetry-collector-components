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
// document is published individually (not batched) to bound memory
// use during large syncs.
//
// The mapping here differs from the beats entity analytics input in
// two ways:
//
//   - event.action uses bare values ("deleted", "discovered",
//     "modified") rather than prefixed values ("user-deleted",
//     "device-discovered"). The entity kind is carried separately
//     in asset.type.
//   - event.kind distinguishes deletions ("event") from upserts
//     ("asset"). The beats path emits no event.kind; the ingest
//     pipeline sets it unconditionally to "asset".
//
// Both differences are resolved by the ingest pipeline. If the
// pipeline is absent, the output schema will differ from beats.
func newPublisher(cons consumer.Logs, provider, scopeName, scopeVersion string) entcollect.Publisher {
	return func(ctx context.Context, doc entcollect.Document) error {
		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("asset.id", doc.ID)

		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName(scopeName)
		sl.Scope().SetVersion(scopeVersion)
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(doc.Timestamp))

		lr.Attributes().PutStr("event.action", doc.Action.String())
		kind := "asset"
		if doc.Action == entcollect.ActionDeleted {
			kind = "event"
		}
		lr.Attributes().PutStr("event.kind", kind)
		lr.Attributes().PutStr("asset.type", doc.Kind.String())
		lr.Attributes().PutStr("labels.identity_source", provider)

		if doc.Fields != nil {
			if err := lr.Body().SetEmptyMap().FromRaw(doc.Fields); err != nil {
				return fmt.Errorf("encoding document body: %w", err)
			}
		}
		return cons.ConsumeLogs(ctx, logs)
	}
}
