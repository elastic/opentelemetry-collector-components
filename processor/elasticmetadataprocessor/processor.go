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

package elasticmetadataprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/elasticmetadataprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

var _ processor.Logs = (*logsProcessor)(nil)

type logsProcessor struct {
	component.StartFunc
	component.ShutdownFunc

	next   consumer.Logs
	logger *zap.Logger
	cfg    *Config
}

func newLogsProcessor(logger *zap.Logger, cfg *Config, next consumer.Logs) *logsProcessor {
	return &logsProcessor{
		logger: logger,
		cfg:    cfg,
		next:   next,
	}
}
func (l *logsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (l *logsProcessor) ConsumeLogs(ctx context.Context, log plog.Logs) error {
	mapping := l.cfg.LogBodyFields
	if len(mapping) == 0 {
		return l.next.ConsumeLogs(ctx, log)
	}

	ctxMetadata := client.FromContext(ctx).Metadata

	// Extract the relevant metadata based on the mapping configuration and prepare it for insertion into the log body.
	metadata := make(map[string]string, len(mapping))
	for mdKey, logKey := range mapping {
		if val := ctxMetadata.Get(mdKey); len(val) > 0 {
			metadata[logKey] = val[0]
		}
	}

	if len(metadata) == 0 {
		// No relevant metadata found, pass through the logs without modification.
		return l.next.ConsumeLogs(ctx, log)
	}

	for _, rl := range log.ResourceLogs().All() {
		for _, sl := range rl.ScopeLogs().All() {
			for _, lr := range sl.LogRecords().All() {
				// Add metadata to body if it's a map
				if lr.Body().Type() == pcommon.ValueTypeMap {
					bodyMap := lr.Body().Map()
					for k, v := range metadata {
						bodyMap.PutStr(k, v)
					}
				}
			}
		}
	}

	return l.next.ConsumeLogs(ctx, log)
}
