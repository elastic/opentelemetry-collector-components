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

package streamlangprocessor

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type logsProcessor struct {
	logger        *zap.Logger
	next          consumer.Logs
	backend       *ClosureBackend
	transportMode bool
}

func newLogsProcessor(logger *zap.Logger, cfg *Config, next consumer.Logs) (processor.Logs, error) {
	steps := FlattenDSLSteps(cfg.Pipeline.Steps, nil)
	backend := NewClosureBackend(Processors())
	if err := backend.Compile(steps); err != nil {
		return nil, err
	}

	return &logsProcessor{
		logger:        logger,
		next:          next,
		backend:       backend,
		transportMode: cfg.TransportMode,
	}, nil
}

func (p *logsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		resource := rl.Resource()
		slls := rl.ScopeLogs()
		for j := 0; j < slls.Len(); j++ {
			lrs := slls.At(j).LogRecords()

			dropSet := make(map[int]bool)
			for k := 0; k < lrs.Len(); k++ {
				record := lrs.At(k)
				var doc *Document
				if p.transportMode {
					doc = NewTransportDocument(record, resource)
				} else {
					doc = NewPdataDocument(record, resource)
				}

				err := p.backend.Execute(doc)
				if err != nil {
					if errors.Is(err, ErrDropDocument) {
						dropSet[k] = true
						continue
					}
					p.logger.Warn("streamlang processor error", zap.Error(err))
				}
			}

			if len(dropSet) > 0 {
				idx := 0
				lrs.RemoveIf(func(_ plog.LogRecord) bool {
					drop := dropSet[idx]
					idx++
					return drop
				})
			}
		}
	}
	return p.next.ConsumeLogs(ctx, ld)
}

func (p *logsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *logsProcessor) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (p *logsProcessor) Shutdown(_ context.Context) error {
	return nil
}
