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

package rawcapture

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/extension/rawsamplingbuffer"
	"github.com/elastic/opentelemetry-collector-components/extension/rawsamplingbuffer/buffer"
)

type rawCaptureProcessor struct {
	config       *Config
	logger       *zap.Logger
	nextConsumer consumer.Logs
	buffer       *buffer.CircularBuffer
}

var _ processor.Logs = (*rawCaptureProcessor)(nil)

func newRawCaptureProcessor(
	config *Config,
	set processor.Settings,
	nextConsumer consumer.Logs,
) (*rawCaptureProcessor, error) {
	return &rawCaptureProcessor{
		config:       config,
		logger:       set.Logger,
		nextConsumer: nextConsumer,
	}, nil
}

func (p *rawCaptureProcessor) Start(ctx context.Context, host component.Host) error {
	// Get the buffer extension
	ext, found := host.GetExtensions()[component.MustNewID(p.config.ExtensionName)]
	if !found {
		return fmt.Errorf("extension %q not found", p.config.ExtensionName)
	}

	bufferExt, ok := ext.(*rawsamplingbuffer.RawSamplingBufferExtension)
	if !ok {
		return fmt.Errorf("extension %q is not a rawsamplingbuffer extension", p.config.ExtensionName)
	}

	p.buffer = bufferExt.GetBuffer()
	p.logger.Info("Raw capture processor started", zap.String("extension", p.config.ExtensionName))
	return nil
}

func (p *rawCaptureProcessor) Shutdown(_ context.Context) error {
	return nil
}

func (p *rawCaptureProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *rawCaptureProcessor) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	resourceLogs := logs.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		rl := resourceLogs.At(i)
		scopeLogs := rl.ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			sl := scopeLogs.At(j)
			logRecords := sl.LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				lr := logRecords.At(k)

				// Generate UUID
				id := uuid.New().String()

				// Clone the log record for storage
				clonedLog := plog.NewLogs()
				clonedRL := clonedLog.ResourceLogs().AppendEmpty()
				rl.Resource().CopyTo(clonedRL.Resource())
				clonedSL := clonedRL.ScopeLogs().AppendEmpty()
				sl.Scope().CopyTo(clonedSL.Scope())
				clonedLR := clonedSL.LogRecords().AppendEmpty()
				lr.CopyTo(clonedLR)

				// Marshal to bytes
				marshaler := &plog.ProtoMarshaler{}
				data, err := marshaler.MarshalLogs(clonedLog)
				if err != nil {
					p.logger.Error("Failed to marshal log record", zap.Error(err))
					if !p.config.SkipOnError {
						return fmt.Errorf("failed to marshal log record: %w", err)
					}
					continue
				}

				// Store in buffer
				if err := p.buffer.Store(id, data); err != nil {
					p.logger.Error("Failed to store log in buffer",
						zap.Error(err),
						zap.String("id", id),
					)
					if !p.config.SkipOnError {
						return fmt.Errorf("failed to store log in buffer: %w", err)
					}
					continue
				}

				// Add UUID as attribute
				lr.Attributes().PutStr(p.config.AttributeKey, id)
			}
		}
	}

	return p.nextConsumer.ConsumeLogs(ctx, logs)
}
