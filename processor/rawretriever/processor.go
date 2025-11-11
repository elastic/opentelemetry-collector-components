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

package rawretriever

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/extension/rawsamplingbuffer"
	"github.com/elastic/opentelemetry-collector-components/extension/rawsamplingbuffer/buffer"
)

type rawRetrieverProcessor struct {
	config       *Config
	logger       *zap.Logger
	nextConsumer consumer.Logs
	buffer       *buffer.CircularBuffer
}

var _ processor.Logs = (*rawRetrieverProcessor)(nil)

func newRawRetrieverProcessor(
	config *Config,
	set processor.Settings,
	nextConsumer consumer.Logs,
) (*rawRetrieverProcessor, error) {
	return &rawRetrieverProcessor{
		config:       config,
		logger:       set.Logger,
		nextConsumer: nextConsumer,
	}, nil
}

func (p *rawRetrieverProcessor) Start(ctx context.Context, host component.Host) error {
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
	p.logger.Info("Raw retriever processor started", zap.String("extension", p.config.ExtensionName))
	return nil
}

func (p *rawRetrieverProcessor) Shutdown(_ context.Context) error {
	return nil
}

func (p *rawRetrieverProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *rawRetrieverProcessor) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	// Create a new Logs structure to hold the retrieved raw logs
	retrievedLogs := plog.NewLogs()

	totalLogs := logs.LogRecordCount()
	p.logger.Info("Raw retriever processing batch", zap.Int("total_logs", totalLogs))

	resourceLogs := logs.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		rl := resourceLogs.At(i)
		scopeLogs := rl.ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			sl := scopeLogs.At(j)
			logRecords := sl.LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				lr := logRecords.At(k)

				// Get stream.name for logging
				processedStreamName := ""
				if streamAttr, ok := lr.Attributes().Get("stream.name"); ok {
					processedStreamName = streamAttr.AsString()
				}

				// Get UUID from attributes
				idValue, found := lr.Attributes().Get(p.config.AttributeKey)
				if !found {
					p.logger.Warn("UUID attribute not found in log record",
						zap.String("attribute_key", p.config.AttributeKey),
						zap.String("processed_stream_name", processedStreamName),
					)
					if p.config.OnRetrievalError == "error" {
						return fmt.Errorf("UUID attribute %q not found", p.config.AttributeKey)
					} else if p.config.OnRetrievalError == "keep_processed" {
						// Add the processed log to retrieved logs
						p.addLogToRetrieved(&retrievedLogs, rl, sl, lr)
					}
					// If "drop", do nothing (log is dropped)
					continue
				}

				id := idValue.Str()

				p.logger.Info("Retrieving raw log",
					zap.String("id", id),
					zap.String("processed_stream_name", processedStreamName),
				)

				// Retrieve raw log from buffer
				rawData, err := p.buffer.Retrieve(id)
				if err != nil {
					p.logger.Warn("Failed to retrieve raw log from buffer",
						zap.Error(err),
						zap.String("id", id),
						zap.String("processed_stream_name", processedStreamName),
					)
					if p.config.OnRetrievalError == "error" {
						return fmt.Errorf("failed to retrieve raw log: %w", err)
					} else if p.config.OnRetrievalError == "keep_processed" {
						// Add the processed log to retrieved logs
						p.addLogToRetrieved(&retrievedLogs, rl, sl, lr)
					}
					// If "drop", do nothing (log is dropped)
					continue
				}

				// Unmarshal raw log
				unmarshaler := &plog.ProtoUnmarshaler{}
				rawLog, err := unmarshaler.UnmarshalLogs(rawData)
				if err != nil {
					p.logger.Error("Failed to unmarshal raw log",
						zap.Error(err),
						zap.String("id", id),
						zap.String("processed_stream_name", processedStreamName),
					)
					if p.config.OnRetrievalError == "error" {
						return fmt.Errorf("failed to unmarshal raw log: %w", err)
					} else if p.config.OnRetrievalError == "keep_processed" {
						// Add the processed log to retrieved logs
						p.addLogToRetrieved(&retrievedLogs, rl, sl, lr)
					}
					// If "drop", do nothing (log is dropped)
					continue
				}

				// Remove UUID attribute if configured and preserve stream.name
				if rawLog.ResourceLogs().Len() > 0 {
					rawRL := rawLog.ResourceLogs().At(0)
					if rawRL.ScopeLogs().Len() > 0 {
						rawSL := rawRL.ScopeLogs().At(0)
						if rawSL.LogRecords().Len() > 0 {
							rawLR := rawSL.LogRecords().At(0)

							// Get original stream.name from raw log (if any)
							rawStreamName := ""
							if rawStreamAttr, ok := rawLR.Attributes().Get("stream.name"); ok {
								rawStreamName = rawStreamAttr.AsString()
							}

							if p.config.RemoveAttribute {
								rawLR.Attributes().Remove(p.config.AttributeKey)
							}

							// Preserve stream.name from the processed log
							if streamName, found := lr.Attributes().Get("target_stream"); found {
								rawLR.Attributes().PutStr("target_stream", streamName.Str())
								p.logger.Info("Preserved target_stream attribute",
									zap.String("id", id),
									zap.String("raw_stream_name", rawStreamName),
									zap.String("processed_stream_name", processedStreamName),
									zap.String("final_stream_name", streamName.Str()),
								)
							} else {
								p.logger.Info("No target_stream to preserve",
									zap.String("id", id),
									zap.String("raw_stream_name", rawStreamName),
								)
							}

							// Add the raw log to retrieved logs
							p.addLogToRetrieved(&retrievedLogs, rawRL, rawSL, rawLR)
						}
					}
				}
			}
		}
	}

	// Only pass to next consumer if we have any logs
	retrievedCount := retrievedLogs.LogRecordCount()
	if retrievedCount == 0 {
		p.logger.Info("No logs retrieved", zap.Int("total_logs", totalLogs))
		return nil
	}

	p.logger.Info("Sending retrieved raw logs",
		zap.Int("retrieved_logs", retrievedCount),
		zap.Int("total_logs", totalLogs),
	)

	return p.nextConsumer.ConsumeLogs(ctx, retrievedLogs)
}

// addLogToRetrieved adds a log record to the retrieved logs structure.
func (p *rawRetrieverProcessor) addLogToRetrieved(
	retrievedLogs *plog.Logs,
	sourceRL plog.ResourceLogs,
	sourceSL plog.ScopeLogs,
	sourceLR plog.LogRecord,
) {
	// Find or create resource logs
	var destRL plog.ResourceLogs
	found := false
	for i := 0; i < retrievedLogs.ResourceLogs().Len(); i++ {
		rl := retrievedLogs.ResourceLogs().At(i)
		if sourceRL.Resource().Attributes().AsRaw() != nil &&
			resourceEquals(sourceRL.Resource(), rl.Resource()) {
			destRL = rl
			found = true
			break
		}
	}
	if !found {
		destRL = retrievedLogs.ResourceLogs().AppendEmpty()
		sourceRL.Resource().CopyTo(destRL.Resource())
	}

	// Find or create scope logs
	var destSL plog.ScopeLogs
	found = false
	for i := 0; i < destRL.ScopeLogs().Len(); i++ {
		sl := destRL.ScopeLogs().At(i)
		if sourceSL.Scope().Name() == sl.Scope().Name() {
			destSL = sl
			found = true
			break
		}
	}
	if !found {
		destSL = destRL.ScopeLogs().AppendEmpty()
		sourceSL.Scope().CopyTo(destSL.Scope())
	}

	// Add log record
	sourceLR.CopyTo(destSL.LogRecords().AppendEmpty())
}

// resourceEquals checks if two resources are equal (simplified comparison).
func resourceEquals(r1, r2 pcommon.Resource) bool {
	return r1.Attributes().Len() == r2.Attributes().Len()
}
