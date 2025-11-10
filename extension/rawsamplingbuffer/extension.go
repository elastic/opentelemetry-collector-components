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

package rawsamplingbuffer

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/extension/rawsamplingbuffer/buffer"
)

// RawSamplingBufferExtension implements the extension that provides a shared buffer.
type RawSamplingBufferExtension struct {
	config *Config
	logger *zap.Logger
	buffer *buffer.CircularBuffer
	cancel context.CancelFunc
}

var _ extension.Extension = (*RawSamplingBufferExtension)(nil)

// newRawSamplingBufferExtension creates a new instance of the extension.
func newRawSamplingBufferExtension(config *Config, set extension.Settings) *RawSamplingBufferExtension {
	return &RawSamplingBufferExtension{
		config: config,
		logger: set.TelemetrySettings.Logger,
	}
}

// Start implements extension.Extension.
func (e *RawSamplingBufferExtension) Start(ctx context.Context, _ component.Host) error {
	e.buffer = buffer.NewCircularBuffer(
		e.config.BufferSize,
		e.config.MaxEntrySize,
		e.config.TTL,
	)

	// Start background cleanup goroutine
	cleanupCtx, cancel := context.WithCancel(ctx)
	e.cancel = cancel

	go e.cleanupLoop(cleanupCtx)

	e.logger.Info("Raw sampling buffer extension started",
		zap.Int("buffer_size", e.config.BufferSize),
		zap.Int("max_entry_size", e.config.MaxEntrySize),
		zap.Duration("ttl", e.config.TTL),
	)

	return nil
}

// Shutdown implements extension.Extension.
func (e *RawSamplingBufferExtension) Shutdown(_ context.Context) error {
	if e.cancel != nil {
		e.cancel()
	}
	e.logger.Info("Raw sampling buffer extension stopped")
	return nil
}

// GetBuffer returns the circular buffer for processors to use.
func (e *RawSamplingBufferExtension) GetBuffer() *buffer.CircularBuffer {
	return e.buffer
}

// cleanupLoop periodically cleans expired entries from the buffer.
func (e *RawSamplingBufferExtension) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(e.config.TTL / 2) // Clean at half the TTL interval
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			removed := e.buffer.CleanExpired()
			if removed > 0 {
				e.logger.Debug("Cleaned expired entries from buffer",
					zap.Int("removed", removed),
				)
			}
		}
	}
}
