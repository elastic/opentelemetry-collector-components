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

package pipeline

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

// startSignalPipelines starts all signal pipelines in the collection
func (m *Manager) startSignalPipelines(ctx context.Context, signalPipelines map[string]*SignalPipeline) error {
	for pipelineName, sp := range signalPipelines {
		m.logger.Info("Starting signal pipeline", zap.String("pipeline_name", pipelineName))

		// Start all receivers
		for _, receiver := range sp.Receivers {
			if err := receiver.Start(ctx, m.host); err != nil {
				return fmt.Errorf("failed to start receiver in pipeline %s: %w", pipelineName, err)
			}
		}

		// Start all processors
		for _, processor := range sp.Processors {
			if err := processor.Start(ctx, m.host); err != nil {
				return fmt.Errorf("failed to start processor in pipeline %s: %w", pipelineName, err)
			}
		}

		// Start all exporters
		for _, exporter := range sp.Exporters {
			if err := exporter.Start(ctx, m.host); err != nil {
				return fmt.Errorf("failed to start exporter in pipeline %s: %w", pipelineName, err)
			}
		}
	}
	return nil
}

// shutdownSignalPipelines shuts down all signal pipelines in the collection
func (m *Manager) shutdownSignalPipelines(ctx context.Context, signalPipelines map[string]*SignalPipeline) {
	for pipelineName, sp := range signalPipelines {
		m.logger.Info("Shutting down signal pipeline", zap.String("pipeline_name", pipelineName))

		// Shutdown receivers first to stop data flow
		for _, receiver := range sp.Receivers {
			if err := receiver.Shutdown(ctx); err != nil {
				m.logger.Error("Failed to shutdown receiver",
					zap.String("pipeline_name", pipelineName),
					zap.Error(err))
			}
		}

		// Shutdown processors
		for _, processor := range sp.Processors {
			if err := processor.Shutdown(ctx); err != nil {
				m.logger.Error("Failed to shutdown processor",
					zap.String("pipeline_name", pipelineName),
					zap.Error(err))
			}
		}

		// Shutdown exporters last
		for _, exporter := range sp.Exporters {
			if err := exporter.Shutdown(ctx); err != nil {
				m.logger.Error("Failed to shutdown exporter",
					zap.String("pipeline_name", pipelineName),
					zap.Error(err))
			}
		}
	}
}
