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

package elasticdynamictransform

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

const (
	// Type is the component type name
	Type = "elasticdynamictransform"

	// Stability level
	stability = component.StabilityLevelAlpha
)

// NewFactory creates a new processor factory
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType(Type),
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, stability),
	)
}

// createDefaultConfig creates the default configuration for the processor
func createDefaultConfig() component.Config {
	return &Config{
		ReloadInterval:     30 * time.Second,
		FallbackMode:       FallbackModePassthrough,
		InitialWaitTimeout: 5 * time.Second,
	}
}

// createLogsProcessor creates a logs processor based on this config
func createLogsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	pCfg := cfg.(*Config)

	return newDynamicTransformProcessor(ctx, set, pCfg, nextConsumer)
}
