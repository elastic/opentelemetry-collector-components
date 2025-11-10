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

package samplingdecide

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/pkg/samplingconfig"
)

type samplingDecideProcessor struct {
	config       *Config
	logger       *zap.Logger
	nextConsumer consumer.Logs
	condition    *ottl.Condition[ottllog.TransformContext]
	rand         *rand.Rand

	// Dynamic configuration support
	configProvider samplingconfig.Provider

	// Cache for compiled dynamic conditions
	mu             sync.RWMutex
	conditionCache map[string]*ottl.Condition[ottllog.TransformContext]
	ottlParser     ottl.Parser[ottllog.TransformContext]
}

var _ processor.Logs = (*samplingDecideProcessor)(nil)

func newSamplingDecideProcessor(
	config *Config,
	set processor.Settings,
	nextConsumer consumer.Logs,
	condition *ottl.Condition[ottllog.TransformContext],
) (*samplingDecideProcessor, error) {
	// Create OTTL parser for dynamic conditions
	ottlParser, err := ottllog.NewParser(
		make(map[string]ottl.Factory[ottllog.TransformContext]),
		set.TelemetrySettings,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTTL parser: %w", err)
	}

	return &samplingDecideProcessor{
		config:         config,
		logger:         set.Logger,
		nextConsumer:   nextConsumer,
		condition:      condition,
		rand:           rand.New(rand.NewSource(rand.Int63())),
		conditionCache: make(map[string]*ottl.Condition[ottllog.TransformContext]),
		ottlParser:     ottlParser,
	}, nil
}

func (p *samplingDecideProcessor) Start(_ context.Context, host component.Host) error {
	// Look up sampling config extension if configured
	if p.config.ExtensionName != "" {
		extensions := host.GetExtensions()
		for id, ext := range extensions {
			if id.String() == p.config.ExtensionName {
				if provider, ok := ext.(samplingconfig.Provider); ok {
					p.configProvider = provider
					p.logger.Info("Using dynamic sampling configuration",
						zap.String("extension", p.config.ExtensionName),
					)
					break
				}
			}
		}
		if p.configProvider == nil {
			p.logger.Warn("Sampling config extension not found, using static configuration",
				zap.String("extension", p.config.ExtensionName),
			)
		}
	}

	p.logger.Info("Sampling decide processor started",
		zap.String("condition", p.config.Condition),
		zap.Float64("sample_rate", p.config.SampleRate),
		zap.Bool("dynamic_config", p.configProvider != nil),
	)
	return nil
}

func (p *samplingDecideProcessor) Shutdown(_ context.Context) error {
	return nil
}

func (p *samplingDecideProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// getConditionAndRate returns the condition and sample rate to use for the given log.
// If dynamic config is available, it queries the extension for a matching rule.
// Otherwise, it returns the static configuration.
func (p *samplingDecideProcessor) getConditionAndRate(ctx context.Context, lr plog.LogRecord, rl plog.ResourceLogs) (*ottl.Condition[ottllog.TransformContext], float64, error) {
	// Try dynamic config first
	if p.configProvider != nil {
		// Extract stream name from attributes
		streamName := ""
		if streamAttr, ok := lr.Attributes().Get("stream.name"); ok {
			streamName = streamAttr.AsString()
		}

		// Convert resource attributes to map
		resourceAttrs := rl.Resource().Attributes().AsRaw()

		// Query for matching rule
		rule, err := p.configProvider.GetSamplingRule(ctx, streamName, resourceAttrs)
		if err != nil {
			p.logger.Warn("Failed to get sampling rule from extension", zap.Error(err))
		} else if rule != nil {
			// Get or compile condition for this rule
			cond, err := p.getOrCompileCondition(rule.Condition)
			if err != nil {
				p.logger.Warn("Failed to compile dynamic condition",
					zap.String("rule_id", rule.ID),
					zap.String("condition", rule.Condition),
					zap.Error(err),
				)
			} else {
				return cond, rule.SampleRate, nil
			}
		}
	}

	// Fall back to static config
	return p.condition, p.config.SampleRate, nil
}

// getOrCompileCondition gets a cached compiled condition or compiles a new one.
// Returns nil condition (no error) for empty condition strings, which means "always match".
func (p *samplingDecideProcessor) getOrCompileCondition(conditionStr string) (*ottl.Condition[ottllog.TransformContext], error) {
	// Empty condition means "always match" - return nil without error
	if conditionStr == "" {
		return nil, nil
	}

	p.mu.RLock()
	cond, ok := p.conditionCache[conditionStr]
	p.mu.RUnlock()

	if ok {
		return cond, nil
	}

	// Compile new condition
	parsedCond, err := p.ottlParser.ParseCondition(conditionStr)
	if err != nil {
		return nil, err
	}

	// Cache it
	p.mu.Lock()
	p.conditionCache[conditionStr] = parsedCond
	p.mu.Unlock()

	return parsedCond, nil
}

func (p *samplingDecideProcessor) ConsumeLogs(ctx context.Context, logs plog.Logs) error {
	// Create a new Logs structure to hold the filtered logs
	filteredLogs := plog.NewLogs()

	resourceLogs := logs.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		rl := resourceLogs.At(i)
		scopeLogs := rl.ScopeLogs()

		// Track if we need to create resource logs entry
		var filteredRL plog.ResourceLogs
		rlCreated := false

		for j := 0; j < scopeLogs.Len(); j++ {
			sl := scopeLogs.At(j)
			logRecords := sl.LogRecords()

			// Track if we need to create scope logs entry
			var filteredSL plog.ScopeLogs
			slCreated := false

			for k := 0; k < logRecords.Len(); k++ {
				lr := logRecords.At(k)

				// Get applicable condition and sample rate (static or dynamic)
				condition, sampleRate, err := p.getConditionAndRate(ctx, lr, rl)
				if err != nil {
					p.logger.Warn("Failed to get condition and rate", zap.Error(err))
					continue
				}

				// Create OTTL transform context
				tCtx := ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl)

				// Evaluate condition (nil condition means "always match")
				match := true
				if condition != nil {
					var err error
					match, err = condition.Eval(ctx, tCtx)
					if err != nil {
						p.logger.Warn("Failed to evaluate condition", zap.Error(err))
						continue
					}
				}

				// Apply invert logic
				shouldSample := match
				if p.config.InvertMatch {
					shouldSample = !match
				}

				// If condition doesn't match (after invert), drop the log
				if !shouldSample {
					continue
				}

				// Apply sample rate
				if p.rand.Float64() >= sampleRate {
					continue
				}

				// Log passed sampling decision - create containers if needed
				if !rlCreated {
					filteredRL = filteredLogs.ResourceLogs().AppendEmpty()
					rl.Resource().CopyTo(filteredRL.Resource())
					filteredRL.SetSchemaUrl(rl.SchemaUrl())
					rlCreated = true
				}

				if !slCreated {
					filteredSL = filteredRL.ScopeLogs().AppendEmpty()
					sl.Scope().CopyTo(filteredSL.Scope())
					filteredSL.SetSchemaUrl(sl.SchemaUrl())
					slCreated = true
				}

				lr.CopyTo(filteredSL.LogRecords().AppendEmpty())
			}
		}
	}

	// Only pass to next consumer if we have any logs left
	if filteredLogs.LogRecordCount() == 0 {
		return nil
	}

	return p.nextConsumer.ConsumeLogs(ctx, filteredLogs)
}
