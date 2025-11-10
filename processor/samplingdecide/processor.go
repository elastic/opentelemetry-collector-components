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
	"math/rand"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type samplingDecideProcessor struct {
	config       *Config
	logger       *zap.Logger
	nextConsumer consumer.Logs
	condition    *ottl.Condition[ottllog.TransformContext]
	rand         *rand.Rand
}

var _ processor.Logs = (*samplingDecideProcessor)(nil)

func newSamplingDecideProcessor(
	config *Config,
	set processor.Settings,
	nextConsumer consumer.Logs,
	condition *ottl.Condition[ottllog.TransformContext],
) (*samplingDecideProcessor, error) {
	return &samplingDecideProcessor{
		config:       config,
		logger:       set.Logger,
		nextConsumer: nextConsumer,
		condition:    condition,
		rand:         rand.New(rand.NewSource(rand.Int63())),
	}, nil
}

func (p *samplingDecideProcessor) Start(_ context.Context, _ component.Host) error {
	p.logger.Info("Sampling decide processor started",
		zap.String("condition", p.config.Condition),
		zap.Float64("sample_rate", p.config.SampleRate),
	)
	return nil
}

func (p *samplingDecideProcessor) Shutdown(_ context.Context) error {
	return nil
}

func (p *samplingDecideProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
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

				// Create OTTL transform context
				tCtx := ottllog.NewTransformContext(lr, sl.Scope(), rl.Resource(), sl, rl)

				// Evaluate condition
				match, err := p.condition.Eval(ctx, tCtx)
				if err != nil {
					p.logger.Warn("Failed to evaluate condition", zap.Error(err))
					continue
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
				if p.rand.Float64() >= p.config.SampleRate {
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
