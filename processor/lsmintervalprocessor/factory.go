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

package lsmintervalprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
)

// NewFactory returns a new factory for the Metrics Generation processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, metadata.MetricsStability))
}

func createDefaultConfig() component.Config {
	return &Config{
		Intervals: []IntervalConfig{
			{Duration: 60 * time.Second},
		},
	}
}

func createMetricsProcessor(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	processorConfig, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsing error")
	}

	intervalDefs := make([]intervalDef, 0, len(processorConfig.Intervals))
	for _, ivl := range processorConfig.Intervals {
		ivlDef := intervalDef{Duration: ivl.Duration}
		if len(ivl.Statements) > 0 {
			parser, err := ottldatapoint.NewParser(
				ottlfuncs.StandardFuncs[ottldatapoint.TransformContext](),
				set.TelemetrySettings,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to create ottl parser for interval %s: %w",
					ivl.Duration, err,
				)
			}
			statements, err := parser.ParseStatements(ivl.Statements)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to parse ottl statements for interval %s: %w",
					ivl.Duration, err,
				)
			}
			statementSeqs := ottldatapoint.NewStatementSequence(
				statements,
				set.TelemetrySettings,
				ottldatapoint.WithStatementSequenceErrorMode(ottl.PropagateError),
			)
			ivlDef.Statements = &statementSeqs
		}
		intervalDefs = append(intervalDefs, ivlDef)
	}

	return newProcessor(processorConfig, intervalDefs, set.Logger, nextConsumer)
}

type intervalDef struct {
	Duration   time.Duration
	Statements *ottl.StatementSequence[ottldatapoint.TransformContext]
}
