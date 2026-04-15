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

package enrichments // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments"

import (
	"context"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/enrichments/config"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/routing"
)

// LogEnricher enriches a single ResourceLogs for a specific mapping mode.
type LogEnricher interface {
	EnrichResourceLogs(ctx context.Context, rl plog.ResourceLogs)
}

// ecsLogEnricher contains the shared ECS log enrichment pipeline
// embedded by APMLogEnricher and OTelLogEnricher.
type ecsLogEnricher struct {
	enricher                       *Enricher
	hostIPEnabled                  bool
	serviceNameInDataStreamDataset bool
}

func (e *ecsLogEnricher) enrichResourceLogs(ctx context.Context, rl plog.ResourceLogs) {
	ecsPreProcessResource(ctx, rl.Resource(), routing.DataStreamTypeLogs, e.serviceNameInDataStreamDataset, e.hostIPEnabled)
	e.enricher.EnrichResourceLogs(rl)
}

// APMLogEnricher handles elastic APM intake log events in ECS mode.
type APMLogEnricher struct {
	ecsLogEnricher
}

func (e *APMLogEnricher) EnrichResourceLogs(ctx context.Context, rl plog.ResourceLogs) {
	e.enrichResourceLogs(ctx, rl)
}

// NewAPMLogEnricher creates a LogEnricher for elastic APM intake events.
func NewAPMLogEnricher(baseCfg config.Config, hostIPEnabled bool, serviceNameInDataStreamDataset bool) *APMLogEnricher {
	cfg := ecsAPMConfig(baseCfg)
	// disable the transaction result enrichment to avoid deriving a value
	// when the provided result is empty to match existing apm-data logic
	cfg.Transaction.Result.Enabled = false
	return &APMLogEnricher{
		ecsLogEnricher: ecsLogEnricher{
			enricher:                       NewEnricher(cfg),
			hostIPEnabled:                  hostIPEnabled,
			serviceNameInDataStreamDataset: serviceNameInDataStreamDataset,
		},
	}
}

// OTelLogEnricher handles elastic OTel log events in ECS mode.
type OTelLogEnricher struct {
	ecsLogEnricher
}

func (e *OTelLogEnricher) EnrichResourceLogs(ctx context.Context, rl plog.ResourceLogs) {
	e.enrichResourceLogs(ctx, rl)
}

// NewOTelLogEnricher creates a LogEnricher for elastic OTel events.
func NewOTelLogEnricher(baseCfg config.Config, hostIPEnabled bool, serviceNameInDataStreamDataset bool) *OTelLogEnricher {
	cfg := ecsOTelConfig(baseCfg)
	cfg.Log.TranslateUnsupportedAttributes.Enabled = true
	// disable the transaction result enrichment to avoid deriving a value
	// when the provided result is empty to match existing apm-data logic
	cfg.Transaction.Result.Enabled = false
	return &OTelLogEnricher{
		ecsLogEnricher: ecsLogEnricher{
			enricher:                       NewEnricher(cfg),
			hostIPEnabled:                  hostIPEnabled,
			serviceNameInDataStreamDataset: serviceNameInDataStreamDataset,
		},
	}
}

// DefaultLogEnricher handles non-ECS log events.
type DefaultLogEnricher struct {
	enricher *Enricher
}

func (e *DefaultLogEnricher) EnrichResourceLogs(_ context.Context, rl plog.ResourceLogs) {
	e.enricher.EnrichResourceLogs(rl)
}

// NewDefaultLogEnricher creates a LogEnricher for non-ECS log events.
func NewDefaultLogEnricher(baseCfg config.Config) *DefaultLogEnricher {
	return &DefaultLogEnricher{
		enricher: NewEnricher(baseCfg),
	}
}
