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

package elasticapmconnector // import "github.com/elastic/opentelemetry-collector-components/connector/elasticapmconnector"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/signaltometricsconnector"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor"
)

var (
	lsmintervalFactory     = lsmintervalprocessor.NewFactory()
	signaltometricsFactory = signaltometricsconnector.NewFactory()
)

type elasticapmConnector struct {
	cfg         *Config
	set         connector.Settings
	lsminterval processor.Metrics
}

func newElasticAPMConnector(
	ctx context.Context,
	cfg *Config,
	set connector.Settings,
	nextConsumer consumer.Metrics,
) (*elasticapmConnector, error) {
	lsmintervalSettings := processor.Settings{
		ID:                set.ID,
		TelemetrySettings: set.TelemetrySettings,
		BuildInfo:         set.BuildInfo,
	}
	lsmintervalSettings.ID = component.NewIDWithName(lsmintervalFactory.Type(), set.ID.Name())
	lsminterval, err := lsmintervalFactory.CreateMetrics(
		ctx,
		lsmintervalSettings,
		cfg.lsmConfig(),
		nextConsumer,
	)
	if err != nil {
		return nil, err
	}
	return &elasticapmConnector{
		cfg:         cfg,
		set:         set,
		lsminterval: lsminterval,
	}, nil
}

func (c *elasticapmConnector) Start(ctx context.Context, host component.Host) error {
	return c.lsminterval.Start(ctx, host)
}

func (c *elasticapmConnector) Shutdown(ctx context.Context) error {
	return c.lsminterval.Shutdown(ctx)
}

func (c *elasticapmConnector) newLogsConsumer(ctx context.Context) (consumer.Logs, error) {
	set := c.signaltometricsSettings()
	return signaltometricsFactory.CreateLogsToMetrics(ctx, set, c.cfg.signaltometricsConfig(), c.lsminterval)
}

func (c *elasticapmConnector) newMetricsConsumer(ctx context.Context) (consumer.Metrics, error) {
	set := c.signaltometricsSettings()
	return signaltometricsFactory.CreateMetricsToMetrics(ctx, set, c.cfg.signaltometricsConfig(), c.lsminterval)
}

func (c *elasticapmConnector) newTracesToMetrics(ctx context.Context) (consumer.Traces, error) {
	set := c.signaltometricsSettings()
	baseConsumer, err := signaltometricsFactory.CreateTracesToMetrics(ctx, set, c.cfg.signaltometricsConfig(), c.lsminterval)
	if err != nil {
		return nil, err
	}
	// Wrap the base consumer to enrich spans with transaction.root attribute
	return &transactionRootEnricher{next: baseConsumer}, nil
}

// transactionRootEnricher wraps a traces consumer to add the 'transaction.root'
// boolean attribute which is true when the span has a ParentSpanID.
type transactionRootEnricher struct {
	next consumer.Traces
}

// ConsumeTraces iterates through all spans and sets the 'transaction.root'
// attribute to true if the span has no parent (ParentSpanID is empty).
// Forwards the traces to the next consumer.
func (e *transactionRootEnricher) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				span.Attributes().PutBool("transaction.root", span.ParentSpanID().IsEmpty())
			}
		}
	}
	return e.next.ConsumeTraces(ctx, td)
}

func (e *transactionRootEnricher) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (c *elasticapmConnector) signaltometricsSettings() connector.Settings {
	signaltometricsSettings := c.set
	signaltometricsSettings.ID = component.NewIDWithName(signaltometricsFactory.Type(), c.set.ID.Name())
	return signaltometricsSettings
}
