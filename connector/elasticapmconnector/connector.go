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
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"

	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/signaltometricsconnector"

	"github.com/elastic/opentelemetry-collector-components/internal/agentname"
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
	baseConsumer, err := signaltometricsFactory.CreateLogsToMetrics(ctx, set, c.cfg.signaltometricsConfig(), c.lsminterval)
	if err != nil {
		return nil, err
	}
	// Wrap the base consumer to derive agent.name: the connector may be used
	// without elasticapmprocessor in the pipeline, or with skip_enrichment
	// enabled, in which case agent.name is not set upstream for this signal.
	return &logsResourceEnricher{next: baseConsumer}, nil
}

func (c *elasticapmConnector) newMetricsConsumer(ctx context.Context) (consumer.Metrics, error) {
	set := c.signaltometricsSettings()
	baseConsumer, err := signaltometricsFactory.CreateMetricsToMetrics(ctx, set, c.cfg.signaltometricsConfig(), c.lsminterval)
	if err != nil {
		return nil, err
	}
	// Wrap the base consumer to derive agent.name: the connector may be used
	// without elasticapmprocessor in the pipeline, or with skip_enrichment
	// enabled, in which case agent.name is not set upstream for this signal.
	return &metricsResourceEnricher{next: baseConsumer}, nil
}

func (c *elasticapmConnector) newTracesToMetrics(ctx context.Context) (consumer.Traces, error) {
	set := c.signaltometricsSettings()
	baseConsumer, err := signaltometricsFactory.CreateTracesToMetrics(ctx, set, c.cfg.signaltometricsConfig(), c.lsminterval)
	if err != nil {
		return nil, err
	}
	// Wrap the base consumer to enrich spans
	return &spanEnricher{next: baseConsumer}, nil
}

// spanEnricher wraps a traces consumer to add the
// 'transaction.root' and `span.name` attributes.
// These attributes are needed for transaction and span destination metrics.
type spanEnricher struct {
	next consumer.Traces
}

// ConsumeTraces iterates through all spans to set attributes
// required to correctly generate metrics.
// Forwards the traces to the next consumer.
func (e *spanEnricher) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				span.Attributes().PutBool("transaction.root", span.ParentSpanID().IsEmpty())
				span.Attributes().PutStr("span.name", span.Name())
			}
		}
	}
	return e.next.ConsumeTraces(ctx, td)
}

func (e *spanEnricher) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// metricsResourceEnricher wraps a metrics consumer to derive the agent.name
// resource attribute via agentname.Derive before aggregation.
type metricsResourceEnricher struct {
	next consumer.Metrics
}

func (e *metricsResourceEnricher) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		setAgentName(rms.At(i).Resource())
	}
	return e.next.ConsumeMetrics(ctx, md)
}

func (e *metricsResourceEnricher) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// logsResourceEnricher wraps a logs consumer to derive the agent.name
// resource attribute via agentname.Derive before aggregation.
type logsResourceEnricher struct {
	next consumer.Logs
}

func (e *logsResourceEnricher) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		setAgentName(rls.At(i).Resource())
	}
	return e.next.ConsumeLogs(ctx, ld)
}

func (e *logsResourceEnricher) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

const (
	attrAgentName   = "agent.name"
	attrSDKName     = "telemetry.sdk.name"
	attrSDKLanguage = "telemetry.sdk.language"
	attrDistroName  = "telemetry.distro.name"
)

// setAgentName derives the agent.name resource attribute using agentname.Derive.
// It is a no-op if agent.name is already set, e.g. by a classic Elastic APM agent,
// preserving the existing value (including empty string) to match the behaviour of
// the attribute.PutStr guard used by elasticapmprocessor.
func setAgentName(resource pcommon.Resource) {
	attrs := resource.Attributes()
	if _, ok := attrs.Get(attrAgentName); ok {
		return
	}
	sdkName, _ := attrs.Get(attrSDKName)
	sdkLanguage, _ := attrs.Get(attrSDKLanguage)
	distroName, _ := attrs.Get(attrDistroName)
	attrs.PutStr(attrAgentName, agentname.Derive(sdkName.Str(), sdkLanguage.Str(), distroName.Str()))
}

func (c *elasticapmConnector) signaltometricsSettings() connector.Settings {
	signaltometricsSettings := c.set
	signaltometricsSettings.ID = component.NewIDWithName(signaltometricsFactory.Type(), c.set.ID.Name())
	return signaltometricsSettings
}
