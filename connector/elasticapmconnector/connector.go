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
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
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
	baseConsumer, err := signaltometricsFactory.CreateLogsToMetrics(ctx, set, c.cfg.signaltometricsConfig(), c.lsminterval)
	if err != nil {
		return nil, err
	}
	// Wrap the base consumer to derive agent.name, since it isn't set by
	// elasticapmprocessor for the logs signal.
	return &logsResourceEnricher{next: baseConsumer}, nil
}

func (c *elasticapmConnector) newMetricsConsumer(ctx context.Context) (consumer.Metrics, error) {
	set := c.signaltometricsSettings()
	baseConsumer, err := signaltometricsFactory.CreateMetricsToMetrics(ctx, set, c.cfg.signaltometricsConfig(), c.lsminterval)
	if err != nil {
		return nil, err
	}
	// Wrap the base consumer to derive agent.name, since it isn't set by
	// elasticapmprocessor for the metrics signal.
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
// resource attribute, mirroring what elasticapmprocessor does for traces.
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
// resource attribute, mirroring what elasticapmprocessor does for traces.
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

// setAgentName derives the agent.name resource attribute from
// telemetry.sdk.name, telemetry.sdk.language and telemetry.distro.name,
// the same way elasticapmprocessor does for traces (see setAgentName in
// processor/elasticapmprocessor/internal/enrichments/resource.go; keep the
// two in sync). It is a no-op if agent.name is already set, e.g. by a
// classic Elastic APM agent. Otherwise it always sets a value, defaulting
// to "otlp" when no telemetry.* attributes are present, so that a service
// never ends up with a different agent.name on its traces than on its
// metrics/logs-derived aggregates.
func setAgentName(resource pcommon.Resource) {
	attrs := resource.Attributes()
	if _, ok := attrs.Get("agent.name"); ok {
		return
	}

	var sdkName, sdkLanguage, distroName string
	attrs.Range(func(k string, v pcommon.Value) bool {
		switch k {
		case "telemetry.sdk.name":
			sdkName = v.Str()
		case "telemetry.sdk.language":
			sdkLanguage = v.Str()
		case "telemetry.distro.name":
			distroName = v.Str()
		}
		return true
	})

	agentName := "otlp"
	if sdkName != "" {
		agentName = sdkName
	}
	switch {
	case distroName != "":
		lang := "unknown"
		if sdkLanguage != "" {
			lang = sdkLanguage
		}
		agentName = fmt.Sprintf("%s/%s/%s", agentName, lang, distroName)
	case sdkLanguage != "":
		agentName = fmt.Sprintf("%s/%s", agentName, sdkLanguage)
	}
	attrs.PutStr("agent.name", agentName)
}

func (c *elasticapmConnector) signaltometricsSettings() connector.Settings {
	signaltometricsSettings := c.set
	signaltometricsSettings.ID = component.NewIDWithName(signaltometricsFactory.Type(), c.set.ID.Name())
	return signaltometricsSettings
}
