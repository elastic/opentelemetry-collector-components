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

package elasticapmprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"

	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector"
	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/fanoutconsumer"
	"github.com/elastic/opentelemetry-collector-components/processor/elastictraceprocessor"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor"
)

var (
	lsmintervalFactory     = lsmintervalprocessor.NewFactory()
	signaltometricsFactory = signaltometricsconnector.NewFactory()
	elastictraceFactory    = elastictraceprocessor.NewFactory()
)

type elasticapmProcessor struct {
	cfg         *Config
	set         processor.Settings
	nextMetrics metricsConsumerShim // lsm -> next
	lsminterval processor.Metrics
}

func newElasticAPMProcessor(ctx context.Context, cfg *Config, set processor.Settings) (*elasticapmProcessor, error) {
	p := &elasticapmProcessor{cfg: cfg, set: set}
	lsminterval, err := lsmintervalFactory.CreateMetrics(ctx, set, cfg.lsmConfig(), &p.nextMetrics)
	if err != nil {
		return nil, err
	}
	p.lsminterval = lsminterval
	return p, nil
}

func (p *elasticapmProcessor) Start(ctx context.Context, host component.Host) error {
	if p.nextMetrics.Metrics == nil {
		p.nextMetrics.Metrics = nopMetricsConsumer{}
	}
	return p.lsminterval.Start(ctx, host)
}

func (p *elasticapmProcessor) Shutdown(ctx context.Context) error {
	if p.lsminterval != nil {
		if err := p.lsminterval.Shutdown(ctx); err != nil {
			fmt.Println(err)
			return err
		}
	}
	p.nextMetrics.Metrics = nil
	return nil
}

func (p *elasticapmProcessor) setNextMetrics(nextConsumer consumer.Metrics) error {
	// nextConsumer is the next component in the pipeline after elasticapmprocessor,
	// and we need to record it here to use in lsmintervalprocessor.
	if p.nextMetrics.Metrics != nil {
		return errors.New("elasticapm must not be used in multiple metrics pipelines")
	}
	p.nextMetrics.Metrics = nextConsumer
	return nil
}

func (p *elasticapmProcessor) newLogsConsumer(ctx context.Context, nextConsumer consumer.Logs) (consumer.Logs, error) {
	// Insert signaltometrics and lsmintervalprocessor before the next consumer.
	signaltometricsConsumer, err := signaltometricsFactory.CreateLogsToMetrics(
		ctx,
		connector.Settings(p.set),
		p.cfg.signaltometricsConfig(),
		p.lsminterval,
	)
	if err != nil {
		return nil, err
	}

	// Fan out to signaltometricsconnector and the next consumer.
	fanout := fanoutconsumer.NewLogs([]consumer.Logs{signaltometricsConsumer, nextConsumer})
	return fanout, nil
}

func (p *elasticapmProcessor) newMetricsConsumer(ctx context.Context, nextConsumer consumer.Metrics) (consumer.Metrics, error) {
	// Insert signaltometrics and lsmintervalprocessor before the next consumer.
	signaltometricsConsumer, err := signaltometricsFactory.CreateMetricsToMetrics(
		ctx,
		connector.Settings(p.set),
		p.cfg.signaltometricsConfig(),
		p.lsminterval,
	)
	if err != nil {
		return nil, err
	}

	// Fan out to signaltometricsconnector and the next consumer.
	fanout := fanoutconsumer.NewMetrics([]consumer.Metrics{signaltometricsConsumer, nextConsumer})
	return fanout, nil
}

func (p *elasticapmProcessor) newTracesConsumer(ctx context.Context, nextConsumer consumer.Traces) (consumer.Traces, error) {
	signaltometricsConsumer, err := signaltometricsFactory.CreateTracesToMetrics(
		ctx,
		connector.Settings(p.set),
		p.cfg.signaltometricsConfig(),
		p.lsminterval,
	)
	if err != nil {
		return nil, err
	}

	// Fan out to signaltometricsconnector and the next consumer.
	fanout := fanoutconsumer.NewTraces([]consumer.Traces{signaltometricsConsumer, nextConsumer})

	// Insert elastictraceprocessor at the beginning to enrich spans first.
	return elastictraceFactory.CreateTraces(ctx, p.set, elastictraceFactory.CreateDefaultConfig(), fanout)
}

type metricsConsumerShim struct {
	consumer.Metrics
}

type nopMetricsConsumer struct{}

func (nopMetricsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (nopMetricsConsumer) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	return nil
}
