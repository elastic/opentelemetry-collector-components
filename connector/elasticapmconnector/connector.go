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
	"go.opentelemetry.io/collector/processor"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/connector/signaltometricsconnector"
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
	lsmintervalSettings := processor.Settings(set)
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
	return signaltometricsFactory.CreateTracesToMetrics(ctx, set, c.cfg.signaltometricsConfig(), c.lsminterval)
}

func (c *elasticapmConnector) signaltometricsSettings() connector.Settings {
	signaltometricsSettings := c.set
	signaltometricsSettings.ID = component.NewIDWithName(signaltometricsFactory.Type(), c.set.ID.Name())
	return signaltometricsSettings
}
