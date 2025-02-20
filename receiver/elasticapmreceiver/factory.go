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

package elasticapmreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver/internal/metadata"
	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver/internal/sharedcomponent"
)

const (
	defaultEndpoint = "localhost:8200"
)

// NewFactory creates a new factory for the elasticapm receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
		receiver.WithTraces(createTracesReceiver, metadata.TracesStability),
	)
}

// createDefaultConfig creates a default config with the endpoint set to port 8200.
func createDefaultConfig() component.Config {
	defaultServerConfig := confighttp.NewDefaultServerConfig()
	defaultServerConfig.Endpoint = defaultEndpoint

	// TODO: Remove this once we have a proper way to configure TLS
	defaultServerConfig.TLSSetting = nil

	return &Config{
		ServerConfig: defaultServerConfig,
	}
}

// createLogsReceiver creates a logs receiver with the given configuration.
func createLogsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	oCfg := cfg.(*Config)
	r, err := receivers.LoadOrStore(oCfg, func() (*elasticAPMReceiver, error) {
		return newElasticAPMReceiver(oCfg, set)
	})
	if err != nil {
		return nil, err
	}
	r.Unwrap().nextLogs = consumer
	return r, nil
}

// createMetricsReceiver creates a metrics receiver with the given configuration.
func createMetricsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	oCfg := cfg.(*Config)
	r, err := receivers.LoadOrStore(oCfg, func() (*elasticAPMReceiver, error) {
		return newElasticAPMReceiver(oCfg, set)
	})
	if err != nil {
		return nil, err
	}
	r.Unwrap().nextMetrics = consumer
	return r, nil
}

// createTracesReceiver creates a traces receiver with the given configuration.
func createTracesReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	consumer consumer.Traces,
) (receiver.Traces, error) {
	oCfg := cfg.(*Config)
	r, err := receivers.LoadOrStore(oCfg, func() (*elasticAPMReceiver, error) {
		return newElasticAPMReceiver(oCfg, set)
	})
	if err != nil {
		return nil, err
	}
	r.Unwrap().nextTraces = consumer
	return r, nil
}

var receivers = sharedcomponent.NewMap[*Config, *elasticAPMReceiver]()
