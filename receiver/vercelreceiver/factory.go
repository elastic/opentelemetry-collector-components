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

package vercelreceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/elastic/opentelemetry-collector-components/internal/sharedcomponent"
	"github.com/elastic/opentelemetry-collector-components/receiver/vercelreceiver/internal/metadata"
)

// NewFactory returns a new factory for the Vercel receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
	)
}

func createLogsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	next consumer.Logs,
) (receiver.Logs, error) {
	vercelCfg := cfg.(*Config)
	r, err := loadOrStoreReceiver(vercelCfg, set)
	if err != nil {
		return nil, err
	}

	r.Unwrap().registerLogsConsumer(next)
	return r, nil
}

func createMetricsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (receiver.Metrics, error) {
	vercelCfg := cfg.(*Config)
	r, err := loadOrStoreReceiver(vercelCfg, set)
	if err != nil {
		return nil, err
	}

	r.Unwrap().registerMetricsConsumer(next)
	return r, nil
}

func loadOrStoreReceiver(cfg *Config, set receiver.Settings) (*sharedcomponent.Component[*vercelReceiver], error) {
	return receivers.LoadOrStore(
		cfg,
		func() (*vercelReceiver, error) {
			return newReceiver(cfg, nil, nil, set), nil
		},
	)
}

// receivers lets one Vercel receiver ID serve both logs and metrics pipelines.
// The collector creates receivers per signal, so without this shared map the
// logs and metrics factories would each start an HTTP server for the same
// config and collide on the listener endpoint.
var receivers = sharedcomponent.NewMap[*Config, *vercelReceiver]()
