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

package dynamicroutingconnector // import "github.com/elastic/opentelemetry-collector-components/connector/dynamicroutingconnector"

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap"
)

type tracesConnector struct {
	logger *zap.Logger
	cfg    *Config
	router *router[consumer.Traces]
}

func newTracesConnector(
	set connector.Settings,
	config component.Config,
	traces consumer.Traces,
) (*tracesConnector, error) {
	cfg := config.(*Config)
	tr, ok := traces.(connector.TracesRouterAndConsumer)
	if !ok {
		return nil, errors.New("expected connector to be a router and consumer")
	}

	router, err := newRouter(cfg, set.TelemetrySettings, tr.Consumer, pipeline.SignalTraces)
	if err != nil {
		return nil, fmt.Errorf("failed to create router: %w", err)
	}

	return &tracesConnector{
		logger: set.Logger,
		cfg:    cfg,
		router: router,
	}, nil
}

func (c *tracesConnector) Start(ctx context.Context, host component.Host) error {
	return c.router.Start(ctx, host)
}

func (c *tracesConnector) Shutdown(ctx context.Context) error {
	return c.router.Shutdown(ctx)
}

func (c *tracesConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *tracesConnector) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return c.router.Process(ctx).ConsumeTraces(ctx, td)
}
