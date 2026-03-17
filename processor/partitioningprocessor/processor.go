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

package partitioningprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/partitioningprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type partitioningProcessor struct {
	component.StartFunc
	component.ShutdownFunc

	nextLogs     consumer.Logs
	nextMetrics  consumer.Metrics
	nextTraces   consumer.Traces
	nextProfiles xconsumer.Profiles
}

func (p *partitioningProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (p *partitioningProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	return p.nextLogs.ConsumeLogs(ctx, ld)
}

func (p *partitioningProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	return p.nextMetrics.ConsumeMetrics(ctx, md)
}

func (p *partitioningProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return p.nextTraces.ConsumeTraces(ctx, td)
}

func (p *partitioningProcessor) ConsumeProfiles(ctx context.Context, pd pprofile.Profiles) error {
	return p.nextProfiles.ConsumeProfiles(ctx, pd)
}
