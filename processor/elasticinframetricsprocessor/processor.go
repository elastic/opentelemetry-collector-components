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

package elasticinframetricsprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/elasticinframetricsprocessor"

import (
	"context"
	"errors"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"

	"github.com/elastic/opentelemetry-lib/remappers/hostmetrics"
	"github.com/elastic/opentelemetry-lib/remappers/kubernetesmetrics"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// remapper interface defines the Remap method that should be implemented by different remappers
type remapper interface {
	Remap(pmetric.ScopeMetrics, pmetric.MetricSlice, pcommon.Resource)
}

var _ processor.Metrics = (*ElasticinframetricsProcessor)(nil)

type ElasticinframetricsProcessor struct {
	component.StartFunc
	component.ShutdownFunc

	cfg       *Config
	logger    *zap.Logger
	remappers []remapper
	next      consumer.Metrics
}

func newProcessor(set processor.Settings, cfg *Config, next consumer.Metrics) *ElasticinframetricsProcessor {
	remappers := make([]remapper, 0)
	if cfg.AddSystemMetrics {
		remappers = append(remappers, hostmetrics.NewRemapper(set.Logger, hostmetrics.WithSystemIntegrationDataset(true)))
	}
	if cfg.AddK8sMetrics {
		remappers = append(remappers, kubernetesmetrics.NewRemapper(set.Logger, kubernetesmetrics.WithKubernetesIntegrationDataset(true)))
	}
	return &ElasticinframetricsProcessor{
		cfg:       cfg,
		logger:    set.Logger,
		remappers: remappers,
		next:      next,
	}
}

func (p *ElasticinframetricsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeMetrics processes the given metrics and applies remappers if configured.
func (p *ElasticinframetricsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	hasRemappedMetrics := false
	remappedMetrics := pmetric.NewMetrics()
	for resIndex := range md.ResourceMetrics().Len() {
		resourceMetric := md.ResourceMetrics().At(resIndex)
		rm := resourceMetric.Resource()
		remappedResourceMetrics := remappedMetrics.ResourceMetrics().AppendEmpty()
		rm.CopyTo(remappedResourceMetrics.Resource())
		for scopeIndex := range resourceMetric.ScopeMetrics().Len() {
			scopeMetric := resourceMetric.ScopeMetrics().At(scopeIndex)
			remappedMetricsSlice := pmetric.NewMetricSlice()
			for _, r := range p.remappers {
				r.Remap(scopeMetric, remappedMetricsSlice, rm)
			}
			if remappedMetricsSlice.Len() > 0 {
				remappedScopeMetric := remappedResourceMetrics.ScopeMetrics().AppendEmpty()
				remappedMetricsSlice.MoveAndAppendTo(remappedScopeMetric.Metrics())
				hasRemappedMetrics = true
			}
		}
	}
	var errs []error
	if !p.cfg.DropOriginal {
		errs = append(errs, p.next.ConsumeMetrics(ctx, md))
	}
	if hasRemappedMetrics {
		ecsCtx := client.NewContext(ctx, withMappingMode(client.FromContext(ctx), "ecs"))
		errs = append(errs, p.next.ConsumeMetrics(ecsCtx, remappedMetrics))
	}

	return errors.Join(errs...)
}

func withMappingMode(info client.Info, mode string) client.Info {
	return client.Info{
		Addr:     info.Addr,
		Auth:     info.Auth,
		Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {mode}}),
	}
}
