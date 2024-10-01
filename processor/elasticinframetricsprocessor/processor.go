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
	"fmt"

	"github.com/elastic/opentelemetry-lib/remappers/common"
	"github.com/elastic/opentelemetry-lib/remappers/hostmetrics"
	"github.com/elastic/opentelemetry-lib/remappers/kubernetesmetrics"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

const OTelRemappedLabel = common.OTelRemappedLabel

// remapper interface defines the Remap method that should be implemented by different remappers
type remapper interface {
	Remap(pmetric.ScopeMetrics, pmetric.MetricSlice, pcommon.Resource)
}

type ElasticinframetricsProcessor struct {
	cfg       *Config
	logger    *zap.Logger
	remappers []remapper
}

func newProcessor(set processor.Settings, cfg *Config) *ElasticinframetricsProcessor {
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
	}
}

// processMetrics processes the given metrics and applies remappers if configured.
func (p *ElasticinframetricsProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	for resIndex := range md.ResourceMetrics().Len() {
		resourceMetric := md.ResourceMetrics().At(resIndex)
		rm := resourceMetric.Resource()
		for scopeIndex := range resourceMetric.ScopeMetrics().Len() {
			scopeMetric := resourceMetric.ScopeMetrics().At(scopeIndex)
			for _, r := range p.remappers {
				r.Remap(scopeMetric, scopeMetric.Metrics(), rm)
			}
			for metric := range scopeMetric.Metrics().Len() {
				fmt.Println(scopeMetric.Metrics().At(metric).Name())
			}
		}
	}
	// drop_original=true will keep only the metrics that have been remapped based on the presense of OTelRemappedLabel label.
	// See  https://github.com/elastic/opentelemetry-lib/blob/6d89cbad4221429570107eb4a4968cf8a2ff919f/remappers/common/const.go#L31
	if p.cfg.DropOriginal {
		newMetic := pmetric.NewMetrics()
		for resIndex := range md.ResourceMetrics().Len() {
			resourceMetric := md.ResourceMetrics().At(resIndex)
			rmNew := newMetic.ResourceMetrics().AppendEmpty()

			// We need to copy Resource().Attributes() because those inlcude additional attributes of the metrics
			resourceMetric.Resource().Attributes().CopyTo(rmNew.Resource().Attributes())
			for scopeIndex := range resourceMetric.ScopeMetrics().Len() {
				scopeMetric := resourceMetric.ScopeMetrics().At(scopeIndex)
				rmScope := rmNew.ScopeMetrics().AppendEmpty()

				// Iterate over the metrics
				for metricIndex := range scopeMetric.Metrics().Len() {
					metric := scopeMetric.Metrics().At(metricIndex)

					if metric.Type() == pmetric.MetricTypeGauge {
						for dataPointIndex := range metric.Gauge().DataPoints().Len() {
							if oTelRemappedLabel, ok := metric.Gauge().DataPoints().At(dataPointIndex).Attributes().Get(OTelRemappedLabel); ok {
								if oTelRemappedLabel.Bool() {
									metric.CopyTo(rmScope.Metrics().AppendEmpty())
								}
							}
						}
					} else if metric.Type() == pmetric.MetricTypeSum {
						for dataPointIndex := range metric.Sum().DataPoints().Len() {
							if oTelRemappedLabel, ok := metric.Sum().DataPoints().At(dataPointIndex).Attributes().Get(OTelRemappedLabel); ok {
								if oTelRemappedLabel.Bool() {
									resourceMetric.Resource().Attributes().CopyTo(rmNew.Resource().Attributes())
									metric.CopyTo(rmScope.Metrics().AppendEmpty())
								}
							}
						}
					}

				}
			}
		}
		return newMetic, nil
	}

	return md, nil
}
