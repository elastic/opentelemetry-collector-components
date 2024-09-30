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

	"github.com/elastic/opentelemetry-lib/remappers/hostmetrics"
	"github.com/elastic/opentelemetry-lib/remappers/kubernetesmetrics"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

const OTelRemappedLabel = "otel_remapped"

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
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetric := md.ResourceMetrics().At(i)
		rm := resourceMetric.Resource()
		for j := 0; j < resourceMetric.ScopeMetrics().Len(); j++ {
			scopeMetric := resourceMetric.ScopeMetrics().At(j)
			for _, r := range p.remappers {
				r.Remap(scopeMetric, scopeMetric.Metrics(), rm)
			}
		}
	}
	// override=True will keep only the metrics that have been remapped based on the presense of OTelRemappedLabel label.
	// See  https://github.com/elastic/opentelemetry-lib/blob/6d89cbad4221429570107eb4a4968cf8a2ff919f/remappers/common/const.go#L31
	if p.cfg.Override {
		newmetic := pmetric.NewMetrics()
		rmnew := newmetic.ResourceMetrics().AppendEmpty()
		rmscope := rmnew.ScopeMetrics().AppendEmpty()
		for i := 0; i < md.ResourceMetrics().Len(); i++ {
			resourceMetric := md.ResourceMetrics().At(i)

			for j := 0; j < resourceMetric.ScopeMetrics().Len(); j++ {
				scopeMetric := resourceMetric.ScopeMetrics().At(j)
				for l := 0; l < scopeMetric.Metrics().Len(); l++ {
					metric := scopeMetric.Metrics().At(l)
					if metric.Type().String() == "Gauge" {
						for m := 0; m < metric.Gauge().DataPoints().Len(); m++ {
							if oTelRemappedLabel, ok := metric.Gauge().DataPoints().At(m).Attributes().Get(OTelRemappedLabel); ok {
								if oTelRemappedLabel.Bool() {
									metric.CopyTo(rmscope.Metrics().AppendEmpty())
								}
							}
						}
					} else if metric.Type().String() == "Sum" {
						for m := 0; m < metric.Sum().DataPoints().Len(); m++ {
							if oTelRemappedLabel, ok := metric.Sum().DataPoints().At(m).Attributes().Get(OTelRemappedLabel); ok {
								if oTelRemappedLabel.Bool() {
									metric.CopyTo(rmscope.Metrics().AppendEmpty())
								}
							}
						}
					}

				}
			}
		}
		return newmetic, nil
	}

	return md, nil
}
