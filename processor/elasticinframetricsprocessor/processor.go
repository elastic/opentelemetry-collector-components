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
	"iter"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticinframetricsprocessor/internal/metadata"
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

	// Valid returns true if the remapper should be applied to the given scope metrics.
	Valid(pmetric.ScopeMetrics) bool
}

type ElasticinframetricsProcessor struct {
	cfg       *Config
	set       processor.Settings
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
		set:       set,
		logger:    set.Logger,
		remappers: remappers,
	}
}

// processMetrics processes the given metrics and applies remappers if configured.
func (p *ElasticinframetricsProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	for _, resourceMetrics := range md.ResourceMetrics().All() {
		for _, scopeMetrics := range resourceMetrics.ScopeMetrics().All() {
			for _, r := range p.remappers {
				if !r.Valid(scopeMetrics) {
					continue
				}
				p.remapScopeMetrics(r, scopeMetrics, resourceMetrics.Resource())
				break // At most one remapper should be applied
			}
		}
	}
	return md, nil
}

func (p *ElasticinframetricsProcessor) remapScopeMetrics(
	r remapper,
	scopeMetrics pmetric.ScopeMetrics,
	resource pcommon.Resource,
) {
	scope := scopeMetrics.Scope()
	if _, ok := scope.Attributes().Get(common.OTelRemappedLabel); ok {
		// These metrics have already been remapped.
		return
	}
	scope.Attributes().PutBool(common.OTelRemappedLabel, true)

	// Also check if there are any remapped metrics by iterating over the
	// metrics, to handle metrics from older versions of the processor that
	// do not set scope attributes.
	for range remappedMetrics(scopeMetrics.Metrics()) {
		// Found remapped metrics.
		return
	}

	result := scopeMetrics.Metrics()
	if p.cfg.DropOriginal {
		result = pmetric.NewMetricSlice()
	}
	r.Remap(scopeMetrics, result, resource)
	if p.cfg.DropOriginal {
		// This overrides the existing metrics with just the remapped ones.
		//
		// When dropping the original metrics we update the scope name to
		// the processor's scope name, since original scope name is no longer
		// relevant.
		result.CopyTo(scopeMetrics.Metrics())
		scope.SetName(metadata.ScopeName)
		scope.SetVersion(p.set.BuildInfo.Version)
	}
}

func remappedMetrics(ms pmetric.MetricSlice) iter.Seq[pmetric.Metric] {
	return func(yield func(pmetric.Metric) bool) {
		for _, metric := range ms.All() {
			if isRemappedMetric(metric) {
				if !yield(metric) {
					return
				}
			}
		}
	}
}

func isRemappedMetric(metric pmetric.Metric) bool {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		for _, dp := range metric.Gauge().DataPoints().All() {
			if attr, ok := dp.Attributes().Get(OTelRemappedLabel); ok && attr.Bool() {
				return true
			}
		}
	case pmetric.MetricTypeSum:
		for _, dp := range metric.Sum().DataPoints().All() {
			if attr, ok := dp.Attributes().Get(OTelRemappedLabel); ok && attr.Bool() {
				return true
			}
		}
	}
	return false
}
