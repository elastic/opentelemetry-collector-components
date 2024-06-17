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

// remapper interface defines the Remap method that should be implemented by different remappers
type remapper interface {
	Remap(pmetric.ScopeMetrics, pmetric.MetricSlice, pcommon.Resource)
}

type ElasticinframetricsProcessor struct {
	cfg       *Config
	logger    *zap.Logger
	remappers []remapper
}

func newProcessor(set processor.CreateSettings, cfg *Config) *ElasticinframetricsProcessor {
	var remappers []remapper
	// Initialize the remapper slice if AddSystemMetrics is enabled
	if cfg.AddSystemMetrics {
		remappers = []remapper{
			hostmetrics.NewRemapper(set.Logger, hostmetrics.WithSystemIntegrationDataset(true)),
		}
	}
	if cfg.AddK8sMetrics {
		remappers = []remapper{
			kubernetesmetrics.NewRemapper(set.Logger, kubernetesmetrics.WithKubernetesIntegrationDataset(true)),
		}
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
	return md, nil
}
