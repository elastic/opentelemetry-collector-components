package elasticinframetricsprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

func TestProcessMetrics(t *testing.T) {
	testCases := []struct {
		name               string
		cfg                *Config
		createMetrics      func() pmetric.Metrics
		expectedHostname   string
		expectedMetricName string
		expetedMetricValue int64
	}{
		{
			name: "ProcessMetrics when AddSystemMetrics is enabled",
			cfg:  &Config{AddSystemMetrics: true},
			createMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr("host.name", "test-host")
				sm := rm.ScopeMetrics().AppendEmpty()
				metric := sm.Metrics().AppendEmpty()
				metric.SetName("test.metric")
				dp := metric.SetEmptySum().DataPoints().AppendEmpty()
				dp.SetIntValue(10)
				return md
			},
			expectedHostname:   "test-host",
			expectedMetricName: "test.metric",
			expetedMetricValue: 10,
		},
		{
			name: "ProcessMetrics when AddSystemMetrics is disabled",
			cfg:  &Config{AddSystemMetrics: false},
			createMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr("host.name", "test-host")
				sm := rm.ScopeMetrics().AppendEmpty()
				metric := sm.Metrics().AppendEmpty()
				metric.SetName("test.metric")
				return md
			},
			expectedHostname:   "",
			expectedMetricName: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			set := processor.CreateSettings{
				TelemetrySettings: component.TelemetrySettings{
					Logger: zap.NewNop(),
				},
			}

			p := newProcessor(set, tc.cfg)

			md := tc.createMetrics()
			_, err := p.processMetrics(context.Background(), md)

			assert.NoError(t, err)

			// Check if remapper was initialized based on the config
			if tc.cfg.AddSystemMetrics {
				assert.NotEmpty(t, p.remappers, "expected remapper to be initialized")
				// Check remapping results
				if len(p.remappers) > 0 {
					rm := md.ResourceMetrics().At(0)
					resource := rm.Resource()
					scopeMetric := rm.ScopeMetrics().At(0)
					metric := scopeMetric.Metrics().At(0)
					dpvalue := metric.Sum().DataPoints().At(0).IntValue()
					hostname, ok := resource.Attributes().Get("host.name")
					assert.True(t, ok, "expected attribute 'host.name'")
					assert.Equal(t, tc.expectedHostname, hostname.Str(), "expected resource attribute to be 'test-host'")
					assert.Equal(t, tc.expectedMetricName, metric.Name(), "expected metric name to be 'test.metric'")
					assert.Equal(t, tc.expetedMetricValue, dpvalue, "expected metric value to be 10")

				}
			} else {
				assert.Empty(t, p.remappers, "expected no remapper to be initialized")
			}
		})
	}
}
