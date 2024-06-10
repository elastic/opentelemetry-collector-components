package elasticinframetricsprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// mockRemapper is a mock implementation of the remapper interface for testing purposes.
type mockRemapper struct {
	called      bool
	scopeMetric pmetric.ScopeMetrics
	metricSlice pmetric.MetricSlice
	resource    pcommon.Resource
}

func (m *mockRemapper) Remap(scopeMetric pmetric.ScopeMetrics, metricSlice pmetric.MetricSlice, resource pcommon.Resource) {
	m.called = true
	m.scopeMetric = scopeMetric
	m.metricSlice = metricSlice
	m.resource = resource
}

func TestProcessMetrics(t *testing.T) {
	testCases := []struct {
		name                 string
		cfg                  *Config
		createMetrics        func() pmetric.Metrics
		expectedRemapperCall bool
		expectedHostname     string
		expectedMetricName   string
	}{
		{
			name: "processes metrics with system metrics",
			cfg:  &Config{AddSystemMetrics: true},
			createMetrics: func() pmetric.Metrics {
				md := pmetric.NewMetrics()
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutStr("host.name", "test-host")
				sm := rm.ScopeMetrics().AppendEmpty()
				metric := sm.Metrics().AppendEmpty()
				metric.SetName("test.metric")
				return md
			},
			expectedRemapperCall: true,
			expectedHostname:     "test-host",
			expectedMetricName:   "test.metric",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			set := processor.CreateSettings{
				TelemetrySettings: component.TelemetrySettings{
					Logger: zap.NewNop(),
				},
			}

			mockRemap := &mockRemapper{}
			p := &ElasticinframetricsProcessor{
				cfg:       tc.cfg,
				logger:    set.Logger,
				remappers: []remapper{mockRemap},
			}

			md := tc.createMetrics()
			_, err := p.processMetrics(context.Background(), md)

			assert.NoError(t, err)
			assert.Equal(t, tc.expectedRemapperCall, mockRemap.called, "expected remapper call")

			if tc.expectedRemapperCall {
				hostname, ok := mockRemap.resource.Attributes().Get("host.name")
				assert.True(t, ok, "expected attribute 'host.name'")
				assert.Equal(t, tc.expectedHostname, hostname.Str(), "expected resource attribute to be 'test-host'")

				metricname := mockRemap.scopeMetric.Metrics().At(0).Name()
				assert.Equal(t, tc.expectedMetricName, metricname, "expected metric name to be 'test.metric'")
			}
		})
	}
}
