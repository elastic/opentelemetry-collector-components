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

package spanmetricsconnectorv2

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestConnector(t *testing.T) {
	testCases := []string{
		"with_default",
		"with_attributes",
		"with_missing_attribute",
		"with_missing_attribute_default_value",
		"with_custom_histogram_buckets",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			factory := NewFactory()
			settings := connectortest.NewNopSettings()
			settings.TelemetrySettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
			next := &consumertest.MetricsSink{}

			dir := filepath.Join("testdata", tc)
			cfg := createDefaultConfig()
			cm, err := confmaptest.LoadConf(filepath.Join(dir, "config.yaml"))
			require.NoError(t, err)
			sub, err := cm.Sub(component.NewIDWithName(metadata.Type, "").String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(&cfg))

			connector, err := factory.CreateTracesToMetrics(ctx, settings, cfg, next)
			require.NoError(t, err)
			require.IsType(t, &spanMetrics{}, connector)

			inputTraces, err := golden.ReadTraces(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)
			expectedMetrics, err := golden.ReadMetrics(filepath.Join(dir, "output.yaml"))
			require.NoError(t, err)

			require.NoError(t, connector.ConsumeTraces(ctx, inputTraces))
			assert.NoError(t, pmetrictest.CompareMetrics(
				expectedMetrics,
				next.AllMetrics()[0],
				pmetrictest.IgnoreMetricsOrder(),
				pmetrictest.IgnoreTimestamp(),
			))
		})
	}
}