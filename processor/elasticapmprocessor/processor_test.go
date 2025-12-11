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

package elasticapmprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/elastictraceprocessor"

import (
	"context"
	"flag"
	"path/filepath"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/ptracetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/metadata"
)

var update = flag.Bool("update", false, "Flag to generate/updated the expected yaml files")

// TestProcessor does some basic tests to check if enrichment is happening.
// More exhaustive test for the logic are left to the library.
func TestProcessor(t *testing.T) {
	testCases := []string{
		"elastic_txn_http",
		"elastic_txn_messaging",
		"elastic_txn_db",

		"elastic_span_http",
		"elastic_span_messaging",
		"elastic_span_db",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			factory := NewFactory()
			settings := processortest.NewNopSettings(metadata.Type)
			settings.TelemetrySettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
			next := &consumertest.TracesSink{}

			tp, err := factory.CreateTraces(ctx, settings, createDefaultConfig(), next)

			require.NoError(t, err)
			require.IsType(t, &TraceProcessor{}, tp)

			dir := filepath.Join("testdata", tc)
			inputTraces, err := golden.ReadTraces(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)

			outputFile := filepath.Join(dir, "output.yaml")
			expectedTraces, err := golden.ReadTraces(outputFile)
			require.NoError(t, err)

			require.NoError(t, tp.ConsumeTraces(ctx, inputTraces))
			actual := next.AllTraces()[0]
			if *update {
				err := golden.WriteTraces(t, outputFile, actual)
				assert.NoError(t, err)
			}
			assert.NoError(t, ptracetest.CompareTraces(expectedTraces, actual))
		})
	}
}

// TestECSTraces does a basic test to check if traces are processed correctly when ECS mode is enabled in the client metadata.
func TestECSTraces(t *testing.T) {
	testcases := map[string]struct {
		input  string
		output string
		cfg    *Config
	}{
		"span-db": {
			input:  "testdata/ecs/elastic_span_db/input.yaml",
			output: "testdata/ecs/elastic_span_db/output.yaml",
			cfg: func() *Config {
				cfg := createDefaultConfig().(*Config)
				cfg.ServiceNameInDataStreamDataset = true
				return cfg
			}(),
		},
		"hostname-settings": {
			input:  "testdata/elastic_hostname/metrics_input.yaml",
			output: "testdata/elastic_hostname/metrics_output.yaml",
			cfg: func() *Config {
				cfg := createDefaultConfig().(*Config)
				// Disable default hostname enrichment from opentelemetry-lib
				// to only test processor logic
				cfg.Resource.OverrideHostName.Enabled = false
				return cfg
			}(),
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			ctx := client.NewContext(context.Background(), client.Info{
				Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
			})
			cancel := func() {}
			defer cancel()

			factory := NewFactory()
			settings := processortest.NewNopSettings(metadata.Type)
			settings.TelemetrySettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
			next := &consumertest.TracesSink{}

			tp, err := factory.CreateTraces(ctx, settings, createDefaultConfig(), next)

			require.NoError(t, err)
			require.IsType(t, &TraceProcessor{}, tp)

			inputTraces, err := golden.ReadTraces(tc.input)
			require.NoError(t, err)

			expectedTraces, err := golden.ReadTraces(tc.output)
			require.NoError(t, err)

			require.NoError(t, tp.ConsumeTraces(ctx, inputTraces))
			actual := next.AllTraces()[0]
			if *update {
				err := golden.WriteTraces(t, tc.output, actual)
				assert.NoError(t, err)
			}
			assert.NoError(t, ptracetest.CompareTraces(expectedTraces, actual))
		})
	}
}

// TestSkipEnrichmentLogs tests that logs are only enriched when skipEnrichment is false or when mapping mode is ecs
func TestSkipEnrichmentLogs(t *testing.T) {
	testCases := []struct {
		name           string
		skipEnrichment bool
		mappingMode    string
	}{
		{
			name:           "logs_false",
			skipEnrichment: false,
			mappingMode:    "",
		},
		{
			name:           "logs_false_ecs",
			skipEnrichment: false,
			mappingMode:    "ecs",
		},
		{
			name:           "logs_true_ecs",
			skipEnrichment: true,
			mappingMode:    "ecs",
		},
		{
			name:           "logs_true_no_ecs",
			skipEnrichment: true,
			mappingMode:    "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.mappingMode != "" {
				ctx = client.NewContext(ctx, client.Info{
					Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {tc.mappingMode}}),
				})
			}

			factory := NewFactory()
			settings := processortest.NewNopSettings(metadata.Type)
			settings.TelemetrySettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
			next := &consumertest.LogsSink{}

			cfg := createDefaultConfig().(*Config)
			cfg.SkipEnrichment = tc.skipEnrichment

			lp, err := factory.CreateLogs(ctx, settings, cfg, next)
			require.NoError(t, err)

			dir := filepath.Join("testdata", "skip_enrichment")
			inputLogs, err := golden.ReadLogs(filepath.Join(dir, tc.name+"_input.yaml"))
			require.NoError(t, err)

			outputFile := filepath.Join(dir, tc.name+"_output.yaml")
			require.NoError(t, lp.ConsumeLogs(ctx, inputLogs))
			actual := next.AllLogs()[0]
			if *update {
				err := golden.WriteLogs(t, outputFile, actual)
				assert.NoError(t, err)
			}
			expectedLogs, err := golden.ReadLogs(outputFile)
			require.NoError(t, err)
			assert.NoError(t, plogtest.CompareLogs(expectedLogs, actual))
		})
	}
}

// TestSkipEnrichmentMetrics tests that metrics are only enriched when skipEnrichment is false or when mapping mode is ecs
func TestSkipEnrichmentMetrics(t *testing.T) {
	testCases := []struct {
		name           string
		skipEnrichment bool
		mappingMode    string
	}{
		{
			name:           "metrics_false",
			skipEnrichment: false,
			mappingMode:    "",
		},
		{
			name:           "metrics_false_ecs",
			skipEnrichment: false,
			mappingMode:    "ecs",
		},
		{
			name:           "metrics_true_ecs",
			skipEnrichment: true,
			mappingMode:    "ecs",
		},
		{
			name:           "metrics_true_no_ecs",
			skipEnrichment: true,
			mappingMode:    "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.mappingMode != "" {
				ctx = client.NewContext(ctx, client.Info{
					Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {tc.mappingMode}}),
				})
			}

			factory := NewFactory()
			settings := processortest.NewNopSettings(metadata.Type)
			settings.TelemetrySettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
			next := &consumertest.MetricsSink{}

			cfg := createDefaultConfig().(*Config)
			cfg.SkipEnrichment = tc.skipEnrichment

			mp, err := factory.CreateMetrics(ctx, settings, cfg, next)
			require.NoError(t, err)

			dir := filepath.Join("testdata", "skip_enrichment")
			inputMetrics, err := golden.ReadMetrics(filepath.Join(dir, tc.name+"_input.yaml"))
			require.NoError(t, err)

			outputFile := filepath.Join(dir, tc.name+"_output.yaml")
			require.NoError(t, mp.ConsumeMetrics(ctx, inputMetrics))
			actual := next.AllMetrics()[0]
			if *update {
				err := golden.WriteMetrics(t, outputFile, actual)
				assert.NoError(t, err)
			}
			expectedMetrics, err := golden.ReadMetrics(outputFile)
			require.NoError(t, err)
			assert.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actual, pmetrictest.IgnoreMetricsOrder(), pmetrictest.IgnoreResourceMetricsOrder()))
		})
	}
}

func TestECSLogs(t *testing.T) {

	testcases := map[string]struct {
		input  string
		output string
		cfg    *Config
	}{
		"servicename-in-datastream": {
			input:  "testdata/elastic_apm/logs_input.yaml",
			output: "testdata/elastic_apm/logs_output.yaml",
			cfg: func() *Config {
				cfg := createDefaultConfig().(*Config)
				cfg.ServiceNameInDataStreamDataset = true
				return cfg
			}(),
		},
		"hostname-settings": {
			input:  "testdata/elastic_hostname/logs_input.yaml",
			output: "testdata/elastic_hostname/logs_output.yaml",
			cfg: func() *Config {
				cfg := createDefaultConfig().(*Config)
				// Disable default hostname enrichment from opentelemetry-lib
				// to only test processor logic
				cfg.Resource.OverrideHostName.Enabled = false
				return cfg
			}(),
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ctx = client.NewContext(ctx, client.Info{
				Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
			})
			cfg := createDefaultConfig().(*Config)
			cfg.ServiceNameInDataStreamDataset = true

			factory := NewFactory()
			settings := processortest.NewNopSettings(metadata.Type)
			settings.TelemetrySettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
			next := &consumertest.LogsSink{}

			lp, err := factory.CreateLogs(ctx, settings, cfg, next)
			require.NoError(t, err)

			inputLogs, err := golden.ReadLogs(tc.input)
			require.NoError(t, err)

			require.NoError(t, lp.ConsumeLogs(ctx, inputLogs))
			actual := next.AllLogs()[0]

			expectedLogs, err := golden.ReadLogs(tc.output)
			require.NoError(t, err)
			assert.NoError(t, plogtest.CompareLogs(expectedLogs, actual))
		})
	}
}

func TestECSMetrics(t *testing.T) {
	testcases := map[string]struct {
		input  string
		output string
		cfg    *Config
	}{
		"servicename-in-datastream": {
			input:  "testdata/elastic_apm/metrics_input.yaml",
			output: "testdata/elastic_apm/metrics_output.yaml",
			cfg: func() *Config {
				cfg := createDefaultConfig().(*Config)
				cfg.ServiceNameInDataStreamDataset = true
				return cfg
			}(),
		},
		"hostname-settings": {
			input:  "testdata/elastic_hostname/metrics_input.yaml",
			output: "testdata/elastic_hostname/metrics_output.yaml",
			cfg: func() *Config {
				cfg := createDefaultConfig().(*Config)
				// Disable default hostname enrichment from opentelemetry-lib
				// to only test processor logic
				cfg.Resource.OverrideHostName.Enabled = false
				return cfg
			}(),
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			ctx = client.NewContext(ctx, client.Info{
				Metadata: client.NewMetadata(map[string][]string{"x-elastic-mapping-mode": {"ecs"}}),
			})

			factory := NewFactory()
			settings := processortest.NewNopSettings(metadata.Type)
			settings.TelemetrySettings.Logger = zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
			next := &consumertest.MetricsSink{}

			mp, err := factory.CreateMetrics(ctx, settings, tc.cfg, next)
			require.NoError(t, err)

			inputMetrics, err := golden.ReadMetrics(tc.input)
			require.NoError(t, err)

			require.NoError(t, mp.ConsumeMetrics(ctx, inputMetrics))
			actual := next.AllMetrics()[0]

			expectedMetrics, err := golden.ReadMetrics(tc.output)
			require.NoError(t, err)
			assert.NoError(t, pmetrictest.CompareMetrics(expectedMetrics, actual))
		})
	}
}
