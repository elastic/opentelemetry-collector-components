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
	"path/filepath"
	"testing"

	"go.opentelemetry.io/collector/client"

	"github.com/elastic/opentelemetry-collector-components/processor/elasticapmprocessor/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/ptracetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

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
			expectedTraces, err := golden.ReadTraces(filepath.Join(dir, "output.yaml"))
			require.NoError(t, err)

			require.NoError(t, tp.ConsumeTraces(ctx, inputTraces))
			assert.NoError(t, ptracetest.CompareTraces(expectedTraces, next.AllTraces()[0]))
		})
	}
}

// TestProcessorECS does a basic test to check if traces are processed correctly when ECS mode is enabled in the client metadata.
func TestProcessorECS(t *testing.T) {
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
	require.IsType(t, &Processor{}, tp)

	inputTraces, err := golden.ReadTraces("testdata/ecs/elastic_span_db/input.yaml")
	require.NoError(t, err)
	expectedTraces, err := golden.ReadTraces("testdata/ecs/elastic_span_db/output.yaml")
	require.NoError(t, err)

	require.NoError(t, tp.ConsumeTraces(ctx, inputTraces))
	assert.NoError(t, ptracetest.CompareTraces(expectedTraces, next.AllTraces()[0]))
}
