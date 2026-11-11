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

package elasticapmintakereceiver

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/elastic/opentelemetry-collector-components/internal/testutil"
	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/metadata"
)

func TestWithMappingModeContextOverridesExistingMode(t *testing.T) {
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"x-elastic-target-id":    {"proj-abc123"},
			"x-elastic-mapping-mode": {"ecs"},
		}),
	})

	got := client.FromContext(withMappingModeContext(ctx, MappingModeOTel))

	require.Equal(t, []string{"proj-abc123"}, got.Metadata.Get("x-elastic-target-id"))
	require.Equal(t, []string{"otel"}, got.Metadata.Get("x-elastic-mapping-mode"))
}

func TestConfiguredMappingModePropagation(t *testing.T) {
	table := map[string]struct {
		mappingMode     string
		includeMetadata bool
		expectedMode    string
	}{
		"ecs config stamps ecs": {
			mappingMode:  MappingModeECS,
			expectedMode: "ecs",
		},
		"otel config stamps otel": {
			mappingMode:  MappingModeOTel,
			expectedMode: "otel",
		},
		"otel config overrides a client-supplied header": {
			mappingMode:     MappingModeOTel,
			includeMetadata: true,
			expectedMode:    "otel",
		},
		"ecs config overrides a client-supplied header": {
			mappingMode:     MappingModeECS,
			includeMetadata: true,
			expectedMode:    "ecs",
		},
	}
	for tname, tcase := range table {
		t.Run(tname, func(t *testing.T) {
			factory := NewFactory()
			testEndpoint := testutil.GetAvailableLocalAddress(t)
			cfg := factory.CreateDefaultConfig().(*Config)
			cfg.ServerConfig.NetAddr.Endpoint = testEndpoint
			cfg.ServerConfig.IncludeMetadata = tcase.includeMetadata
			cfg.MappingMode = tcase.mappingMode

			set := receivertest.NewNopSettings(metadata.Type)
			nextTrace := new(consumertest.TracesSink)
			rcvr, err := factory.CreateTraces(context.Background(), set, cfg, nextTrace)
			require.NoError(t, err)
			require.NoError(t, rcvr.Start(context.Background(), componenttest.NewNopHost()))
			defer func() {
				require.NoError(t, rcvr.Shutdown(context.Background()))
			}()

			sendInputWithHeaders(t, "transactions_spans.ndjson", testEndpoint, http.Header{
				"X-Elastic-Mapping-Mode": {"spoofed"},
			})

			ctxs := nextTrace.Contexts()
			require.GreaterOrEqual(t, len(ctxs), 1)
			md := client.FromContext(ctxs[0]).Metadata
			require.Equal(t, []string{tcase.expectedMode}, md.Get("x-elastic-mapping-mode"))
		})
	}
}
