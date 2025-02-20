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

package elasticapmreceiver

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"bytes"
	"net/http"

	"github.com/elastic/opentelemetry-collector-components/internal/testutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/ptracetest"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

var inputFiles = []struct {
	inputNdJsonFileName        string
	outputExpectedYamlFileName string
}{
	{"invalid_ids.ndjson", "invalid_ids_expected.yaml"},
	{"transactions.ndjson", "transactions_expected.yaml"},
	{"spans.ndjson", "spans_expected.yaml"},
	{"unknown-span-type.ndjson", "unknown-span-type_expected.yaml"},
	{"transactions_spans.ndjson", "transactions_spans_expected.yaml"},
}

func TestTransactionsAndSpans(t *testing.T) {
	factory := NewFactory()
	testEndpoint := testutil.GetAvailableLocalAddress(t)
	cfg := &Config{
		ServerConfig: confighttp.ServerConfig{
			Endpoint: testEndpoint,
		}}

	set := receivertest.NewNopSettings()
	nextTrace := new(consumertest.TracesSink)
	receiver, _ := factory.CreateTraces(context.Background(), set, cfg, nextTrace)

	if err := receiver.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Errorf("Starting receiver failed: %v", err)
	}
	defer func() {
		if err := receiver.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown failed: %v", err)
		}
	}()

	for _, tt := range inputFiles {
		t.Run(tt.inputNdJsonFileName, func(t *testing.T) {
			runComparison(t, tt.inputNdJsonFileName, tt.outputExpectedYamlFileName, &receiver, nextTrace, testEndpoint)
		})
	}
}

func runComparison(t *testing.T, inputJsonFileName string, expectedYamlFileName string, rec *receiver.Traces,
	nextTrace *consumertest.TracesSink, testEndpoint string) {

	testData := "testdata"
	nextTrace.Reset()

	data, err := os.ReadFile(filepath.Join(testData, inputJsonFileName))
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	resp, err := http.Post("http://"+testEndpoint+intakePath, "application/x-ndjson", bytes.NewBuffer(data))

	if err != nil {
		t.Fatalf("failed to send HTTP request: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("unexpected status code: %v", resp.StatusCode)
	}

	actualTraces := nextTrace.AllTraces()[0]
	expectedFile := filepath.Join(testData, expectedYamlFileName)
	expectedTraces, err := golden.ReadTraces(expectedFile)
	require.NoError(t, err)

	// Use this line to generate the expected yaml file:
	// golden.WriteTraces(t, expectedFile, actualTraces)

	require.NoError(t, ptracetest.CompareTraces(expectedTraces, actualTraces, ptracetest.IgnoreStartTimestamp(),
		ptracetest.IgnoreEndTimestamp()))
}
