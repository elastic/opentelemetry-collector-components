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

	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver/internal/sharedcomponent"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/ptracetest"
)

func TestTransactionsAndSpans(t *testing.T) {
	runComparison(t, "transactions.ndjson", "transactions_expected.yaml")
	runComparison(t, "spans.ndjson", "spans_expected.yaml")
	runComparison(t, "unknown-span-type.ndjson", "unknown-span-type_expected.yaml")
	runComparison(t, "transactions_spans.ndjson", "transactions_spans_expected.yaml")
}

func runComparison(t *testing.T, inputJsonFileName string, expectedYamlFileName string) {

	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	testData := "testdata"

	set := receivertest.NewNopSettings()
	nextTrace := new(consumertest.TracesSink)
	rec, _ := factory.CreateTraces(context.Background(), set, cfg, nextTrace)
	rec.Start(context.Background(), componenttest.NewNopHost())

	data, err := os.ReadFile(filepath.Join(testData, inputJsonFileName))
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	receiver := extractElasticAPMReceiver(rec)
	if receiver == nil {
		t.Fatal("Failed to extract elasticAPMReceiver")
	}

	protocol := "https"
	if receiver.cfg.TLSSetting == nil {
		protocol = "http"
	}
	resp, err := http.Post(protocol+"://"+receiver.cfg.Endpoint+intakePath, "application/x-ndjson", bytes.NewBuffer(data))
	if err != nil {
		t.Fatalf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("unexpected status code: %v", resp.StatusCode)
	}

	actualTraces := nextTrace.AllTraces()[0]
	expectedFile := filepath.Join(testData, expectedYamlFileName)
	expectedTraces, _ := golden.ReadTraces(expectedFile)

	require.NoError(t, ptracetest.CompareTraces(expectedTraces, actualTraces, ptracetest.IgnoreStartTimestamp(),
		ptracetest.IgnoreEndTimestamp()))

	rec.Shutdown(context.Background())
}

func extractElasticAPMReceiver(rec interface{}) *elasticAPMReceiver {
	if comp, ok := rec.(*sharedcomponent.Component[*elasticAPMReceiver]); ok {
		return comp.Unwrap()
	}
	return nil
}
