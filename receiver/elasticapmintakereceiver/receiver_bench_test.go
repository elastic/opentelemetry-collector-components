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
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/elastic/opentelemetry-collector-components/internal/testutil"
	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/metadata"
)

// payloadGlobalLabelsNoShadow has 10 transactions with metadata global labels
// (tag1 string, tag2 numeric). No event shadows any global label.
var payloadGlobalLabelsNoShadow = []byte(`{"metadata": {"service": {"name": "bench-svc", "agent": {"name": "elastic-node", "version": "1.0.0"}}, "labels": {"tag1": "global_val", "tag2": 42}}}
{"transaction": {"id": "aa00000000000001", "trace_id": "aa00000000000001aa00000000000001", "name": "tx1", "type": "request", "duration": 1, "timestamp": 1000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000002", "trace_id": "aa00000000000002aa00000000000002", "name": "tx2", "type": "request", "duration": 1, "timestamp": 2000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000003", "trace_id": "aa00000000000003aa00000000000003", "name": "tx3", "type": "request", "duration": 1, "timestamp": 3000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000004", "trace_id": "aa00000000000004aa00000000000004", "name": "tx4", "type": "request", "duration": 1, "timestamp": 4000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000005", "trace_id": "aa00000000000005aa00000000000005", "name": "tx5", "type": "request", "duration": 1, "timestamp": 5000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000006", "trace_id": "aa00000000000006aa00000000000006", "name": "tx6", "type": "request", "duration": 1, "timestamp": 6000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000007", "trace_id": "aa00000000000007aa00000000000007", "name": "tx7", "type": "request", "duration": 1, "timestamp": 7000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000008", "trace_id": "aa00000000000008aa00000000000008", "name": "tx8", "type": "request", "duration": 1, "timestamp": 8000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000009", "trace_id": "aa00000000000009aa00000000000009", "name": "tx9", "type": "request", "duration": 1, "timestamp": 9000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa0000000000000a", "trace_id": "aa0000000000000aaa0000000000000a", "name": "tx10", "type": "request", "duration": 1, "timestamp": 10000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
`)

// payloadGlobalLabelsWithShadow has 10 transactions with metadata global
// labels (tag1 string, tag2 numeric). Transaction 3 shadows tag1 and
// transaction 7 shadows tag2, producing two overflow groups.
var payloadGlobalLabelsWithShadow = []byte(`{"metadata": {"service": {"name": "bench-svc", "agent": {"name": "elastic-node", "version": "1.0.0"}}, "labels": {"tag1": "global_val", "tag2": 42}}}
{"transaction": {"id": "aa00000000000001", "trace_id": "aa00000000000001aa00000000000001", "name": "tx1", "type": "request", "duration": 1, "timestamp": 1000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000002", "trace_id": "aa00000000000002aa00000000000002", "name": "tx2", "type": "request", "duration": 1, "timestamp": 2000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000003", "trace_id": "aa00000000000003aa00000000000003", "name": "tx3", "type": "request", "duration": 1, "timestamp": 3000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}, "context": {"tags": {"tag1": "event_val"}}}}
{"transaction": {"id": "aa00000000000004", "trace_id": "aa00000000000004aa00000000000004", "name": "tx4", "type": "request", "duration": 1, "timestamp": 4000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000005", "trace_id": "aa00000000000005aa00000000000005", "name": "tx5", "type": "request", "duration": 1, "timestamp": 5000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000006", "trace_id": "aa00000000000006aa00000000000006", "name": "tx6", "type": "request", "duration": 1, "timestamp": 6000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000007", "trace_id": "aa00000000000007aa00000000000007", "name": "tx7", "type": "request", "duration": 1, "timestamp": 7000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}, "context": {"tags": {"tag2": 99}}}}
{"transaction": {"id": "aa00000000000008", "trace_id": "aa00000000000008aa00000000000008", "name": "tx8", "type": "request", "duration": 1, "timestamp": 8000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa00000000000009", "trace_id": "aa00000000000009aa00000000000009", "name": "tx9", "type": "request", "duration": 1, "timestamp": 9000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
{"transaction": {"id": "aa0000000000000a", "trace_id": "aa0000000000000aaa0000000000000a", "name": "tx10", "type": "request", "duration": 1, "timestamp": 10000000, "outcome": "success", "sampled": true, "span_count": {"started": 0}}}
`)

func BenchmarkProcessBatch(b *testing.B) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"global_labels_no_shadow", payloadGlobalLabelsNoShadow},
		{"global_labels_with_shadow", payloadGlobalLabelsWithShadow},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			factory := NewFactory()
			endpoint := testutil.GetAvailableLocalAddress(b)
			cfg := &Config{
				ServerConfig: confighttp.ServerConfig{
					NetAddr: confignet.AddrConfig{
						Endpoint:  endpoint,
						Transport: confignet.TransportTypeTCP,
					},
				},
			}

			set := receivertest.NewNopSettings(metadata.Type)
			sink := new(consumertest.TracesSink)
			rcv, err := factory.CreateTraces(context.Background(), set, cfg, sink)
			if err != nil {
				b.Fatal(err)
			}
			if err := rcv.Start(context.Background(), componenttest.NewNopHost()); err != nil {
				b.Fatal(err)
			}
			defer func() { _ = rcv.Shutdown(context.Background()) }()

			url := "http://" + endpoint + intakeV2EventsPath

			// Warm up: send one request to ensure the server is ready.
			resp, err := http.Post(url, "application/x-ndjson", bytes.NewReader(tc.payload))
			if err != nil {
				b.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()

			sink.Reset()
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				resp, err := http.Post(url, "application/x-ndjson", bytes.NewReader(tc.payload))
				if err != nil {
					b.Fatal(err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		})
	}
}
