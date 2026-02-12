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

package loadgenreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver"

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

func TestTracesGenerator_doneCh(t *testing.T) {
	const maxReplay = 2
	for _, concurrency := range []int{1, 2} {
		t.Run(fmt.Sprintf("concurrency=%d", concurrency), func(t *testing.T) {
			doneCh := make(chan Stats)
			sink := &consumertest.TracesSink{}
			cfg := createDefaultReceiverConfig(nil, nil, doneCh, nil)
			cfg.(*Config).Traces.MaxReplay = maxReplay
			cfg.(*Config).Concurrency = concurrency
			r, _ := createTracesReceiver(context.Background(), receiver.Settings{
				ID: component.ID{},
				TelemetrySettings: component.TelemetrySettings{
					Logger: zap.NewNop(),
				},
				BuildInfo: component.BuildInfo{},
			}, cfg, sink)
			err := r.Start(context.Background(), componenttest.NewNopHost())
			assert.NoError(t, err)
			defer func() {
				assert.NoError(t, r.Shutdown(context.Background()))
			}()
			stats := <-doneCh
			want := maxReplay * bytes.Count(demoTraces, []byte("\n"))
			assert.Equal(t, want, stats.Requests)
			assert.Equal(t, want, len(sink.AllTraces()))
			assert.Equal(t, sink.SpanCount(), stats.Spans)
		})
	}
}

func TestTracesGenerator_MaxBufferSizeAttr(t *testing.T) {
	dummyData := `{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"my.service"}}]},"scopeSpans":[{"spans":[{"traceId":"5B8EFFF798038103D269B633813FC60C","spanId":"EEE19B7EC3C1B174","parentSpanId":"EEE19B7EC3C1B173","name":"I'm a server span","startTimeUnixNano":"1727411470107912000","endTimeUnixNano":"1727411470107912000","kind":2,"attributes":[{"key":"my.span.attr","value":{"stringValue":"some value"}}]}]}]}]}`
	for _, maxBufferSize := range []int{0, 10} {
		t.Run(fmt.Sprintf("max_buffer_size=%d", maxBufferSize), func(t *testing.T) {
			dir := t.TempDir()
			filePath := filepath.Join(dir, strings.ReplaceAll(t.Name(), "/", "_")+".jsonl")
			content := []byte(dummyData)
			require.NoError(t, os.WriteFile(filePath, content, 0o644))

			doneCh := make(chan Stats)
			cfg := createDefaultReceiverConfig(nil, nil, doneCh, nil)
			cfg.(*Config).Traces.MaxBufferSize = maxBufferSize
			cfg.(*Config).Traces.JsonlFile = JsonlFile(filePath)

			_, err := createTracesReceiver(context.Background(), receiver.Settings{
				ID: component.ID{},
				TelemetrySettings: component.TelemetrySettings{
					Logger: zap.NewNop(),
				},
				BuildInfo: component.BuildInfo{},
			}, cfg, consumertest.NewNop())
			if maxBufferSize == 0 {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, bufio.ErrTooLong.Error())
			}
		})
	}
}
