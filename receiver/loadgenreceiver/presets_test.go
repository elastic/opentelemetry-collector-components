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

package loadgenreceiver

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

func TestLogsPresetData(t *testing.T) {
	demo, err := logsPresetData("")
	require.NoError(t, err)
	assert.Equal(t, demoLogs, demo)

	logs, err := logsPresetData(presetVercelLogs)
	require.NoError(t, err)
	assert.Equal(t, vercelLogs, logs)
	assert.Equal(t, 2, bytes.Count(logs, []byte("\n")))

	si, err := logsPresetData(presetVercelSpeedInsights)
	require.NoError(t, err)
	assert.Equal(t, vercelSpeedInsights, si)
	assert.Equal(t, 3, bytes.Count(si, []byte("\n")))

	both, err := logsPresetData(presetVercelBoth)
	require.NoError(t, err)
	assert.Equal(t, 5, bytes.Count(both, []byte("\n")))
	assert.Contains(t, string(both), "soak-vercel-1")
	assert.Contains(t, string(both), "vercel.speed_insights.v1")

	_, err = logsPresetData("unknown")
	require.Error(t, err)
}

func TestLogsPresetMutuallyExclusiveWithJSONLFile(t *testing.T) {
	cfg := createDefaultReceiverConfig(nil, nil, nil, nil).(*Config)
	cfg.Logs.Preset = presetVercelLogs
	cfg.Logs.Path = "testdata/logs.jsonl"
	require.EqualError(t, cfg.Validate(), "logs::preset and logs::jsonl_file are mutually exclusive")
}

func TestLogsGenerator_VercelPreset(t *testing.T) {
	doneCh := make(chan Stats)
	sink := &consumertest.LogsSink{}
	cfg := createDefaultReceiverConfig(doneCh, nil, nil, nil).(*Config)
	cfg.Logs.Preset = presetVercelBoth
	cfg.Logs.MaxReplay = 1
	cfg.Concurrency = 1

	r, err := createLogsReceiver(context.Background(), receiver.Settings{
		ID: component.ID{},
		TelemetrySettings: component.TelemetrySettings{
			Logger: zap.NewNop(),
		},
	}, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() {
		assert.NoError(t, r.Shutdown(context.Background()))
	})

	stats := <-doneCh
	assert.Equal(t, 5, stats.Requests)
	assert.Equal(t, 5, len(sink.AllLogs()))
}
