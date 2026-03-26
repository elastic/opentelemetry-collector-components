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

package elasticmetadataprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestLogProcessing(t *testing.T) {
	t.Run("Single Key mapping", func(t *testing.T) {
		sink := &consumertest.LogsSink{}

		processor := newLogsProcessor(zap.NewNop(),
			&Config{
				LogBodyFields: map[string]string{
					"source_key": "dest_key",
				},
			},
			sink)

		logs := plog.NewLogs()
		lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.Body().SetEmptyMap()

		info := client.Info{}
		info.Metadata = client.NewMetadata(map[string][]string{
			"source_key": {"value1"},
		})

		err := processor.ConsumeLogs(client.NewContext(t.Context(), info), logs)
		require.NoError(t, err)

		require.Equal(t, 1, sink.LogRecordCount())
		record := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)

		// check in context for destination key and value
		got, ok := record.Body().Map().Get("dest_key")
		require.True(t, ok)
		require.Equal(t, "value1", got.AsString())
	})

	t.Run("Multiple key mapping", func(t *testing.T) {
		sink := &consumertest.LogsSink{}

		processor := newLogsProcessor(zap.NewNop(),
			&Config{
				LogBodyFields: map[string]string{
					"cloud.provider": "cloud_provider",
					"cloud.region":   "cloud_region",
					"host.name":      "hostname",
				},
			},
			sink)

		logs := plog.NewLogs()
		lr := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.Body().SetEmptyMap()

		info := client.Info{}
		info.Metadata = client.NewMetadata(map[string][]string{
			"cloud.provider": {"aws"},
			"cloud.region":   {"us-east-1"},
			"host.name":      {"my-host"},
		})

		err := processor.ConsumeLogs(client.NewContext(t.Context(), info), logs)
		require.NoError(t, err)

		require.Equal(t, 1, sink.LogRecordCount())
		record := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
		body := record.Body().Map()

		// Check for each destination key and value in the log body.
		got, ok := body.Get("cloud_provider")
		require.True(t, ok)
		require.Equal(t, "aws", got.AsString())

		got, ok = body.Get("cloud_region")
		require.True(t, ok)
		require.Equal(t, "us-east-1", got.AsString())

		got, ok = body.Get("hostname")
		require.True(t, ok)
		require.Equal(t, "my-host", got.AsString())
	})
}
