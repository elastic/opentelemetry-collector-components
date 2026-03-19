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
	sink := &consumertest.LogsSink{}

	sourceKey := "keyA_Source"
	destinationKey := "keyA_Destination"
	value := "valueA"

	processor := newLogsProcessor(zap.NewNop(),
		&Config{
			LogMetadata: map[string]string{
				sourceKey: destinationKey,
			},
		},
		sink)

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetEmptyMap()

	m := map[string][]string{}
	m[sourceKey] = []string{value}

	info := client.Info{}
	info.Metadata = client.NewMetadata(m)

	err := processor.ConsumeLogs(client.NewContext(t.Context(), info), logs)
	require.NoError(t, err)

	require.Equal(t, 1, sink.LogRecordCount())
	record := sink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)

	got, ok := record.Body().Map().Get(destinationKey)
	require.True(t, ok)
	require.Equal(t, value, got.AsString())
}
