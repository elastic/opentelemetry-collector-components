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
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

func TestLogsGenerator_doneCh(t *testing.T) {
	const maxReplay = 2
	doneCh := make(chan Stats)
	sink := &consumertest.LogsSink{}
	r, _ := createLogsReceiver(context.Background(), receiver.Settings{
		ID: component.ID{},
		TelemetrySettings: component.TelemetrySettings{
			Logger: zap.NewNop(),
		},
		BuildInfo: component.BuildInfo{},
	}, &Config{Logs: LogsConfig{
		MaxReplay: maxReplay,
		doneCh:    doneCh,
	}}, sink)
	err := r.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err)
	defer r.Shutdown(context.Background())
	stats := <-doneCh
	assert.Equal(t, maxReplay*bytes.Count(demoLogs, []byte("\n")), stats.Requests)
}
