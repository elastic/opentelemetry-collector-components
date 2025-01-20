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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

func BenchmarkMetricsGenerator(b *testing.B) {
	doneCh := make(chan Stats)
	cfg := createDefaultReceiverConfig(nil, doneCh, nil)
	cfg.(*Config).Metrics.MaxReplay = b.N
	r, _ := createMetricsReceiver(context.Background(), receiver.Settings{
		ID: component.ID{},
		TelemetrySettings: component.TelemetrySettings{
			Logger: zap.NewNop(),
		},
		BuildInfo: component.BuildInfo{},
	}, cfg, consumertest.NewNop())
	b.ResetTimer()
	err := r.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(b, err)
	defer func() {
		err := r.Shutdown(context.Background())
		require.NoError(b, err)
	}()
	<-doneCh
}
