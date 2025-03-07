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

package integrationreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/elastic/opentelemetry-collector-components/receiver/integrationreceiver/internal/metadata"
)

func TestCreateReceiver(t *testing.T) {
	factory := NewFactory()
	cfg := createDefaultConfig()

	params := receivertest.NewNopSettings(metadata.Type)

	lConsumer := consumertest.NewNop()
	lReceiver, err := factory.CreateLogs(context.Background(), params, cfg, lConsumer)
	assert.NoError(t, err, "receiver creation failed")
	assert.NotNil(t, lReceiver, "receiver creation failed")

	mConsumer := consumertest.NewNop()
	mReceiver, err := factory.CreateMetrics(context.Background(), params, cfg, mConsumer)
	assert.NoError(t, err, "receiver creation failed")
	assert.NotNil(t, mReceiver, "receiver creation failed")

	tConsumer := consumertest.NewNop()
	tReceiver, err := factory.CreateTraces(context.Background(), params, cfg, tConsumer)
	assert.NoError(t, err, "receiver creation failed")
	assert.NotNil(t, tReceiver, "receiver creation failed")
}
