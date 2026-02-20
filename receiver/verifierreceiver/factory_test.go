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

package verifierreceiver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/elastic/opentelemetry-collector-components/receiver/verifierreceiver/internal/metadata"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	require.NotNil(t, factory)

	assert.Equal(t, "verifier", factory.Type().String())
}

func TestCreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()

	require.NotNil(t, cfg)
	config, ok := cfg.(*Config)
	require.True(t, ok)

	assert.Empty(t, config.Policies)
	assert.Equal(t, "on_demand", config.VerificationType)
}

func TestCreateLogsReceiver(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)

	// Set up required configuration per RFC structure
	cfg.CloudConnectorID = "cc-test-001"
	cfg.VerificationID = "verify-test-001"
	cfg.Policies = []PolicyConfig{
		{
			PolicyID:   "policy-1",
			PolicyName: "Test Policy",
			Integrations: []IntegrationConfig{
				{
					IntegrationID:   "int-001",
					IntegrationType: "aws_cloudtrail",
					IntegrationName: "AWS CloudTrail",
					Config: map[string]interface{}{
						"account_id": "123456789012",
						"region":     "us-east-1",
					},
				},
			},
		},
	}

	consumer := consumertest.NewNop()
	receiver, err := factory.CreateLogs(
		context.Background(),
		receivertest.NewNopSettings(metadata.Type),
		cfg,
		consumer,
	)

	require.NoError(t, err)
	require.NotNil(t, receiver)

	// Verify it can start and shutdown
	err = receiver.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	err = receiver.Shutdown(context.Background())
	require.NoError(t, err)
}
