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

package verifierreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/verifierreceiver"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"

	"github.com/elastic/opentelemetry-collector-components/receiver/verifierreceiver/internal/metadata"
)

// NewFactory creates a new factory for the verifier receiver.
// The verifier receiver supports multiple cloud providers (AWS, Azure, GCP, Okta)
// and verifies permissions for configured integrations.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
	)
}

// createDefaultConfig creates the default configuration for the receiver.
// Provider credentials are optional and can be configured per-provider.
func createDefaultConfig() component.Config {
	return &Config{
		VerificationType: "on_demand",
		Providers:        ProvidersConfig{},
		Policies:         []PolicyConfig{},
	}
}

// createLogsReceiver creates a new logs receiver instance.
// The receiver initializes verifiers for configured providers and
// verifies permissions based on the configured policies.
func createLogsReceiver(
	_ context.Context,
	params receiver.Settings,
	cfg component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	config := cfg.(*Config)
	return newVerifierReceiver(params, config, consumer), nil
}
