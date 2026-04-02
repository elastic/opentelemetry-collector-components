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

package akamaisiemreceiver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configopaque"
)

func validConfig() *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.Endpoint = "https://akab-test.luna.akamaiapis.net"
	cfg.ConfigIDs = "12345"
	cfg.Authentication = EdgeGridAuth{
		ClientToken:  configopaque.String("ct"),
		ClientSecret: configopaque.String("cs"),
		AccessToken:  configopaque.String("at"),
	}
	return cfg
}

func TestConfigValidate_Valid(t *testing.T) {
	cfg := validConfig()
	require.NoError(t, cfg.Validate())
}

func TestConfigValidate_MissingEndpoint(t *testing.T) {
	cfg := validConfig()
	cfg.Endpoint = ""
	assert.ErrorContains(t, cfg.Validate(), "endpoint is required")
}

func TestConfigValidate_MissingConfigIDs(t *testing.T) {
	cfg := validConfig()
	cfg.ConfigIDs = ""
	assert.ErrorContains(t, cfg.Validate(), "config_ids is required")
}

func TestConfigValidate_MissingAuth(t *testing.T) {
	cfg := validConfig()
	cfg.Authentication.ClientToken = ""
	assert.ErrorContains(t, cfg.Validate(), "auth.client_token is required")

	cfg = validConfig()
	cfg.Authentication.ClientSecret = ""
	assert.ErrorContains(t, cfg.Validate(), "auth.client_secret is required")

	cfg = validConfig()
	cfg.Authentication.AccessToken = ""
	assert.ErrorContains(t, cfg.Validate(), "auth.access_token is required")
}

func TestConfigValidate_InvalidPollInterval(t *testing.T) {
	cfg := validConfig()
	cfg.PollInterval = 0
	assert.ErrorContains(t, cfg.Validate(), "poll_interval must be greater than 0")
}

func TestConfigValidate_InitialLookbackExceeds12h(t *testing.T) {
	cfg := validConfig()
	cfg.InitialLookback = 13 * time.Hour
	assert.ErrorContains(t, cfg.Validate(), "initial_lookback cannot exceed")
}

func TestConfigValidate_EventLimitExceedsMax(t *testing.T) {
	cfg := validConfig()
	cfg.EventLimit = 700000
	assert.ErrorContains(t, cfg.Validate(), "event_limit cannot exceed")
}

func TestConfigValidate_NegativeOffsetTTL(t *testing.T) {
	cfg := validConfig()
	cfg.OffsetTTL = -1
	assert.ErrorContains(t, cfg.Validate(), "offset_ttl must be non-negative")
}

func TestConfigDefaults(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	assert.Equal(t, time.Minute, cfg.PollInterval)
	assert.Equal(t, 12*time.Hour, cfg.InitialLookback)
	assert.Equal(t, 10000, cfg.EventLimit)
	assert.Equal(t, 120*time.Second, cfg.OffsetTTL)
	assert.Equal(t, 3, cfg.MaxRecoveryAttempts)
	assert.Equal(t, 2, cfg.InvalidTimestampRetries)
	assert.Equal(t, 1000, cfg.BatchSize)
	assert.Equal(t, 4, cfg.StreamBufferSize)
	assert.Equal(t, "raw", cfg.OutputFormat)
}

func TestConfigValidate_InvalidBatchSize(t *testing.T) {
	cfg := validConfig()
	cfg.BatchSize = 0
	assert.ErrorContains(t, cfg.Validate(), "batch_size must be greater than 0")
}

func TestConfigValidate_InvalidStreamBufferSize(t *testing.T) {
	cfg := validConfig()
	cfg.StreamBufferSize = 0
	assert.ErrorContains(t, cfg.Validate(), "stream_buffer_size must be greater than 0")
}
