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
	"go.opentelemetry.io/collector/component"
)

type (
	JsonlFile string
)

// Config defines configuration for HostMetrics receiver.
type Config struct {
	Metrics MetricsConfig `mapstructure:"metrics"`
	Logs    LogsConfig    `mapstructure:"logs"`
	Traces  TracesConfig  `mapstructure:"traces"`
}

type MetricsConfig struct {
	// JsonlFile is an optional configuration option to specify the path to
	// get the base generated signals from.
	JsonlFile `mapstructure:"jsonl_file"`
	
	// MaxReplay is an optional configuration to specify the number of times the file is replayed.
	MaxReplay int `mapstructure:"max_replay"`
	// doneCh is only non-nil when the receiver is created with NewFactoryWithDone.
	// It is to notify the caller of collector that receiver finished replaying the file for MaxReplay number of times.
	doneCh chan TelemetryStats
}

type LogsConfig struct {
	// JsonlFile is an optional configuration option to specify the path to
	// get the base generated signals from.
	JsonlFile `mapstructure:"jsonl_file"`

	// MaxReplay is an optional configuration to specify the number of times the file is replayed.
	MaxReplay int `mapstructure:"max_replay"`
	// doneCh is only non-nil when the receiver is created with NewFactoryWithDone.
	// It is to notify the caller of collector that receiver finished replaying the file for MaxReplay number of times.
	doneCh chan TelemetryStats
}

type TracesConfig struct {
	// JsonlFile is an optional configuration option to specify the path to
	// get the base generated signals from.
	JsonlFile `mapstructure:"jsonl_file"`

	// MaxReplay is an optional configuration to specify the number of times the file is replayed.
	MaxReplay int `mapstructure:"max_replay"`
	// doneCh is only non-nil when the receiver is created with NewFactoryWithDone.
	// It is to notify the caller of collector that receiver finished replaying the file for MaxReplay number of times.
	doneCh chan TelemetryStats
}

var _ component.Config = (*Config)(nil)

// Validate checks the receiver configuration is valid
func (cfg *Config) Validate() error {
	return nil
}
