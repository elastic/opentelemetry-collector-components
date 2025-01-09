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
	"time"

	"go.opentelemetry.io/collector/component"
)

type (
	Throughput float64
	JsonFile   string
)

// Config defines configuration for HostMetrics receiver.
type Config struct {
	Metrics MetricsConfig `mapstructure:"metrics"`
	Logs    LogsConfig    `mapstructure:"logs"`
	Traces  TracesConfig  `mapstructure:"traces"`
}

type MetricsConfig struct {
	Throughput `mapstructure:"throughput"`
	// JsonFile is an optional configuration option to specify the path to
	// get the base generated signals from.
	JsonFile `mapstructure:"json_file"`
}

type LogsConfig struct {
	Throughput `mapstructure:"throughput"`
	// JsonFile is an optional configuration option to specify the path to
	// get the base generated signals from.
	JsonFile `mapstructure:"json_file"`
}

type TracesConfig struct {
	Throughput `mapstructure:"throughput"`
	// JsonFile is an optional configuration option to specify the path to
	// get the base generated signals from.
	JsonFile `mapstructure:"json_file"`
	// Spans duration timestamps are randomly set between 0ms and the
	// max_spans_interval duration value. Defaults to 3000ms.
	MaxSpansInterval time.Duration  `mapstructure:"max_spans_interval"`
	Services         ServicesConfig `mapstructure:"services"`
}

type ServicesConfig struct {
	// RandomizedNameCount specifies the number of randomized service names to generate.
	// This field is only relevant when is greater than 0. If not set or less than 1,
	// pre-loaded services will be used.
	RandomizedNameCount int `mapstructure:"randomized_name_count"`
}

var _ component.Config = (*Config)(nil)

// Validate checks the receiver configuration is valid
func (cfg *Config) Validate() error {
	return nil
}
