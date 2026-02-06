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
	"fmt"

	"go.opentelemetry.io/collector/component"
)

const maxScannerBufSize = 1024 * 1024

type (
	JsonlFile string
)

// Config defines configuration for loadgen receiver.
type Config struct {
	Metrics  MetricsConfig  `mapstructure:"metrics"`
	Logs     LogsConfig     `mapstructure:"logs"`
	Traces   TracesConfig   `mapstructure:"traces"`
	Profiles ProfilesConfig `mapstructure:"profiles"`

	// Concurrency is the amount of concurrency when sending to next consumer.
	// The concurrent workers share the amount of workload, instead of multiplying the amount of workload,
	// i.e. loadgenreceiver still sends up to the same MaxReplay limit.
	// A higher concurrency translates to a higher load.
	// As requests are synchronous, when concurrency is N, there will be N in-flight requests.
	// This is similar to the `agent_replicas` config in apmsoak.
	Concurrency int `mapstructure:"concurrency"`

	// DisablePdataReuse disables the optimization that reuses pdata structures to reduce allocations.
	// It is useful in cases where the optimization causes problems with certain downstream components, e.g. batchprocessor.
	DisablePdataReuse bool `mapstructure:"disable_pdata_reuse"`
}

type SignalConfig struct {
	// MaxReplay is an optional configuration to specify the number of times the file is replayed.
	MaxReplay int `mapstructure:"max_replay"`

	// MaxBufferSize defines the maximum acceptable size for the file content. Set to 0 if you don't want
	// to set a limit.
	MaxBufferSize int `mapstructure:"max_buffer_size"`

	// doneCh is only non-nil when the receiver is created with NewFactoryWithDone.
	// It is to notify the caller of collector that receiver finished replaying the file for MaxReplay number of times.
	doneCh chan Stats
}

type MetricsConfig struct {
	// JsonlFile is an optional configuration option to specify the path to
	// get the base generated signals from.
	JsonlFile `mapstructure:"jsonl_file"`

	SignalConfig `mapstructure:",squash"`

	// AddCounterAttr, if true, adds a loadgenreceiver_counter resource attribute containing increasing counter value to the generated metrics.
	// It can be used to workaround timestamp precision and duplication detection of backends,
	// e.g. Elasticsearch TSDB version_conflict_engine_exception with millisecond-precision timestamp mapping,
	// which will be triggered when loadgenreceiver generates metrics too quickly such that
	// there exists data points of the same metric with the same dimensions within a millisecond but different nanoseconds.
	AddCounterAttr bool `mapstructure:"add_counter_attr"`
}

type LogsConfig struct {
	// JsonlFile is an optional configuration option to specify the path to
	// get the base generated signals from.
	JsonlFile `mapstructure:"jsonl_file"`

	SignalConfig `mapstructure:",squash"`
}

type TracesConfig struct {
	// JsonlFile is an optional configuration option to specify the path to
	// get the base generated signals from.
	JsonlFile `mapstructure:"jsonl_file"`

	SignalConfig `mapstructure:",squash"`
}

type ProfilesConfig struct {
	// JsonlFile is an optional configuration option to specify the path to
	// get the base generated signals from.
	JsonlFile `mapstructure:"jsonl_file"`

	SignalConfig `mapstructure:",squash"`
}

var _ component.Config = (*Config)(nil)

// Validate checks the receiver configuration is valid
func (cfg *Config) Validate() error {
	if cfg.Logs.MaxReplay < 0 {
		return fmt.Errorf("logs::max_replay must be >= 0")
	}
	if cfg.Metrics.MaxReplay < 0 {
		return fmt.Errorf("metrics::max_replay must be >= 0")
	}
	if cfg.Traces.MaxReplay < 0 {
		return fmt.Errorf("traces::max_replay must be >= 0")
	}
	if cfg.Profiles.MaxReplay < 0 {
		return fmt.Errorf("profiles::max_replay must be >= 0")
	}
	if cfg.Logs.MaxBufferSize < 0 {
		return fmt.Errorf("logs::max_buffer_size must be >= 0")
	}
	if cfg.Metrics.MaxBufferSize < 0 {
		return fmt.Errorf("metrics::max_buffer_size must be >= 0")
	}
	if cfg.Traces.MaxBufferSize < 0 {
		return fmt.Errorf("traces::max_buffer_size must be >= 0")
	}
	if cfg.Profiles.MaxBufferSize < 0 {
		return fmt.Errorf("profiles::max_buffer_size must be >= 0")
	}
	return nil
}
