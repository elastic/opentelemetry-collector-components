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

type (
	JsonlFile string
)

// Config defines configuration for loadgen receiver.
type Config struct {
	Metrics MetricsConfig `mapstructure:"metrics"`
	Logs    LogsConfig    `mapstructure:"logs"`
	Traces  TracesConfig  `mapstructure:"traces"`

	// Concurrency is the amount of concurrency when sending to next consumer.
	// The concurrent workers share the amount of workload, instead of multiplying the amount of workload,
	// i.e. loadgenreceiver still sends up to the same MaxReplay limit.
	// A higher concurrency translates to a higher load.
	// As requests are synchronous, when concurrency is N, there will be N in-flight requests.
	// This is similar to the `agent_replicas` config in apmsoak.
	Concurrency int `mapstructure:"concurrency"`

	// PerfReusePdata enables reusing pdata data structures to reduce allocation and GC pressure on the loadgenreceiver.
	// This optimization is not compatible with fanoutconsumer, i.e. in pipelines where there are more than 1 consumer,
	// as fanoutconsumer will mark the pdata struct as read only and cannot be reused.
	// See https://github.com/open-telemetry/opentelemetry-collector/blob/461a3558086a03ab13ea121d12e28e185a1c79b0/internal/fanoutconsumer/logs.go#L70
	PerfReusePdata bool `mapstructure:"perf_reuse_pdata"`
}

type MetricsConfig struct {
	// JsonlFile is an optional configuration option to specify the path to
	// get the base generated signals from.
	JsonlFile `mapstructure:"jsonl_file"`

	// MaxReplay is an optional configuration to specify the number of times the file is replayed.
	MaxReplay int `mapstructure:"max_replay"`
	// doneCh is only non-nil when the receiver is created with NewFactoryWithDone.
	// It is to notify the caller of collector that receiver finished replaying the file for MaxReplay number of times.
	doneCh chan Stats
}

type LogsConfig struct {
	// JsonlFile is an optional configuration option to specify the path to
	// get the base generated signals from.
	JsonlFile `mapstructure:"jsonl_file"`

	// MaxReplay is an optional configuration to specify the number of times the file is replayed.
	MaxReplay int `mapstructure:"max_replay"`
	// doneCh is only non-nil when the receiver is created with NewFactoryWithDone.
	// It is to notify the caller of collector that receiver finished replaying the file for MaxReplay number of times.
	doneCh chan Stats
}

type TracesConfig struct {
	// JsonlFile is an optional configuration option to specify the path to
	// get the base generated signals from.
	JsonlFile `mapstructure:"jsonl_file"`

	// MaxReplay is an optional configuration to specify the number of times the file is replayed.
	MaxReplay int `mapstructure:"max_replay"`
	// doneCh is only non-nil when the receiver is created with NewFactoryWithDone.
	// It is to notify the caller of collector that receiver finished replaying the file for MaxReplay number of times.
	doneCh chan Stats
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
	return nil
}
