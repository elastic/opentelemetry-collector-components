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

package lsmintervalprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor"

import (
	"time"

	"go.opentelemetry.io/collector/component"
)

var _ component.Config = (*Config)(nil)

type Config struct {
	// Directory is the data directory used by the database to store files.
	// If the directory is empty in-memory storage is used.
	Directory string `mapstructure:"directory"`
	// Intervals is a list of time durations that the processor will
	// aggregate over. The intervals must be in increasing order and the
	// all interval values must be a factor of the smallest interval.
	// TODO (lahsivjar): Make specifying interval easier. We can just
	// optimize the timer to run on differnt times and remove any
	// restriction on different interval configuration.
	Intervals []time.Duration `mapstructure:"intervals"`
}

func (config *Config) Validate() error {
	// TODO (lahsivjar): Add validation for interval duration
	return nil
}
