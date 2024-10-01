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
	// PassThrough is a configuration that determines whether summary
	// metrics should be passed through as they are or aggregated. This
	// is because they lead to lossy aggregations.
	PassThrough PassThrough `mapstructure:"pass_through"`
	// Intervals is a list of interval configuration that the processor
	// will aggregate over. The interval duration must be in increasing
	// order and must be a factor of the smallest interval duration.
	// TODO (lahsivjar): Make specifying interval easier. We can just
	// optimize the timer to run on differnt times and remove any
	// restriction on different interval configuration.
	Intervals []IntervalConfig `mapstructure:"intervals"`
}

// PassThrough determines whether metrics should be passed through as they
// are or aggregated.
type PassThrough struct {
	// Summary is a flag that determines whether summary metrics should
	// be passed through as they are or aggregated. Since summaries don't
	// have an associated temporality, we assume that summaries are
	// always cumulative.
	Summary bool `mapstructure:"summary"`
}

type IntervalConfig struct {
	Duration time.Duration `mapstructure:"duration"`
	// Statements are a list of OTTL statements to be executed on the
	// metrics produced for a given interval. The list of available
	// OTTL paths for datapoints can be checked at:
	// https://pkg.go.dev/github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottldatapoint#section-readme
	// The list of available OTTL editors can be checked at:
	// https://pkg.go.dev/github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs#section-readme
	Statements []string `mapstructure:"statements"`
}

func (config *Config) Validate() error {
	// TODO (lahsivjar): Add validation for interval duration
	return nil
}
