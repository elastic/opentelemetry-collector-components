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

package dynamicroutingconnector // import "github.com/elastic/opentelemetry-collector-components/connector/dynamicroutingconnector"

import (
	"errors"
	"sort"
	"time"

	"go.opentelemetry.io/collector/pipeline"
)

type Config struct {
	// TODO(lahsivjar): Revisit the decision to route to default pipeline
	// if NO metadata key results in empty str OR if primary key doesn't exist.
	DefaultPipelines   []pipeline.ID   `mapstructure:"default_pipelines"`
	EvaluationInterval time.Duration   `mapstructure:"evalaution_interval"`
	Pipelines          [][]pipeline.ID `mapstructure:"pipelines"`
	Thresholds         []int           `mapstructure:"thresholds"`
	PrimaryMetadataKey string          `mapstructure:"primary_metadata_key"`
	MetadataKeys       []string        `mapstructure:"metadata_keys"`
}

func (c *Config) Validate() error {
	if len(c.Pipelines) == 0 {
		return errors.New("atleast one pipeline needs to be defined")
	}
	if len(c.Pipelines)+1 != len(c.Thresholds) {
		return errors.New("pipelines need to be defined for each threshold bucket, including +inf")
	}
	if !sort.IntsAreSorted(c.Thresholds) {
		return errors.New("thresolds is expected to be in increasing order")
	}

	for i := 1; i < len(c.Thresholds); i++ {
		if c.Thresholds[i] == c.Thresholds[i-1] {
			return errors.New("thresholds are expected to be unique")
		}
	}
	return nil
}
