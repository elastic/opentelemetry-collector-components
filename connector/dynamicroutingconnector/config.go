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
	"cmp"
	"errors"
	"math"
	"slices"
	"time"

	"go.opentelemetry.io/collector/pipeline"
)

type Config struct {
	PrimaryMetadataKeys []string          `mapstructure:"primary_metadata_keys"`
	DefaultPipelines    []pipeline.ID     `mapstructure:"default_pipelines"`
	EvaluationInterval  time.Duration     `mapstructure:"evaluation_interval"`
	DynamicPipelines    []DynamicPipeline `mapstructure:"dynamic_pipelines"`
	MetadataKeys        []string          `mapstructure:"metadata_keys"`
}

type DynamicPipeline struct {
	Pipelines []pipeline.ID `mapstructure:"pipelines"`
	MaxCount  float64       `mapstructure:"max_count"`
}

func (c *Config) Validate() error {
	if len(c.PrimaryMetadataKeys) == 0 {
		return errors.New("atleast one primary_metadata_key must be defined")
	}
	if len(c.DefaultPipelines) == 0 {
		return errors.New("default pipeline must be specified")
	}
	if len(c.DynamicPipelines) == 0 {
		return errors.New("atleast one pipeline needs to be defined")
	}
	if !math.IsInf(c.DynamicPipelines[len(c.DynamicPipelines)-1].MaxCount, 1) {
		return errors.New("last dynamic pipeline must have max count set to positive infinity (.inf)")
	}
	if !slices.IsSortedFunc(c.DynamicPipelines, func(a, b DynamicPipeline) int {
		return cmp.Compare(a.MaxCount, b.MaxCount)
	}) {
		return errors.New("pipelines must be defined in ascending order of max_count")
	}
	return nil
}
