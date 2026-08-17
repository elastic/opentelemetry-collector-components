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
	"fmt"
	"math"
	"slices"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pipeline"
	"go.uber.org/zap"
)

type Config struct {
	RoutingKeys       RoutingKeys       `mapstructure:"routing_keys"`
	DefaultPipelines  []pipeline.ID     `mapstructure:"default_pipelines"`
	RecordingInterval time.Duration     `mapstructure:"recording_interval"`
	TTL               time.Duration     `mapstructure:"ttl"`
	RoutingPipelines  []RoutingPipeline `mapstructure:"routing_pipelines"`
	StaticRoutes      []StaticRoute     `mapstructure:"static_routes"`
}

// StaticRoute maps a set of OTTL conditions to a fixed pipeline,
// bypassing cardinality-based dynamic routing entirely.
// Conditions are evaluated with OR semantics: any condition matching routes the
// request. Use OTTL and/or operators within a single condition for AND logic.
// Supported paths: otelcol.client.metadata["<key>"] (see ottlotelcol context).
type StaticRoute struct {
	Conditions []string      `mapstructure:"conditions"`
	Pipelines  []pipeline.ID `mapstructure:"pipelines"`
}

type RoutingPipeline struct {
	Pipelines      []pipeline.ID `mapstructure:"pipelines"`
	MaxCardinality float64       `mapstructure:"max_cardinality"`
}

type RoutingKeys struct {
	PartitionBy []string `mapstructure:"partition_by"`
	MeasureBy   []string `mapstructure:"measure_by"`
}

func (c *Config) Validate() error {
	if len(c.RoutingKeys.PartitionBy) == 0 {
		return errors.New("atleast one key for routing_keys.partition_by must be defined")
	}
	if len(c.DefaultPipelines) == 0 {
		return errors.New("default pipeline must be specified")
	}
	if len(c.RoutingPipelines) == 0 {
		return errors.New("atleast one pipeline needs to be defined")
	}
	nopSettings := component.TelemetrySettings{Logger: zap.NewNop()}
	for i, sr := range c.StaticRoutes {
		if len(sr.Conditions) == 0 {
			return fmt.Errorf("static_routes[%d]: at least one condition must be specified", i)
		}
		if _, err := newStaticConditionSequence(sr.Conditions, nopSettings); err != nil {
			return fmt.Errorf("static_routes[%d]: invalid condition: %w", i, err)
		}
		if len(sr.Pipelines) == 0 {
			return fmt.Errorf("static_routes[%d]: at least one pipeline must be specified", i)
		}
	}
	if c.RecordingInterval <= 0 {
		return errors.New("recording_interval must be greater than zero")
	}
	if c.TTL <= 0 {
		return errors.New("ttl must be greater than zero")
	}
	if c.TTL < c.RecordingInterval {
		return errors.New("ttl must be greater than or equal to recording_interval")
	}
	if !math.IsInf(c.RoutingPipelines[len(c.RoutingPipelines)-1].MaxCardinality, 1) {
		return errors.New("last dynamic pipeline must have max count set to positive infinity (.inf)")
	}
	if !slices.IsSortedFunc(c.RoutingPipelines, func(a, b RoutingPipeline) int {
		return cmp.Compare(a.MaxCardinality, b.MaxCardinality)
	}) {
		return errors.New("pipelines must be defined in ascending order of max_cardinality")
	}
	return nil
}
