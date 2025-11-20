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

package profilingmetricsconnector // import "github.com/elastic/opentelemetry-collector-components/connector/profilingmetricsconnector"

import "github.com/elastic/opentelemetry-collector-components/connector/profilingmetricsconnector/internal/metadata"

// Aggregation applies Match as a regular expression on function strings
// and generates a metric with Label if it matches.
type Aggregation struct {
	Match string `mapstructure:"match"`
	Label string `mapstructure:"label"`
}

// Config represents the receiver config settings within the collector's config.yaml
type Config struct {
	metadata.MetricsBuilderConfig `mapstructure:",squash"`
	// Generate metrics based on frame information (including frame type,
	// supersedes ByFrameType).
	ByFrame bool `mapstructure:"by_frame"`

	// Generate metrics based on frame type.
	ByFrameType bool `mapstructure:"by_frametype"`

	// Generate metrics based on functional classification.
	// Currently only Java and Go are supported.
	ByClassification bool `mapstructure:"by_classification"`

	// CustomAggregations allows to generate custom metrics.
	CustomAggregations []Aggregation `mapstructure:"aggregations"`
}
