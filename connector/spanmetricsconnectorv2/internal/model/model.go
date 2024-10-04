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

package model // import "github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/model"

import (
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

type AttributeKeyValue struct {
	Key          string
	DefaultValue pcommon.Value
}

type MetricKey struct {
	Name        string
	Description string
}

type MetricDef[K any] struct {
	Key                        MetricKey
	EphemeralResourceAttribute bool
	IncludeResourceAttributes  []AttributeKeyValue
	Attributes                 []AttributeKeyValue
	Counter                    *config.Counter
	ValueCountMetric           ValueCountMetric[K]
}

type ValueCountMetric[K any] struct {
	Unit                 config.MetricUnit
	ValueStatement       *ottl.Statement[K]
	CountStatement       *ottl.Statement[K]
	ExponentialHistogram *config.ExponentialHistogram
	ExplicitHistogram    *config.ExplicitHistogram
	Summary              *config.Summary
	SumAndCount          *config.SumAndCount
}
