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

type MetricDef struct {
	Key                        MetricKey
	EphemeralResourceAttribute bool
	IncludeResourceAttributes  []AttributeKeyValue
	Attributes                 []AttributeKeyValue
	SpanDuration               SpanDuration
	Counter                    *config.Counter
}

func (m MetricDef) CountDefined() bool {
	return m.Counter != nil
}

func (m MetricDef) SpanDurationDefined() bool {
	return m.SpanDuration.defined()
}

type SpanDuration struct {
	Unit                 config.MetricUnit
	ExponentialHistogram *config.ExponentialHistogram
	ExplicitHistogram    *config.ExplicitHistogram
	Summary              *config.Summary
	SumAndCount          *config.SumAndCount
}

func (a SpanDuration) defined() bool {
	return a.ExponentialHistogram != nil ||
		a.ExplicitHistogram != nil ||
		a.Summary != nil ||
		a.SumAndCount != nil
}
