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

package model // import "github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/model"

import (
	"errors"
	"fmt"

	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/config"
	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/ottlget"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"go.opentelemetry.io/collector/component"
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

type ExplicitHistogram[K any] struct {
	Buckets []float64
	Count   *ottl.Statement[K]
	Value   *ottl.Statement[K]
}

func (h *ExplicitHistogram[K]) fromConfig(
	mi *config.Histogram,
	parser ottl.Parser[K],
) error {
	if mi == nil {
		return nil
	}

	var err error
	h.Buckets = mi.Buckets
	if mi.Count != "" {
		h.Count, err = parser.ParseStatement(ottlget.ConvertToStatement(mi.Count))
		if err != nil {
			return fmt.Errorf("failed to parse count statement for explicit histogram: %w", err)
		}
	}
	h.Value, err = parser.ParseStatement(ottlget.ConvertToStatement(mi.Value))
	if err != nil {
		return fmt.Errorf("failed to parse value statement for explicit histogram: %w", err)
	}
	return nil
}

type ExponentialHistogram[K any] struct {
	MaxSize int32
	Count   *ottl.Statement[K]
	Value   *ottl.Statement[K]
}

func (h *ExponentialHistogram[K]) fromConfig(
	mi *config.ExponentialHistogram,
	parser ottl.Parser[K],
) error {
	if mi == nil {
		return nil
	}

	var err error
	h.MaxSize = mi.MaxSize
	if mi.Count != "" {
		h.Count, err = parser.ParseStatement(ottlget.ConvertToStatement(mi.Count))
		if err != nil {
			return fmt.Errorf("failed to parse count statement for exponential histogram: %w", err)
		}
	}
	h.Value, err = parser.ParseStatement(ottlget.ConvertToStatement(mi.Value))
	if err != nil {
		return fmt.Errorf("failed to parse value statement for exponential histogram: %w", err)
	}
	return nil
}

type Sum[K any] struct {
	Value *ottl.Statement[K]
}

func (s *Sum[K]) fromConfig(
	mi *config.Sum,
	parser ottl.Parser[K],
) error {
	if mi == nil {
		return nil
	}

	var err error
	s.Value, err = parser.ParseStatement(ottlget.ConvertToStatement(mi.Value))
	if err != nil {
		return fmt.Errorf("failed to parse value statement for sum: %w", err)
	}
	return nil
}

type MetricDef[K any] struct {
	Key                       MetricKey
	Unit                      string
	IncludeResourceAttributes []AttributeKeyValue
	Attributes                []AttributeKeyValue
	Conditions                *ottl.ConditionSequence[K]
	ExponentialHistogram      *ExponentialHistogram[K]
	ExplicitHistogram         *ExplicitHistogram[K]
	Sum                       *Sum[K]
}

func (md *MetricDef[K]) FromMetricInfo(
	mi config.MetricInfo,
	parser ottl.Parser[K],
	telemetrySettings component.TelemetrySettings,
) error {
	md.Key.Name = mi.Name
	md.Key.Description = mi.Description
	md.Unit = mi.Unit

	var err error
	md.IncludeResourceAttributes, err = parseAttributeConfigs(mi.IncludeResourceAttributes)
	if err != nil {
		return fmt.Errorf("failed to parse include resource attribute config: %w", err)
	}
	md.Attributes, err = parseAttributeConfigs(mi.Attributes)
	if err != nil {
		return fmt.Errorf("failed to parse attribute config: %w", err)
	}
	if len(mi.Conditions) > 0 {
		conditions, err := parser.ParseConditions(mi.Conditions)
		if err != nil {
			return fmt.Errorf("failed to parse OTTL conditions: %w", err)
		}
		condSeq := ottl.NewConditionSequence(
			conditions,
			telemetrySettings,
			ottl.WithLogicOperation[K](ottl.Or),
		)
		md.Conditions = &condSeq
	}
	if mi.Histogram != nil {
		md.ExplicitHistogram = new(ExplicitHistogram[K])
		if err := md.ExplicitHistogram.fromConfig(mi.Histogram, parser); err != nil {
			return fmt.Errorf("failed to parse histogram config: %w", err)
		}
	}
	if mi.ExponentialHistogram != nil {
		md.ExponentialHistogram = new(ExponentialHistogram[K])
		if err := md.ExponentialHistogram.fromConfig(mi.ExponentialHistogram, parser); err != nil {
			return fmt.Errorf("failed to parse histogram config: %w", err)
		}
	}
	if mi.Sum != nil {
		md.Sum = new(Sum[K])
		if err := md.Sum.fromConfig(mi.Sum, parser); err != nil {
			return fmt.Errorf("failed to parse sum config: %w", err)
		}
	}
	return nil
}

func parseAttributeConfigs(cfgs []config.Attribute) ([]AttributeKeyValue, error) {
	var errs []error
	kvs := make([]AttributeKeyValue, len(cfgs))
	for i, attr := range cfgs {
		val := pcommon.NewValueEmpty()
		if err := val.FromRaw(attr.DefaultValue); err != nil {
			errs = append(errs, err)
		}
		kvs[i] = AttributeKeyValue{
			Key:          attr.Key,
			DefaultValue: val,
		}
	}

	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return kvs, nil
}
