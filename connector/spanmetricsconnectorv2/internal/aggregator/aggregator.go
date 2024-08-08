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

package aggregator // import "github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/aggregator"

import (
	"errors"
	"time"

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/config"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// metricUnitToDivider gives a value that could used to divide the
// nano precision duration to the required unit specified in config.
var metricUnitToDivider = map[config.MetricUnit]float64{
	config.MetricUnitNs: float64(time.Nanosecond.Nanoseconds()),
	config.MetricUnitUs: float64(time.Microsecond.Nanoseconds()),
	config.MetricUnitMs: float64(time.Millisecond.Nanoseconds()),
	config.MetricUnitS:  float64(time.Second.Nanoseconds()),
}

type Aggregator struct {
	explicitBounds *explicitHistogram
}

func NewAggregator() *Aggregator {
	return &Aggregator{}
}

func (a *Aggregator) Add(
	md model.MetricDef,
	srcAttrs pcommon.Map,
	spanDuration time.Duration,
) error {
	filteredAttrs := pcommon.NewMap()
	for _, definedAttr := range md.Attributes {
		if srcAttr, ok := srcAttrs.Get(definedAttr.Key); ok {
			srcAttr.CopyTo(filteredAttrs.PutEmpty(definedAttr.Key))
			continue
		}
		if definedAttr.DefaultValue.Type() != pcommon.ValueTypeEmpty {
			definedAttr.DefaultValue.CopyTo(filteredAttrs.PutEmpty(definedAttr.Key))
		}
	}

	// If all the configured attributes are not present in source
	// metric then don't count them.
	if filteredAttrs.Len() != len(md.Attributes) {
		return nil
	}

	value := float64(spanDuration.Nanoseconds()) / metricUnitToDivider[md.Unit]

	var errs []error
	if md.ExplicitHistogram != nil {
		if a.explicitBounds == nil {
			a.explicitBounds = newExplicitBounds()
		}
		if err := a.explicitBounds.Add(
			md.Key, value, filteredAttrs, *md.ExplicitHistogram,
		); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (a *Aggregator) Move(
	md model.MetricDef,
	dest pmetric.MetricSlice,
) {
	if md.ExplicitHistogram != nil {
		if a.explicitBounds == nil {
			// nothing is added, return
			return
		}
		a.explicitBounds.Move(md.Key, dest)
	}
}

func (a *Aggregator) Size() int {
	var size int
	if a.explicitBounds != nil {
		size += a.explicitBounds.Size()
	}
	return size
}

func (a *Aggregator) Reset() {
	if a.explicitBounds != nil {
		a.explicitBounds.Reset()
	}
}
