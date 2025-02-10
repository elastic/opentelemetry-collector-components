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

package merger // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger"

import (
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
)

var _ pebble.ValueMerger = (*Merger)(nil)

type Merger struct {
	current           *Value
	resourceLimitCfg  config.LimitConfig
	scopeLimitCfg     config.LimitConfig
	metricLimitCfg    config.LimitConfig
	datapointLimitCfg config.LimitConfig
}

func New(
	v *Value,
	resLimit, scopeLimit, metricLimit, datapointLimit config.LimitConfig,
) *Merger {
	return &Merger{
		current:           v,
		resourceLimitCfg:  resLimit,
		scopeLimitCfg:     scopeLimit,
		metricLimitCfg:    metricLimit,
		datapointLimitCfg: datapointLimit,
	}
}

func (m *Merger) MergeNewer(value []byte) error {
	op := NewValue(
		m.resourceLimitCfg,
		m.scopeLimitCfg,
		m.metricLimitCfg,
		m.datapointLimitCfg,
	)
	if err := op.Unmarshal(value); err != nil {
		return err
	}
	return m.current.Merge(op)
}

func (m *Merger) MergeOlder(value []byte) error {
	op := NewValue(
		m.resourceLimitCfg,
		m.scopeLimitCfg,
		m.metricLimitCfg,
		m.datapointLimitCfg,
	)
	if err := op.Unmarshal(value); err != nil {
		return err
	}
	return m.current.Merge(op)
}

func (m *Merger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	data, err := m.current.Marshal()
	return data, nil, err
}
