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
	"fmt"
	"io"
	"slices"

	"github.com/cockroachdb/pebble"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
)

var _ pebble.ValueMerger = (*Merger)(nil)

const pebbleMergerName = "pmetrics_merger"

// NewPebbleMerger returns the Pebble merger used by the processor during
// compact and memtable combine.
//
// Operands are cloned as they arrive and decoded at Finish. A lone operand
// is returned as is, skipping Unmarshal and AppendBinary.
func NewPebbleMerger(
	resourceLimit, scopeLimit, metricLimit, datapointLimit config.LimitConfig,
	maxExponentialHistogramBuckets int,
) *pebble.Merger {
	return &pebble.Merger{
		Name: pebbleMergerName,
		Merge: func(_ []byte, value []byte) (pebble.ValueMerger, error) {
			// Pebble keeps ownership of value and may reuse it before Finish.
			return &Merger{
				operands:                       [][]byte{slices.Clone(value)},
				resourceLimitCfg:               resourceLimit,
				scopeLimitCfg:                  scopeLimit,
				metricLimitCfg:                 metricLimit,
				datapointLimitCfg:              datapointLimit,
				maxExponentialHistogramBuckets: maxExponentialHistogramBuckets,
			}, nil
		},
	}
}

// Merger implements pebble.ValueMerger for pmetric values in the LSM.
//
// Operands are buffered newest first, the order pebble delivers them in, and
// folded at Finish from the back, oldest first, into the oldest value. Every
// operand is decoded and replayed once, the large stored value is merged into
// rather than copied, and admission within one merge is first come, first
// served by write order whichever direction pebble delivers the operands in.
// See "Merge order" in the README.
type Merger struct {
	// operands holds a copy of every operand, newest first unless
	// oldestFirst is set. Pebble uses a single direction per merge.
	operands    [][]byte
	oldestFirst bool

	resourceLimitCfg               config.LimitConfig
	scopeLimitCfg                  config.LimitConfig
	metricLimitCfg                 config.LimitConfig
	datapointLimitCfg              config.LimitConfig
	maxExponentialHistogramBuckets int
}

// MergeNewer implements pebble.ValueMerger. Pebble only calls it during
// reverse iteration, which the processor never does.
func (m *Merger) MergeNewer(value []byte) error {
	m.operands = append(m.operands, slices.Clone(value))
	m.oldestFirst = true
	return nil
}

// MergeOlder implements pebble.ValueMerger.
func (m *Merger) MergeOlder(value []byte) error {
	m.operands = append(m.operands, slices.Clone(value))
	return nil
}

func (m *Merger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	last := len(m.operands) - 1
	if last == 0 {
		return m.operands[0], nil, nil
	}
	if m.oldestFirst {
		slices.Reverse(m.operands)
	}
	acc, err := m.unmarshal(m.operands[last])
	if err != nil {
		return nil, nil, err
	}
	for i := last - 1; i >= 0; i-- {
		op, err := m.unmarshal(m.operands[i])
		if err != nil {
			return nil, nil, err
		}
		if err := acc.Merge(op); err != nil {
			return nil, nil, err
		}
	}
	buf, err := acc.AppendBinary(nil)
	if err != nil {
		return nil, nil, err
	}
	return buf, nil, nil
}

func (m *Merger) unmarshal(value []byte) (*Value, error) {
	v := NewValue(
		m.resourceLimitCfg,
		m.scopeLimitCfg,
		m.metricLimitCfg,
		m.datapointLimitCfg,
		m.maxExponentialHistogramBuckets,
	)
	if err := v.Unmarshal(value); err != nil {
		return nil, fmt.Errorf("failed to unmarshal value from db: %w", err)
	}
	return v, nil
}
