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
// Merge clones the first operand and does not unmarshal it. Unmarshal
// runs when a sibling operand arrives. Finish returns the clone when no
// sibling arrived. That skips Unmarshal and AppendBinary on a lone MERGE.
func NewPebbleMerger(
	resourceLimit, scopeLimit, metricLimit, datapointLimit config.LimitConfig,
	maxExponentialHistogramBuckets int,
) *pebble.Merger {
	return &pebble.Merger{
		Name: pebbleMergerName,
		Merge: func(_ []byte, value []byte) (pebble.ValueMerger, error) {
			// Pebble keeps ownership of value and may reuse it before Finish.
			return &Merger{
				raw:                            slices.Clone(value),
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
type Merger struct {
	current *Value
	// raw is a copy of the first merge operand. Merge sets it.
	// ensureUnmarshaled clears it. Finish returns it when current is nil.
	raw []byte

	resourceLimitCfg               config.LimitConfig
	scopeLimitCfg                  config.LimitConfig
	metricLimitCfg                 config.LimitConfig
	datapointLimitCfg              config.LimitConfig
	maxExponentialHistogramBuckets int
}

// MergeNewer implements pebble.ValueMerger.
func (m *Merger) MergeNewer(value []byte) error {
	return m.mergeOperand(value)
}

// MergeOlder implements pebble.ValueMerger.
func (m *Merger) MergeOlder(value []byte) error {
	return m.mergeOperand(value)
}

// mergeOperand unmarshals the first operand if needed, unmarshals value,
// and merges the two Values.
func (m *Merger) mergeOperand(value []byte) error {
	if err := m.ensureUnmarshaled(); err != nil {
		return err
	}
	op := NewValue(
		m.resourceLimitCfg,
		m.scopeLimitCfg,
		m.metricLimitCfg,
		m.datapointLimitCfg,
		m.maxExponentialHistogramBuckets,
	)
	if err := op.Unmarshal(value); err != nil {
		return err
	}
	return m.current.Merge(op)
}

// ensureUnmarshaled unmarshals raw into current. A second call is a no-op.
func (m *Merger) ensureUnmarshaled() error {
	if m.current != nil {
		return nil
	}
	v := NewValue(
		m.resourceLimitCfg,
		m.scopeLimitCfg,
		m.metricLimitCfg,
		m.datapointLimitCfg,
		m.maxExponentialHistogramBuckets,
	)
	if err := v.Unmarshal(m.raw); err != nil {
		return fmt.Errorf("failed to unmarshal value from db: %w", err)
	}
	m.current = v
	m.raw = nil
	return nil
}

func (m *Merger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	if m.current == nil {
		return m.raw, nil, nil
	}
	buf, err := m.current.AppendBinary(nil)
	if err != nil {
		return nil, nil, err
	}
	return buf, nil, nil
}
