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
	"sync"

	"github.com/cockroachdb/pebble"
)

var _ pebble.ValueMerger = (*Merger)(nil)

type Merger struct {
	bufferPool sync.Pool
	current    *Value
}

func New(v *Value) *Merger {
	return &Merger{
		current: v,
	}
}

func (m *Merger) MergeNewer(value []byte) error {
	op := NewValue(
		m.current.resourceLimitCfg,
		m.current.scopeLimitCfg,
		m.current.metricLimitCfg,
		m.current.datapointLimitCfg,
		m.current.maxExponentialHistogramBuckets,
	)
	if err := op.Unmarshal(value); err != nil {
		return err
	}
	return m.current.Merge(op)
}

func (m *Merger) MergeOlder(value []byte) error {
	op := NewValue(
		m.current.resourceLimitCfg,
		m.current.scopeLimitCfg,
		m.current.metricLimitCfg,
		m.current.datapointLimitCfg,
		m.current.maxExponentialHistogramBuckets,
	)
	if err := op.Unmarshal(value); err != nil {
		return err
	}
	return m.current.Merge(op)
}

func (m *Merger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	pb, ok := m.bufferPool.Get().(*pooledBuffer)
	if !ok {
		pb = &pooledBuffer{pool: &m.bufferPool}
	}
	newBuf, err := m.current.AppendBinary(pb.buf[:0])
	if err != nil {
		m.bufferPool.Put(pb)
		return nil, nil, err
	}
	pb.buf = newBuf
	return newBuf, pb, nil
}

type pooledBuffer struct {
	pool *sync.Pool
	buf  []byte
}

func (b *pooledBuffer) Close() error {
	b.buf = b.buf[:0]
	b.pool.Put(b)
	return nil
}
