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

package lsmintervalprocessor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/metadata"
)

const (
	logTestTenants       = 3
	logTestDPsPerRequest = 4
)

// logTestHarness creates processors that share a data directory, so a test
// can stop one without a clean shutdown and start another in its place.
type logTestHarness struct {
	t             *testing.T
	dir           string
	cfg           config.PreAggregationConfig
	exported      atomic.Int64
	resourceLimit config.LimitConfig
	intervals     []config.IntervalConfig
}

func newLogTestHarness(t *testing.T, cfg config.PreAggregationConfig) *logTestHarness {
	return &logTestHarness{t: t, dir: t.TempDir(), cfg: cfg}
}

func (h *logTestHarness) start() *Processor {
	h.t.Helper()
	next, _ := consumer.NewMetrics(consumer.ConsumeMetricsFunc(
		func(_ context.Context, md pmetric.Metrics) error {
			h.exported.Add(sumIntValues(md))
			return nil
		},
	))
	cfg := &config.Config{
		Directory:                      h.dir,
		Intervals:                      []config.IntervalConfig{{Duration: time.Minute}},
		MetadataKeys:                   []string{"tenant"},
		ExponentialHistogramMaxBuckets: 160,
		PreAggregation:                 h.cfg,
		ResourceLimit:                  h.resourceLimit,
		Storage:                        config.DefaultStorageConfig(),
	}
	if h.intervals != nil {
		cfg.Intervals = h.intervals
	}
	require.NoError(h.t, cfg.Validate())
	p, err := NewFactory().CreateMetrics(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, next)
	require.NoError(h.t, err)
	require.NoError(h.t, p.Start(context.Background(), componenttest.NewNopHost()))
	proc := p.(*Processor)
	proc.cancel()
	<-proc.exportStopped
	proc.processingTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return proc
}

// send consumes n requests spread over the test tenants and returns the
// total value sent.
func (h *logTestHarness) send(p *Processor, n int) int64 {
	h.t.Helper()
	for r := 0; r < n; r++ {
		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("requests")
		sum := m.SetEmptySum()
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		sum.SetIsMonotonic(true)
		for d := 0; d < logTestDPsPerRequest; d++ {
			dp := sum.DataPoints().AppendEmpty()
			dp.Attributes().PutInt("series", int64(d))
			dp.SetIntValue(1)
		}
		ctx := client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{
				"tenant": {fmt.Sprint("tenant-", r%logTestTenants)},
			}),
		})
		require.NoError(h.t, p.ConsumeMetrics(ctx, md))
	}
	return int64(n * logTestDPsPerRequest)
}

// crash stops p the way a killed process would: nothing in memory is
// flushed, and only what was already committed or written to files remains.
func (*logTestHarness) crash(t *testing.T, p *Processor) {
	t.Helper()
	p.cancel()
	<-p.exportStopped
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.batch != nil {
		require.NoError(t, p.batch.Close())
		p.batch = nil
	}
	require.NoError(t, p.db.Close())
	p.db = nil
	if p.preAggLog != nil {
		require.NoError(t, p.preAggLog.close())
	}
	p.telemetryBuilder.Shutdown()
}

func (h *logTestHarness) logDir() string {
	return filepath.Join(h.dir, preAggLogDir)
}

func sumIntValues(md pmetric.Metrics) int64 {
	var total int64
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				if ms.At(k).Type() != pmetric.MetricTypeSum {
					continue
				}
				dps := ms.At(k).Sum().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					total += dps.At(l).IntValue()
				}
			}
		}
	}
	return total
}

func TestPreAggregationLogRecoversAfterCrash(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cfg      config.PreAggregationConfig
		wantLoss bool
	}{
		{
			name:     "log=off",
			cfg:      config.PreAggregationConfig{Enabled: true},
			wantLoss: true,
		},
		{
			name: "log=on",
			cfg:  config.PreAggregationConfig{Enabled: true, Log: config.PreAggregationLogConfig{Enabled: true}},
		},
		{
			name: "log=on/size_triggered_flushes",
			cfg:  config.PreAggregationConfig{Enabled: true, MaxSize: 2 << 10, Log: config.PreAggregationLogConfig{Enabled: true}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newLogTestHarness(t, tc.cfg)
			p := h.start()
			sent := h.send(p, 200)
			h.crash(t, p)

			p = h.start()
			sent += h.send(p, 50)
			require.NoError(t, p.Shutdown(context.Background()))

			if tc.wantLoss {
				assert.Less(t, h.exported.Load(), sent)
				return
			}
			assert.Equal(t, sent, h.exported.Load())
		})
	}
}

// TestPreAggregationLogSkipsCoveredSegment covers a crash after a flush was
// committed but before its log segment was deleted: the segment must not be
// replayed again.
func TestPreAggregationLogSkipsCoveredSegment(t *testing.T) {
	h := newLogTestHarness(t, config.PreAggregationConfig{Enabled: true, Log: config.PreAggregationLogConfig{Enabled: true}})
	p := h.start()
	sent := h.send(p, 100)

	p.mu.Lock()
	covered := p.preAggLog.seq
	segment, err := os.ReadFile(p.preAggLog.segmentPath(covered))
	require.NoError(t, err)
	require.NotEmpty(t, segment)
	require.NoError(t, p.flushPendingLocked())
	require.Equal(t, covered, p.preAggLogCovered)
	p.mu.Unlock()
	_, err = os.Stat(p.preAggLog.segmentPath(covered))
	require.ErrorIs(t, err, os.ErrNotExist)

	// Put the covered segment back, as if the process had died before
	// deleting it.
	require.NoError(t, os.WriteFile(p.preAggLog.segmentPath(covered), segment, 0o600))
	sent += h.send(p, 30)
	h.crash(t, p)

	p = h.start()
	require.NoError(t, p.Shutdown(context.Background()))
	assert.Equal(t, sent, h.exported.Load())
}

// TestPreAggregationLogTornTail covers a crash in the middle of an append:
// replay stops at the incomplete record and keeps everything before it.
func TestPreAggregationLogTornTail(t *testing.T) {
	h := newLogTestHarness(t, config.PreAggregationConfig{Enabled: true, Log: config.PreAggregationLogConfig{Enabled: true}})
	p := h.start()
	sent := h.send(p, 80)
	active := p.preAggLog.segmentPath(p.preAggLog.seq)
	h.crash(t, p)

	f, err := os.OpenFile(active, os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	// A header claiming a long body, followed by only part of it.
	_, err = f.Write([]byte{0, 0, 1, 0, 0xde, 0xad, 0xbe, 0xef, 1, 2, 3})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	p = h.start()
	require.NoError(t, p.Shutdown(context.Background()))
	assert.Equal(t, sent, h.exported.Load())
}

// TestPreAggregationLogRetriesFailedFlush covers a flush whose commit failed
// after the values were dropped from memory: the next flush reads them back
// from the log.
func TestPreAggregationLogRetriesFailedFlush(t *testing.T) {
	h := newLogTestHarness(t, config.PreAggregationConfig{Enabled: true, Log: config.PreAggregationLogConfig{Enabled: true}})
	p := h.start()
	sent := h.send(p, 60)

	// Do what flushPendingLocked does up to the commit, then drop the batch
	// as a failed commit would.
	p.mu.Lock()
	seq := p.preAggLog.seq + 1
	f, err := p.preAggLog.createSegment(seq)
	require.NoError(t, err)
	for _, pv := range p.pending {
		pv.mu.Lock()
		pv.flushed = true
		pv.mu.Unlock()
	}
	clear(p.pending)
	p.pendingSize.Store(0)
	_, err = p.preAggLog.activate(f, seq)
	require.NoError(t, err)
	p.mu.Unlock()

	sent += h.send(p, 40)
	require.NoError(t, p.Shutdown(context.Background()))
	assert.Equal(t, sent, h.exported.Load())

	segments, err := os.ReadDir(h.logDir())
	require.NoError(t, err)
	assert.Len(t, segments, 1, "only the empty active segment should remain")
}

func TestPreAggregationLogRecoversExpiredWindows(t *testing.T) {
	h := newLogTestHarness(t, config.PreAggregationConfig{Enabled: true, Log: config.PreAggregationLogConfig{Enabled: true}})
	p := h.start()
	sent := h.send(p, 10)
	h.crash(t, p)
	p = h.start()
	p.processingTime = p.processingTime.Add(2 * time.Minute)
	require.NoError(t, p.Shutdown(context.Background()))
	assert.Equal(t, sent, h.exported.Load())
}

// Multiple processing windows collapse to the same longer-interval key.
// Replay must preserve both chronological operand order and segment boundaries.
func TestPreAggregationLogReplayOrder(t *testing.T) {
	h := newLogTestHarness(t, config.PreAggregationConfig{Enabled: true, Log: config.PreAggregationLogConfig{Enabled: true}})
	p := h.start()
	longer := p.intervals[0]
	longer.Duration = 10 * time.Minute
	p.intervals = append(p.intervals, longer)
	var seqs []uint64
	for i := 0; i < 3; i++ {
		seqs = append(seqs, p.preAggLog.seq)
		// Distinguishable operands, including two segments in the same window.
		h.send(p, (i+1)*3)
		f, err := p.preAggLog.createSegment(p.preAggLog.seq + 1)
		require.NoError(t, err)
		_, err = p.preAggLog.activate(f, p.preAggLog.seq+1)
		require.NoError(t, err)
		clear(p.pending)
		if i == 1 {
			p.processingTime = p.processingTime.Add(time.Minute)
		}
	}
	b := p.db.NewBatch()
	_, err := p.replayPreAggLog(p.preAggLog, b, seqs)
	require.NoError(t, err)
	reader := b.Reader()
	var got []int64
	for {
		_, key, value, ok, err := reader.Next()
		require.NoError(t, err)
		if !ok {
			break
		}
		var k merger.Key
		require.NoError(t, k.Unmarshal(key))
		if k.Interval != 10*time.Minute {
			continue
		}
		v := merger.NewValue(config.LimitConfig{}, config.LimitConfig{}, config.LimitConfig{}, config.LimitConfig{}, 160)
		require.NoError(t, v.Unmarshal(value))
		md, _, err := v.Finalize()
		require.NoError(t, err)
		got = append(got, sumIntValues(md))
	}
	require.Equal(t, []int64{4, 4, 4, 8, 8, 8, 12, 12, 12}, got)
	require.NoError(t, b.Close())
	h.crash(t, p)
}

func TestPreAggregationLogRetainsFailedExport(t *testing.T) {
	h := newLogTestHarness(t, config.PreAggregationConfig{Enabled: true, Log: config.PreAggregationLogConfig{Enabled: true}})
	p := h.start()
	sent := h.send(p, 30)
	require.NoError(t, p.flushPendingLocked())
	original := p.next
	p.next, _ = consumer.NewMetrics(func(context.Context, pmetric.Metrics) error { return fmt.Errorf("downstream unavailable") })
	boundary := p.processingTime.Add(time.Minute)
	require.Error(t, p.export(context.Background(), boundary))
	p.next = original
	require.NoError(t, p.export(context.Background(), boundary))
	require.Equal(t, sent, h.exported.Load())
	require.NoError(t, p.export(context.Background(), boundary))
	require.Equal(t, sent, h.exported.Load(), "successfully exported window must be deleted")
	require.NoError(t, p.Shutdown(context.Background()))
}

func TestPreAggregationLogReplayOverflowMatchesLive(t *testing.T) {
	var results []pmetric.Metrics
	for _, recoverLog := range []bool{false, true} {
		h := newLogTestHarness(t, config.PreAggregationConfig{Enabled: true, Log: config.PreAggregationLogConfig{Enabled: true}})
		h.resourceLimit = config.LimitConfig{MaxCardinality: 1, Overflow: config.OverflowConfig{Attributes: []config.Attribute{{Key: "overflow", Value: true}}}}
		h.intervals = []config.IntervalConfig{{Duration: time.Minute}, {Duration: 10 * time.Minute}}
		p := h.start()
		for window := 0; window < 3; window++ {
			md := pmetric.NewMetrics()
			for i := 0; i < 3; i++ {
				rm := md.ResourceMetrics().AppendEmpty()
				rm.Resource().Attributes().PutInt("service", int64(window+i))
				m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
				m.SetName("count")
				sum := m.SetEmptySum()
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				dp := sum.DataPoints().AppendEmpty()
				dp.SetIntValue(int64(window + i + 1))
			}
			require.NoError(t, p.ConsumeMetrics(context.Background(), md))
			if recoverLog {
				f, err := p.preAggLog.createSegment(p.preAggLog.seq + 1)
				require.NoError(t, err)
				_, err = p.preAggLog.activate(f, p.preAggLog.seq+1)
				require.NoError(t, err)
				clear(p.pending)
			} else {
				require.NoError(t, p.flushPendingLocked())
			}
			p.processingTime = p.processingTime.Add(time.Minute)
		}
		if recoverLog {
			// Reopen with both intervals before replay; the harness defaults to one.
			h.crash(t, p)
			p = h.start()
		}
		result := pmetric.NewMetrics()
		p.next, _ = consumer.NewMetrics(func(_ context.Context, md pmetric.Metrics) error {
			rms := md.ResourceMetrics()
			for i := 0; i < rms.Len(); i++ {
				rms.At(i).CopyTo(result.ResourceMetrics().AppendEmpty())
			}
			return nil
		})
		p.processingTime = p.processingTime.Add(10 * time.Minute)
		require.NoError(t, p.Shutdown(context.Background()))
		results = append(results, result)
	}
	require.NoError(t, pmetrictest.CompareMetrics(results[0], results[1], pmetrictest.IgnoreMetricsOrder()))
}
