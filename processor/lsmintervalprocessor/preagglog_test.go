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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/metadata"
)

const (
	logTestTenants       = 3
	logTestDPsPerRequest = 4
)

// logTestHarness creates processors that share a data directory, so a test
// can stop one without a clean shutdown and start another in its place.
type logTestHarness struct {
	t        *testing.T
	dir      string
	cfg      config.PreAggregationConfig
	exported atomic.Int64
}

func newLogTestHarness(t *testing.T, cfg config.PreAggregationConfig) *logTestHarness {
	// Recovered values keep their original processing window, which a final
	// export only covers if it is still the current window. Avoid starting
	// right before a window boundary.
	if untilNext := time.Until(time.Now().Truncate(time.Minute).Add(time.Minute)); untilNext < 15*time.Second {
		time.Sleep(untilNext + 100*time.Millisecond)
	}
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
	}
	require.NoError(h.t, cfg.Validate())
	p, err := NewFactory().CreateMetrics(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, next)
	require.NoError(h.t, err)
	require.NoError(h.t, p.Start(context.Background(), componenttest.NewNopHost()))
	return p.(*Processor)
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
