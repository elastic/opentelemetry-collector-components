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
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/metadata"
)

// BenchmarkPreAggregation measures sustained ingestion with APM-shaped
// metrics on an on-disk database using 1s/2s/4s windows, with pre-aggregation off, on, and on with
// the log. Run it with a long benchtime, for example:
//
//	go test -run '^$' -bench BenchmarkPreAggregation -benchtime 24s -count 3 .
func BenchmarkPreAggregation(b *testing.B) {
	modes := []struct {
		name string
		cfg  config.PreAggregationConfig
	}{
		{name: "pre_aggregation=off"},
		{name: "pre_aggregation=on", cfg: config.PreAggregationConfig{Enabled: true}},
		{name: "pre_aggregation=on,max_size=256MiB", cfg: config.PreAggregationConfig{Enabled: true, MaxSize: 256 << 20}},
		{name: "pre_aggregation=on,max_size=1GiB", cfg: config.PreAggregationConfig{Enabled: true, MaxSize: 1 << 30}},
		{name: "pre_aggregation=on,log", cfg: config.PreAggregationConfig{Enabled: true, Log: config.PreAggregationLogConfig{Enabled: true}}},
		{name: "pre_aggregation=on,log,max_size=256MiB", cfg: config.PreAggregationConfig{Enabled: true, MaxSize: 256 << 20, Log: config.PreAggregationLogConfig{Enabled: true}}},
		{name: "pre_aggregation=on,log,max_size=1GiB", cfg: config.PreAggregationConfig{Enabled: true, MaxSize: 1 << 30, Log: config.PreAggregationLogConfig{Enabled: true}}},
	}
	cardinalities := []struct {
		name string
		apmBenchShape
	}{
		{name: "low", apmBenchShape: apmBenchShape{services: 5, instances: 2, transactions: 20, destinations: 5, groupsPerRequest: 10}},
		{name: "high", apmBenchShape: apmBenchShape{services: 40, instances: 4, transactions: 200, destinations: 40, groupsPerRequest: 30}},
	}
	for _, card := range cardinalities {
		requests := card.generate(512)
		for _, tenants := range []int{1, 16} {
			for _, callers := range []int{1, 8} {
				for _, mode := range modes {
					name := fmt.Sprintf("cardinality=%s/tenants=%d/callers=%d/%s", card.name, tenants, callers, mode.name)
					b.Run(name, func(b *testing.B) {
						benchmarkPreAggregation(b, mode.cfg, requests, tenants, callers)
					})
				}
			}
		}
	}
}

func benchmarkPreAggregation(b *testing.B, preAgg config.PreAggregationConfig, requests []pmetric.Metrics, tenants, callers int) {
	cfg := &config.Config{
		Directory: b.TempDir(),
		Intervals: []config.IntervalConfig{
			{Duration: time.Second}, {Duration: 2 * time.Second}, {Duration: 4 * time.Second},
		},
		MetadataKeys:                   []string{"tenant"},
		ResourceLimit:                  config.LimitConfig{MaxCardinality: 8000},
		ScopeLimit:                     config.LimitConfig{MaxCardinality: 4000},
		MetricLimit:                    config.LimitConfig{MaxCardinality: 4000},
		DatapointLimit:                 config.LimitConfig{MaxCardinality: 4000},
		ExponentialHistogramMaxBuckets: 160,
		PreAggregation:                 preAgg,
		Storage:                        config.DefaultStorageConfig(),
	}
	var exported atomic.Int64
	sink, err := consumer.NewMetrics(func(_ context.Context, md pmetric.Metrics) error { exported.Add(benchSummary(md)); return nil })
	if err != nil {
		b.Fatal(err)
	}
	p, err := NewFactory().CreateMetrics(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	if err != nil {
		b.Fatal(err)
	}
	if err := p.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		b.Fatal(err)
	}
	ctxs := make([]context.Context, tenants)
	for i := range ctxs {
		ctxs[i] = client.NewContext(context.Background(), client.Info{
			Metadata: client.NewMetadata(map[string][]string{"tenant": {fmt.Sprint("tenant-", i)}}),
		})
	}
	var sizer pmetric.ProtoMarshaler

	var next atomic.Int64
	var peakHeap atomic.Uint64
	stopSampling := make(chan struct{})
	samplingDone := make(chan struct{})
	go func() {
		defer close(samplingDone)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
				runtime.ReadMemStats(&ms)
				if ms.HeapInuse > peakHeap.Load() {
					peakHeap.Store(ms.HeapInuse)
				}
			}
		}
	}()
	cpuBefore := processCPU()
	b.ResetTimer()
	start := time.Now()
	var wg sync.WaitGroup
	for c := 0; c < callers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1) - 1
				if i >= int64(b.N) {
					return
				}
				if err := p.ConsumeMetrics(ctxs[int(i)%tenants], requests[int(i)%len(requests)]); err != nil {
					b.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	intakeElapsed := time.Since(start)
	intakeCPU := processCPU() - cpuBefore
	proc := p.(*Processor)
	// Stop intake/export races, drain every partial interval, then force
	// outstanding memtable/compaction work to finish before final accounting.
	proc.cancel()
	<-proc.exportStopped
	if err := proc.flushPendingLocked(); err != nil {
		b.Fatal(err)
	}
	if proc.batch != nil {
		if err := proc.batch.Commit(proc.wOpts); err != nil {
			b.Fatal(err)
		}
		if err := proc.batch.Close(); err != nil {
			b.Fatal(err)
		}
		proc.batch = nil
	}
	for _, ivl := range proc.intervals {
		if err := proc.export(context.Background(), proc.processingTime.Truncate(ivl.Duration).Add(ivl.Duration)); err != nil {
			b.Fatal(err)
		}
	}
	if err := proc.db.Flush(); err != nil {
		b.Fatal(err)
	}
	if err := proc.db.Compact([]byte{0}, []byte{255, 255, 255, 255}, true); err != nil {
		b.Fatal(err)
	}
	elapsed := time.Since(start)
	cpu := processCPU() - cpuBefore
	b.StopTimer()
	close(stopSampling)
	<-samplingDone

	// Account for the actual request sequence, including the partial cycle.
	var dps, inputBytes, wantSummary int64
	for i, md := range requests {
		n := int64(b.N / len(requests))
		if i < b.N%len(requests) {
			n++
		}
		dps += n * int64(md.DataPointCount())
		inputBytes += n * int64(sizer.MetricsSize(md))
		wantSummary += n * benchSummary(md) * int64(len(cfg.Intervals))
	}
	if got := exported.Load(); got != wantSummary {
		b.Fatalf("exported service_summary=%d, want %d", got, wantSummary)
	}
	b.ReportMetric(float64(dps)/intakeElapsed.Seconds(), "intake-dps/s")
	b.ReportMetric(float64(dps)/intakeCPU.Seconds(), "intake-dps/cpu-s")
	b.ReportMetric(float64(dps)/elapsed.Seconds(), "completed-dps/s")
	b.ReportMetric(float64(dps)/cpu.Seconds(), "completed-dps/cpu-s")
	b.ReportMetric((elapsed - intakeElapsed).Seconds(), "drain-s")
	b.ReportMetric(intakeElapsed.Seconds()/4, "longest-windows")
	b.ReportMetric(float64(dirSize(cfg.Directory))/(1<<20), "disk-MiB")
	reportBytesWritten(b, proc, float64(inputBytes))
	b.ReportMetric(float64(peakHeap.Load())/(1<<20), "peak-heap-MiB")
	if err := p.Shutdown(context.Background()); err != nil {
		b.Fatal(err)
	}

}

func processCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func dirSize(dir string) int64 {
	var total int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

// apmBenchShape generates requests shaped like the metrics produced by the
// elasticapm connector: one resource per service instance, a service
// summary, transaction duration histograms and success counts per
// transaction group, and destination response times.
type apmBenchShape struct {
	services, instances, transactions, destinations, groupsPerRequest int
}

func (s apmBenchShape) generate(n int) []pmetric.Metrics {
	rng := rand.New(rand.NewSource(1))
	out := make([]pmetric.Metrics, n)
	now := pcommon.NewTimestampFromTime(time.Now())
	for i := range out {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		svc := rng.Intn(s.services)
		res := rm.Resource().Attributes()
		res.PutStr("service.name", fmt.Sprint("service-", svc))
		res.PutStr("deployment.environment", "production")
		res.PutStr("telemetry.sdk.language", "java")
		res.PutStr("agent.name", "opentelemetry/java")
		res.PutStr("service.version", "1.4.2")
		res.PutStr("service.instance.id", fmt.Sprintf("service-%d-%d", svc, rng.Intn(s.instances)))
		res.PutStr("host.name", fmt.Sprint("host-", rng.Intn(64)))
		res.PutStr("cloud.provider", "aws")
		res.PutStr("cloud.region", "us-east-1")
		res.PutStr("process.runtime.name", "OpenJDK Runtime Environment")
		res.PutStr("process.runtime.version", "21.0.4")
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("elasticapm")

		summary := sm.Metrics().AppendEmpty()
		summary.SetName("service_summary")
		sum := summary.SetEmptySum()
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		dp := sum.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("metricset.name", "service_summary")
		dp.SetTimestamp(now)
		dp.SetIntValue(int64(1 + rng.Intn(500)))

		hist := sm.Metrics().AppendEmpty()
		hist.SetName("transaction.duration.histogram")
		eh := hist.SetEmptyExponentialHistogram()
		eh.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		success := sm.Metrics().AppendEmpty()
		success.SetName("event.success_count")
		sc := success.SetEmptyHistogram()
		sc.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
		for g := 0; g < s.groupsPerRequest; g++ {
			attrs := pcommon.NewMap()
			attrs.PutStr("metricset.name", "transaction")
			attrs.PutBool("transaction.root", true)
			attrs.PutStr("transaction.name", fmt.Sprintf("GET /api/v1/resource-%d", rng.Intn(s.transactions)))
			attrs.PutStr("transaction.type", "request")
			attrs.PutStr("transaction.result", []string{"HTTP 2xx", "HTTP 4xx", "HTTP 5xx"}[rng.Intn(3)])
			attrs.PutStr("event.outcome", []string{"success", "failure"}[rng.Intn(2)])

			edp := eh.DataPoints().AppendEmpty()
			attrs.CopyTo(edp.Attributes())
			edp.SetTimestamp(now)
			edp.SetScale(3)
			edp.Positive().SetOffset(int32(400 + rng.Intn(40)))
			var count uint64
			for k := 0; k < 12; k++ {
				c := uint64(rng.Intn(8))
				edp.Positive().BucketCounts().Append(c)
				count += c
			}
			edp.SetCount(count)
			edp.SetSum(float64(count) * 1500)

			sdp := sc.DataPoints().AppendEmpty()
			attrs.CopyTo(sdp.Attributes())
			sdp.SetTimestamp(now)
			sdp.ExplicitBounds().Append(1)
			sdp.BucketCounts().Append(uint64(rng.Intn(3)), count)
			sdp.SetCount(count)
			sdp.SetSum(float64(count))
		}

		for _, name := range []string{"span.destination.service.response_time.sum.us", "span.destination.service.response_time.count"} {
			m := sm.Metrics().AppendEmpty()
			m.SetName(name)
			ds := m.SetEmptySum()
			ds.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
			for d := 0; d < s.groupsPerRequest/3; d++ {
				ddp := ds.DataPoints().AppendEmpty()
				ddp.Attributes().PutStr("metricset.name", "service_destination")
				ddp.Attributes().PutStr("span.destination.service.resource", fmt.Sprint("db-", rng.Intn(s.destinations)))
				ddp.Attributes().PutStr("event.outcome", "success")
				ddp.SetTimestamp(now)
				ddp.SetIntValue(int64(rng.Intn(10000)))
			}
		}
		out[i] = md
	}
	return out
}

func benchSummary(md pmetric.Metrics) int64 {
	var n int64
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.Name() == "service_summary" {
					dps := m.Sum().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						n += dps.At(d).IntValue()
					}
				}
			}
		}
	}
	return n
}

// Includes the final flush, export tombstones, and forced compaction. This is
// settled processor work, not total filesystem/device write amplification.
func reportBytesWritten(b *testing.B, p *Processor, inputBytes float64) {
	m := p.db.Metrics()
	var flushed, compacted uint64
	for _, l := range m.Levels {
		flushed += l.BytesFlushed
		compacted += l.BytesCompacted
	}
	var logBytes uint64
	if p.preAggLog != nil {
		p.preAggLog.mu.Lock()
		logBytes = p.preAggLog.written
		p.preAggLog.mu.Unlock()
	}
	b.ReportMetric(float64(m.WAL.BytesWritten+flushed+compacted+logBytes)/inputBytes, "write-amp")
	b.ReportMetric(inputBytes/(1<<20), "in-MiB")
}
