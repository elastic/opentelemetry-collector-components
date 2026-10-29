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
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/config"
	"github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/metadata"
)

// BenchmarkPreAggregation measures sustained ingestion with APM-shaped
// metrics on an on-disk database, with pre-aggregation off, on, and on with
// the log. It also reports the bytes written to disk, relative to the
// protobuf size of the input.
//
// Time is compressed twelvefold: the intervals are 5s, 50s, and 5m instead
// of 1m, 10m, and 60m, so a run of a few seconds crosses several window
// boundaries. For example:
//
//	go test -run '^$' -bench BenchmarkPreAggregation -benchtime 15s .
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
			{Duration: 5 * time.Second}, {Duration: 50 * time.Second}, {Duration: 5 * time.Minute},
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
	p, err := NewFactory().CreateMetrics(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
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
	var dpsPerRequest, bytesPerRequests int64
	var sizer pmetric.ProtoMarshaler
	for _, md := range requests {
		dpsPerRequest += int64(md.DataPointCount())
		bytesPerRequests += int64(sizer.MetricsSize(md))
	}

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
	elapsed := time.Since(start)
	cpu := processCPU() - cpuBefore
	b.StopTimer()
	close(stopSampling)
	<-samplingDone

	dps := float64(b.N) * float64(dpsPerRequest) / float64(len(requests))
	b.ReportMetric(dps/elapsed.Seconds(), "datapoints/s")
	b.ReportMetric(dps/cpu.Seconds(), "datapoints/cpu-s")
	b.ReportMetric(float64(dirSize(cfg.Directory))/(1<<20), "disk-MiB")
	reportBytesWritten(b, p.(*Processor), float64(b.N)*float64(bytesPerRequests)/float64(len(requests)))
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

// reportBytesWritten reports the bytes written by Pebble (write-ahead log,
// flushes, and compactions) and by the pre-aggregation log, and their sum
// relative to the input size.
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
	const mib = 1 << 20
	total := float64(m.WAL.BytesWritten + flushed + compacted + logBytes)
	b.ReportMetric(inputBytes/mib, "in-MiB")
	b.ReportMetric(float64(m.WAL.BytesWritten)/mib, "wal-MiB")
	b.ReportMetric(float64(flushed)/mib, "flush-MiB")
	b.ReportMetric(float64(compacted)/mib, "compact-MiB")
	b.ReportMetric(float64(logBytes)/mib, "log-MiB")
	b.ReportMetric(total/inputBytes, "write-amp")
}
