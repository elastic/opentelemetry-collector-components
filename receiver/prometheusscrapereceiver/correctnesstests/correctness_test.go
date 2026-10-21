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

package correctnesstests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	promconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"

	scraper "github.com/elastic/opentelemetry-collector-components/receiver/prometheusscrapereceiver"
	prwexporter "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter"
	upstream "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
)

const textFixture = `# HELP http_requests_total Total requests.
# TYPE http_requests_total counter
http_requests_total{code="200",method="get"} 10
http_requests_total{code="500",method="get"} 2
# HELP temp Temperature.
# TYPE temp gauge
temp 21.5
# HELP lat Latency.
# TYPE lat histogram
lat_bucket{le="0.00001"} 1
lat_bucket{le="0.5"} 2
lat_bucket{le="1.0"} 2
lat_bucket{le="+Inf"} 3
lat_sum 1.5
lat_count 3
# HELP rpc RPC durations.
# TYPE rpc summary
rpc{quantile="0.5"} 2
rpc{quantile="0.99"} 9
rpc_sum 10
rpc_count 5
# TYPE mystery untyped
mystery 7
# TYPE nan_gauge gauge
nan_gauge NaN
# TYPE quiet summary
quiet{quantile="0.5"} NaN
quiet_sum 0
quiet_count 0
# TYPE target_info gauge
target_info{service_name="svc",service_version="1.0"} 1
# TYPE otel_scope_info gauge
otel_scope_info{otel_scope_name="scope",otel_scope_version="2"} 1
# TYPE stuff_created gauge
stuff_created 1.7e9
`

const openMetricsFixture = `# HELP reqs Requests.
# TYPE reqs counter
reqs_total{code="200"} 10
reqs_created{code="200"} 1.7e9
# TYPE lat histogram
lat_bucket{le="0.00001"} 1
lat_bucket{le="1.0"} 2
lat_bucket{le="+Inf"} 3
lat_sum 1.5
lat_count 3
lat_created 1.7e9
# TYPE rpc summary
rpc{quantile="0.5"} 2
rpc_sum 10
rpc_count 5
# TYPE build info
build_info{version="1.2.3"} 1
# TYPE target_info info
target_info{service_name="svc"} 1
# EOF
`

// labels that RW1 sanitization could rewrite.
const unusualLabelsFixture = `# TYPE odd gauge
odd{a__b="1",_foo="2"} 1
`

type fixture struct {
	name        string
	contentType string
	body        string
	native      bool
}

// nativeHistogramFixture exposes classic and native histograms in the protobuf
// format, which is the only one carrying native histograms.
func nativeHistogramFixture() fixture {
	reg := prometheus.NewRegistry()
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                           "rpc_duration",
		Help:                           "RPC durations.",
		NativeHistogramBucketFactor:    1.1,
		NativeHistogramZeroThreshold:   0.001,
		NativeHistogramMaxBucketNumber: 100,
	}, []string{"method"})
	for _, v := range []float64{0.0001, 0.5, 0.7, 2, 2.5, 8, -1.5} {
		h.WithLabelValues("get").Observe(v)
	}
	h.WithLabelValues("put").Observe(3)
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "temp", Help: "Temperature."})
	g.Set(21.5)
	reg.MustRegister(h, g)

	mfs, err := reg.Gather()
	if err != nil {
		panic(err)
	}
	format := expfmt.NewFormat(expfmt.TypeProtoDelim)
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, format)
	for _, mf := range mfs {
		if err := enc.Encode(mf); err != nil {
			panic(err)
		}
	}
	return fixture{"native_histogram", string(format), buf.String(), true}
}

var fixtures = []fixture{
	{"text", "text/plain; version=0.0.4", textFixture, false},
	{"openmetrics", "application/openmetrics-text; version=1.0.0; charset=utf-8", openMetricsFixture, false},
	nativeHistogramFixture(),
}

// sample is one remote write sample: a float (bits keep NaN and StaleNaN apart)
// or a native histogram.
type sample struct {
	ts   int64
	bits uint64
	hist *nativeHist
}

// series is a flattened view of a remote write time series, keyed by its sorted labels.
// value, ts and hist describe the earliest sample, samples holds every sample received.
type series struct {
	labels  map[string]string
	value   float64
	ts      int64
	hist    *nativeHist // set for native histogram samples
	samples []sample
}

// nativeHist is a comparable view of a native histogram: sparse buckets are
// expanded to absolute counts per bucket index.
type nativeHist struct {
	schema        int32
	count         uint64
	sum           float64
	zeroCount     uint64
	zeroThreshold float64
	positive      map[int32]int64
	negative      map[int32]int64
}

// expand turns spans and delta counts into absolute counts per bucket index.
// Empty buckets are dropped: the OTel to remote write conversion fills gaps
// between spans with explicit zero buckets, which is the same histogram.
func expand(spans []prompb.BucketSpan, deltas []int64) map[int32]int64 {
	out := map[int32]int64{}
	idx, count, d := int32(0), int64(0), 0
	for _, sp := range spans {
		idx += sp.Offset
		for range sp.Length {
			count += deltas[d]
			d++
			if count != 0 {
				out[idx] = count
			}
			idx++
		}
	}
	return out
}

func histFromProm(h *histogram.Histogram) *nativeHist {
	conv := func(spans []histogram.Span, deltas []int64) map[int32]int64 {
		ps := make([]prompb.BucketSpan, len(spans))
		for i, sp := range spans {
			ps[i] = prompb.BucketSpan{Offset: sp.Offset, Length: sp.Length}
		}
		return expand(ps, deltas)
	}
	return &nativeHist{
		schema: h.Schema, count: h.Count, sum: h.Sum, zeroCount: h.ZeroCount, zeroThreshold: h.ZeroThreshold,
		positive: conv(h.PositiveSpans, h.PositiveBuckets), negative: conv(h.NegativeSpans, h.NegativeBuckets),
	}
}

func histFromPRW(h prompb.Histogram) *nativeHist {
	return &nativeHist{
		schema: h.Schema, count: h.GetCountInt(), sum: h.Sum, zeroCount: h.GetZeroCountInt(), zeroThreshold: h.ZeroThreshold,
		positive: expand(h.PositiveSpans, h.PositiveDeltas), negative: expand(h.NegativeSpans, h.NegativeDeltas),
	}
}

type seriesSet map[string]series

func key(lbls map[string]string) string {
	names := make([]string, 0, len(lbls))
	for n := range lbls {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		fmt.Fprintf(&b, "%s=%q,", n, lbls[n])
	}
	return b.String()
}

func (s seriesSet) add(lbls map[string]string, ts int64, v float64, h *nativeHist) {
	k := key(lbls)
	cur, ok := s[k]
	cur.labels = lbls
	cur.samples = append(cur.samples, sample{ts: ts, bits: math.Float64bits(v), hist: h})
	// Keep the earliest sample so that the first scrape is compared by diff.
	if !ok || ts < cur.ts {
		cur.value, cur.ts, cur.hist = v, ts, h
	}
	s[k] = cur
}

func (s seriesSet) hasName(name string) bool {
	for _, v := range s {
		if v.labels[model.MetricNameLabel] == name {
			return true
		}
	}
	return false
}

// sameValue compares the exact bits, so that an ordinary NaN and Prometheus'
// StaleNaN (a NaN with a specific payload) are different values.
func sameValue(a, b float64) bool {
	return math.Float64bits(a) == math.Float64bits(b)
}

// volatile series carry values that change between scrapes by design.
func volatile(lbls map[string]string) bool {
	return lbls[model.MetricNameLabel] == "scrape_duration_seconds"
}

// diff reports differences between the expected and the actual series sets.
func diff(want, got seriesSet) []string {
	var out []string
	for k, w := range want {
		g, ok := got[k]
		switch {
		case !ok:
			out = append(out, "missing:    "+k)
		case (w.hist != nil || g.hist != nil) && !reflect.DeepEqual(w.hist, g.hist):
			out = append(out, fmt.Sprintf("histogram:  %s want=%+v got=%+v", k, w.hist, g.hist))
		case w.hist == nil && !volatile(w.labels) && !sameValue(w.value, g.value):
			out = append(out, fmt.Sprintf("value:      %s want=%v got=%v", k, w.value, g.value))
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			out = append(out, "unexpected: "+k)
		}
	}
	sort.Strings(out)
	return out
}

// sortedSamples returns the first k samples of the series ordered by timestamp.
func sortedSamples(sr series, k int) []sample {
	out := append([]sample(nil), sr.samples...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].ts < out[j].ts })
	if len(out) > k {
		out = out[:k]
	}
	return out
}

// diffSamples compares the first k samples of every series, bit exact, so that a lost
// later sample or a stale marker turned into an ordinary NaN is reported. Timestamps
// are compared for the series accepted by checkTS (explicit timestamps); the others
// carry the scrape time, which differs between the two runs.
func diffSamples(want, got seriesSet, k int, checkTS func(map[string]string) bool) []string {
	var out []string
	for key, w := range want {
		g, ok := got[key]
		if !ok {
			out = append(out, "missing:    "+key)
			continue
		}
		ws, gs := sortedSamples(w, k), sortedSamples(g, k)
		if len(ws) != len(gs) {
			out = append(out, fmt.Sprintf("samples:    %s want=%d got=%d", key, len(ws), len(gs)))
			continue
		}
		for i := range ws {
			switch {
			case checkTS(w.labels) && ws[i].ts != gs[i].ts:
				out = append(out, fmt.Sprintf("timestamp:  %s sample %d want=%d got=%d", key, i, ws[i].ts, gs[i].ts))
			case (ws[i].hist != nil || gs[i].hist != nil) && !reflect.DeepEqual(ws[i].hist, gs[i].hist):
				out = append(out, fmt.Sprintf("histogram:  %s sample %d want=%+v got=%+v", key, i, ws[i].hist, gs[i].hist))
			case ws[i].hist == nil && !volatile(w.labels) && ws[i].bits != gs[i].bits:
				out = append(out, fmt.Sprintf("value:      %s sample %d want=%#x got=%#x", key, i, ws[i].bits, gs[i].bits))
			}
		}
	}
	for key := range got {
		if _, ok := want[key]; !ok {
			out = append(out, "unexpected: "+key)
		}
	}
	sort.Strings(out)
	return out
}

// find returns the series with the given metric name.
func (s seriesSet) find(name string) (series, bool) {
	for _, sr := range s {
		if sr.labels[model.MetricNameLabel] == name {
			return sr, true
		}
	}
	return series{}, false
}

func isStale(sm sample) bool { return sm.hist == nil && sm.bits == value.StaleNaN }

func target(t *testing.T, contentType, body string) string {
	t.Helper()
	return dynamicTarget(t, contentType, func(int) string { return body })
}

// dynamicTarget serves body(k) on the k-th request (k starts at 1), so that two
// independent targets expose the same sequence of scrapes.
func dynamicTarget(t *testing.T, contentType string, body func(k int) string) string {
	t.Helper()
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write([]byte(body(int(requests.Add(1)))))
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func scrapeConfig(addr string, native bool) map[string]any {
	job := map[string]any{
		"job_name":        "job",
		"scrape_interval": "100ms",
		"scrape_timeout":  "90ms",
		"static_configs":  []any{map[string]any{"targets": []any{addr}}},
	}
	if native {
		job["scrape_native_histograms"] = true
		job["scrape_protocols"] = []any{"PrometheusProto"}
	}
	return map[string]any{"scrape_configs": []any{job}}
}

// recordingAppendable is the control: it stores what Prometheus' own scrape loop
// hands to a storage for the fixture.
type recordingAppendable struct {
	mu    sync.Mutex
	first seriesSet // the first scrape
	all   seriesSet // every scrape
}

func (r *recordingAppendable) snapshotAll() seriesSet {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(seriesSet, len(r.all))
	for k, v := range r.all {
		v.samples = append([]sample(nil), v.samples...)
		out[k] = v
	}
	return out
}

func (r *recordingAppendable) AppenderV2(context.Context) storage.AppenderV2 {
	return &recordingAppender{parent: r, set: seriesSet{}}
}

type recordingAppender struct {
	parent *recordingAppendable
	set    seriesSet
}

func (a *recordingAppender) Append(_ storage.SeriesRef, ls labels.Labels, _, t int64, v float64, h *histogram.Histogram, _ *histogram.FloatHistogram, _ storage.AppendV2Options) (storage.SeriesRef, error) {
	if h != nil {
		a.set.add(ls.Map(), t, 0, histFromProm(h))
	} else {
		a.set.add(ls.Map(), t, v, nil)
	}
	return storage.SeriesRef(ls.Hash() | 1), nil
}

func (a *recordingAppender) Commit() error {
	a.parent.mu.Lock()
	defer a.parent.mu.Unlock()
	if a.parent.first == nil {
		a.parent.first = a.set
	}
	if a.parent.all == nil {
		a.parent.all = seriesSet{}
	}
	for _, sr := range a.set {
		for _, sm := range sr.samples {
			a.parent.all.add(sr.labels, sm.ts, math.Float64frombits(sm.bits), sm.hist)
		}
	}
	return nil
}

func (a *recordingAppender) Rollback() error { return nil }

// controlSeries scrapes addr with the plain Prometheus scrape manager and returns
// the series of the first scrape.
func controlSeries(t *testing.T, addr string, native bool) seriesSet {
	t.Helper()
	rec := startControl(t, addr, native)
	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.first != nil
	}, 10*time.Second, 20*time.Millisecond)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.first
}

// startControl starts the plain Prometheus scrape manager against addr, recording
// everything its scrape loop hands to a storage.
func startControl(t *testing.T, addr string, native bool) *recordingAppendable {
	t.Helper()
	nativeCfg := ""
	if native {
		nativeCfg = "    scrape_native_histograms: true\n    scrape_protocols: [PrometheusProto]\n"
	}
	yml := fmt.Sprintf(`
scrape_configs:
  - job_name: job
    scrape_interval: 100ms
    scrape_timeout: 90ms
    fallback_scrape_protocol: PrometheusText0.0.4
%s    static_configs:
      - targets: ["%s"]
`, nativeCfg, addr)
	return startControlYAML(t, yml, addr)
}

// startControlYAML is startControl with a full Prometheus configuration (one job
// named "job" scraping addr).
func startControlYAML(t *testing.T, yml, addr string) *recordingAppendable {
	t.Helper()
	cfg, err := promconfig.Load(yml, slog.Default())
	require.NoError(t, err)

	rec := &recordingAppendable{}
	// Same scrape options as the receiver under test.
	m, err := scrape.NewManager(&scrape.Options{AppendMetadata: true, ParseST: false}, slog.Default(), nil, nil, rec, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, m.ApplyConfig(cfg))

	tsets := make(chan map[string][]*targetgroup.Group, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = m.Run(tsets)
	}()
	tsets <- map[string][]*targetgroup.Group{"job": {{
		Targets: []model.LabelSet{{model.AddressLabel: model.LabelValue(addr)}},
	}}}
	t.Cleanup(func() { m.Stop(); <-done })
	return rec
}

// writeSink is a fake remote write endpoint.
type writeSink struct {
	mu  sync.Mutex
	set seriesSet
	srv *httptest.Server
}

func newWriteSink(t *testing.T) *writeSink {
	t.Helper()
	ws := &writeSink{set: seriesSet{}}
	ws.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		raw, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var wr prompb.WriteRequest
		if err := proto.Unmarshal(raw, &wr); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ws.mu.Lock()
		defer ws.mu.Unlock()
		for _, ts := range wr.Timeseries {
			lbls := make(map[string]string, len(ts.Labels))
			for _, l := range ts.Labels {
				lbls[l.Name] = l.Value
			}
			for _, s := range ts.Samples {
				ws.set.add(lbls, s.Timestamp, s.Value, nil)
			}
			for _, h := range ts.Histograms {
				if value.IsStaleNaN(h.Sum) {
					// A native histogram with a StaleNaN sum is the stale marker of the series,
					// which Prometheus itself appends as a float StaleNaN.
					ws.set.add(lbls, h.Timestamp, math.Float64frombits(value.StaleNaN), nil)
					continue
				}
				ws.set.add(lbls, h.Timestamp, 0, histFromPRW(h))
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(ws.srv.Close)
	return ws
}

func (ws *writeSink) snapshot() seriesSet {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	out := make(seriesSet, len(ws.set))
	for k, v := range ws.set {
		v.samples = append([]sample(nil), v.samples...)
		out[k] = v
	}
	return out
}

// requiredExporterConfig is the exporter configuration documented in the README.
func requiredExporterConfig(endpoint string) map[string]any {
	return map[string]any{
		"endpoint":             endpoint,
		"translation_strategy": "UnderscoreEscapingWithoutSuffixes",
		"disable_scope_info":   true,
		"retry_on_failure":     map[string]any{"enabled": false},
		"remote_write_queue":   map[string]any{"enabled": false},
	}
}

// defaultExporterConfig leaves the translation strategy at its default.
func defaultExporterConfig(endpoint string) map[string]any {
	return map[string]any{
		"endpoint":           endpoint,
		"retry_on_failure":   map[string]any{"enabled": false},
		"remote_write_queue": map[string]any{"enabled": false},
	}
}

type receiverKind int

const (
	ourReceiver receiverKind = iota
	upstreamReceiver
)

// runPipeline runs <receiver> -> prometheusremotewriteexporter -> fake endpoint and
// returns what the endpoint received for the first scrape. The exporter is configured
// from YAML-like config and goes through Validate, as in a real collector.
func runPipeline(t *testing.T, kind receiverKind, addr string, native bool, exporterCfg func(string) map[string]any) seriesSet {
	t.Helper()
	sink := startPipeline(t, kind, addr, native, exporterCfg)
	require.Eventually(t, func() bool { return sink.snapshot().hasName("up") }, 10*time.Second, 20*time.Millisecond,
		"no data reached the remote write endpoint")
	return sink.snapshot()
}

// startPipeline starts <receiver> -> prometheusremotewriteexporter -> fake endpoint and
// returns the endpoint, which keeps accumulating what it receives.
func startPipeline(t *testing.T, kind receiverKind, addr string, native bool, exporterCfg func(string) map[string]any) *writeSink {
	t.Helper()
	return startPipelineConfig(t, kind, scrapeConfig(addr, native), exporterCfg)
}

// startPipelineConfig is startPipeline with a full receiver `config` (Prometheus configuration).
func startPipelineConfig(t *testing.T, kind receiverKind, promCfg map[string]any, exporterCfg func(string) map[string]any) *writeSink {
	t.Helper()
	ctx := context.Background()
	host := componenttest.NewNopHost()
	sink := newWriteSink(t)

	expFactory := prwexporter.NewFactory()
	expCfg := expFactory.CreateDefaultConfig()
	require.NoError(t, confmap.NewFromStringMap(exporterCfg(sink.srv.URL+"/api/v1/write")).Unmarshal(expCfg))
	require.NoError(t, confmap.Validate(expCfg))
	exp, err := expFactory.CreateMetrics(ctx, exportertest.NewNopSettings(component.MustNewType("prometheusremotewrite")), expCfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(ctx, host))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(ctx)) })

	var rcvFactory receiver.Factory
	switch kind {
	case ourReceiver:
		rcvFactory = scraper.NewFactory()
	default:
		rcvFactory = upstream.NewFactory()
	}
	rcvCfg := rcvFactory.CreateDefaultConfig()
	require.NoError(t, confmap.NewFromStringMap(map[string]any{"config": promCfg}).Unmarshal(rcvCfg))
	require.NoError(t, confmap.Validate(rcvCfg))
	rcv, err := rcvFactory.CreateMetrics(ctx, receivertest.NewNopSettings(rcvFactory.Type()), rcvCfg, exp)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(ctx, host))
	t.Cleanup(func() { require.NoError(t, rcv.Shutdown(ctx)) })
	return sink
}

// TestRoundTripMatchesPrometheus: receiver + exporter (documented config) must
// store exactly what Prometheus' own scrape loop produces for the same target.
func TestRoundTripMatchesPrometheus(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			addr := target(t, f.contentType, f.body)
			want := controlSeries(t, addr, f.native)
			require.True(t, want.hasName("up"))
			got := runPipeline(t, ourReceiver, addr, f.native, requiredExporterConfig)
			assert.Empty(t, diff(want, got), "round trip differs from Prometheus")
		})
	}
}

// TestExporterKeepsUnusualLabels documents that, with the required exporter
// configuration, labels with double underscores or a leading underscore are
// written unchanged. If a future exporter version sanitizes them, this test
// fails and the README limitations need to be updated.
func TestExporterKeepsUnusualLabels(t *testing.T) {
	addr := target(t, "text/plain; version=0.0.4", unusualLabelsFixture)
	got := runPipeline(t, ourReceiver, addr, false, requiredExporterConfig)
	for _, s := range got {
		if s.labels[model.MetricNameLabel] != "odd" {
			continue
		}
		assert.Equal(t, "1", s.labels["a__b"])
		assert.Equal(t, "2", s.labels["_foo"])
		return
	}
	require.Fail(t, "series odd not found")
}

// TestUpstreamReceiverDiff is informational: it logs what the upstream receiver +
// exporter (default strategy) change compared to Prometheus.
func TestUpstreamReceiverDiff(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			addr := target(t, f.contentType, f.body)
			want := controlSeries(t, addr, f.native)
			got := runPipeline(t, upstreamReceiver, addr, f.native, defaultExporterConfig)
			d := diff(want, got)
			t.Logf("upstream prometheusreceiver differs from Prometheus in %d series:\n%s", len(d), strings.Join(d, "\n"))
		})
	}
}

// TestDefaultStrategyBreaksRoundTrip guards the README requirement: with the
// default exporter translation strategy the monotonic Sums get renamed.
func TestDefaultStrategyBreaksRoundTrip(t *testing.T) {
	addr := target(t, "text/plain; version=0.0.4", textFixture)
	want := controlSeries(t, addr, false)
	got := runPipeline(t, ourReceiver, addr, false, defaultExporterConfig)
	d := diff(want, got)
	t.Logf("default strategy differences:\n%s", strings.Join(d, "\n"))
	assert.NotEmpty(t, d)
	assert.True(t, got.hasName("lat_bucket_total"), "expected lat_bucket to be renamed to lat_bucket_total")
	assert.False(t, got.hasName("lat_bucket"))
}

// scrapes is the number of samples per series compared by the sequence tests.
const scrapes = 4

// explicitBase is the first explicit timestamp (ms) exposed by sequenceTextBody.
const explicitBase = int64(1_700_000_000_000)

// resetValues is the sequence of reset_total: the counter resets on the third scrape.
var resetValues = []float64{100, 200, 5, 15}

func resetValue(k int) int { return int(resetValues[(k-1)%len(resetValues)]) }

// sequenceTextBody is what the k-th scrape of the target exposes. The values are a
// function of k only, so that two targets expose the same sequence:
//   - ctr_total grows with every scrape (later samples must not be lost),
//   - ts_gauge carries an explicit timestamp (it must survive unchanged),
//   - nan_gauge is an ordinary NaN (it must not become a stale marker),
//   - reset_total is a counter that resets (the raw values must be passed through),
//   - vanishing disappears from the third scrape on, so Prometheus appends a stale marker.
func sequenceTextBody(k int) string {
	body := fmt.Sprintf(`# TYPE ctr_total counter
ctr_total %d
# TYPE reset_total counter
reset_total %d
# TYPE ts_gauge gauge
ts_gauge %d %d
# TYPE nan_gauge gauge
nan_gauge NaN
`, k*10, resetValue(k), k, explicitBase+int64(k)*1000)
	if k <= 2 {
		body += fmt.Sprintf("# TYPE vanishing gauge\nvanishing %d\n", k)
	}
	return body
}

// sequenceProtoBody is the protobuf equivalent with a native histogram that grows with
// every scrape and disappears from the third scrape on.
func sequenceProtoBody(k int) string {
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "ctr_total", Help: "Counter."})
	c.Add(float64(k * 10))
	r := prometheus.NewCounter(prometheus.CounterOpts{Name: "reset_total", Help: "Counter that resets."})
	r.Add(float64(resetValue(k)))
	// A native histogram with an explicit timestamp is never marked stale by Prometheus.
	ts := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "ts_duration", Help: "Durations with an explicit timestamp.",
		NativeHistogramBucketFactor: 1.1, NativeHistogramMaxBucketNumber: 100,
	})
	ts.Observe(float64(k))
	reg.MustRegister(c, r, ts)
	if k <= 2 {
		h := prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "rpc_duration", Help: "RPC durations.",
			NativeHistogramBucketFactor: 1.1, NativeHistogramMaxBucketNumber: 100,
		})
		for i := 1; i <= k; i++ {
			h.Observe(float64(i))
		}
		reg.MustRegister(h)
	}
	mfs, err := reg.Gather()
	if err != nil {
		panic(err)
	}
	stamp := explicitBase + int64(k)*1000
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeProtoDelim))
	for _, mf := range mfs {
		if mf.GetName() == "ts_duration" {
			for _, m := range mf.GetMetric() {
				m.TimestampMs = &stamp
			}
		}
		if err := enc.Encode(mf); err != nil {
			panic(err)
		}
	}
	return buf.String()
}

// withoutInstance drops the instance label, which holds the (different) address of the
// two targets.
func withoutInstance(s seriesSet) seriesSet {
	out := seriesSet{}
	for _, sr := range s {
		lbls := make(map[string]string, len(sr.labels))
		for k, v := range sr.labels {
			if k != "instance" {
				lbls[k] = v
			}
		}
		for _, sm := range sr.samples {
			out.add(lbls, sm.ts, math.Float64frombits(sm.bits), sm.hist)
		}
	}
	return out
}

// sequenceReady is true once the series of the first scrapes and the stale marker of the
// vanishing series were received.
func sequenceReady(vanishing string) func(seriesSet) bool {
	return func(s seriesSet) bool {
		c, ok := s.find("ctr_total")
		if !ok || len(c.samples) < scrapes {
			return false
		}
		v, ok := s.find(vanishing)
		if !ok {
			return false
		}
		for _, sm := range v.samples {
			if isStale(sm) {
				return true
			}
		}
		return false
	}
}

// TestRoundTripSamplesTimestampsAndStaleMarkers compares several samples per series,
// explicit timestamps and the exact stale markers (bit for bit) between what Prometheus'
// scrape loop produces and what comes out of the receiver and the exporter.
func TestRoundTripSamplesTimestampsAndStaleMarkers(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        func(int) string
		native      bool
		vanishing   string
	}{
		{"text", "text/plain; version=0.0.4", sequenceTextBody, false, "vanishing"},
		{"native_histogram", string(expfmt.NewFormat(expfmt.TypeProtoDelim)), sequenceProtoBody, true, "rpc_duration"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Two independent targets, so that both see scrape 1, 2, 3...
			rec := startControl(t, dynamicTarget(t, tt.contentType, tt.body), tt.native)
			sink := startPipeline(t, ourReceiver, dynamicTarget(t, tt.contentType, tt.body), tt.native, requiredExporterConfig)

			// On a timeout keep going: the diff below says what is missing or different.
			ready := sequenceReady(tt.vanishing)
			assert.Eventually(t, func() bool { return ready(rec.snapshotAll()) && ready(sink.snapshot()) },
				15*time.Second, 50*time.Millisecond, "the scrapes and the stale marker were not received")

			want, got := withoutInstance(rec.snapshotAll()), withoutInstance(sink.snapshot())
			explicitTS := func(lbls map[string]string) bool {
				return lbls[model.MetricNameLabel] == "ts_gauge" || lbls[model.MetricNameLabel] == "ts_duration"
			}
			assert.Empty(t, diffSamples(want, got, scrapes, explicitTS), "round trip differs from Prometheus")

			// Independent of the control: what must come out.
			ctr, ok := got.find("ctr_total")
			require.True(t, ok)
			var values []float64
			for _, sm := range sortedSamples(ctr, scrapes) {
				values = append(values, math.Float64frombits(sm.bits))
			}
			assert.Equal(t, []float64{10, 20, 30, 40}, values, "later samples of a series are kept")

			reset, ok := got.find("reset_total")
			require.True(t, ok)
			var resets []float64
			for _, sm := range sortedSamples(reset, scrapes) {
				resets = append(resets, math.Float64frombits(sm.bits))
			}
			assert.Equal(t, resetValues, resets, "a counter reset is passed through, not corrected")

			vanishing, ok := got.find(tt.vanishing)
			require.True(t, ok)
			vs := sortedSamples(vanishing, 3)
			require.Len(t, vs, 3)
			assert.False(t, isStale(vs[0]) || isStale(vs[1]), "values before the series vanished are not stale")
			assert.True(t, isStale(vs[2]), "the stale marker keeps the exact StaleNaN bits")
			if tt.native {
				require.NotNil(t, vs[0].hist)
				require.NotNil(t, vs[1].hist)
				assert.EqualValues(t, 1, vs[0].hist.count)
				assert.EqualValues(t, 2, vs[1].hist.count)
			}

			if tt.native {
				hist, ok := got.find("ts_duration")
				require.True(t, ok)
				var stamps []int64
				for _, sm := range sortedSamples(hist, scrapes) {
					require.NotNil(t, sm.hist, "native histogram with an explicit timestamp")
					stamps = append(stamps, sm.ts)
				}
				assert.Equal(t, []int64{explicitBase + 1000, explicitBase + 2000, explicitBase + 3000, explicitBase + 4000}, stamps,
					"the explicit timestamp of a native histogram is kept")
				return
			}
			ts, ok := got.find("ts_gauge")
			require.True(t, ok)
			var stamps []int64
			for _, sm := range sortedSamples(ts, scrapes) {
				stamps = append(stamps, sm.ts)
			}
			assert.Equal(t, []int64{explicitBase + 1000, explicitBase + 2000, explicitBase + 3000, explicitBase + 4000}, stamps,
				"explicit timestamps are kept")

			nan, ok := got.find("nan_gauge")
			require.True(t, ok)
			for _, sm := range sortedSamples(nan, scrapes) {
				assert.True(t, math.IsNaN(math.Float64frombits(sm.bits)))
				assert.False(t, isStale(sm), "an ordinary NaN is not turned into a stale marker")
			}
		})
	}
}

// addExternalLabels applies global.external_labels the way Prometheus does when it
// writes a series out: a label is added only if the series does not have it.
func addExternalLabels(s seriesSet, ext map[string]string) seriesSet {
	out := seriesSet{}
	for _, sr := range s {
		lbls := make(map[string]string, len(sr.labels)+len(ext))
		for k, v := range ext {
			lbls[k] = v
		}
		for k, v := range sr.labels {
			lbls[k] = v
		}
		for _, sm := range sr.samples {
			out.add(lbls, sm.ts, math.Float64frombits(sm.bits), sm.hist)
		}
	}
	return out
}

// TestRoundTripExternalAndRoutingLabels covers what the README documents for routing:
// labels added by relabeling (data_stream_dataset/namespace) and global.external_labels
// must reach the endpoint, and an external label must not override a label the series has.
func TestRoundTripExternalAndRoutingLabels(t *testing.T) {
	addr := target(t, "text/plain; version=0.0.4", `# TYPE routed gauge
routed 1
# TYPE own_cluster gauge
own_cluster{cluster="mine"} 2
`)
	ext := map[string]string{"cluster": "c1", "region": "eu"}

	// The control only applies the relabeling: external labels are added when writing out.
	rec := startControlYAML(t, fmt.Sprintf(`
global:
  external_labels: {cluster: c1, region: eu}
scrape_configs:
  - job_name: job
    scrape_interval: 100ms
    scrape_timeout: 90ms
    fallback_scrape_protocol: PrometheusText0.0.4
    relabel_configs:
      - {target_label: data_stream_dataset, replacement: myapp}
      - {target_label: data_stream_namespace, replacement: production}
    static_configs:
      - targets: ["%s"]
`, addr), addr)
	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.first != nil
	}, 10*time.Second, 20*time.Millisecond)
	rec.mu.Lock()
	want := addExternalLabels(rec.first, ext)
	rec.mu.Unlock()

	sink := startPipelineConfig(t, ourReceiver, map[string]any{
		"global": map[string]any{"external_labels": map[string]any{"cluster": "c1", "region": "eu"}},
		"scrape_configs": []any{map[string]any{
			"job_name":        "job",
			"scrape_interval": "100ms",
			"scrape_timeout":  "90ms",
			"relabel_configs": []any{
				map[string]any{"target_label": "data_stream_dataset", "replacement": "myapp"},
				map[string]any{"target_label": "data_stream_namespace", "replacement": "production"},
			},
			"static_configs": []any{map[string]any{"targets": []any{addr}}},
		}},
	}, requiredExporterConfig)
	require.Eventually(t, func() bool { return sink.snapshot().hasName("up") }, 10*time.Second, 20*time.Millisecond)
	got := sink.snapshot()

	assert.Empty(t, diff(want, got), "round trip differs from Prometheus plus external labels")

	routed, ok := got.find("routed")
	require.True(t, ok)
	assert.Equal(t, "myapp", routed.labels["data_stream_dataset"])
	assert.Equal(t, "production", routed.labels["data_stream_namespace"])
	assert.Equal(t, "c1", routed.labels["cluster"])
	assert.Equal(t, "eu", routed.labels["region"])
	own, ok := got.find("own_cluster")
	require.True(t, ok)
	assert.Equal(t, "mine", own.labels["cluster"], "an external label does not override the series' own label")
	up, ok := got.find("up")
	require.True(t, ok)
	assert.Equal(t, "myapp", up.labels["data_stream_dataset"], "report series are routed too")
}
