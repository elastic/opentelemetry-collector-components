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

// series is a flattened view of a remote write time series, keyed by its sorted labels.
type series struct {
	labels map[string]string
	value  float64
	ts     int64
	hist   *nativeHist // set for native histogram samples
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
	// Keep the earliest sample so that the first scrape is compared.
	if cur, ok := s[k]; !ok || ts < cur.ts {
		s[k] = series{labels: lbls, value: v, ts: ts, hist: h}
	}
}

func (s seriesSet) hasName(name string) bool {
	for _, v := range s {
		if v.labels[model.MetricNameLabel] == name {
			return true
		}
	}
	return false
}

func sameValue(a, b float64) bool {
	return a == b || (math.IsNaN(a) && math.IsNaN(b))
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

func target(t *testing.T, contentType, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write([]byte(body))
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
	first seriesSet
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
	return nil
}

func (a *recordingAppender) Rollback() error { return nil }

// controlSeries scrapes addr with the plain Prometheus scrape manager and returns
// the series of the first scrape.
func controlSeries(t *testing.T, addr string, native bool) seriesSet {
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

	require.Eventually(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.first != nil
	}, 10*time.Second, 20*time.Millisecond)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.first
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
	require.NoError(t, confmap.NewFromStringMap(map[string]any{"config": scrapeConfig(addr, native)}).Unmarshal(rcvCfg))
	require.NoError(t, confmap.Validate(rcvCfg))
	rcv, err := rcvFactory.CreateMetrics(ctx, receivertest.NewNopSettings(rcvFactory.Type()), rcvCfg, exp)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(ctx, host))
	t.Cleanup(func() { require.NoError(t, rcv.Shutdown(ctx)) })

	require.Eventually(t, func() bool { return sink.snapshot().hasName("up") }, 10*time.Second, 20*time.Millisecond,
		"no data reached the remote write endpoint")
	return sink.snapshot()
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
