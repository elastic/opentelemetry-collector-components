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

package prometheusscrapereceiver

import (
	"bytes"
	"context"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/model/value"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

const textFixture = `# HELP http_requests_total Total requests.
# TYPE http_requests_total counter
http_requests_total{code="200"} 10
# HELP temp Temperature.
# TYPE temp gauge
temp 21.5
# HELP lat Latency.
# TYPE lat histogram
lat_bucket{le="0.00001"} 1
lat_bucket{le="1.0"} 2
lat_bucket{le="+Inf"} 3
lat_sum 1.5
lat_count 3
# HELP rpc RPC.
# TYPE rpc summary
rpc{quantile="0.5"} 2
rpc_sum 10
rpc_count 5
# TYPE target_info gauge
target_info{service_name="svc"} 1
# TYPE otel_scope_info gauge
otel_scope_info{otel_scope_name="scope"} 1
# TYPE odd gauge
odd{a__b="1",_foo="2"} 1
`

const openMetricsFixture = `# HELP reqs Requests.
# TYPE reqs counter
reqs_total{code="200"} 10
reqs_created{code="200"} 1.7e9
# TYPE lat histogram
lat_bucket{le="0.00001"} 1
lat_bucket{le="+Inf"} 3
lat_sum 1.5
lat_count 3
lat_created 1.7e9
# TYPE target_info info
target_info{service_name="svc"} 1
# EOF
`

type target struct {
	srv    *httptest.Server
	body   atomic.Value
	scrape atomic.Int32
}

func newTarget(t *testing.T, contentType, body string) *target {
	t.Helper()
	tg := &target{}
	tg.body.Store(body)
	tg.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		tg.scrape.Add(1)
		w.Header().Set("Content-Type", contentType)
		_, _ = w.Write([]byte(tg.body.Load().(string)))
	}))
	t.Cleanup(tg.srv.Close)
	return tg
}

func e2eConfig(t *testing.T, tg *target, extra ...map[string]any) *Config {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	job := map[string]any{
		"job_name":        "e2e",
		"scrape_interval": "100ms",
		"scrape_timeout":  "90ms",
		"static_configs": []any{map[string]any{
			"targets": []any{strings.TrimPrefix(tg.srv.URL, "http://")},
		}},
	}
	for _, e := range extra {
		maps.Copy(job, e)
	}
	conf := confmap.NewFromStringMap(map[string]any{
		"config": map[string]any{"scrape_configs": []any{job}},
	})
	require.NoError(t, conf.Unmarshal(cfg))
	require.NoError(t, confmap.Validate(cfg))
	return cfg
}

func startScraper(t *testing.T, tg *target, extra ...map[string]any) *consumertest.MetricsSink {
	t.Helper()
	cfg := e2eConfig(t, tg, extra...)

	sink := new(consumertest.MetricsSink)
	r, err := createMetricsReceiver(context.Background(), receivertest.NewNopSettings(typ), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, r.Shutdown(context.Background())) })
	return sink
}

// flatten returns name -> datapoint attribute maps for every sample received.
type sample struct {
	name  string
	attrs map[string]string
	val   float64
	typ   pmetric.MetricType
}

func samples(sink *consumertest.MetricsSink) []sample {
	var out []sample
	for _, md := range sink.AllMetrics() {
		for i := range md.ResourceMetrics().Len() {
			rm := md.ResourceMetrics().At(i)
			for j := range rm.ScopeMetrics().Len() {
				ms := rm.ScopeMetrics().At(j).Metrics()
				for k := range ms.Len() {
					m := ms.At(k)
					var dps pmetric.NumberDataPointSlice
					if m.Type() == pmetric.MetricTypeSum {
						dps = m.Sum().DataPoints()
					} else {
						dps = m.Gauge().DataPoints()
					}
					for d := range dps.Len() {
						attrs := map[string]string{}
						for key, v := range dps.At(d).Attributes().All() {
							attrs[key] = v.Str()
						}
						out = append(out, sample{m.Name(), attrs, dps.At(d).DoubleValue(), m.Type()})
					}
				}
			}
		}
	}
	return out
}

func find(ss []sample, name string, match map[string]string) (sample, bool) {
	for _, s := range ss {
		if s.name != name {
			continue
		}
		ok := true
		for k, v := range match {
			if s.attrs[k] != v {
				ok = false
			}
		}
		if ok {
			return s, true
		}
	}
	return sample{}, false
}

func requireFound(t *testing.T, ss []sample, name string, match map[string]string) sample {
	t.Helper()
	s, ok := find(ss, name, match)
	require.Truef(t, ok, "series %s%v not found", name, match)
	return s
}

func waitScrapes(t *testing.T, sink *consumertest.MetricsSink) {
	t.Helper()
	require.Eventually(t, func() bool { return len(sink.AllMetrics()) >= 2 }, 10*time.Second, 50*time.Millisecond)
}

func TestE2EText(t *testing.T) {
	tg := newTarget(t, "text/plain; version=0.0.4", textFixture)
	sink := startScraper(t, tg)
	waitScrapes(t, sink)

	// Empty resource and scope for every batch.
	for _, md := range sink.AllMetrics() {
		for i := range md.ResourceMetrics().Len() {
			assert.Zero(t, md.ResourceMetrics().At(i).Resource().Attributes().Len())
			assert.Empty(t, md.ResourceMetrics().At(i).ScopeMetrics().At(0).Scope().Name())
		}
	}

	ss := samples(sink)
	job := map[string]string{"job": "e2e", "instance": strings.TrimPrefix(tg.srv.URL, "http://")}

	s := requireFound(t, ss, "http_requests_total", map[string]string{"code": "200"})
	assert.Equal(t, pmetric.MetricTypeSum, s.typ)
	assert.InDelta(t, 10, s.val, 0)
	assert.Equal(t, "e2e", s.attrs["job"])
	assert.Equal(t, job["instance"], s.attrs["instance"])

	assert.Equal(t, pmetric.MetricTypeGauge, requireFound(t, ss, "temp", job).typ)

	// Names and le/quantile strings are verbatim; the classic parts stay flat.
	requireFound(t, ss, "lat_bucket", map[string]string{"le": "1e-05"}) // normalized by the Prometheus parser, as Prometheus itself stores it
	requireFound(t, ss, "lat_bucket", map[string]string{"le": "1.0"})
	requireFound(t, ss, "lat_bucket", map[string]string{"le": "+Inf"})
	assert.Equal(t, pmetric.MetricTypeSum, requireFound(t, ss, "lat_sum", job).typ)
	assert.Equal(t, pmetric.MetricTypeSum, requireFound(t, ss, "lat_count", job).typ)
	assert.Equal(t, pmetric.MetricTypeGauge, requireFound(t, ss, "rpc", map[string]string{"quantile": "0.5"}).typ)
	assert.Equal(t, pmetric.MetricTypeSum, requireFound(t, ss, "rpc_sum", job).typ)

	// Not consumed into resource/scope.
	requireFound(t, ss, "target_info", map[string]string{"service_name": "svc"})
	requireFound(t, ss, "otel_scope_info", map[string]string{"otel_scope_name": "scope"})
	requireFound(t, ss, "odd", map[string]string{"a__b": "1", "_foo": "2"})

	// Report series.
	for _, name := range []string{"up", "scrape_duration_seconds", "scrape_samples_scraped", "scrape_samples_post_metric_relabeling", "scrape_series_added"} {
		requireFound(t, ss, name, job)
	}
	assert.InDelta(t, 1, requireFound(t, ss, "up", job).val, 0)
}

func TestE2EOpenMetrics(t *testing.T) {
	tg := newTarget(t, "application/openmetrics-text; version=1.0.0; charset=utf-8", openMetricsFixture)
	sink := startScraper(t, tg)
	waitScrapes(t, sink)

	ss := samples(sink)
	requireFound(t, ss, "reqs_total", map[string]string{"code": "200"})
	// _created stays an ordinary series.
	requireFound(t, ss, "reqs_created", map[string]string{"code": "200"})
	requireFound(t, ss, "lat_created", nil)
	requireFound(t, ss, "lat_bucket", map[string]string{"le": "1e-05"}) // normalized by the Prometheus parser, as Prometheus itself stores it
	requireFound(t, ss, "target_info", map[string]string{"service_name": "svc"})
}

func TestE2EStaleness(t *testing.T) {
	tg := newTarget(t, "text/plain; version=0.0.4", "# TYPE a gauge\na 1\n# TYPE b gauge\nb 1\n")
	sink := startScraper(t, tg)
	waitScrapes(t, sink)

	tg.body.Store("# TYPE a gauge\na 1\n")
	require.Eventually(t, func() bool {
		for _, s := range samples(sink) {
			if s.name == "b" && value.IsStaleNaN(s.val) {
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "stale marker for b not emitted")

	for _, s := range samples(sink) {
		if s.name == "a" {
			assert.False(t, value.IsStaleNaN(s.val))
		}
	}
}

func TestE2EShutdownStopsScraping(t *testing.T) {
	tg := newTarget(t, "text/plain; version=0.0.4", "# TYPE a gauge\na 1\n")

	cfg := e2eConfig(t, tg)
	sink := new(consumertest.MetricsSink)
	r, err := createMetricsReceiver(context.Background(), receivertest.NewNopSettings(typ), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
	waitScrapes(t, sink)
	require.NoError(t, r.Shutdown(context.Background()))

	n := tg.scrape.Load()
	time.Sleep(400 * time.Millisecond)
	assert.Equal(t, n, tg.scrape.Load(), "scrapes continued after shutdown")
}

// protoBody exposes the registry in the delimited protobuf format, which is the
// only exposition format that carries native histograms.
func protoBody(t *testing.T, reg *prometheus.Registry) (string, string) {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	format := expfmt.NewFormat(expfmt.TypeProtoDelim)
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, format)
	for _, mf := range mfs {
		require.NoError(t, enc.Encode(mf))
	}
	return string(format), buf.String()
}

var nativeHistogramScrape = map[string]any{
	"scrape_native_histograms": true,
	"scrape_protocols":         []any{"PrometheusProto"},
}

func newNativeHistogramRegistry(t *testing.T) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewRegistry()
	h := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:                           "rpc_duration",
		Help:                           "RPC durations.",
		NativeHistogramBucketFactor:    1.1,
		NativeHistogramZeroThreshold:   0.001,
		NativeHistogramMaxBucketNumber: 100,
	})
	for _, v := range []float64{0.0001, 0.5, 0.7, 2, 2.5, 8} {
		h.Observe(v)
	}
	reg.MustRegister(h)
	return reg
}

func TestE2ENativeHistogram(t *testing.T) {
	contentType, body := protoBody(t, newNativeHistogramRegistry(t))
	tg := newTarget(t, contentType, body)
	sink := startScraper(t, tg, nativeHistogramScrape)
	waitScrapes(t, sink)

	var dp pmetric.ExponentialHistogramDataPoint
	found := false
	for _, md := range sink.AllMetrics() {
		ms := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
		for i := range ms.Len() {
			if ms.At(i).Name() == "rpc_duration" && ms.At(i).Type() == pmetric.MetricTypeExponentialHistogram {
				dp = ms.At(i).ExponentialHistogram().DataPoints().At(0)
				found = true
			}
		}
	}
	require.True(t, found, "no exponential histogram received")
	assert.EqualValues(t, 6, dp.Count())
	assert.InDelta(t, 0.0001+0.5+0.7+2+2.5+8, dp.Sum(), 1e-9)
	assert.EqualValues(t, 1, dp.ZeroCount())
	assert.InDelta(t, 0.001, dp.ZeroThreshold(), 0)
	assert.Equal(t, "e2e", mustAttr(t, dp, "job"))

	var total uint64
	for _, c := range dp.Positive().BucketCounts().AsRaw() {
		total += c
	}
	assert.EqualValues(t, 5, total)
}

func mustAttr(t *testing.T, dp pmetric.ExponentialHistogramDataPoint, k string) string {
	t.Helper()
	v, ok := dp.Attributes().Get(k)
	require.True(t, ok)
	return v.Str()
}

func TestE2ENativeHistogramStaleness(t *testing.T) {
	contentType, body := protoBody(t, newNativeHistogramRegistry(t))
	tg := newTarget(t, contentType, body)
	sink := startScraper(t, tg, nativeHistogramScrape)
	waitScrapes(t, sink)

	_, empty := protoBody(t, prometheus.NewRegistry())
	tg.body.Store(empty)

	require.Eventually(t, func() bool {
		for _, md := range sink.AllMetrics() {
			ms := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics()
			for i := range ms.Len() {
				if ms.At(i).Name() != "rpc_duration" || ms.At(i).Type() != pmetric.MetricTypeExponentialHistogram {
					continue
				}
				dps := ms.At(i).ExponentialHistogram().DataPoints()
				for j := range dps.Len() {
					if value.IsStaleNaN(dps.At(j).Sum()) {
						return true
					}
				}
			}
		}
		return false
	}, 15*time.Second, 50*time.Millisecond, "stale marker for native histogram not emitted")
}
