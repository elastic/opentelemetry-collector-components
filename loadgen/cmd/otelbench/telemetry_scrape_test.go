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

package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const telemetryFixture = `# HELP otelcol_exporter_sent_metric_points Number of metric points successfully sent to destination.
# TYPE otelcol_exporter_sent_metric_points_total counter
otelcol_exporter_sent_metric_points_total{exporter="prometheusremotewrite/elasticsearch"} 100
otelcol_exporter_sent_metric_points_total{exporter="otlp"} 50
# HELP otelcol_exporter_send_failed_metric_points Number of metric points that failed to be sent.
# TYPE otelcol_exporter_send_failed_metric_points_total counter
otelcol_exporter_send_failed_metric_points_total{exporter="prometheusremotewrite/elasticsearch"} 3
# HELP otelcol_receiver_accepted_metric_points Number of metric points accepted.
# TYPE otelcol_receiver_accepted_metric_points_total counter
otelcol_receiver_accepted_metric_points_total{receiver="metricsgen"} 200
# HELP otelcol_process_uptime Uptime of the process.
# TYPE otelcol_process_uptime_total counter
otelcol_process_uptime_total 12.5
`

func TestParseTelemetrySumsAcrossLabelSets(t *testing.T) {
	snap, err := parseTelemetry(strings.NewReader(telemetryFixture))
	require.NoError(t, err)
	require.True(t, snap.valid)
	assert.Equal(t, float64(150), snap.sent, "sent should sum all exporter label sets")
	assert.Equal(t, float64(3), snap.failed)
	assert.Equal(t, float64(200), snap.accepted)
}

func TestScrapeMetricsEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/metrics", r.URL.Path)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(telemetryFixture))
	}))
	defer srv.Close()

	// srv.URL is like http://127.0.0.1:port; strip scheme to exercise host:port handling.
	endpoint := strings.TrimPrefix(srv.URL, "http://")

	snap, err := scrapeMetricsEndpoint(context.Background(), endpoint)
	require.NoError(t, err)
	assert.Equal(t, float64(150), snap.sent)
	assert.Equal(t, float64(3), snap.failed)
	assert.Equal(t, float64(200), snap.accepted)
	assert.False(t, snap.at.IsZero())
}

func TestScrapeMetricsEndpointDoesNotUseDefaultClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(telemetryFixture))
	}))
	defer srv.Close()

	defaultClient := http.DefaultClient
	http.DefaultClient = &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, assert.AnError
		}),
	}
	t.Cleanup(func() {
		http.DefaultClient = defaultClient
	})

	snap, err := scrapeMetricsEndpoint(context.Background(), srv.URL)
	require.NoError(t, err)
	assert.Equal(t, float64(150), snap.sent)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestScrapeMetricsEndpointBadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := scrapeMetricsEndpoint(context.Background(), srv.URL)
	require.Error(t, err)
}

func TestMetricsScrapeURL(t *testing.T) {
	tests := map[string]string{
		"127.0.0.1:8889":                   "http://127.0.0.1:8889/metrics",
		"http://127.0.0.1:8889":            "http://127.0.0.1:8889/metrics",
		"http://127.0.0.1:8889/metrics":    "http://127.0.0.1:8889/metrics",
		"https://collector.local:8889/foo": "https://collector.local:8889/foo",
	}
	for in, want := range tests {
		assert.Equal(t, want, metricsScrapeURL(in), "input %q", in)
	}
}

func TestEphemeralPortReleasesSelectedPort(t *testing.T) {
	port, err := ephemeralPort("127.0.0.1")
	require.NoError(t, err)
	require.Positive(t, port)

	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	require.NoError(t, err)
	require.NoError(t, ln.Close())
}

func TestMetricsGeneratorConfigFilesSelectsTelemetryPort(t *testing.T) {
	original := findAvailableMetricsTelemetryPort
	findAvailableMetricsTelemetryPort = func(host string) (int, error) {
		assert.Equal(t, "127.0.0.1", host)
		return 8891, nil
	}
	t.Cleanup(func() {
		findAvailableMetricsTelemetryPort = original
	})

	endpoint, configFiles, err := metricsGeneratorConfigFiles("config-prw.yaml", "127.0.0.1")
	require.NoError(t, err)

	assert.Equal(t, "127.0.0.1:8891", endpoint)
	assert.Equal(t, []string{
		"config-prw.yaml",
		`yaml:service::telemetry::metrics::readers: [{pull: {exporter: {prometheus: {host: "127.0.0.1", port: 8891}}}}]`,
	}, configFiles)
}

func TestMetricsGenSeedConfigFiles(t *testing.T) {
	assert.Equal(t, []string{
		"yaml:receivers::metricsgen::seed: 124",
	}, metricsGenSeedConfigFiles(124))
}

func TestMetricsGenStartNowMinusConfigFiles(t *testing.T) {
	assert.Equal(t, []string{
		"yaml:receivers::metricsgen::start_now_minus: 3m",
	}, metricsGenStartNowMinusConfigFiles(3))
}

func TestRunMetricsGenBenchPassesBenchmarkNAsStartNowMinus(t *testing.T) {
	original := benchmarkMetricsGen
	benchmarkMetricsGen = func(f func(*testing.B)) testing.BenchmarkResult {
		f(&testing.B{N: 1})
		final := &testing.B{N: 2}
		f(final)
		return testing.BenchmarkResult{N: final.N}
	}
	t.Cleanup(func() {
		benchmarkMetricsGen = original
	})

	var overrides [][]string
	result, err := runMetricsGenBench(
		context.Background(),
		[]string{"config-prw.yaml"},
		defaultMetricsGenSeed,
		func(ctx context.Context, configFiles []string) (metricsGenRunStats, error) {
			overrides = append(overrides, configFiles[len(configFiles)-2:])
			return metricsGenRunStats{
				sent:           100,
				failed:         10,
				activeDuration: 2 * time.Second,
			}, nil
		},
	)
	require.NoError(t, err)

	assert.Equal(t, [][]string{
		{
			"yaml:receivers::metricsgen::seed: 123",
			"yaml:receivers::metricsgen::start_now_minus: 1m",
		},
		{
			"yaml:receivers::metricsgen::seed: 124",
			"yaml:receivers::metricsgen::start_now_minus: 2m",
		},
	}, overrides)
	assert.Equal(t, 2, result.N)
	assert.Equal(t, 2*time.Second, result.T)
	assert.Equal(t, 50.0, result.Extra["metric_points/s"])
	assert.Equal(t, 5.0, result.Extra["failed_metric_points/s"])
	assert.Equal(t, 2.0, result.Extra["duration_s"])
	assert.NotContains(t, result.Extra, "attempted_metric_points")
}

func TestPrintMetricsTelemetryEndpoint(t *testing.T) {
	var out bytes.Buffer
	printMetricsTelemetryEndpoint(&out, "127.0.0.1:8891")
	assert.Equal(t, "metricsgen: scraping collector telemetry from 127.0.0.1:8891\n", out.String())
}

func TestReportMetricsGenBenchmarkUsesMetricPointUnits(t *testing.T) {
	stdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Stdout = stdout
	})
	os.Stdout = w

	reportMetricsGenBenchmark(testing.BenchmarkResult{
		N: 3,
		T: 6 * time.Second,
		Extra: map[string]float64{
			"duration_s":             6,
			"metric_points/s":        50,
			"failed_metric_points/s": 5,
		},
	})

	require.NoError(t, w.Close())
	output, err := io.ReadAll(r)
	require.NoError(t, err)

	assert.Contains(t, string(output), "BenchmarkOTelbench/metricsgen")
	assert.Regexp(t, regexp.MustCompile(`BenchmarkOTelbench/metricsgen\s+3\s+`), string(output))
	assert.Contains(t, string(output), "duration_s")
	assert.Contains(t, string(output), "metric_points/s")
	assert.Contains(t, string(output), "failed_metric_points/s")
	assert.NotContains(t, string(output), "attempted_metric_points")
	assert.NotContains(t, string(output), "samples/s")
	assert.NotContains(t, string(output), "failed_samples/s")
}
