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

func TestParsePortRange(t *testing.T) {
	r, err := parsePortRange("8889-8999")
	require.NoError(t, err)
	assert.Equal(t, portRange{Start: 8889, End: 8999}, r)

	_, err = parsePortRange("8999-8889")
	require.Error(t, err)
}

func TestFirstAvailablePortSkipsOccupiedPorts(t *testing.T) {
	occupied, occupiedPort := listenOnFirstPortWithFreeNeighbor(t)
	defer occupied.Close()

	port, err := firstAvailablePort("127.0.0.1", portRange{Start: occupiedPort, End: occupiedPort + 1})
	require.NoError(t, err)
	assert.Equal(t, occupiedPort+1, port)
}

func TestRandomAvailablePortUsesShuffledOrder(t *testing.T) {
	start := firstTwoAvailablePorts(t)
	original := shufflePorts
	shufflePorts = func(ports []int) {
		ports[0], ports[1] = ports[1], ports[0]
	}
	t.Cleanup(func() {
		shufflePorts = original
	})

	port, err := randomAvailablePort("127.0.0.1", portRange{Start: start, End: start + 1})
	require.NoError(t, err)
	assert.Equal(t, start+1, port)
}

func listenOnFirstPortWithFreeNeighbor(t *testing.T) (net.Listener, int) {
	t.Helper()
	for port := 30000; port < 65535; port++ {
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		occupied, err := net.Listen("tcp", addr)
		if err != nil {
			continue
		}
		nextAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port+1))
		next, err := net.Listen("tcp", nextAddr)
		if err == nil {
			require.NoError(t, next.Close())
			return occupied, port
		}
		require.NoError(t, occupied.Close())
	}
	t.Fatal("could not find consecutive available ports")
	return nil, 0
}

func firstTwoAvailablePorts(t *testing.T) int {
	t.Helper()
	for port := 30000; port < 65535; port++ {
		firstAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		first, err := net.Listen("tcp", firstAddr)
		if err != nil {
			continue
		}
		secondAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port+1))
		second, err := net.Listen("tcp", secondAddr)
		require.NoError(t, first.Close())
		if err == nil {
			require.NoError(t, second.Close())
			return port
		}
	}
	t.Fatal("could not find consecutive available ports")
	return 0
}

func TestMetricsGeneratorConfigFilesSelectsTelemetryPort(t *testing.T) {
	original := findAvailableMetricsTelemetryPort
	findAvailableMetricsTelemetryPort = func(host string, ports portRange) (int, error) {
		assert.Equal(t, "127.0.0.1", host)
		assert.Equal(t, portRange{Start: 8889, End: 8999}, ports)
		return 8891, nil
	}
	t.Cleanup(func() {
		findAvailableMetricsTelemetryPort = original
	})

	endpoint, configFiles, err := metricsGeneratorConfigFiles("config-prw.yaml", "127.0.0.1", portRange{Start: 8889, End: 8999})
	require.NoError(t, err)

	assert.Equal(t, "127.0.0.1:8891", endpoint)
	assert.Equal(t, []string{
		"config-prw.yaml",
		`yaml:service::telemetry::metrics::readers: [{pull: {exporter: {prometheus: {host: "127.0.0.1", port: 8891}}}}]`,
	}, configFiles)
}

func TestPrintMetricsTelemetryEndpoint(t *testing.T) {
	var out bytes.Buffer
	printMetricsTelemetryEndpoint(&out, "127.0.0.1:8891")
	assert.Equal(t, "metrics-generator: scraping collector telemetry from 127.0.0.1:8891\n", out.String())
}

func TestReportMetricsGenBenchmarkUsesMetricPointUnits(t *testing.T) {
	now := time.Now()
	poller := &telemetryPoller{
		latest: telemetrySnapshot{
			sent:   100,
			failed: 10,
			at:     now.Add(time.Second),
			valid:  true,
		},
		firstSeen: now,
	}

	stdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Stdout = stdout
	})
	os.Stdout = w

	reportMetricsGenBenchmark(poller, time.Second)

	require.NoError(t, w.Close())
	output, err := io.ReadAll(r)
	require.NoError(t, err)

	assert.Contains(t, string(output), "metric_points/s")
	assert.Contains(t, string(output), "failed_metric_points/s")
	assert.NotContains(t, string(output), "samples/s")
	assert.NotContains(t, string(output), "failed_samples/s")
}
