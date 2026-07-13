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
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// Collector self-telemetry counter names (before the Prometheus "_total" suffix)
// used to derive metricsgen backfill throughput. See
// https://opentelemetry.io/docs/collector/internal-telemetry/#basic-level-metrics.
const (
	metricSentMetricPoints     = "otelcol_exporter_sent_metric_points"
	metricFailedMetricPoints   = "otelcol_exporter_send_failed_metric_points"
	metricAcceptedMetricPoints = "otelcol_receiver_accepted_metric_points"
)

var telemetryScrapeClient = &http.Client{
	Transport: &http.Transport{
		Proxy:             http.ProxyFromEnvironment,
		DisableKeepAlives: true,
	},
}

func ephemeralPort(host string) (int, error) {
	ln, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

// telemetrySnapshot holds cumulative counter totals scraped once from the
// collector's Prometheus self-telemetry endpoint.
type telemetrySnapshot struct {
	sent     float64
	failed   float64
	accepted float64
	at       time.Time
	valid    bool
}

// scrapeMetricsEndpoint fetches and parses the collector self-telemetry once.
// endpoint may be given as host:port or a full URL; the "/metrics" path is
// appended when no path is present.
func scrapeMetricsEndpoint(ctx context.Context, endpoint string) (telemetrySnapshot, error) {
	url := metricsScrapeURL(endpoint)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return telemetrySnapshot{}, err
	}
	resp, err := telemetryScrapeClient.Do(req)
	if err != nil {
		return telemetrySnapshot{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return telemetrySnapshot{}, fmt.Errorf("scrape %s: unexpected status %d", url, resp.StatusCode)
	}

	return parseTelemetry(resp.Body)
}

// metricsScrapeURL normalizes an endpoint into a scrapeable /metrics URL.
func metricsScrapeURL(endpoint string) string {
	url := endpoint
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}
	if !strings.Contains(strings.TrimPrefix(strings.TrimPrefix(url, "http://"), "https://"), "/") {
		url = strings.TrimRight(url, "/") + "/metrics"
	}
	return url
}

// parseTelemetry parses Prometheus text exposition and sums the counters we care about.
func parseTelemetry(r io.Reader) (telemetrySnapshot, error) {
	// TextParser's zero value uses an unset name-validation scheme (which panics
	// on parse), so construct it explicitly. UTF8 is the collector's default.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(r)
	if err != nil {
		return telemetrySnapshot{}, err
	}
	return telemetrySnapshot{
		sent:     sumCounter(families, metricSentMetricPoints),
		failed:   sumCounter(families, metricFailedMetricPoints),
		accepted: sumCounter(families, metricAcceptedMetricPoints),
		at:       time.Now(),
		valid:    true,
	}, nil
}

// sumCounter sums all series of a counter metric family. The Prometheus
// exposition format appends a "_total" suffix to counters, so both the base and
// suffixed names are accepted.
func sumCounter(families map[string]*dto.MetricFamily, name string) float64 {
	var total float64
	for _, candidate := range []string{name, name + "_total"} {
		fam, ok := families[candidate]
		if !ok {
			continue
		}
		for _, m := range fam.GetMetric() {
			if c := m.GetCounter(); c != nil {
				total += c.GetValue()
			}
		}
	}
	return total
}

// telemetryPoller periodically scrapes a collector self-telemetry endpoint and
// retains the latest successful snapshot. Because metricsgen with
// exit_after_end tears down the telemetry endpoint on completion, callers use
// the last retained snapshot rather than scraping after the run ends.
type telemetryPoller struct {
	mu        sync.Mutex
	latest    telemetrySnapshot
	firstSeen time.Time // time of the first scrape where any data points were observed
}

// startTelemetryPoller launches a background poller that scrapes endpoint every
// interval until ctx is cancelled.
func startTelemetryPoller(ctx context.Context, endpoint string, interval time.Duration) *telemetryPoller {
	p := &telemetryPoller{}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				snap, err := scrapeMetricsEndpoint(ctx, endpoint)
				if err != nil {
					continue
				}
				p.mu.Lock()
				p.latest = snap
				if p.firstSeen.IsZero() && (snap.sent > 0 || snap.accepted > 0) {
					p.firstSeen = snap.at
				}
				p.mu.Unlock()
			}
		}
	}()
	return p
}

// snapshot returns the latest retained snapshot and the time the first non-zero
// sample was observed.
func (p *telemetryPoller) snapshot() (telemetrySnapshot, time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.latest, p.firstSeen
}
