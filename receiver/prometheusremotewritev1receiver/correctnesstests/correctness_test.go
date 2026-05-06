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
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	remoteapi "github.com/prometheus/client_golang/exp/api/remote"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/opentelemetry-collector-components/internal/testutil"
	"github.com/elastic/opentelemetry-collector-components/receiver/prometheusremotewritev1receiver"
	prwexporter "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestCorrectness_BasicRoundTrip(t *testing.T) {
	dataRecvAddr, dataRecv := newDataReceiver(t)
	rcvAddr := buildPipeline(t, dataRecvAddr)

	now := time.Now().UnixMilli()
	sent := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "instance", Value: "localhost:8080"},
					{Name: "job", Value: "web"},
					{Name: "method", Value: "GET"},
					{Name: "status", Value: "200"},
				},
				Samples: []prompb.Sample{
					{Value: 42.0, Timestamp: now},
				},
			},
		},
	}

	sendWriteRequest(t, rcvAddr, sent)
	waitForTimeSeries(t, dataRecv, 1)

	assertTimeSeriesEqual(t, sent.Timeseries, dataRecv.allTimeSeries())
}

func TestCorrectness_MultipleTimeSeries(t *testing.T) {
	dataRecvAddr, dataRecv := newDataReceiver(t)
	rcvAddr := buildPipeline(t, dataRecvAddr)

	now := time.Now().UnixMilli()
	sent := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "up"},
					{Name: "instance", Value: "host1:9090"},
					{Name: "job", Value: "serviceA"},
				},
				Samples: []prompb.Sample{{Value: 1.0, Timestamp: now}},
			},
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "up"},
					{Name: "instance", Value: "host2:9090"},
					{Name: "job", Value: "serviceB"},
				},
				Samples: []prompb.Sample{{Value: 0.0, Timestamp: now}},
			},
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "process_cpu_seconds_total"},
					{Name: "instance", Value: "host1:9090"},
					{Name: "job", Value: "serviceA"},
				},
				Samples: []prompb.Sample{{Value: 3.14, Timestamp: now}},
			},
		},
	}

	sendWriteRequest(t, rcvAddr, sent)
	waitForTimeSeries(t, dataRecv, len(sent.Timeseries))

	assertTimeSeriesEqual(t, sent.Timeseries, dataRecv.allTimeSeries())
}

func TestCorrectness_MultipleSamplesPerSeries(t *testing.T) {
	dataRecvAddr, dataRecv := newDataReceiver(t)
	rcvAddr := buildPipeline(t, dataRecvAddr)

	startTs := time.Now().UnixMilli()
	sent := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "temperature_celsius"},
					{Name: "instance", Value: "sensor1"},
					{Name: "job", Value: "iot"},
				},
				Samples: []prompb.Sample{
					{Value: 20.1, Timestamp: startTs},
					{Value: 20.5, Timestamp: startTs + 15000},
					{Value: 21.3, Timestamp: startTs + 30000},
				},
			},
		},
	}

	sendWriteRequest(t, rcvAddr, sent)
	waitForTimeSeries(t, dataRecv, 1)

	assertTimeSeriesEqual(t, sent.Timeseries, dataRecv.allTimeSeries())
}

func TestCorrectness_MultipleRequests(t *testing.T) {
	dataRecvAddr, dataRecv := newDataReceiver(t)
	rcvAddr := buildPipeline(t, dataRecvAddr)

	startTs := time.Now().UnixMilli()
	const n = 3
	var allSent []prompb.TimeSeries

	for i := range n {
		ts := prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "counter"},
				{Name: "instance", Value: "host1"},
				{Name: "job", Value: "svc"},
				{Name: "request_num", Value: string(rune('0' + i))},
			},
			Samples: []prompb.Sample{
				{Value: float64(i * 100), Timestamp: startTs + int64(i)*1000},
			},
		}
		wr := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{ts}}
		sendWriteRequest(t, rcvAddr, wr)
		allSent = append(allSent, ts)
	}

	waitForTimeSeries(t, dataRecv, n)
	assertTimeSeriesEqual(t, allSent, dataRecv.allTimeSeries())
}

func TestCorrectness_Labels(t *testing.T) {
	dataRecvAddr, dataRecv := newDataReceiver(t)
	rcvAddr := buildPipeline(t, dataRecvAddr)

	now := time.Now().UnixMilli()
	sent := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "http_request_duration_seconds"},
					{Name: "cluster", Value: "prod-eu"},
					{Name: "datacenter", Value: "eu-west-1"},
					{Name: "environment", Value: "production"},
					{Name: "instance", Value: "api-server:8080"},
					{Name: "job", Value: "api"},
					{Name: "method", Value: "POST"},
					{Name: "path", Value: "/v1/users"},
					{Name: "status_code", Value: "200"},
				},
				Samples: []prompb.Sample{{Value: 0.023, Timestamp: now}},
			},
		},
	}

	sendWriteRequest(t, rcvAddr, sent)
	waitForTimeSeries(t, dataRecv, 1)

	assertTimeSeriesEqual(t, sent.Timeseries, dataRecv.allTimeSeries())
}

// helpers

// dataReceiver accepts Prometheus remote write
// requests forwarded by the exporter, decodes
// them and stores the received time-series for comparison.
type dataReceiver struct {
	server *http.Server

	mu       sync.Mutex
	received []prompb.TimeSeries
}

func newDataReceiver(t *testing.T) (string, *dataReceiver) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	dr := &dataReceiver{}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/write", dr.handleWrite)
	dr.server = &http.Server{Handler: mux}

	go func() { _ = dr.server.Serve(ln) }()
	t.Cleanup(func() { _ = dr.server.Shutdown(context.Background()) })

	return ln.Addr().String(), dr
}

func (dr *dataReceiver) handleWrite(w http.ResponseWriter, req *http.Request) {
	compressed, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	raw, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, "snappy decode: "+err.Error(), http.StatusBadRequest)
		return
	}

	var wr prompb.WriteRequest
	if err := proto.Unmarshal(raw, &wr); err != nil {
		http.Error(w, "proto unmarshal: "+err.Error(), http.StatusBadRequest)
		return
	}

	dr.mu.Lock()
	dr.received = append(dr.received, wr.Timeseries...)
	dr.mu.Unlock()

	w.WriteHeader(http.StatusNoContent)
}

func (dr *dataReceiver) allTimeSeries() []prompb.TimeSeries {
	dr.mu.Lock()
	defer dr.mu.Unlock()
	out := make([]prompb.TimeSeries, len(dr.received))
	copy(out, dr.received)
	return out
}

func waitForTimeSeries(t *testing.T, dr *dataReceiver, n int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return len(dr.allTimeSeries()) >= n
	}, 10*time.Second, 20*time.Millisecond, "timed out waiting for %d time-series", n)
}

func buildPipeline(t *testing.T, dataRecvAddr string) string {
	t.Helper()

	ctx := context.Background()
	host := componenttest.NewNopHost()

	// prwexporter
	expFactory := prwexporter.NewFactory()
	expCfg := expFactory.CreateDefaultConfig().(*prwexporter.Config)
	expCfg.ClientConfig = confighttp.NewDefaultClientConfig()
	expCfg.ClientConfig.Endpoint = "http://" + dataRecvAddr + "/api/v1/write"
	expCfg.TranslationStrategy = "NoTranslation"
	expCfg.RemoteWriteProtoMsg = remoteapi.WriteV1MessageType

	expSettings := exportertest.NewNopSettings(component.MustNewType("prometheusremotewrite"))
	exp, err := expFactory.CreateMetrics(ctx, expSettings, expCfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(ctx, host))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(ctx)) })

	// prwv1-receiver
	rcvAddr := testutil.GetAvailableLocalAddress(t)
	rcvFactory := prometheusremotewritev1receiver.NewFactory()
	rcvCfg := rcvFactory.CreateDefaultConfig().(*prometheusremotewritev1receiver.Config)
	rcvCfg.ServerConfig = confighttp.ServerConfig{
		NetAddr: confignet.AddrConfig{
			Endpoint:  rcvAddr,
			Transport: confignet.TransportTypeTCP,
		},
	}
	rcvSettings := receivertest.NewNopSettings(component.MustNewType("prometheusremotewritev1"))
	rcv, err := rcvFactory.CreateMetrics(ctx, rcvSettings, rcvCfg, exp)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(ctx, host))
	t.Cleanup(func() { require.NoError(t, rcv.Shutdown(ctx)) })

	return rcvAddr
}

// sendWriteRequest marshals wr as raw protobuf (no compression) and sends a POST request
// to the receiver endpoint.
func sendWriteRequest(t *testing.T, rcvAddr string, wr *prompb.WriteRequest) {
	t.Helper()
	body, err := proto.Marshal(wr)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "http://"+rcvAddr+"/api/v1/write", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func normalizeTimeSeries(ts prompb.TimeSeries) prompb.TimeSeries {
	labels := make([]prompb.Label, len(ts.Labels))
	copy(labels, ts.Labels)
	sort.Slice(labels, func(i, j int) bool { return labels[i].Name < labels[j].Name })
	return prompb.TimeSeries{Labels: labels, Samples: ts.Samples}
}

func sortKey(ts prompb.TimeSeries) string {
	n := normalizeTimeSeries(ts)
	var b bytes.Buffer
	for _, l := range n.Labels {
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(l.Value)
		b.WriteByte(',')
	}
	return b.String()
}

// assertTimeSeriesEqual sorts the timeseries by labels and compares them
func assertTimeSeriesEqual(t *testing.T, want, got []prompb.TimeSeries) {
	t.Helper()

	require.Len(t, got, len(want), "number of received time-series")

	normalize := func(in []prompb.TimeSeries) []prompb.TimeSeries {
		out := make([]prompb.TimeSeries, len(in))
		for i, ts := range in {
			out[i] = normalizeTimeSeries(ts)
		}
		sort.Slice(out, func(i, j int) bool { return sortKey(out[i]) < sortKey(out[j]) })
		return out
	}

	wantN := normalize(want)
	gotN := normalize(got)

	for i := range wantN {
		assert.ElementsMatch(t, wantN[i].Labels, gotN[i].Labels,
			"labels mismatch for time-series %d", i)
		require.Len(t, gotN[i].Samples, len(wantN[i].Samples),
			"sample count mismatch for time-series %d", i)
		for j, s := range wantN[i].Samples {
			assert.InDelta(t, s.Value, gotN[i].Samples[j].Value, 1e-9,
				"sample value mismatch at time-series %d, sample %d", i, j)
			assert.Equal(t, s.Timestamp, gotN[i].Samples[j].Timestamp,
				"sample timestamp mismatch at time-series %d, sample %d", i, j)
		}
	}
}
