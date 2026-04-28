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

package prometheusremotewritev1receiver

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func startReceiver(t *testing.T, sink *consumertest.MetricsSink) string {
	t.Helper()
	addr := freeAddr(t)
	cfg := &Config{
		ServerConfig: confighttp.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint:  addr,
				Transport: confignet.TransportTypeTCP,
			},
		},
	}
	set := receivertest.NewNopSettings(typ)
	rcvr, err := newReceiver(set, cfg, sink)
	require.NoError(t, err)

	host := componenttest.NewNopHost()
	require.NoError(t, rcvr.Start(context.Background(), host))
	t.Cleanup(func() {
		require.NoError(t, rcvr.Shutdown(context.Background()))
	})
	return addr
}

// doWrite serialises wr and POSTs it to url with the required Content-Type.
func doWrite(t *testing.T, addr string, wr *prompb.WriteRequest) *http.Response {
	t.Helper()
	body, err := proto.Marshal(wr)
	require.NoError(t, err)

	url := "http://" + addr + "/api/v1/write"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func waitForN(t *testing.T, sink *consumertest.MetricsSink, n int) {
	t.Helper()
	assert.Eventually(t, func() bool {
		return len(sink.AllMetrics()) >= n
	}, 5*time.Second, 10*time.Millisecond, "timed out waiting for %d metric batch(es)", n)
}

func TestE2E_BasicMetricsReceived(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	addr := startReceiver(t, sink)

	now := time.Now().UnixMilli()
	wr := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "job", Value: "web"},
					{Name: "instance", Value: "localhost:8080"},
					{Name: "method", Value: "GET"},
					{Name: "status", Value: "200"},
				},
				Samples: []prompb.Sample{{Value: 42.0, Timestamp: now}},
			},
		},
	}

	resp := doWrite(t, addr, wr)
	defer resp.Body.Close() //nolint:errcheck // it's a test
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	waitForN(t, sink, 1)

	allMetrics := sink.AllMetrics()
	require.Len(t, allMetrics, 1)

	md := allMetrics[0]
	require.Equal(t, 1, md.ResourceMetrics().Len())

	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	require.Equal(t, 1, sm.Metrics().Len())

	m := sm.Metrics().At(0)
	assert.Equal(t, "http_requests_total", m.Name())
	assert.Equal(t, pmetric.MetricTypeGauge, m.Type())

	dp := m.Gauge().DataPoints().At(0)
	assert.InDelta(t, 42.0, dp.DoubleValue(), 1e-9)

	attrMethod, ok := dp.Attributes().Get("method")
	require.True(t, ok)
	assert.Equal(t, "GET", attrMethod.Str())
}

func TestE2E_MultipleConsecutiveRequests(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	addr := startReceiver(t, sink)

	const n = 5
	for i := 0; i < n; i++ {
		wr := &prompb.WriteRequest{
			Timeseries: []prompb.TimeSeries{
				{
					Labels:  []prompb.Label{{Name: "__name__", Value: "counter"}, {Name: "job", Value: "svc"}},
					Samples: []prompb.Sample{{Value: float64(i), Timestamp: int64(i+1) * 1000}},
				},
			},
		}
		resp := doWrite(t, addr, wr)
		resp.Body.Close() //nolint:errcheck // it's a test
		assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	}

	waitForN(t, sink, n)
	assert.Len(t, sink.AllMetrics(), n)
}

func TestE2E_DifferentJobsShareSingleResource(t *testing.T) {
	// Series from different job/instance pairs all land in a single attribute-less
	// ResourceMetrics; job/instance become data point attributes.
	sink := new(consumertest.MetricsSink)
	addr := startReceiver(t, sink)

	wr := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "up"}, {Name: "job", Value: "serviceA"}, {Name: "instance", Value: "host1:8080"}},
				Samples: []prompb.Sample{{Value: 1.0, Timestamp: 1000}},
			},
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "up"}, {Name: "job", Value: "serviceB"}, {Name: "instance", Value: "host2:8080"}},
				Samples: []prompb.Sample{{Value: 0.0, Timestamp: 1000}},
			},
		},
	}

	resp := doWrite(t, addr, wr)
	defer resp.Body.Close() //nolint:errcheck // it's a test
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	waitForN(t, sink, 1)

	md := sink.AllMetrics()[0]
	require.Equal(t, 1, md.ResourceMetrics().Len(), "all series must share a single ResourceMetrics")
	assert.Equal(t, 0, md.ResourceMetrics().At(0).Resource().Attributes().Len())
	assert.Equal(t, 2, md.MetricCount())

	// job and instance must appear as data point attributes.
	dp0 := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0)
	job, ok := dp0.Attributes().Get("job")
	require.True(t, ok)
	assert.Equal(t, "serviceA", job.Str())
}

func TestE2E_TargetInfoTreatedAsRegularMetric(t *testing.T) {
	// target_info receives no special treatment and is forwarded as a regular
	// Gauge metric; its labels become data point attributes.
	sink := new(consumertest.MetricsSink)
	addr := startReceiver(t, sink)

	wr := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "target_info"},
					{Name: "job", Value: "worker"},
					{Name: "instance", Value: "node1"},
					{Name: "os_type", Value: "linux"},
					{Name: "service_version", Value: "1.0.0"},
				},
				Samples: []prompb.Sample{{Value: 1.0, Timestamp: 1000}},
			},
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "up"}, {Name: "job", Value: "worker"}, {Name: "instance", Value: "node1"}},
				Samples: []prompb.Sample{{Value: 1.0, Timestamp: 1000}},
			},
		},
	}

	resp := doWrite(t, addr, wr)
	defer resp.Body.Close() //nolint:errcheck // it's a test
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	waitForN(t, sink, 1)

	md := sink.AllMetrics()[0]
	require.Equal(t, 1, md.ResourceMetrics().Len())

	// target_info is a regular metric now; both timeseries produce metrics.
	assert.Equal(t, 2, md.MetricCount())
	assert.Equal(t, 0, md.ResourceMetrics().At(0).Resource().Attributes().Len())

	// target_info labels must be on data point attributes, not resource attributes.
	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	tiDp := sm.Metrics().At(0).Gauge().DataPoints().At(0)
	osType, ok := tiDp.Attributes().Get("os_type")
	require.True(t, ok)
	assert.Equal(t, "linux", osType.Str())
	assert.Equal(t, 1.0, tiDp.DoubleValue())
}

func TestE2E_MissingContentType(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	addr := startReceiver(t, sink)

	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/api/v1/write", bytes.NewReader([]byte{}))
	require.NoError(t, err)
	// Deliberately omit Content-Type.

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck // it's a test

	assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)
	assert.Empty(t, sink.AllMetrics())
}

func TestE2E_WrongContentType(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	addr := startReceiver(t, sink)

	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/api/v1/write", bytes.NewReader([]byte("{}")))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck // it's a test

	assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode)
	assert.Empty(t, sink.AllMetrics())
}

func TestE2E_InvalidProtobuf(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	addr := startReceiver(t, sink)

	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/api/v1/write", bytes.NewReader([]byte("garbage bytes")))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck // it's a test

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Empty(t, sink.AllMetrics())
}

func TestE2E_EmptyWriteRequest(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	addr := startReceiver(t, sink)

	resp := doWrite(t, addr, &prompb.WriteRequest{})
	defer resp.Body.Close() //nolint:errcheck // it's a test

	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Empty(t, sink.AllMetrics())
}

func TestE2E_GracefulShutdown(t *testing.T) {
	sink := new(consumertest.MetricsSink)
	addr := freeAddr(t)
	cfg := &Config{
		ServerConfig: confighttp.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint:  addr,
				Transport: confignet.TransportTypeTCP,
			},
		},
	}
	set := receivertest.NewNopSettings(typ)
	rcvr, err := newReceiver(set, cfg, sink)
	require.NoError(t, err)

	host := componenttest.NewNopHost()
	require.NoError(t, rcvr.Start(context.Background(), host))

	// Send one successful request while the server is running.
	wr := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "up"}, {Name: "job", Value: "svc"}},
				Samples: []prompb.Sample{{Value: 1.0, Timestamp: 1000}},
			},
		},
	}
	resp := doWrite(t, addr, wr)
	resp.Body.Close() //nolint:errcheck // it's a test
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Shutdown must complete without error.
	require.NoError(t, rcvr.Shutdown(context.Background()))

	// After shutdown the server no longer accepts connections.
	_, connErr := http.Post("http://"+addr+"/api/v1/write", "application/x-protobuf", nil) //nolint:noctx
	assert.Error(t, connErr, "expected a connection error after shutdown")
}
