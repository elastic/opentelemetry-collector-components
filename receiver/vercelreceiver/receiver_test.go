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

package vercelreceiver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/elastic/opentelemetry-collector-components/internal/vercel"
	"github.com/elastic/opentelemetry-collector-components/receiver/vercelreceiver/hosttest"
)

func TestFactorySharesReceiver(t *testing.T) {
	endpoint := freeEndpoint(t)
	cfg := createDefaultConfig().(*Config)
	cfg.NetAddr.Endpoint = endpoint
	cfg.Encoding.Extension = component.MustNewID("vercel")

	factory := NewFactory()
	logsReceiver, err := factory.CreateLogs(
		t.Context(),
		receivertest.NewNopSettings(component.MustNewType("vercel")),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)
	metricsReceiver, err := factory.CreateMetrics(
		t.Context(),
		receivertest.NewNopSettings(component.MustNewType("vercel")),
		cfg,
		consumertest.NewNop(),
	)
	require.NoError(t, err)

	host := hosttest.NewCustomHostWithExtensions(map[component.ID]component.Component{
		cfg.Encoding.Extension: &fakeVercelEncodingExtension{},
	})
	require.NoError(t, logsReceiver.Start(t.Context(), host))
	defer func() {
		require.NoError(t, logsReceiver.Shutdown(t.Context()))
	}()
	require.NoError(t, metricsReceiver.Start(t.Context(), host))
	require.NoError(t, metricsReceiver.Shutdown(t.Context()))
}

func TestRouteAcceptsPost(t *testing.T) {
	cases := []struct {
		name            string
		payloads        []vercel.Payload
		wantLogRecords  int
		wantMetricCount int
	}{
		{
			name: "logs",
			payloads: []vercel.Payload{
				{
					Signal: vercel.SignalLogs,
					Logs:   newLogs(),
				},
			},
			wantLogRecords: 1,
		},
		{
			name: "logs payloads until EOF",
			payloads: []vercel.Payload{
				{
					Signal: vercel.SignalLogs,
					Logs:   newLogs(),
				},
				{
					Signal: vercel.SignalLogs,
					Logs:   newLogs(),
				},
			},
			wantLogRecords: 2,
		},
		{
			name: "metrics",
			payloads: []vercel.Payload{
				{
					Signal:  vercel.SignalMetrics,
					Metrics: newMetrics(1),
				},
			},
			wantMetricCount: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			cfg.NetAddr.Endpoint = "localhost:0"
			cfg.Encoding.Extension = component.MustNewID("vercel")

			logsSink := &consumertest.LogsSink{}
			metricsSink := &consumertest.MetricsSink{}
			rcvr := newReceiver(cfg, logsSink, metricsSink, receivertest.NewNopSettings(component.MustNewType("vercel")))
			require.NoError(t, rcvr.Start(t.Context(), hosttest.NewCustomHostWithExtensions(map[component.ID]component.Component{
				cfg.Encoding.Extension: &fakeVercelEncodingExtension{
					payloads: tc.payloads,
				},
			})))
			defer func() {
				require.NoError(t, rcvr.Shutdown(t.Context()))
			}()

			resp, err := http.Post("http://"+rcvr.listener.Addr().String()+defaultRoute, "application/json", bytes.NewBufferString(`{}`))
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, tc.wantLogRecords, logsSink.LogRecordCount())
			require.Equal(t, tc.wantMetricCount, metricCount(metricsSink.AllMetrics()))
		})
	}
}

func TestRouteRequiresConsumer(t *testing.T) {
	cases := []struct {
		name    string
		payload vercel.Payload
	}{
		{
			name: "logs",
			payload: vercel.Payload{
				Signal: vercel.SignalLogs,
				Logs:   newLogs(),
			},
		},
		{
			name: "metrics",
			payload: vercel.Payload{
				Signal:  vercel.SignalMetrics,
				Metrics: newMetrics(1),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			cfg.NetAddr.Endpoint = "localhost:0"
			cfg.Route = "/vercel"
			cfg.Encoding.Extension = component.MustNewID("vercel")

			rcvr := newReceiver(cfg, nil, nil, receivertest.NewNopSettings(component.MustNewType("vercel")))
			require.NoError(t, rcvr.Start(t.Context(), hosttest.NewCustomHostWithExtensions(map[component.ID]component.Component{
				cfg.Encoding.Extension: &fakeVercelEncodingExtension{
					payloads: []vercel.Payload{tc.payload},
				},
			})))
			defer func() {
				require.NoError(t, rcvr.Shutdown(t.Context()))
			}()

			resp, err := http.Post("http://"+rcvr.listener.Addr().String()+"/vercel", "application/json", bytes.NewBufferString(`{}`))
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		})
	}
}

func TestRouteReturnsErrors(t *testing.T) {
	parserErr := errors.New("parser error")
	consumerErr := errors.New("consumer error")
	cases := []struct {
		name            string
		extension       *fakeVercelEncodingExtension
		logsConsumer    consumer.Logs
		metricsConsumer consumer.Metrics
		wantStatus      int
	}{
		{
			name: "parser creation",
			extension: &fakeVercelEncodingExtension{
				parserErr: parserErr,
			},
			logsConsumer:    consumertest.NewNop(),
			metricsConsumer: consumertest.NewNop(),
			wantStatus:      http.StatusBadRequest,
		},
		{
			name: "parser next",
			extension: &fakeVercelEncodingExtension{
				nextErr: parserErr,
			},
			logsConsumer:    consumertest.NewNop(),
			metricsConsumer: consumertest.NewNop(),
			wantStatus:      http.StatusBadRequest,
		},
		{
			name: "logs consumer",
			extension: &fakeVercelEncodingExtension{
				payloads: []vercel.Payload{
					{
						Signal: vercel.SignalLogs,
						Logs:   newLogs(),
					},
				},
			},
			logsConsumer:    consumertest.NewErr(consumerErr),
			metricsConsumer: consumertest.NewNop(),
			wantStatus:      http.StatusServiceUnavailable,
		},
		{
			name: "metrics consumer",
			extension: &fakeVercelEncodingExtension{
				payloads: []vercel.Payload{
					{
						Signal:  vercel.SignalMetrics,
						Metrics: newMetrics(1),
					},
				},
			},
			logsConsumer:    consumertest.NewNop(),
			metricsConsumer: consumertest.NewErr(consumerErr),
			wantStatus:      http.StatusServiceUnavailable,
		},
		{
			name: "unsupported signal",
			extension: &fakeVercelEncodingExtension{
				payloads: []vercel.Payload{
					{
						Signal: vercel.Signal("traces"),
					},
				},
			},
			logsConsumer:    consumertest.NewNop(),
			metricsConsumer: consumertest.NewNop(),
			wantStatus:      http.StatusBadRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			cfg.NetAddr.Endpoint = "localhost:0"
			cfg.Encoding.Extension = component.MustNewID("vercel")

			rcvr := newReceiver(
				cfg,
				tc.logsConsumer,
				tc.metricsConsumer,
				receivertest.NewNopSettings(component.MustNewType("vercel")),
			)
			require.NoError(t, rcvr.Start(t.Context(), hosttest.NewCustomHostWithExtensions(map[component.ID]component.Component{
				cfg.Encoding.Extension: tc.extension,
			})))
			defer func() {
				require.NoError(t, rcvr.Shutdown(t.Context()))
			}()

			resp, err := http.Post("http://"+rcvr.listener.Addr().String()+defaultRoute, "application/json", bytes.NewBufferString(`{}`))
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, tc.wantStatus, resp.StatusCode)
		})
	}
}

func TestStartRequiresVercelEncoding(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.NetAddr.Endpoint = "localhost:0"
	cfg.Encoding.Extension = component.MustNewID("vercel")

	cases := []struct {
		name       string
		extensions map[component.ID]component.Component
		wantError  string
	}{
		{
			name:       "unknown extension",
			extensions: map[component.ID]component.Component{},
			wantError:  `unknown extension "vercel"`,
		},
		{
			name: "wrong extension type",
			extensions: map[component.ID]component.Component{
				cfg.Encoding.Extension: &fakeExtension{},
			},
			wantError: `extension "vercel" is not a Vercel encoding extension`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rcvr := newReceiver(cfg, consumertest.NewNop(), nil, receivertest.NewNopSettings(component.MustNewType("vercel")))
			err := rcvr.Start(t.Context(), hosttest.NewCustomHostWithExtensions(tc.extensions))
			require.EqualError(t, err, tc.wantError)
		})
	}
}

type fakeVercelEncodingExtension struct {
	payloads  []vercel.Payload
	parserErr error
	nextErr   error
}

func (*fakeVercelEncodingExtension) Start(context.Context, component.Host) error {
	return nil
}

func (*fakeVercelEncodingExtension) Shutdown(context.Context) error {
	return nil
}

func (f *fakeVercelEncodingExtension) NewVercelParser(reader io.Reader, _ ...encoding.DecoderOption) (vercel.PayloadParser, error) {
	_, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if f.parserErr != nil {
		return nil, f.parserErr
	}
	return &fakeVercelPayloadParser{payloads: f.payloads, nextErr: f.nextErr}, nil
}

type fakeVercelPayloadParser struct {
	payloads []vercel.Payload
	index    int
	nextErr  error
}

func (p *fakeVercelPayloadParser) Next() (vercel.Payload, error) {
	if p.nextErr != nil {
		return vercel.Payload{}, p.nextErr
	}
	if p.index >= len(p.payloads) {
		return vercel.Payload{}, io.EOF
	}
	payload := p.payloads[p.index]
	p.index++
	return payload, nil
}

type fakeExtension struct{}

func (*fakeExtension) Start(context.Context, component.Host) error {
	return nil
}

func (*fakeExtension) Shutdown(context.Context) error {
	return nil
}

func newLogs() plog.Logs {
	logs := plog.NewLogs()
	logRecords := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	logRecords.AppendEmpty()
	return logs
}

func metricCount(metrics []pmetric.Metrics) int {
	count := 0
	for _, item := range metrics {
		count += item.MetricCount()
	}
	return count
}

func newMetrics(count int) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	metricSlice := metrics.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	for range count {
		metricSlice.AppendEmpty()
	}
	return metrics
}

func freeEndpoint(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().String()
}
