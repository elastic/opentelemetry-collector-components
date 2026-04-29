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

package prometheusremotewritev1receiver // import "github.com/elastic/opentelemetry-collector-components/receiver/prometheusremotewritev1receiver"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"

	"github.com/gogo/protobuf/proto"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

type prometheusRWv1Receiver struct {
	settings     receiver.Settings
	config       *Config
	nextConsumer consumer.Metrics

	server     *http.Server
	shutdownWG sync.WaitGroup

	obsrecv *receiverhelper.ObsReport
}

func newReceiver(settings receiver.Settings, cfg *Config, nextConsumer consumer.Metrics) (*prometheusRWv1Receiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		Transport:              "http",
		ReceiverCreateSettings: settings,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create obsreport: %w", err)
	}

	return &prometheusRWv1Receiver{
		settings:     settings,
		config:       cfg,
		nextConsumer: nextConsumer,
		obsrecv:      obsrecv,
	}, nil
}

func (r *prometheusRWv1Receiver) Start(ctx context.Context, host component.Host) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/write", r.handleWrite)

	var err error
	r.server, err = r.config.ToServer(ctx, host.GetExtensions(), r.settings.TelemetrySettings, mux)
	if err != nil {
		return fmt.Errorf("failed to create HTTP server: %w", err)
	}

	r.settings.Logger.Info("Starting Prometheus Remote Write v1 receiver",
		zap.String("endpoint", r.config.NetAddr.Endpoint))

	var listener net.Listener
	if listener, err = r.config.ToListener(ctx); err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}

	r.shutdownWG.Go(func() {
		if err := r.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			r.settings.Logger.Error("HTTP server error", zap.Error(err))
		}
	})
	return nil
}

func (r *prometheusRWv1Receiver) Shutdown(ctx context.Context) error {
	if r.server == nil {
		return nil
	}
	err := r.server.Shutdown(ctx)
	if err == nil {
		r.shutdownWG.Wait()
	}
	return err
}

// handleWrite is the HTTP handler for the remote write endpoint.
// It validates the request per the v1 spec, decodes it, translates to OTLP, and forwards to the next consumer.
func (r *prometheusRWv1Receiver) handleWrite(w http.ResponseWriter, req *http.Request) {
	obsCtx := r.obsrecv.StartMetricsOp(req.Context())
	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		r.settings.Logger.Warn("Request missing Content-Type header")
		http.Error(w, "Content-Type header is required", http.StatusUnsupportedMediaType)
		r.obsrecv.EndMetricsOp(obsCtx, "prometheusremotewritev1receiver", 0, errors.New("Request missing Content-Type header"))
		return
	}
	// The v1 spec mandates application/x-protobuf. No proto= parameter is needed.
	if !strings.HasPrefix(contentType, "application/x-protobuf") {
		r.settings.Logger.Warn("Unsupported Content-Type", zap.String("content_type", contentType))
		http.Error(w, "Content-Type must be application/x-protobuf", http.StatusUnsupportedMediaType)
		r.obsrecv.EndMetricsOp(obsCtx, "prometheusremotewritev1receiver", 0, errors.New("Unsupported Content-Type"))
		return
	}

	reqBody, err := io.ReadAll(req.Body)
	if err != nil {
		r.settings.Logger.Warn("Failed to read request body", zap.Error(err))
		http.Error(w, fmt.Sprintf("read request body: %v", err), http.StatusBadRequest)
		r.obsrecv.EndMetricsOp(obsCtx, "prometheusremotewritev1receiver", 0, err)
		return
	}

	var wr prompb.WriteRequest
	if err := proto.Unmarshal(reqBody, &wr); err != nil {
		r.settings.Logger.Warn("Protobuf unmarshal failed", zap.Error(err))
		http.Error(w, fmt.Sprintf("protobuf unmarshal: %v", err), http.StatusBadRequest)
		r.obsrecv.EndMetricsOp(obsCtx, "prometheusremotewritev1receiver", 0, err)
		return
	}

	md, isInvalid := r.translate(&wr)

	if md.MetricCount() > 0 {
		err = r.nextConsumer.ConsumeMetrics(req.Context(), md)
		if err != nil {
			r.obsrecv.EndMetricsOp(obsCtx, "prometheusremotewritev1receiver", md.MetricCount(), err)
			r.settings.Logger.Error("Failed to consume metrics", zap.Error(err))
			if consumererror.IsPermanent(err) {
				http.Error(w, err.Error(), http.StatusBadRequest)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
	}
	// if request is invalid, we return 400 even if some metrics were accepted.
	if isInvalid {
		r.obsrecv.EndMetricsOp(obsCtx, "prometheusremotewritev1receiver", md.MetricCount(), errors.New("one or more time series were missing the __name__ label and were dropped"))
		http.Error(w, "one or more time series were missing the __name__ label and were dropped", http.StatusBadRequest)
		return
	}
	r.obsrecv.EndMetricsOp(obsCtx, "prometheusremotewritev1receiver", md.MetricCount(), nil)
	w.WriteHeader(http.StatusNoContent)
}

// translate converts a v1 WriteRequest into OTLP pmetric.Metrics.
//
// Each timeseries maps directly to one Gauge metric whose samples become data
// points. All labels except __name__ (including job and instance) are stored
// as data point attributes. There are no resource attributes and no grouping
// by metric name or resource.
// isInvalid is true if any time series were missing the __name__ label and were dropped.
func (r *prometheusRWv1Receiver) translate(wr *prompb.WriteRequest) (pmetric.Metrics, bool) {
	labelsBuilder := labels.NewScratchBuilder(0)

	md := pmetric.NewMetrics()
	scope := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	var isInvalid bool

	for i := range wr.Timeseries {
		ts := &wr.Timeseries[i]

		ls := ts.ToLabels(&labelsBuilder, []string{})

		metricName := ls.Get("__name__")
		if metricName == "" {
			isInvalid = true
			r.settings.Logger.Warn("Dropping time series with missing __name__ label")
			continue
		}

		m := scope.Metrics().AppendEmpty()
		m.SetName(metricName)
		m.SetEmptyGauge()

		attrs := buildAttributes(ts.Labels)

		for _, s := range ts.Samples {
			tsNanos := pcommon.Timestamp(s.Timestamp * int64(time.Millisecond))
			dp := m.Gauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(tsNanos)
			attrs.CopyTo(dp.Attributes())
			dp.SetDoubleValue(s.Value)
		}
	}

	return md, isInvalid
}

func buildAttributes(labels []prompb.Label) pcommon.Map {
	attrs := pcommon.NewMap()
	for _, lbl := range labels {
		if lbl.Name != "__name__" {
			attrs.PutStr(lbl.Name, lbl.Value)
		}
	}
	return attrs
}
