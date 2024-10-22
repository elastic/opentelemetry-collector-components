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

package elasticapmreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/elastic/apm-data/input/elasticapm"
	"github.com/elastic/apm-data/model/modelpb"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// TODO report different formats for intakev2 and rumv3?
const dataFormatElasticAPM = "elasticapm"

// elasticAPMReceiver implements support for receiving Logs, Metrics, and Traces from Elastic APM agents.
type elasticAPMReceiver struct {
	cfg       *Config
	obsreport *receiverhelper.ObsReport
	settings  receiver.Settings

	nextTraces  consumer.Traces
	nextMetrics consumer.Metrics
	nextLogs    consumer.Logs

	httpServer *http.Server
	shutdownWG sync.WaitGroup
}

// newElasticAPMReceiver just creates the OpenTelemetry receiver services. It is the caller's
// responsibility to invoke the respective Start*Reception methods as well
// as the various Stop*Reception methods to end it.
func newElasticAPMReceiver(cfg *Config, set receiver.Settings) (*elasticAPMReceiver, error) {
	obsreport, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             set.ID,
		Transport:              "http",
		ReceiverCreateSettings: set,
	})
	if err != nil {
		return nil, err
	}

	return &elasticAPMReceiver{
		cfg:       cfg,
		settings:  set,
		obsreport: obsreport,
	}, nil
}

// Start runs an HTTP server for receiving data from Elastic APM agents.
func (r *elasticAPMReceiver) Start(ctx context.Context, host component.Host) error {
	if err := r.startHTTPServer(ctx, host); err != nil {
		return errors.Join(err, r.Shutdown(ctx))
	}
	return nil
}

func (r *elasticAPMReceiver) startHTTPServer(ctx context.Context, host component.Host) error {
	httpMux := http.NewServeMux()

	elasticAPMEventsHandler := r.newElasticAPMEventsHandler()
	httpMux.HandleFunc("/intake/v2/events", elasticAPMEventsHandler)
	// TODO rum v2, v3

	var err error
	if r.httpServer, err = r.cfg.ToServer(
		ctx, host, r.settings.TelemetrySettings, httpMux,
		confighttp.WithErrorHandler(errorHandler),
	); err != nil {
		return err
	}

	r.settings.Logger.Info("Starting HTTP server", zap.String("endpoint", r.cfg.ServerConfig.Endpoint))
	var hln net.Listener
	if hln, err = r.cfg.ServerConfig.ToListener(ctx); err != nil {
		return err
	}

	r.shutdownWG.Add(1)
	go func() {
		defer r.shutdownWG.Done()
		if errHTTP := r.httpServer.Serve(hln); errHTTP != nil && !errors.Is(errHTTP, http.ErrServerClosed) {
			componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(errHTTP))
		}
	}()
	return nil
}

// Shutdown is a method to turn off receiving.
func (r *elasticAPMReceiver) Shutdown(ctx context.Context) error {
	var err error
	if r.httpServer != nil {
		err = r.httpServer.Shutdown(ctx)
	}
	r.shutdownWG.Wait()
	return err
}

func errorHandler(w http.ResponseWriter, r *http.Request, errMsg string, statusCode int) {
	// TODO
}

func (r *elasticAPMReceiver) newElasticAPMEventsHandler() http.HandlerFunc {
	var (
		// TODO make semaphore size configurable and/or find a different way
		// to limit concurrency that fits better with OTel Collector.
		sem = semaphore.NewWeighted(100)

		// TODO make event size configurable
		maxEventSize = 1024 * 1024 // 1MiB

		// TODO make batch size configurable?
		batchSize = 10
	)

	batchProcessor := modelpb.ProcessBatchFunc(r.processBatch)
	elasticapmProcessor := elasticapm.NewProcessor(elasticapm.Config{
		Logger:       r.settings.Logger,
		MaxEventSize: maxEventSize,
		Semaphore:    sem,
	})

	return func(w http.ResponseWriter, r *http.Request) {
		statusCode := http.StatusAccepted
		baseEvent := &modelpb.APMEvent{}
		baseEvent.Event = &modelpb.Event{}
		baseEvent.Event.Received = modelpb.FromTime(time.Now())

		var result struct {
			Accepted int         `json:"accepted"`
			Errors   []jsonError `json:"errors,omitempty"`
		}

		var elasticapmResult elasticapm.Result
		streamErr := elasticapmProcessor.HandleStream(
			r.Context(),
			baseEvent,
			r.Body,
			batchSize,
			batchProcessor,
			&elasticapmResult,
		)
		_ = streamErr
		// TODO record metrics about errors?

		result.Accepted = elasticapmResult.Accepted
		// TODO process elasticapmResult.Errors, add to result
		// TODO process streamErr, conditionally add to result
		// TODO process r.Context().Err(), conditionally add to result

		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(&result)
	}
}

func (r *elasticAPMReceiver) processBatch(ctx context.Context, batch *modelpb.Batch) error {
	ld := plog.NewLogs()
	md := pmetric.NewMetrics()
	td := ptrace.NewTraces()
	for _, event := range *batch {
		timestampNanos := event.GetTimestamp()
		timestamp := time.Unix(
			int64(timestampNanos/uint64(time.Nanosecond)),
			int64(timestampNanos%uint64(time.Nanosecond)),
		)

		// TODO record metrics about events processed by type?
		// TODO translate events to pdata types
		switch event.Type() {
		case modelpb.MetricEventType:
			rm := md.ResourceMetrics().AppendEmpty()
			sm := rm.ScopeMetrics().AppendEmpty()
			metricset := event.GetMetricset()

			// TODO interval, doc_count
			// TODO how can we attach metricset.name?
			for _, sample := range metricset.GetSamples() {
				m := sm.Metrics().AppendEmpty()
				m.SetName(sample.GetName())
				m.SetUnit(sample.GetUnit())
				// TODO set attributes (dimensions/labels)
				switch sample.GetType() {
				case modelpb.MetricType_METRIC_TYPE_COUNTER:
					dp := m.SetEmptySum().DataPoints().AppendEmpty()
					dp.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
					dp.SetDoubleValue(sample.GetValue())
				case modelpb.MetricType_METRIC_TYPE_GAUGE:
					dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
					dp.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
					dp.SetDoubleValue(sample.GetValue())
				case modelpb.MetricType_METRIC_TYPE_HISTOGRAM:
					// TODO histograms
				case modelpb.MetricType_METRIC_TYPE_SUMMARY:
					// TODO summaries
				default:
					return fmt.Errorf("unhandled metric type %q", sample.GetType())
				}
			}
		case modelpb.ErrorEventType:
			// TODO
		case modelpb.LogEventType:
			// TODO
		case modelpb.SpanEventType, modelpb.TransactionEventType:
			rs := td.ResourceSpans().AppendEmpty()
			ss := rs.ScopeSpans().AppendEmpty()
			s := ss.Spans().AppendEmpty()

			duration := time.Duration(event.GetEvent().GetDuration())
			s.SetStartTimestamp(pcommon.NewTimestampFromTime(timestamp))
			s.SetEndTimestamp(pcommon.NewTimestampFromTime(timestamp.Add(duration)))

			// TODO set attributes
			isTransaction := event.Type() == modelpb.TransactionEventType
			if isTransaction {
				transaction := event.GetTransaction()
				s.SetName(transaction.GetName())
			} else {
				span := event.GetSpan()
				s.SetName(span.GetName())
			}
		default:
			return fmt.Errorf("unhandled event type %q", event.Type())
		}
	}
	var errs []error
	if numRecords := ld.LogRecordCount(); numRecords != 0 {
		ctx := r.obsreport.StartLogsOp(ctx)
		err := r.nextLogs.ConsumeLogs(ctx, ld)
		r.obsreport.EndLogsOp(ctx, dataFormatElasticAPM, numRecords, err)
		errs = append(errs, err)
	}
	if numDataPoints := md.DataPointCount(); numDataPoints != 0 {
		ctx := r.obsreport.StartMetricsOp(ctx)
		err := r.nextMetrics.ConsumeMetrics(ctx, md)
		r.obsreport.EndMetricsOp(ctx, dataFormatElasticAPM, numDataPoints, err)
		errs = append(errs, err)
	}
	if numSpans := td.SpanCount(); numSpans != 0 {
		ctx := r.obsreport.StartTracesOp(ctx)
		err := r.nextTraces.ConsumeTraces(ctx, td)
		r.obsreport.EndTracesOp(ctx, dataFormatElasticAPM, numSpans, err)
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

type jsonError struct {
	Message  string `json:"message"`
	Document string `json:"document,omitempty"`
}
