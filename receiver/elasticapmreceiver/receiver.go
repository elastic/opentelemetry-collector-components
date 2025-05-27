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
	"hash"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/elastic/apm-data/input/elasticapm"
	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/apm-data/model/modelprocessor"
	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmreceiver/internal/mappers"
	"github.com/elastic/opentelemetry-lib/agentcfg"
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

const (
	agentConfigPath    = "/config/v1/agents"
	intakeV2EventsPath = "/intake/v2/events"
)

type agentCfgFetcherFactory = func(context.Context, component.Host) (agentcfg.Fetcher, error)

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

	fetcherFactory agentCfgFetcherFactory
	cancelFn       context.CancelFunc
}

// newElasticAPMReceiver just creates the OpenTelemetry receiver services. It is the caller's
// responsibility to invoke the respective Start*Reception methods as well
// as the various Stop*Reception methods to end it.
func newElasticAPMReceiver(fetcher agentCfgFetcherFactory, cfg *Config, set receiver.Settings) (*elasticAPMReceiver, error) {
	obsreport, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             set.ID,
		Transport:              "http",
		ReceiverCreateSettings: set,
	})
	if err != nil {
		return nil, err
	}

	return &elasticAPMReceiver{
		cfg:            cfg,
		settings:       set,
		obsreport:      obsreport,
		fetcherFactory: fetcher,
	}, nil
}

// Start runs an HTTP server for receiving data from Elastic APM agents.
func (r *elasticAPMReceiver) Start(ctx context.Context, host component.Host) error {
	ctx, r.cancelFn = context.WithCancel(ctx)
	if err := r.startHTTPServer(ctx, host); err != nil {
		return errors.Join(err, r.Shutdown(ctx))
	}
	return nil
}

func (r *elasticAPMReceiver) startHTTPServer(ctx context.Context, host component.Host) error {
	httpMux := http.NewServeMux()

	httpMux.HandleFunc(intakeV2EventsPath, r.newElasticAPMEventsHandler())
	httpMux.HandleFunc(agentConfigPath, r.newElasticAPMConfigsHandler(ctx, host))
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
	if r.cancelFn != nil {
		r.cancelFn()
	}
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

		var elasticapmResult elasticapm.Result
		baseEvent := &modelpb.APMEvent{}
		baseEvent.Event = &modelpb.Event{}
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

		var result struct {
			Accepted int         `json:"accepted"`
			Errors   []jsonError `json:"errors,omitempty"`
		}
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

	gk := modelprocessor.SetGroupingKey{
		NewHash: func() hash.Hash {
			return xxhash.New()
		},
	}

	if err := gk.ProcessBatch(ctx, batch); err != nil {
		r.settings.Logger.Error("failed to process batch", zap.Error(err))
	}

	for _, event := range *batch {
		timestampNanos := event.GetTimestamp()
		timestamp := time.Unix(
			int64(timestampNanos/1e9), // Convert nanoseconds to seconds
			int64(timestampNanos%1e9), // Remainder in nanoseconds
		)

		// TODO record metrics about events processed by type?
		// TODO translate events to pdata types
		switch event.Type() {
		case modelpb.MetricEventType:
			rm := md.ResourceMetrics().AppendEmpty()
			if err := r.elasticMetricsToOtelMetrics(&rm, event, timestamp, ctx); err != nil {
				return err
			}
		case modelpb.ErrorEventType:
			rl := ld.ResourceLogs().AppendEmpty()
			r.elasticErrorToOtelLogRecord(&rl, event, timestamp, ctx)
		case modelpb.LogEventType:
			// TODO
		case modelpb.SpanEventType, modelpb.TransactionEventType:
			rs := td.ResourceSpans().AppendEmpty()
			s := r.elasticEventToOtelSpan(&rs, event, timestamp)

			isTransaction := event.Type() == modelpb.TransactionEventType
			if isTransaction {
				r.elasticTransactionToOtelSpan(&s, event)
			} else {
				r.elasticSpanToOTelSpan(&s, event)
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

func (r *elasticAPMReceiver) elasticMetricsToOtelMetrics(rm *pmetric.ResourceMetrics, event *modelpb.APMEvent, timestamp time.Time, ctx context.Context) error {
	sm := rm.ScopeMetrics().AppendEmpty()
	metricset := event.GetMetricset()

	// span_breakdown metrics don't have Samples - value is stored directly in event.Span.SelfTime.*
	if metricset.Name == "span_breakdown" {
		r.translateBreakdownMetricsToOtel(rm, event)
		return nil
	}

	samples := metricset.GetSamples()

	// TODO interval, doc_count
	for _, sample := range samples {
		m := sm.Metrics().AppendEmpty()
		m.SetName(sample.GetName())

		switch sample.GetType() {
		case modelpb.MetricType_METRIC_TYPE_COUNTER:
			dp := m.SetEmptySum().DataPoints().AppendEmpty()
			dp.SetDoubleValue(sample.GetValue())
			r.populateDataPointCommon(&dp, event, timestamp)
		// Type does not seem to be enforced in APM server, and many agents send `unspecified` type.
		case modelpb.MetricType_METRIC_TYPE_GAUGE, modelpb.MetricType_METRIC_TYPE_UNSPECIFIED:
			dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
			dp.SetDoubleValue(sample.GetValue())
			r.populateDataPointCommon(&dp, event, timestamp)
		case modelpb.MetricType_METRIC_TYPE_HISTOGRAM:
			// TODO histograms
		case modelpb.MetricType_METRIC_TYPE_SUMMARY:
			// TODO summaries
		default:
			return fmt.Errorf("unhandled metric type %q", sample.GetType())
		}
		mappers.TranslateToOtelResourceAttributes(event, rm.Resource().Attributes())
	}

	return nil
}

type otelDataPoint interface {
	SetTimestamp(pcommon.Timestamp)
	Attributes() pcommon.Map
}

func (r *elasticAPMReceiver) populateDataPointCommon(dp otelDataPoint, event *modelpb.APMEvent, timestamp time.Time) {
	dp.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	mappers.SetDerivedFieldsCommon(event, dp.Attributes())
	mappers.SetDerivedFieldsForMetrics(event, dp.Attributes())
}

func (r *elasticAPMReceiver) translateBreakdownMetricsToOtel(rm *pmetric.ResourceMetrics, event *modelpb.APMEvent) {
	sm := rm.ScopeMetrics().AppendEmpty()
	sum_metric := sm.Metrics().AppendEmpty()
	sum_metric.SetName("span.self_time.sum.us")

	//TODO: without Unit, the es exporter throws this:
	// error	elasticsearchexporter@v0.124.1/bulkindexer.go:367	failed to index document	{"index": "metrics-generic.otel-default", "error.type": "illegal_argument_exception", "error.reason": ""}
	// github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter.flushBulkIndexer
	// github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter@v0.124.1/bulkindexer.go:367
	sum_metric.SetUnit("us")
	sum_dp := createBreakdownMetricsCommon(sum_metric, event)
	sum_dp.SetIntValue(int64(event.Span.SelfTime.Sum))

	count_metric := sm.Metrics().AppendEmpty()
	count_metric.SetName("span.self_time.count")
	count_metric.SetUnit("count")
	count_metric_dp := createBreakdownMetricsCommon(count_metric, event)
	count_metric_dp.SetDoubleValue(float64(event.Span.SelfTime.Count))

	mappers.TranslateToOtelResourceAttributes(event, rm.Resource().Attributes())
	mappers.SetDerivedFieldsForMetrics(event, sum_dp.Attributes())
	mappers.SetDerivedFieldsForMetrics(event, count_metric_dp.Attributes())
}

func createBreakdownMetricsCommon(metric pmetric.Metric, event *modelpb.APMEvent) pmetric.NumberDataPoint {
	g := metric.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()

	attr := dp.Attributes()
	attr.PutStr("transaction.name", event.Transaction.Name)
	attr.PutStr("transaction.type", event.Transaction.Type)
	attr.PutStr("span.type", event.Span.Type)
	attr.PutStr("span.subtype", event.Span.Subtype)
	attr.PutStr("processor.event", "metric")
	dp.SetTimestamp(pcommon.Timestamp(event.Timestamp))

	return dp
}

func (r *elasticAPMReceiver) elasticErrorToOtelLogRecord(rl *plog.ResourceLogs, event *modelpb.APMEvent, timestamp time.Time, ctx context.Context) {
	sl := rl.ScopeLogs().AppendEmpty()
	l := sl.LogRecords().AppendEmpty()

	mappers.SetTopLevelFieldsLogRecord(event, timestamp, l, r.settings.Logger)
	mappers.SetDerivedFieldsForError(event, l.Attributes())
	mappers.SetDerivedResourceAttributes(event, rl.Resource().Attributes())
	mappers.TranslateToOtelResourceAttributes(event, rl.Resource().Attributes())

	if event.Error != nil && event.Error.Log != nil {
		l.Body().SetStr(event.Error.Log.Message)
	}
}

func (r *elasticAPMReceiver) elasticEventToOtelSpan(rs *ptrace.ResourceSpans, event *modelpb.APMEvent, timestamp time.Time) ptrace.Span {
	ss := rs.ScopeSpans().AppendEmpty()
	s := ss.Spans().AppendEmpty()

	mappers.SetTopLevelFieldsSpan(event, timestamp, s, r.settings.Logger)
	mappers.TranslateToOtelResourceAttributes(event, rs.Resource().Attributes())
	mappers.SetDerivedFieldsCommon(event, s.Attributes())
	mappers.SetDerivedResourceAttributes(event, rs.Resource().Attributes())
	r.elasticSpanLinksToOTelSpanLinks(event, s)
	return s
}

func (r *elasticAPMReceiver) elasticSpanLinksToOTelSpanLinks(event *modelpb.APMEvent, s ptrace.Span) {
	if event.Span != nil && event.Span.Links != nil {
		for _, link := range event.Span.Links {
			ptraceSpanLink := s.Links().AppendEmpty()
			traceId, err := mappers.TraceIDFromHex(link.TraceId)
			if err == nil {
				ptraceSpanLink.SetTraceID(traceId)
			} else {
				r.settings.Logger.Error("failed to parse trace ID from span link", zap.String("trace_id", link.TraceId))
			}

			spanId, err := mappers.SpanIdFromHex(link.SpanId)
			if err == nil {
				ptraceSpanLink.SetSpanID(spanId)
			} else {
				r.settings.Logger.Error("failed to parse span ID from span link", zap.String("span_id", link.SpanId))
			}
		}
	}
}

func (r *elasticAPMReceiver) elasticTransactionToOtelSpan(s *ptrace.Span, event *modelpb.APMEvent) {
	s.SetName(event.Transaction.Name)

	mappers.SetDerivedFieldsForTransaction(event, s.Attributes())
	transaction := event.GetTransaction()
	s.SetName(transaction.GetName())
	mappers.TranslateIntakeV2TransactionToOTelAttributes(event, s.Attributes())

	if event.Http != nil && event.Http.Request != nil {
		s.SetKind(ptrace.SpanKindServer)
	} else if event.Message != "" { // this check is TBD
		s.SetKind(ptrace.SpanKindConsumer)
	}
}

func (r *elasticAPMReceiver) elasticSpanToOTelSpan(s *ptrace.Span, event *modelpb.APMEvent) {
	span := event.GetSpan()
	s.SetName(span.GetName())

	mappers.SetDerivedFieldsForSpan(event, s.Attributes())
	mappers.TranslateIntakeV2SpanToOTelAttributes(event, s.Attributes())

	if event.Http != nil || event.Message != "" {
		s.SetKind(ptrace.SpanKindClient)
	}
}

type jsonError struct {
	Message  string `json:"message"`
	Document string `json:"document,omitempty"`
}
