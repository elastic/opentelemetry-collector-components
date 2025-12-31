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

package elasticapmintakereceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/cespare/xxhash"
	"go.opentelemetry.io/collector/client"
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

	"github.com/elastic/apm-data/input/elasticapm"
	"github.com/elastic/apm-data/model/modelpb"
	"github.com/elastic/apm-data/model/modelprocessor"
	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/mappers"
	"github.com/elastic/opentelemetry-lib/agentcfg"
)

// TODO report different formats for intakev2 and rumv3?
const dataFormatElasticAPM = "elasticapm"

const (
	agentConfigPath    = "/config/v1/agents"
	intakeV2EventsPath = "/intake/v2/events"
)

type agentCfgFetcherFactory = func(context.Context, component.Host) (agentcfg.Fetcher, error)

// elasticAPMIntakeReceiver implements support for receiving Logs, Metrics, and Traces from Elastic APM agents.
type elasticAPMIntakeReceiver struct {
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

// newElasticAPMIntakeReceiver just creates the OpenTelemetry receiver services. It is the caller's
// responsibility to invoke the respective Start*Reception methods as well
// as the various Stop*Reception methods to end it.
func newElasticAPMIntakeReceiver(fetcher agentCfgFetcherFactory, cfg *Config, set receiver.Settings) (*elasticAPMIntakeReceiver, error) {
	obsreport, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             set.ID,
		Transport:              "http",
		ReceiverCreateSettings: set,
	})
	if err != nil {
		return nil, err
	}

	return &elasticAPMIntakeReceiver{
		cfg:            cfg,
		settings:       set,
		obsreport:      obsreport,
		fetcherFactory: fetcher,
	}, nil
}

// Start runs an HTTP server for receiving data from Elastic APM agents.
func (r *elasticAPMIntakeReceiver) Start(ctx context.Context, host component.Host) error {
	ctx, r.cancelFn = context.WithCancel(ctx)
	if err := r.startHTTPServer(ctx, host); err != nil {
		return errors.Join(err, r.Shutdown(ctx))
	}
	return nil
}

func (r *elasticAPMIntakeReceiver) startHTTPServer(ctx context.Context, host component.Host) error {
	httpMux := http.NewServeMux()

	httpMux.HandleFunc(intakeV2EventsPath, r.newElasticAPMEventsHandler(func(req *http.Request) context.Context {
		return withECSMappingMode(req.Context(), r.cfg.IncludeMetadata)
	}))
	httpMux.HandleFunc(agentConfigPath, r.newElasticAPMConfigsHandler(ctx, host))
	// TODO rum v2, v3

	var err error
	if r.httpServer, err = r.cfg.ToServer(
		ctx, host.GetExtensions(), r.settings.TelemetrySettings, httpMux,
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
func (r *elasticAPMIntakeReceiver) Shutdown(ctx context.Context) error {
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

func (r *elasticAPMIntakeReceiver) newElasticAPMEventsHandler(ctxFunc func(*http.Request) context.Context) http.HandlerFunc {
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

	return func(w http.ResponseWriter, req *http.Request) {
		statusCode := http.StatusAccepted

		var elasticapmResult elasticapm.Result
		baseEvent := &modelpb.APMEvent{}
		baseEvent.Event = &modelpb.Event{}
		streamErr := elasticapmProcessor.HandleStream(
			ctxFunc(req),
			baseEvent,
			req.Body,
			batchSize,
			batchProcessor,
			&elasticapmResult,
		)
		_ = streamErr
		// TODO record metrics about errors?

		var result struct {
			Accepted int      `json:"accepted"`
			Errors   []string `json:"errors,omitempty"`
		}
		result.Accepted = elasticapmResult.Accepted
		result.Errors = make([]string, 0, len(elasticapmResult.Errors))
		for _, err := range elasticapmResult.Errors {
			result.Errors = append(result.Errors, err.Error())
		}

		if streamErr != nil {
			r.settings.Logger.Error("failed to process APM events stream", zap.Error(streamErr))
			result.Errors = append(result.Errors, streamErr.Error())
		}

		if len(result.Errors) > 0 {
			statusCode = http.StatusBadRequest
		} else if streamErr != nil {
			statusCode = http.StatusInternalServerError
		}

		// TODO process r.Context().Err(), conditionally add to result

		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(&result)
	}
}

func (r *elasticAPMIntakeReceiver) processBatch(ctx context.Context, batch *modelpb.Batch) error {
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

		// TODO record metrics about events processed by type?
		switch event.Type() {
		case modelpb.MetricEventType:
			rm := md.ResourceMetrics().AppendEmpty()

			r.setResourceAttributes(rm.Resource().Attributes(), event)

			if err := r.elasticMetricsToOtelMetrics(&rm, event, timestampNanos); err != nil {
				return err
			}
		case modelpb.ErrorEventType:
			rl := ld.ResourceLogs().AppendEmpty()

			r.setResourceAttributes(rl.Resource().Attributes(), event)

			r.elasticErrorToOtelLogRecord(&rl, event, timestampNanos)
		case modelpb.LogEventType:
			rl := ld.ResourceLogs().AppendEmpty()

			r.setResourceAttributes(rl.Resource().Attributes(), event)

			r.elasticLogToOtelLogRecord(&rl, event, timestampNanos)
		case modelpb.SpanEventType, modelpb.TransactionEventType:
			rs := td.ResourceSpans().AppendEmpty()

			r.setResourceAttributes(rs.Resource().Attributes(), event)

			s := r.elasticEventToOtelSpan(&rs, event, timestampNanos)

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
	if numRecords := ld.LogRecordCount(); numRecords != 0 && r.nextLogs != nil {
		ctx := r.obsreport.StartLogsOp(ctx)
		err := r.nextLogs.ConsumeLogs(ctx, ld)
		r.obsreport.EndLogsOp(ctx, dataFormatElasticAPM, numRecords, err)
		errs = append(errs, err)
	}
	if numDataPoints := md.DataPointCount(); numDataPoints != 0 && r.nextMetrics != nil {
		ctx := r.obsreport.StartMetricsOp(ctx)
		err := r.nextMetrics.ConsumeMetrics(ctx, md)
		r.obsreport.EndMetricsOp(ctx, dataFormatElasticAPM, numDataPoints, err)
		errs = append(errs, err)
	}
	if numSpans := td.SpanCount(); numSpans != 0 && r.nextTraces != nil {
		ctx := r.obsreport.StartTracesOp(ctx)
		err := r.nextTraces.ConsumeTraces(ctx, td)
		r.obsreport.EndTracesOp(ctx, dataFormatElasticAPM, numSpans, err)
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// setResourceAttributes maps event fields to attributes.
// Expects the attribute map to be at the resource level e.g. pmetric.ResourceMetrics.Resource().Attributes().
func (r *elasticAPMIntakeReceiver) setResourceAttributes(attrs pcommon.Map, event *modelpb.APMEvent) {
	mappers.TranslateToOtelResourceAttributes(event, attrs)
	mappers.SetDerivedResourceAttributes(event, attrs)
	mappers.SetElasticSpecificResourceAttributes(event, attrs)
}

func (r *elasticAPMIntakeReceiver) elasticMetricsToOtelMetrics(rm *pmetric.ResourceMetrics, event *modelpb.APMEvent, timestampNanos uint64) error {
	metricset := event.GetMetricset()
	// span_breakdown metrics don't have Samples - value is stored directly in event.Span.SelfTime.*
	if metricset.Name == "span_breakdown" {
		r.translateBreakdownMetricsToOtel(rm, event, timestampNanos)
		return nil
	}

	sm := rm.ScopeMetrics().AppendEmpty()

	samples := metricset.GetSamples()

	// Ignored metricset fields: interval and doc_count.
	// Fields are not decoded from input data to modelpb.Metricset, so they will not ever be set:
	// - https://github.com/elastic/apm-data/blob/main/input/elasticapm/internal/modeldecoder/v2/model.go
	// - https://github.com/elastic/apm-data/blob/main/input/elasticapm/internal/modeldecoder/v2/decoder.go
	for _, sample := range samples {
		m := sm.Metrics().AppendEmpty()
		m.SetName(sample.GetName())

		// Set provided unit without any validation or enumeration.
		// - The apm-data lib does not validate units: https://github.com/elastic/apm-data/blob/main/input/elasticapm/internal/modeldecoder/v2/decoder.go
		// - The ElasticSearch https://github.com/elastic/package-spec/blob/main/spec/integration/data_stream/fields/fields.spec.yml supported units
		//   also meet the OTEL requirements based on https://ucum.org/ucum.
		m.SetUnit(sample.GetUnit())

		switch sample.GetType() {
		case modelpb.MetricType_METRIC_TYPE_COUNTER:
			dp := m.SetEmptySum().DataPoints().AppendEmpty()
			dp.SetDoubleValue(sample.GetValue())
			r.populateDataPointCommon(&dp, event, timestampNanos)
		// Type does not seem to be enforced in APM server, and many agents send `unspecified` type.
		case modelpb.MetricType_METRIC_TYPE_GAUGE, modelpb.MetricType_METRIC_TYPE_UNSPECIFIED:
			dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
			dp.SetDoubleValue(sample.GetValue())
			r.populateDataPointCommon(&dp, event, timestampNanos)
		case modelpb.MetricType_METRIC_TYPE_HISTOGRAM:
			dp := m.SetEmptyHistogram().DataPoints().AppendEmpty()
			r.populateDataPointCommon(&dp, event, timestampNanos)
			populateOTelHistogramDataPoint(sample, &dp)
		case modelpb.MetricType_METRIC_TYPE_SUMMARY:
			// Note: The apm-data lib will reject a valid summary (contains only a count and sum), so
			// this apm summaries will not be converted to OTEL.
			// - https://github.com/elastic/apm-data/blob/main/input/elasticapm/internal/modeldecoder/v2/model.go
			// Validation error:
			// - `validation error: metricset: samples: requires at least one of the fields 'value;values'`
		default:
			return fmt.Errorf("unhandled metric type %q", sample.GetType())
		}
	}

	return nil
}

type otelDataPoint interface {
	SetTimestamp(pcommon.Timestamp)
	Attributes() pcommon.Map
}

func (r *elasticAPMIntakeReceiver) populateDataPointCommon(dp otelDataPoint, event *modelpb.APMEvent, timestampNanos uint64) {
	dp.SetTimestamp(pcommon.Timestamp(timestampNanos))
	mappers.SetDerivedFieldsCommon(event, dp.Attributes())
	mappers.SetDerivedFieldsForMetrics(dp.Attributes())
}

// populateOTelHistogramDataPoint updates the OpenTelemetry HistogramDataPoint with data from the provided Elastic APM histogram sample.
// Assumptions:
// - the histogram values and counts are all non-negative
//
// Sets fields: sum, count, bucket_counts, explicit_bounds. All other optional fields are not set per OTEL metric model:
//   - https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
func populateOTelHistogramDataPoint(sample *modelpb.MetricsetSample, dp *pmetric.HistogramDataPoint) {
	histogram := sample.GetHistogram()
	if histogram == nil {
		return
	}

	// histogram values and count should be non-empty and the same size
	apmHistogramCounts := histogram.GetCounts()
	apmHistogramValues := histogram.GetValues()
	if len(apmHistogramValues) == 0 || len(apmHistogramCounts) == 0 {
		return
	}
	if len(apmHistogramValues) != len(apmHistogramCounts) {
		return
	}

	// sum of the values in the population. If count is zero then this field
	// must be zero.
	//
	// Note: Sum should only be filled out when measuring non-negative discrete
	// events, and is assumed to be monotonic over the values of these events.
	// Negative events *can* be recorded, but sum should not be filled out when
	// doing so.  This is specifically to enforce compatibility w/ OpenMetrics,
	// see: https://github.com/prometheus/OpenMetrics/blob/v1.0.0/specification/OpenMetrics.md#histogram
	sum := 0.0
	for i := 0; i < len(apmHistogramValues); i++ {
		sum += apmHistogramValues[i] * float64(apmHistogramCounts[i])
	}
	dp.SetSum(sum)

	// count is the number of values in the population. Must be non-negative. This
	// value must be equal to the sum of the "count" fields in buckets if a
	// histogram is provided.
	count := uint64(0)
	for _, c := range apmHistogramCounts {
		count += c
	}
	dp.SetCount(count)

	// bucket_counts is an optional field contains the count values of histogram
	// for each bucket.
	//
	// The sum of the bucket_counts must equal the value in the count field.
	//
	// The number of elements in bucket_counts array must be by one greater than
	// the number of elements in explicit_bounds array. The exception to this rule
	// is when the length
	bucketCounts := dp.BucketCounts()
	bucketCounts.FromRaw(apmHistogramCounts)

	// explicit_bounds specifies buckets with explicitly defined bounds for values.
	//
	// The boundaries for bucket at index i are:
	//
	// (-infinity, explicit_bounds[i]] for i == 0
	// (explicit_bounds[i-1], explicit_bounds[i]] for 0 < i < size(explicit_bounds)
	// (explicit_bounds[i-1], +infinity) for i == size(explicit_bounds)
	//
	// The values in the explicit_bounds array must be strictly increasing.
	//
	// Histogram buckets are inclusive of their upper boundary, except the last
	// bucket where the boundary is at infinity. This format is intentionally
	// compatible with the OpenMetrics histogram definition.
	//
	// If bucket_counts length is 0 then explicit_bounds length must also be 0,
	// otherwise the data point is invalid.
	explicitBounds := dp.ExplicitBounds()

	// explicit bounds are derived from the sample.Histogram.Values, where each value is the upper bound for a bucket.
	// Except the last bound value which is implied to be +Inf bucket, so it is not set.
	explicitBounds.FromRaw(apmHistogramValues[:len(apmHistogramValues)-1])
}

func (r *elasticAPMIntakeReceiver) translateBreakdownMetricsToOtel(rm *pmetric.ResourceMetrics, event *modelpb.APMEvent, timestampNanos uint64) {
	sm := rm.ScopeMetrics().AppendEmpty()
	sum_metric := sm.Metrics().AppendEmpty()
	sum_metric.SetName("span.self_time.sum.us")

	// TODO: without Unit, the es exporter throws this:
	// error	elasticsearchexporter@v0.124.1/bulkindexer.go:367	failed to index document	{"index": "metrics-generic.otel-default", "error.type": "illegal_argument_exception", "error.reason": ""}
	// github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter.flushBulkIndexer
	// github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter@v0.124.1/bulkindexer.go:367
	sum_metric.SetUnit("us")
	sum_dp := createBreakdownMetricsCommon(sum_metric, event, timestampNanos)
	sum_dp.SetIntValue(int64(event.GetSpan().GetSelfTime().Sum))

	count_metric := sm.Metrics().AppendEmpty()
	count_metric.SetName("span.self_time.count")
	count_metric.SetUnit("{span}")
	count_metric_dp := createBreakdownMetricsCommon(count_metric, event, timestampNanos)
	count_metric_dp.SetDoubleValue(float64(event.GetSpan().GetSelfTime().Count))
}

func createBreakdownMetricsCommon(metric pmetric.Metric, event *modelpb.APMEvent, timestampNanos uint64) pmetric.NumberDataPoint {
	g := metric.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(timestampNanos))

	attr := dp.Attributes()
	if event.Transaction != nil {
		attr.PutStr("transaction.name", event.Transaction.Name)
		attr.PutStr("transaction.type", event.Transaction.Type)
	}
	if event.Span != nil {
		attr.PutStr("span.type", event.Span.Type)
		attr.PutStr("span.subtype", event.Span.Subtype)
	}

	attr.PutStr("processor.event", "metric")

	mappers.SetDerivedFieldsForMetrics(dp.Attributes())

	return dp
}

func (r *elasticAPMIntakeReceiver) elasticErrorToOtelLogRecord(rl *plog.ResourceLogs, event *modelpb.APMEvent, timestampNanos uint64) {
	sl := rl.ScopeLogs().AppendEmpty()
	l := sl.LogRecords().AppendEmpty()

	mappers.SetTopLevelFieldsLogRecord(event, timestampNanos, l, r.settings.Logger)
	mappers.SetDerivedFieldsForError(event, l.Attributes())
	mappers.TranslateIntakeV2LogToOTelAttributes(event, l.Attributes())

	// apm log events can contain error information. In this case the log is considered an apm error.
	// All fields associated with the log should also be set.
	mappers.SetElasticSpecificFieldsForLog(event, l.Attributes())

	if event.Error != nil && event.Error.Log != nil {
		l.Body().SetStr(event.Error.Log.Message)
	}
}

func (r *elasticAPMIntakeReceiver) elasticLogToOtelLogRecord(rl *plog.ResourceLogs, event *modelpb.APMEvent, timestampNanos uint64) {
	sl := rl.ScopeLogs().AppendEmpty()
	l := sl.LogRecords().AppendEmpty()

	mappers.SetTopLevelFieldsLogRecord(event, timestampNanos, l, r.settings.Logger)
	mappers.SetDerivedFieldsForLog(event, l.Attributes())
	mappers.TranslateIntakeV2LogToOTelAttributes(event, l.Attributes())
	mappers.SetElasticSpecificFieldsForLog(event, l.Attributes())

	l.Body().SetStr(event.Message)

	if event.Log != nil {
		l.SetSeverityText(event.Log.Level)
	}
	if event.Event != nil {
		l.SetSeverityNumber(plog.SeverityNumber(event.Event.Severity))
	}
}

func (r *elasticAPMIntakeReceiver) elasticEventToOtelSpan(rs *ptrace.ResourceSpans, event *modelpb.APMEvent, timestampNanos uint64) ptrace.Span {
	ss := rs.ScopeSpans().AppendEmpty()
	s := ss.Spans().AppendEmpty()

	mappers.SetTopLevelFieldsSpan(event, timestampNanos, s, r.settings.Logger)
	mappers.SetDerivedFieldsCommon(event, s.Attributes())
	r.elasticSpanLinksToOTelSpanLinks(event, s)
	s.SetKind(mapSpanKind(event.GetSpan().GetKind()))
	return s
}

func (r *elasticAPMIntakeReceiver) elasticSpanLinksToOTelSpanLinks(event *modelpb.APMEvent, s ptrace.Span) {
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

func (r *elasticAPMIntakeReceiver) elasticTransactionToOtelSpan(s *ptrace.Span, event *modelpb.APMEvent) {
	transaction := event.GetTransaction()
	s.SetName(transaction.GetName())

	mappers.SetDerivedFieldsForTransaction(event, s.Attributes())
	mappers.TranslateIntakeV2TransactionToOTelAttributes(event, s.Attributes())
	mappers.SetElasticSpecificFieldsForTransaction(event, s.Attributes())
}

func (r *elasticAPMIntakeReceiver) elasticSpanToOTelSpan(s *ptrace.Span, event *modelpb.APMEvent) {
	span := event.GetSpan()
	s.SetName(span.GetName())

	mappers.SetDerivedFieldsForSpan(event, s.Attributes())
	mappers.TranslateIntakeV2SpanToOTelAttributes(event, s.Attributes())
	mappers.SetElasticSpecificFieldsForSpan(event, s.Attributes())
}

func mapSpanKind(kind string) ptrace.SpanKind {
	switch strings.ToUpper(kind) {
	case "INTERNAL":
		return ptrace.SpanKindInternal
	case "CLIENT":
		return ptrace.SpanKindClient
	case "PRODUCER":
		return ptrace.SpanKindProducer
	case "CONSUMER":
		return ptrace.SpanKindConsumer
	case "SERVER":
		return ptrace.SpanKindServer
	default:
		return ptrace.SpanKindUnspecified
	}
}

func withECSMappingMode(ctx context.Context, includeMetadata bool) context.Context {
	return client.NewContext(ctx, withMappingMode(client.FromContext(ctx), "ecs", includeMetadata))
}

func withMappingMode(info client.Info, mode string, includeMetadata bool) client.Info {
	newMeta := make(map[string][]string)
	if includeMetadata {
		for k := range info.Metadata.Keys() {
			newMeta[k] = info.Metadata.Get(k)
		}
	}
	newMeta["x-elastic-mapping-mode"] = []string{mode}
	return client.Info{
		Addr:     info.Addr,
		Auth:     info.Auth,
		Metadata: client.NewMetadata(newMeta),
	}
}
