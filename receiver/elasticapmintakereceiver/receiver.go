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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	xxhashv2 "github.com/cespare/xxhash/v2"
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
	"github.com/elastic/opentelemetry-collector-components/internal/elasticattr"
	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/mappers"
	"github.com/elastic/opentelemetry-lib/agentcfg"
)

// TODO report different formats for intakev2 and rumv3?
const dataFormatElasticAPM = "elasticapm"

const (
	agentConfigPath    = "/config/v1/agents"
	intakeV2EventsPath = "/intake/v2/events"
	statusClientClosed = 499
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

	r.settings.Logger.Info("Starting HTTP server", zap.String("endpoint", r.cfg.NetAddr.Endpoint))

	var hln net.Listener
	if hln, err = r.cfg.ToListener(ctx); err != nil {
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
	batchProcessor := modelpb.ProcessBatchFunc(r.processBatch)
	elasticapmProcessor := elasticapm.NewProcessor(elasticapm.Config{
		Logger:       r.settings.Logger,
		MaxEventSize: r.cfg.MaxEventSize,
		Semaphore:    semaphore.NewWeighted(int64(r.cfg.MaxConcurrentDecoders)),
	})

	return func(w http.ResponseWriter, req *http.Request) {
		statusCode := http.StatusAccepted
		ctx := ctxFunc(req)
		// Ensure stream decoding unblocks quickly once request context is canceled.
		// In this net/http server handler, Request.Body.Close is safe to call
		// even if the server later closes it as part of normal request teardown:
		// the server body Close path is idempotent and concurrent Close unblocks Read.
		stopBodyClose := context.AfterFunc(ctx, func() {
			_ = req.Body.Close()
		})
		defer stopBodyClose()

		var elasticapmResult elasticapm.Result
		baseEvent := &modelpb.APMEvent{}
		baseEvent.Event = &modelpb.Event{}
		streamErr := elasticapmProcessor.HandleStream(
			ctx,
			baseEvent,
			req.Body,
			r.cfg.BatchSize,
			batchProcessor,
			&elasticapmResult,
		)
		// TODO record metrics about errors?

		var result struct {
			Accepted int      `json:"accepted"`
			Errors   []string `json:"errors,omitempty"`
		}
		result.Accepted = elasticapmResult.Accepted
		result.Errors = make([]string, 0, len(elasticapmResult.Errors)+2)
		processError := func(err error, isRequestContextErr bool) {
			result.Errors = append(result.Errors, err.Error())
			// Mirror MIS precedence: once we detect a client-canceled request
			// (499), keep that status to avoid masking it with downstream errors.
			if statusCode != statusClientClosed {
				if errStatusCode := intakeStatusCodeFromErr(err, isRequestContextErr); errStatusCode > statusCode {
					statusCode = errStatusCode
				}
			}
		}
		for _, err := range elasticapmResult.Errors {
			processError(err, false)
		}

		requestContextErr := ctx.Err()

		if streamErr != nil {
			r.settings.Logger.Error("failed to process APM events stream", zap.Error(streamErr))
			// If request context is done and streamErr is context.Canceled, treat
			// streamErr as a client canceled request context error (MIS behavior).
			processError(streamErr, requestContextErr != nil && errors.Is(streamErr, context.Canceled))
		}
		if requestContextErr != nil {
			processError(requestContextErr, true)
		}

		if statusCode >= http.StatusBadRequest {
			w.Header().Set("Connection", "close")
		}

		w.WriteHeader(statusCode)
		_ = json.NewEncoder(w).Encode(&result)
	}
}

func intakeStatusCodeFromErr(err error, isRequestContextErr bool) int {
	code := http.StatusInternalServerError

	var invalidInput *elasticapm.InvalidInputError
	if errors.As(err, &invalidInput) {
		code = http.StatusBadRequest
		if invalidInput.TooLarge {
			code = http.StatusRequestEntityTooLarge
		}
	}

	if isRequestContextErr && errors.Is(err, context.Canceled) {
		return statusClientClosed
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusServiceUnavailable
	}
	return code
}

func (r *elasticAPMIntakeReceiver) processBatch(ctx context.Context, batch *modelpb.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var ld *plog.Logs
	var md *pmetric.Metrics
	var td *ptrace.Traces
	var mainGroups signalGroups
	fpHasher := xxhashv2.New()

	processors := modelprocessor.Chained{
		modelprocessor.SetGroupingKey{
			NewHash: func() hash.Hash {
				return xxhash.New()
			},
		},
		modelprocessor.SetErrorMessage{},
	}

	if err := processors.ProcessBatch(ctx, batch); err != nil {
		r.settings.Logger.Error("failed to process batch", zap.Error(err))
	}

	// Collect label keys marked as Global across all events, assigning each
	// unique key a bit position. Global labels originate from the intake v2
	// metadata and are cloned onto every event by the apm-data library.
	//
	// When an event has a tag with the same key as a metadata label,
	// Labels.Set() replaces the value and resets Global to false on that
	// event only. To match apm-aggregation's per-event behavior
	// (marshalEventGlobalLabels), events that shadow a global key are
	// separated into shadowed batches grouped by their effective global key
	// set (represented as a bitmask). Each shadowed batch is consumed with
	// its own context so that the shadowed key is excluded from
	// "x-elastic-dynamic-resource-attributes" for those events.
	keyIndex := make(map[string]globalKeyInfo)
	var bitPos int
	for _, event := range *batch {
		for key, lv := range event.Labels {
			if lv != nil && lv.Global {
				if _, ok := keyIndex[key]; !ok {
					keyIndex[key] = globalKeyInfo{bitPos: bitPos, prefixedKey: "labels." + key}
					bitPos++
				}
			}
		}
		for key, nv := range event.NumericLabels {
			if nv != nil && nv.Global {
				if _, ok := keyIndex[key]; !ok {
					keyIndex[key] = globalKeyInfo{bitPos: bitPos, prefixedKey: "numeric_labels." + key}
					bitPos++
				}
			}
		}
	}

	// Fast path: no metadata-global keys were discovered in the batch.
	// Skip shadow-mask computation and shadowed-batch bookkeeping entirely.
	if len(keyIndex) == 0 {
		for _, event := range *batch {
			if err := r.appendEvent(event, &ld, &md, &td, &mainGroups, fpHasher); err != nil {
				return err
			}
		}
		return errors.Join(r.consumeOTel(ctx, ld, md, td)...)
	}

	var shadowedBatches []shadowedBatch
	if len(keyIndex) <= 64 {
		// Most real payloads carry a small number of metadata-global keys.
		// Use uint64 masks to avoid big.Int.String() allocations in grouping.
		var shadowIndex map[uint64]int // mask -> index, created lazily
		for _, event := range *batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			mask, shadowed := eventShadowMaskUint64(event, keyIndex)
			if !shadowed {
				if err := r.appendEvent(event, &ld, &md, &td, &mainGroups, fpHasher); err != nil {
					return err
				}
				continue
			}
			sb := getOrCreateShadowedBatchUint64(mask, &shadowIndex, &shadowedBatches)
			if err := r.appendEvent(event, &sb.ld, &sb.md, &sb.td, &sb.groups, fpHasher); err != nil {
				return err
			}
		}
	} else {
		// Fallback for unusually large key sets.
		var shadowIndex map[string]int // mask.String() -> index, created lazily
		for _, event := range *batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			mask, shadowed := eventShadowMaskBigInt(event, keyIndex)
			if !shadowed {
				if err := r.appendEvent(event, &ld, &md, &td, &mainGroups, fpHasher); err != nil {
					return err
				}
				continue
			}
			sb := getOrCreateShadowedBatchBigInt(mask, &shadowIndex, &shadowedBatches)
			if err := r.appendEvent(event, &sb.ld, &sb.md, &sb.td, &sb.groups, fpHasher); err != nil {
				return err
			}
		}
	}

	// Consume the main batch with the full set of global keys.
	mainCtx := ctx
	if len(keyIndex) > 0 {
		mainCtx = withDynamicResourceAttributes(ctx, resolveGlobalKeysBigInt(nil, keyIndex))
	}

	var errs []error
	errs = append(errs, r.consumeOTel(mainCtx, ld, md, td)...)

	// Consume shadowed batches, each with its own global key set.
	for _, sb := range shadowedBatches {
		if err := ctx.Err(); err != nil {
			return err
		}
		var dynamicAttrs []string
		if sb.useSmallMask {
			dynamicAttrs = resolveGlobalKeysUint64(sb.globalKeyMask64, keyIndex)
		} else {
			dynamicAttrs = resolveGlobalKeysBigInt(&sb.globalKeyMask, keyIndex)
		}
		sbCtx := withDynamicResourceAttributes(ctx, dynamicAttrs)
		errs = append(errs, r.consumeOTel(sbCtx, sb.ld, sb.md, sb.td)...)
	}
	return errors.Join(errs...)
}

func getOrCreateShadowedBatchUint64(
	mask uint64,
	shadowIndex *map[uint64]int,
	shadowedBatches *[]shadowedBatch,
) *shadowedBatch {
	if *shadowIndex == nil {
		*shadowIndex = make(map[uint64]int)
	}
	if idx, ok := (*shadowIndex)[mask]; ok {
		return &(*shadowedBatches)[idx]
	}

	(*shadowIndex)[mask] = len(*shadowedBatches)
	*shadowedBatches = append(*shadowedBatches, shadowedBatch{
		useSmallMask:    true,
		globalKeyMask64: mask,
	})
	return &(*shadowedBatches)[len(*shadowedBatches)-1]
}

func getOrCreateShadowedBatchBigInt(
	mask big.Int,
	shadowIndex *map[string]int,
	shadowedBatches *[]shadowedBatch,
) *shadowedBatch {
	maskKey := mask.String()
	if *shadowIndex == nil {
		*shadowIndex = make(map[string]int)
	}
	if idx, ok := (*shadowIndex)[maskKey]; ok {
		return &(*shadowedBatches)[idx]
	}

	(*shadowIndex)[maskKey] = len(*shadowedBatches)
	*shadowedBatches = append(*shadowedBatches, shadowedBatch{globalKeyMask: mask})
	return &(*shadowedBatches)[len(*shadowedBatches)-1]
}

// appendEvent converts an APM event to its OTel representation and appends
// it to the appropriate pdata structure. The pdata pointers are lazily
// initialized on first use. groups caches per-resource ScopeSpans / ScopeLogs
// across events in the same batch so events sharing a resource fingerprint
// share a single ResourceSpans / ResourceLogs entry. Metrics events are not
// grouped — see the comment on signalGroups for the rationale.
func (r *elasticAPMIntakeReceiver) appendEvent(
	event *modelpb.APMEvent,
	ld **plog.Logs,
	md **pmetric.Metrics,
	td **ptrace.Traces,
	groups *signalGroups,
	fpHasher *xxhashv2.Digest,
) error {
	timestampNanos := event.GetTimestamp()

	// TODO record metrics about events processed by type?
	switch event.Type() {
	case modelpb.MetricEventType:
		if *md == nil {
			m := pmetric.NewMetrics()
			*md = &m
		}
		rm := (*md).ResourceMetrics().AppendEmpty()

		r.setResourceAttributes(rm.Resource().Attributes(), event)

		if err := r.elasticMetricsToOtelMetrics(&rm, event, timestampNanos); err != nil {
			return err
		}
	case modelpb.ErrorEventType:
		sl := r.getOrCreateLogScope(event, ld, groups, fpHasher)
		r.elasticErrorToOtelLogRecord(sl, event, timestampNanos)
	case modelpb.LogEventType:
		sl := r.getOrCreateLogScope(event, ld, groups, fpHasher)
		r.elasticLogToOtelLogRecord(sl, event, timestampNanos)
	case modelpb.SpanEventType, modelpb.TransactionEventType:
		ss := r.getOrCreateTraceScope(event, td, groups, fpHasher)
		s := r.elasticEventToOtelSpan(ss, event, timestampNanos)

		if event.Type() == modelpb.TransactionEventType {
			r.elasticTransactionToOtelSpan(&s, event)
			r.appendDroppedSpansStatsSpans(ss, &s, event.GetTransaction().GetDroppedSpansStats())
		} else {
			r.elasticSpanToOTelSpan(&s, event)
		}
	default:
		return fmt.Errorf("unhandled event type %q", event.Type())
	}
	return nil
}

// getOrCreateTraceScope returns the ScopeSpans for the event's resource,
// creating a new ResourceSpans + ScopeSpans (and populating the resource
// attributes) on a cache miss.
func (r *elasticAPMIntakeReceiver) getOrCreateTraceScope(
	event *modelpb.APMEvent,
	td **ptrace.Traces,
	groups *signalGroups,
	fpHasher *xxhashv2.Digest,
) ptrace.ScopeSpans {
	fp := resourceFingerprint(event, fpHasher)
	if ss, ok := groups.traceScope(fp); ok {
		return ss
	}
	if *td == nil {
		tr := ptrace.NewTraces()
		*td = &tr
	}
	rs := (*td).ResourceSpans().AppendEmpty()
	r.setResourceAttributes(rs.Resource().Attributes(), event)
	ss := rs.ScopeSpans().AppendEmpty()
	groups.recordTraceScope(fp, ss)
	return ss
}

// getOrCreateLogScope is the log counterpart of getOrCreateTraceScope.
func (r *elasticAPMIntakeReceiver) getOrCreateLogScope(
	event *modelpb.APMEvent,
	ld **plog.Logs,
	groups *signalGroups,
	fpHasher *xxhashv2.Digest,
) plog.ScopeLogs {
	fp := resourceFingerprint(event, fpHasher)
	if sl, ok := groups.logScope(fp); ok {
		return sl
	}
	if *ld == nil {
		l := plog.NewLogs()
		*ld = &l
	}
	rl := (*ld).ResourceLogs().AppendEmpty()
	r.setResourceAttributes(rl.Resource().Attributes(), event)
	sl := rl.ScopeLogs().AppendEmpty()
	groups.recordLogScope(fp, sl)
	return sl
}

// consumeOTel sends the populated pdata structures to downstream consumers.
// Nil pointers are skipped.
func (r *elasticAPMIntakeReceiver) consumeOTel(ctx context.Context, ld *plog.Logs, md *pmetric.Metrics, td *ptrace.Traces) []error {
	var consumeFns []func() error
	if ld != nil {
		if numRecords := ld.LogRecordCount(); numRecords != 0 && r.nextLogs != nil {
			consumeFns = append(consumeFns, func() error {
				obsCtx := r.obsreport.StartLogsOp(ctx)
				err := r.nextLogs.ConsumeLogs(obsCtx, *ld)
				r.obsreport.EndLogsOp(obsCtx, dataFormatElasticAPM, numRecords, err)
				return err
			})
		}
	}
	if md != nil {
		if numDataPoints := md.DataPointCount(); numDataPoints != 0 && r.nextMetrics != nil {
			consumeFns = append(consumeFns, func() error {
				obsCtx := r.obsreport.StartMetricsOp(ctx)
				err := r.nextMetrics.ConsumeMetrics(obsCtx, *md)
				r.obsreport.EndMetricsOp(obsCtx, dataFormatElasticAPM, numDataPoints, err)
				return err
			})
		}
	}
	if td != nil {
		if numSpans := td.SpanCount(); numSpans != 0 && r.nextTraces != nil {
			consumeFns = append(consumeFns, func() error {
				obsCtx := r.obsreport.StartTracesOp(ctx)
				err := r.nextTraces.ConsumeTraces(obsCtx, *td)
				r.obsreport.EndTracesOp(obsCtx, dataFormatElasticAPM, numSpans, err)
				return err
			})
		}
	}

	if len(consumeFns) == 0 {
		return nil
	}

	errs := make([]error, len(consumeFns))
	if len(consumeFns) == 1 {
		errs[0] = consumeFns[0]()
		return errs
	}

	var wg sync.WaitGroup
	wg.Add(len(consumeFns))
	for i, consume := range consumeFns {
		go func(i int, consume func() error) {
			defer wg.Done()
			errs[i] = consume()
		}(i, consume)
	}
	wg.Wait()
	return errs
}

// globalKeyInfo stores a global label key's bit position and its
// prefixed name for use in x-elastic-dynamic-resource-attributes.
type globalKeyInfo struct {
	bitPos      int
	prefixedKey string
}

// setResourceAttributes maps event fields to attributes.
// Expects the attribute map to be at the resource level e.g. pmetric.ResourceMetrics.Resource().Attributes().
//
// The set of fields written here is defined by mappers.WalkResourceAttributes;
// resourceFingerprint walks the same tree to derive a stable hash, so any
// field added to the walker is automatically reflected in both consumers.
func (r *elasticAPMIntakeReceiver) setResourceAttributes(attrs pcommon.Map, event *modelpb.APMEvent) {
	mappers.WalkResourceAttributes(event, pcommonResourceVisitor{m: attrs})
}

// pcommonResourceVisitor adapts mappers.ResourceAttrVisitor onto a
// pcommon.Map. The wrapper has no state besides the map; method calls
// inline trivially under the optimizer.
type pcommonResourceVisitor struct {
	m pcommon.Map
}

func (v pcommonResourceVisitor) PutStr(key, value string) {
	v.m.PutStr(key, value)
}

func (v pcommonResourceVisitor) PutInt(key string, value int64) {
	v.m.PutInt(key, value)
}

func (v pcommonResourceVisitor) PutBool(key string, value bool) {
	v.m.PutBool(key, value)
}

func (v pcommonResourceVisitor) PutDouble(key string, value float64) {
	v.m.PutDouble(key, value)
}

func (v pcommonResourceVisitor) PutStrSlice(key string, values []string) {
	slice := v.m.PutEmptySlice(key)
	slice.EnsureCapacity(len(values))
	for _, s := range values {
		slice.AppendEmpty().SetStr(s)
	}
}

func (r *elasticAPMIntakeReceiver) elasticMetricsToOtelMetrics(rm *pmetric.ResourceMetrics, event *modelpb.APMEvent, timestampNanos uint64) error {
	metricset := event.GetMetricset()

	// the apm-data library defaults this value to `app` and sets to `span_breakdown` internal span metrics.
	rm.Resource().Attributes().PutStr(elasticattr.MetricsetName, metricset.Name)

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
	mappers.SetDerivedFieldsForMetrics(dp.Attributes())
}

// populateOTelHistogramDataPoint updates the OpenTelemetry HistogramDataPoint with data from the provided Elastic APM histogram sample.
// Assumptions:
// - the histogram values and counts are all non-negative
//
// Sets fields: sum, count, bucket_counts, explicit_bounds, mapping hints.
// All other optional fields are not set per OTEL metric model:
//   - https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
//
// The intake-v2 histogram format has a 1:1 mapping between values and counts
// (len(values) == len(counts)), where each value is a representative value for
// its bucket. To preserve these values through the OTel pipeline without
// midpoint approximation, we:
//  1. Set all values as explicit_bounds and append a zero-count overflow bucket
//     to satisfy the OTel requirement of len(bucket_counts) == len(explicit_bounds) + 1.
//  2. Add the "histogram:raw" mapping hint to signal the Elasticsearch exporter
//     to use the explicit bounds directly as representative values.
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
	// the number of elements in explicit_bounds array.
	//
	// Append a zero-count overflow bucket to satisfy the OTel invariant
	// len(bucket_counts) == len(explicit_bounds) + 1 while preserving all
	// original values as explicit bounds.
	bucketCounts := dp.BucketCounts()
	bucketCounts.FromRaw(append(apmHistogramCounts, 0))

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
	// All intake-v2 values are set as explicit bounds to preserve the original
	// representative values. The zero-count overflow bucket added above ensures
	// no data is attributed to the unbounded (+infinity) range.
	explicitBounds := dp.ExplicitBounds()
	explicitBounds.FromRaw(apmHistogramValues)

	// Add the "histogram:raw" mapping hint to signal the Elasticsearch exporter
	// to use explicit bounds directly as representative values without midpoint
	// approximation.
	hints := dp.Attributes().PutEmptySlice("elasticsearch.mapping.hints")
	hints.AppendEmpty().SetStr("histogram:raw")
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
	// SelfTime.Sum is in nanoseconds. Convert to microseconds to match the metric name (.us)
	// and apm data logic:
	// https://github.com/elastic/apm-data/blob/v1.19.5/model/modeljson/internal/metricset.go#L115
	sum_dp.SetIntValue(int64(event.GetSpan().GetSelfTime().Sum) / 1000)

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
		attr.PutStr(elasticattr.TransactionName, event.Transaction.Name)
		attr.PutStr(elasticattr.TransactionType, event.Transaction.Type)
	}
	if event.Span != nil {
		attr.PutStr(elasticattr.SpanType, event.Span.Type)
		attr.PutStr(elasticattr.SpanSubtype, event.Span.Subtype)
	}

	attr.PutStr(elasticattr.ProcessorEvent, "metric")

	mappers.SetDerivedFieldsForMetrics(dp.Attributes())

	return dp
}

func (r *elasticAPMIntakeReceiver) elasticErrorToOtelLogRecord(sl plog.ScopeLogs, event *modelpb.APMEvent, timestampNanos uint64) {
	l := sl.LogRecords().AppendEmpty()

	mappers.SetTopLevelFieldsLogRecord(event, timestampNanos, l, r.settings.Logger)
	mappers.SetDerivedFieldsForError(event, l.Attributes())
	mappers.TranslateIntakeV2LogToOTelAttributes(event, l.Attributes())

	// apm log events can contain error information. In this case the log is considered an apm error.
	// All fields associated with the log should also be set.
	mappers.SetElasticSpecificFieldsForLog(event, l.Attributes())

	// the modelprocessor.SetErrorMessage sets the correct event.Message based on the available error details
	l.Body().SetStr(event.Message)

	r.setLogSeverity(event, l)
}

func (r *elasticAPMIntakeReceiver) setLogSeverity(event *modelpb.APMEvent, l plog.LogRecord) {
	if event.Log != nil {
		l.SetSeverityText(event.Log.Level)
	}
	if event.Event != nil {
		l.SetSeverityNumber(plog.SeverityNumber(event.Event.Severity))
	}
}

func (r *elasticAPMIntakeReceiver) elasticLogToOtelLogRecord(sl plog.ScopeLogs, event *modelpb.APMEvent, timestampNanos uint64) {
	l := sl.LogRecords().AppendEmpty()

	mappers.SetTopLevelFieldsLogRecord(event, timestampNanos, l, r.settings.Logger)
	mappers.SetDerivedFieldsForLog(event, l.Attributes())
	mappers.TranslateIntakeV2LogToOTelAttributes(event, l.Attributes())
	mappers.SetElasticSpecificFieldsForLog(event, l.Attributes())

	l.Body().SetStr(event.Message)

	r.setLogSeverity(event, l)
}

func (r *elasticAPMIntakeReceiver) elasticEventToOtelSpan(ss ptrace.ScopeSpans, event *modelpb.APMEvent, timestampNanos uint64) ptrace.Span {
	s := ss.Spans().AppendEmpty()

	mappers.SetTopLevelFieldsSpan(event, timestampNanos, s, r.settings.Logger)
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

// appendDroppedSpansStatsSpans expands a transaction's dropped_spans_stats
// (DSS) list into synthetic OTel spans for the elasticapmconnector.
//
// Lifecycle of a DSS entry:
//
//  1. The APM agent reports transaction.dropped_spans_stats on a transaction
//     event. Each entry is a (destination_service_resource,
//     service_target.type, service_target.name, outcome) tuple paired with
//     the pre-aggregated count and total duration of outbound calls the
//     agent did not transmit as individual spans — either dropped against
//     the per-transaction span limit, or composited (sibling spans to the
//     same destination compressed into a parent composite span).
//
//  2. apm-data decodes the entries into modelpb.DroppedSpanStats on the
//     decoded transaction event. SetElasticSpecificFieldsForTransaction in
//     internal/mappers does not write these to the transaction span — they
//     are consumed only by this function.
//
//  3. This function fires once per transaction event during intake. For
//     each non-nil DSS entry it appends one CLIENT span under the
//     transaction's ScopeSpans, with:
//     - TraceID, ParentSpanID, TraceState inherited from the
//     transaction. TraceState inheritance keeps AdjustedCount() in
//     the elasticapmconnector aligned with the parent's sampling
//     weight.
//     - SpanID = xxhash(parent_span_id || stat_index): deterministic,
//     unique within the trace, reproducible across replays.
//     - SpanKind = CLIENT; elasticapmconnector's service_destination
//     rule keys on CLIENT/PRODUCER spans.
//     - StartTimestamp == EndTimestamp == parent.StartTimestamp. The
//     duration is carried in span.composite.sum.us instead, so the
//     wall-clock duration must be zero — the connector's
//     non-composite fallback would otherwise double-count.
//     - elasticsearch.mapping.hints = [_noindex]. elasticsearchexporter
//     drops the span at write time; nothing appears in indexed traces.
//     - span.name = "" (present but empty). The connector's
//     service_destination rule lists span.name as a dimension, so the
//     attribute must exist for OTTL MatchAttributes to fire. The
//     empty value mirrors MIS apm-aggregation's setDroppedSpanStatsKey,
//     which has no span-name dimension, and rolls all DSS rows for a
//     given (destination, target, outcome) tuple into one
//     span.name="" bucket.
//     - destination.service.resource, service.target.{type,name},
//     event.outcome are copied from the DSS entry.
//     - span.composite.sum.us (ns→µs) and span.composite.count carry
//     the pre-aggregated duration sum and call count.
//
//  4. elasticapmconnector's service_destination rule sees the synthetic
//     spans alongside real outbound spans, detects span.composite.sum.us
//     != nil, and emits
//     response_time.sum.us = composite.sum.us * AdjustedCount()
//     response_time.count  = composite.count  * AdjustedCount()
//     reproducing apm-aggregation's setSpanMetrics behaviour.
//
//  5. After connector aggregation the spans flow on through the pipeline.
//     elasticsearchexporter honours the _noindex hint and drops them, so
//     the synthetic spans contribute only to derived metrics.
func (r *elasticAPMIntakeReceiver) appendDroppedSpansStatsSpans(
	ss ptrace.ScopeSpans,
	parent *ptrace.Span,
	stats []*modelpb.DroppedSpanStats,
) {
	if len(stats) == 0 {
		return
	}
	parentSpanID := parent.SpanID()
	for i, stat := range stats {
		if stat == nil {
			continue
		}
		// Derive a deterministic synthetic span ID from the parent span ID and
		// the stat index. Uniqueness within the trace is guaranteed; the spans
		// carry _noindex so they never collide downstream of the exporter.
		d := xxhash.New()
		_, _ = d.Write(parentSpanID[:])
		var idxBuf [4]byte
		binary.BigEndian.PutUint32(idxBuf[:], uint32(i))
		_, _ = d.Write(idxBuf[:])
		var spanID pcommon.SpanID
		binary.BigEndian.PutUint64(spanID[:], d.Sum64())

		s := ss.Spans().AppendEmpty()
		// TraceID: kept for OTLP validity. elasticapmconnector's
		// service_destination rule does not key on it, and elasticsearchexporter
		// skips this span via _noindex, but zero TraceID may trigger warnings or
		// drops in any pipeline processor that enforces OTLP correctness.
		s.SetTraceID(parent.TraceID())
		// SpanID: required by OTLP and required to be unique within the trace
		// to avoid downstream dedup. Derived deterministically above from
		// xxhash(parent_span_id || stat_index) so replays of the same payload
		// produce the same IDs.
		s.SetSpanID(spanID)
		// Start/EndTimestamp are equal and pinned to parent.StartTimestamp.
		// elasticapmconnector buckets data points by span timestamp into the
		// configured aggregation interval (1m / 10m / 60m); pinning to the
		// parent puts the derived metric in the same time bucket as the
		// originating transaction. The wall-clock duration is zero by design
		// — the actual aggregated duration is carried in span.composite.sum.us
		// below.
		s.SetStartTimestamp(parent.StartTimestamp())
		s.SetEndTimestamp(parent.StartTimestamp())
		// TraceState inherited from the parent transaction so AdjustedCount()
		// in elasticapmconnector applies the parent's sampling weight to the
		// derived metric series. The transaction-event path in
		// SetTopLevelFieldsSpan currently does NOT populate TraceState from
		// Transaction.RepresentativeCount (see follow-up task), so today this
		// inheritance is a no-op; once that fix lands the DSS path needs no
		// further change.
		s.TraceState().FromRaw(parent.TraceState().AsRaw())

		attrs := s.Attributes()
		// elasticsearch.mapping.hints = [_noindex]: tells elasticsearchexporter
		// to drop this span at write time so it never reaches the indexed
		// traces store. The span exists purely to feed the metrics pipeline.
		attrs.PutEmptySlice("elasticsearch.mapping.hints").AppendEmpty().SetStr("_noindex")
		// span.name = "" (present but empty): the connector's
		// service_destination rule lists span.name as a required dimension via
		// OTTL MatchAttributes, so the attribute must exist for the rule to
		// fire. The empty value matches apm-aggregation's setDroppedSpanStatsKey
		// which leaves SpanName as the zero value, so all DSS-derived rows for
		// a given (destination, target, outcome) tuple roll into one
		// span.name="" bucket on the metrics side.
		attrs.PutStr(elasticattr.SpanName, "")

		// destination/target/outcome dimensions are lifted verbatim from the
		// DSS entry; they are the exact aggregation key fields
		// apm-aggregation's setDroppedSpanStatsKey uses, and the same keys the
		// connector's service_destination rule groups by. Conditional writes
		// skip empty values; the connector treats absent and "" as the same
		// bucket so this is functionally equivalent to apm-aggregation's
		// always-write behaviour while saving a few KeyValue slots per span.
		if stat.DestinationServiceResource != "" {
			attrs.PutStr(elasticattr.SpanDestinationServiceResource, stat.DestinationServiceResource)
		}
		if stat.ServiceTargetType != "" {
			attrs.PutStr(elasticattr.ServiceTargetType, stat.ServiceTargetType)
		}
		if stat.ServiceTargetName != "" {
			attrs.PutStr(elasticattr.ServiceTargetName, stat.ServiceTargetName)
		}
		if stat.Outcome != "" {
			attrs.PutStr(elasticattr.EventOutcome, stat.Outcome)
		}
		// span.composite.sum.us and span.composite.count are always written
		// (both set to 0 when Duration is nil) so the connector always takes
		// the composite branch of its service_destination rule and emits the
		// zero-data bucket apm-aggregation would produce for the same entry
		// via setDroppedSpanStatsMetrics. Without these the connector's
		// non-composite fallback would emit count=AdjustedCount() rather than 0.
		// Duration.Sum is stored in nanoseconds; the attribute is consumed in
		// microseconds.
		var sumUs, count int64
		if stat.Duration != nil {
			sumUs = time.Duration(stat.Duration.Sum).Microseconds()
			count = int64(stat.Duration.Count)
		}
		attrs.PutInt(elasticattr.SpanCompositeSumUs, sumUs)
		attrs.PutInt(elasticattr.SpanCompositeCount, count)
	}
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

// withDynamicResourceAttributes enriches the context with the global label
// keys under the "x-elastic-dynamic-resource-attributes" metadata key.
// Each key is stored as a separate element in the metadata value slice so
// that downstream OTTL expressions (e.g. otelcol.client.metadata["..."]) can
// consume them directly as a string list.
// The provided globalLabelKeys must be deduplicated by the caller.
func withDynamicResourceAttributes(ctx context.Context, globalLabelKeys []string) context.Context {
	info := client.FromContext(ctx)
	newMeta := make(map[string][]string)
	for k := range info.Metadata.Keys() {
		newMeta[k] = info.Metadata.Get(k)
	}
	newMeta[elasticattr.MetadataDynamicResourceAttributes] = globalLabelKeys
	return client.NewContext(ctx, client.Info{
		Addr:     info.Addr,
		Auth:     info.Auth,
		Metadata: client.NewMetadata(newMeta),
	})
}

// shadowedBatch holds events that shadow at least one global label key
// and share the same effective global key set (represented as a bitmask).
// The pdata fields are lazily initialized by appendEvent on first use.
// groups is the per-batch resource-grouping cache (see signalGroups).
type shadowedBatch struct {
	useSmallMask    bool
	globalKeyMask64 uint64
	globalKeyMask   big.Int
	ld              *plog.Logs
	md              *pmetric.Metrics
	td              *ptrace.Traces
	groups          signalGroups
}

// eventShadowMaskBigInt checks whether the event shadows any global label key
// and returns a bitmask of the global keys that are still Global: true
// on this event along with a bool indicating whether shadowing was
// detected. When shadowed is false, the mask has all bits set (i.e. all
// global keys are retained) and can be ignored — the bool avoids the
// need to compare the mask against a full mask for this common case.
// Events with identical masks need the same set of keys in their
// x-elastic-dynamic-resource-attributes context and can share a batch.
func eventShadowMaskBigInt(event *modelpb.APMEvent, keyIndex map[string]globalKeyInfo) (big.Int, bool) {
	var mask big.Int
	var shadowed bool
	forEachShadowBit(event, keyIndex, func(bitPos int, global bool) {
		if global {
			mask.SetBit(&mask, bitPos, 1)
		} else {
			shadowed = true
		}
	})
	return mask, shadowed
}

// eventShadowMaskUint64 is the compact-mask variant of eventShadowMaskBigInt used when
// len(keyIndex) <= 64. It avoids big.Int and mask.String allocations.
func eventShadowMaskUint64(event *modelpb.APMEvent, keyIndex map[string]globalKeyInfo) (uint64, bool) {
	var mask uint64
	var shadowed bool
	forEachShadowBit(event, keyIndex, func(bitPos int, global bool) {
		if global {
			mask |= 1 << uint(bitPos)
		} else {
			shadowed = true
		}
	})
	return mask, shadowed
}

func forEachShadowBit(event *modelpb.APMEvent, keyIndex map[string]globalKeyInfo, visit func(bitPos int, global bool)) {
	for key, lv := range event.Labels {
		if lv == nil {
			continue
		}
		info, ok := keyIndex[key]
		if !ok {
			continue
		}
		visit(info.bitPos, lv.Global)
	}
	for key, nv := range event.NumericLabels {
		if nv == nil {
			continue
		}
		info, ok := keyIndex[key]
		if !ok {
			continue
		}
		visit(info.bitPos, nv.Global)
	}
}

// resolveGlobalKeysBigInt converts a bitmask back to prefixed global label key
// names. If mask is nil, all keys in keyIndex are included.
func resolveGlobalKeysBigInt(mask *big.Int, keyIndex map[string]globalKeyInfo) []string {
	keys := make([]string, 0, len(keyIndex))
	for _, info := range keyIndex {
		if mask != nil && mask.Bit(info.bitPos) == 0 {
			continue
		}
		keys = append(keys, info.prefixedKey)
	}
	return keys
}

func resolveGlobalKeysUint64(mask uint64, keyIndex map[string]globalKeyInfo) []string {
	keys := make([]string, 0, len(keyIndex))
	for _, info := range keyIndex {
		if mask&(1<<uint(info.bitPos)) == 0 {
			continue
		}
		keys = append(keys, info.prefixedKey)
	}
	return keys
}
