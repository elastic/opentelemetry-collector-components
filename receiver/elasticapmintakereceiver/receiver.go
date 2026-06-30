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
	"net"
	"net/http"
	"sync"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/ndjsondecoder"
	"github.com/elastic/opentelemetry-lib/agentcfg"
)

// TODO report different formats for intakev2 and rumv3?
const dataFormatElasticAPM = "elasticapm"

const (
	agentConfigPath    = "/config/v1/agents"
	intakeV2EventsPath = "/intake/v2/events"
	rootPath           = "/"
	statusClientClosed = 499

	// fakeVersion is returned by the root handler to satisfy APM agent version
	// checks without exposing internal server details. Matches the value used
	// by MIS so agents see consistent behaviour during migration.
	fakeVersion = "8.9.0"
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

	httpMux.HandleFunc("GET /{$}", r.newRootHandler())
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

// newRootHandler returns an unauthenticated handler for GET /. It mirrors the
// MIS behaviour: return HTTP 200 with a JSON version payload so that APM
// agents can confirm they are talking to a compatible server.
func (r *elasticAPMIntakeReceiver) newRootHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(ContentType, "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"version": fakeVersion})
	}
}

func (r *elasticAPMIntakeReceiver) newElasticAPMEventsHandler(ctxFunc func(*http.Request) context.Context) http.HandlerFunc {
	sem := semaphore.NewWeighted(int64(r.cfg.MaxConcurrentDecoders))

	return func(w http.ResponseWriter, req *http.Request) {
		statusCode := http.StatusAccepted
		ctx := ctxFunc(req)
		stopBodyClose := context.AfterFunc(ctx, func() { _ = req.Body.Close() })
		defer stopBodyClose()

		var result struct {
			Accepted int      `json:"accepted"`
			Errors   []string `json:"errors,omitempty"`
		}
		processError := func(err error, isRequestContextErr bool) {
			result.Errors = append(result.Errors, err.Error())
			if statusCode != statusClientClosed {
				if errStatusCode := intakeStatusCodeFromErr(err, isRequestContextErr); errStatusCode > statusCode {
					statusCode = errStatusCode
				}
			}
		}

		if err := sem.Acquire(ctx, 1); err != nil {
			processError(err, true)
			if statusCode >= http.StatusBadRequest {
				w.Header().Set("Connection", "close")
			}
			w.WriteHeader(statusCode)
			_ = json.NewEncoder(w).Encode(&result)
			return
		}
		defer sem.Release(1)

		consumer := ndjsondecoder.BatchConsumer(func(ctx context.Context, ld *plog.Logs, md *pmetric.Metrics, td *ptrace.Traces) error {
			return errors.Join(r.consumeOTel(ctx, ld, md, td)...)
		})

		accepted, streamErrs := ndjsondecoder.HandleStream(ctx, req.Body, r.cfg.BatchSize, r.cfg.MaxEventSize, r.settings.Logger, consumer)

		result.Accepted = accepted
		result.Errors = make([]string, 0, len(streamErrs)+2)
		for _, err := range streamErrs {
			processError(err, false)
		}

		requestContextErr := ctx.Err()
		if errors.Is(requestContextErr, context.Canceled) {
			statusCode = statusClientClosed
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

// intakeStatusCodeFromErr maps a processing error to an HTTP status code.
// Context outcomes are evaluated after invalid-input detection so cancellation
// and deadline semantics can take precedence in the final mapping.
func intakeStatusCodeFromErr(err error, isRequestContextErr bool) int {
	code := http.StatusInternalServerError

	var jsonErr ndjsondecoder.JSONDecodeError
	var validErr ndjsondecoder.ValidationError
	if errors.As(err, &jsonErr) || errors.As(err, &validErr) {
		code = http.StatusBadRequest
	} else if errors.Is(err, ndjsondecoder.ErrLineTooLong) {
		code = http.StatusRequestEntityTooLarge
	}

	// Evaluate final context/grpc outcome here (instead of early-returning above)
	// so request cancellation/deadline can override a provisional invalid-input
	// status when both signals are present.
	switch contextCodeFromErr(err) {
	case codes.Canceled:
		if isRequestContextErr {
			return statusClientClosed
		}
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		return http.StatusServiceUnavailable
	}
	return code
}

func contextCodeFromErr(err error) codes.Code {
	// Handles context errors and wrapped context errors.
	if contextCode := grpcstatus.FromContextError(err).Code(); contextCode == codes.Canceled || contextCode == codes.DeadlineExceeded {
		return contextCode
	}
	// Handles canonical gRPC status errors.
	if grpcCode := grpcstatus.Code(err); grpcCode == codes.Canceled || grpcCode == codes.DeadlineExceeded {
		return grpcCode
	}
	return codes.OK
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
