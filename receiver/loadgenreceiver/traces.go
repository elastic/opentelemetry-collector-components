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

package loadgenreceiver // import "github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver"

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"errors"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver/internal/list"
)

//go:embed testdata/traces.jsonl
var demoTraces []byte

type tracesGenerator struct {
	cfg    *Config
	logger *zap.Logger

	samples *list.LoopingList[ptrace.Traces]

	stats   Stats
	statsMu sync.Mutex

	consumer consumer.Traces

	cancelFn            context.CancelFunc
	inflightConcurrency sync.WaitGroup
}

func createTracesReceiver(
	ctx context.Context,
	set receiver.Settings,
	config component.Config,
	consumer consumer.Traces,
) (receiver.Traces, error) {
	genConfig := config.(*Config)

	parser := ptrace.JSONUnmarshaler{}
	var err error
	sampleTraces := demoTraces

	if genConfig.Traces.JsonlFile != "" {
		sampleTraces, err = os.ReadFile(string(genConfig.Traces.JsonlFile))
		if err != nil {
			return nil, err
		}
	}

	maxBufferSize := genConfig.Traces.MaxBufferSize
	if maxBufferSize == 0 {
		maxBufferSize = len(sampleTraces) + 10 // add some margin
	}

	var items []ptrace.Traces
	scanner := bufio.NewScanner(bytes.NewReader(sampleTraces))
	scanner.Buffer(make([]byte, 0, maxBufferSize), maxBufferSize)
	for scanner.Scan() {
		traceBytes := scanner.Bytes()
		lineTraces, err := parser.UnmarshalTraces(traceBytes)
		if err != nil {
			return nil, err
		}
		items = append(items, lineTraces)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return &tracesGenerator{
		cfg:      genConfig,
		logger:   set.Logger,
		consumer: consumer,
		samples:  list.NewLoopingList(items, genConfig.Traces.MaxReplay),
	}, nil
}

func (ar *tracesGenerator) Start(ctx context.Context, _ component.Host) error {
	startCtx, cancelFn := context.WithCancel(ctx)
	ar.cancelFn = cancelFn

	for i := 0; i < ar.cfg.Concurrency; i++ {
		ar.inflightConcurrency.Add(1)
		go func() {
			defer ar.inflightConcurrency.Done()
			next := ptrace.NewTraces() // per-worker temporary container to avoid allocs
			for {
				select {
				case <-startCtx.Done():
					return
				default:
				}
				if ar.cfg.DisablePdataReuse || next.IsReadOnly() {
					// As the optimization to reuse pdata is not compatible with fanoutconsumer,
					// i.e. in pipelines where there are more than 1 consumer,
					// as fanoutconsumer will mark the pdata struct as read only and cannot be reused.
					// See https://github.com/open-telemetry/opentelemetry-collector/blob/461a3558086a03ab13ea121d12e28e185a1c79b0/internal/fanoutconsumer/logs.go#L70
					next = ptrace.NewTraces()
				}
				err := ar.nextTraces(next)
				if errors.Is(err, list.ErrLoopLimitReached) {
					return
				}
				// For graceful shutdown, use ctx instead of startCtx to shield Consume* from context canceled
				// In other words, Consume* will finish at its own pace, which may take indefinitely long.
				recordCount := next.SpanCount()
				if err := ar.consumer.ConsumeTraces(ctx, next); err != nil {
					ar.logger.Error(err.Error())
					ar.statsMu.Lock()
					ar.stats.FailedRequests++
					ar.stats.FailedSpans += recordCount
					ar.statsMu.Unlock()
				} else {
					ar.statsMu.Lock()
					ar.stats.Requests++
					ar.stats.Spans += recordCount
					ar.statsMu.Unlock()
				}
			}
		}()
	}
	go func() {
		ar.inflightConcurrency.Wait()
		if ar.cfg.Traces.doneCh != nil {
			ar.cfg.Traces.doneCh <- ar.stats
		}
	}()
	return nil
}

func (ar *tracesGenerator) Shutdown(context.Context) error {
	if ar.cancelFn != nil {
		ar.cancelFn()
	}
	ar.inflightConcurrency.Wait()
	return nil
}

func (ar *tracesGenerator) nextTraces(next ptrace.Traces) error {
	sample, err := ar.samples.Next()
	if err != nil {
		return err
	}
	sample.CopyTo(next)

	rm := next.ResourceSpans()
	for i := 0; i < rm.Len(); i++ {
		for j := 0; j < rm.At(i).ScopeSpans().Len(); j++ {
			for k := 0; k < rm.At(i).ScopeSpans().At(j).Spans().Len(); k++ {
				sspan := rm.At(i).ScopeSpans().At(j).Spans().At(k)
				now := time.Now()
				// Set end timestamp to now and maintain the same duration.
				duration := time.Duration(sspan.EndTimestamp() - sspan.StartTimestamp())
				sspan.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
				sspan.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-duration)))
			}
		}
	}

	return nil
}
