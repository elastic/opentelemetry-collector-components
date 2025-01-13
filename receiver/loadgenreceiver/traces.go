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
	"os"
	"time"

	"github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver/internal"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

//go:embed testdata/traces.jsonl
var demoTraces []byte

type tracesGenerator struct {
	cfg    *Config
	logger *zap.Logger

	samples  internal.LoopingList[ptrace.Traces]
	consumer consumer.Traces

	cancelFn context.CancelFunc
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

	var samples []ptrace.Traces
	scanner := bufio.NewScanner(bytes.NewReader(sampleTraces))
	for scanner.Scan() {
		traceBytes := scanner.Bytes()
		lineTraces, err := parser.UnmarshalTraces(traceBytes)
		if err != nil {
			return nil, err
		}
		samples = append(samples, lineTraces)
	}

	return &tracesGenerator{
		cfg:      genConfig,
		logger:   set.Logger,
		consumer: consumer,
		samples:  internal.NewLoopingList(samples),
	}, nil
}

func (ar *tracesGenerator) Start(ctx context.Context, _ component.Host) error {
	startCtx, cancelFn := context.WithCancel(ctx)
	ar.cancelFn = cancelFn

	go func() {
		for {
			select {
			case <-startCtx.Done():
				return
			default:
				if err := ar.consumer.ConsumeTraces(startCtx, ar.nextTraces()); err != nil {
					ar.logger.Error(err.Error())
					continue
				}
			}
		}
	}()
	return nil
}

func (ar *tracesGenerator) Shutdown(context.Context) error {
	if ar.cancelFn != nil {
		ar.cancelFn()
	}
	return nil
}

func (ar *tracesGenerator) nextTraces() ptrace.Traces {
	nextLogs := ptrace.NewTraces()
	ar.samples.Next().CopyTo(nextLogs)

	rm := nextLogs.ResourceSpans()
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

	return nextLogs
}
