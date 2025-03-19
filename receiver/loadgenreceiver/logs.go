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
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver/internal/list"
)

//go:embed testdata/logs.jsonl
var demoLogs []byte

type logsGenerator struct {
	cfg    *Config
	logger *zap.Logger

	samples *list.LoopingList[plog.Logs]

	stats   Stats
	statsMu sync.Mutex

	consumer consumer.Logs

	cancelFn            context.CancelFunc
	inflightConcurrency sync.WaitGroup
}

func createLogsReceiver(
	ctx context.Context,
	set receiver.Settings,
	config component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	genConfig := config.(*Config)

	parser := plog.JSONUnmarshaler{}
	var err error
	sampleLogs := demoLogs

	if genConfig.Logs.JsonlFile != "" {
		sampleLogs, err = os.ReadFile(string(genConfig.Logs.JsonlFile))
		if err != nil {
			return nil, err
		}
	}

	var items []plog.Logs
	scanner := bufio.NewScanner(bytes.NewReader(sampleLogs))
	scanner.Buffer(make([]byte, 0, maxScannerBufSize), maxScannerBufSize)
	for scanner.Scan() {
		logBytes := scanner.Bytes()
		lineLogs, err := parser.UnmarshalLogs(logBytes)
		if err != nil {
			return nil, err
		}
		items = append(items, lineLogs)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return &logsGenerator{
		cfg:      genConfig,
		logger:   set.Logger,
		consumer: consumer,
		samples:  list.NewLoopingList(items, genConfig.Logs.MaxReplay),
	}, nil
}

func (ar *logsGenerator) Start(ctx context.Context, _ component.Host) error {
	startCtx, cancelFn := context.WithCancel(ctx)
	ar.cancelFn = cancelFn

	for i := 0; i < ar.cfg.Concurrency; i++ {
		ar.inflightConcurrency.Add(1)
		go func() {
			defer ar.inflightConcurrency.Done()
			next := plog.NewLogs() // per-worker temporary container to avoid allocs
			for {
				select {
				case <-startCtx.Done():
					return
				default:
				}
				if next.IsReadOnly() {
					// As the optimization to reuse pdata is not compatible with fanoutconsumer,
					// i.e. in pipelines where there are more than 1 consumer,
					// as fanoutconsumer will mark the pdata struct as read only and cannot be reused.
					// See https://github.com/open-telemetry/opentelemetry-collector/blob/461a3558086a03ab13ea121d12e28e185a1c79b0/internal/fanoutconsumer/logs.go#L70
					next = plog.NewLogs()
				}
				err := ar.nextLogs(next)
				if errors.Is(err, list.ErrLoopLimitReached) {
					return
				}
				// For graceful shutdown, use ctx instead of startCtx to shield Consume* from context canceled
				// In other words, Consume* will finish at its own pace, which may take indefinitely long.
				if err := ar.consumer.ConsumeLogs(ctx, next); err != nil {
					ar.logger.Error(err.Error())
					ar.statsMu.Lock()
					ar.stats.FailedRequests++
					ar.stats.FailedLogRecords += next.LogRecordCount()
					ar.statsMu.Unlock()
				} else {
					ar.statsMu.Lock()
					ar.stats.Requests++
					ar.stats.LogRecords += next.LogRecordCount()
					ar.statsMu.Unlock()
				}
			}
		}()
	}
	go func() {
		ar.inflightConcurrency.Wait()
		if ar.cfg.Logs.doneCh != nil {
			ar.cfg.Logs.doneCh <- ar.stats
		}
	}()
	return nil
}

func (ar *logsGenerator) Shutdown(context.Context) error {
	if ar.cancelFn != nil {
		ar.cancelFn()
	}
	ar.inflightConcurrency.Wait()
	return nil
}

func (ar *logsGenerator) nextLogs(next plog.Logs) error {
	now := pcommon.NewTimestampFromTime(time.Now())

	sample, err := ar.samples.Next()
	if err != nil {
		return err
	}
	sample.CopyTo(next)

	rm := next.ResourceLogs()
	for i := 0; i < rm.Len(); i++ {
		for j := 0; j < rm.At(i).ScopeLogs().Len(); j++ {
			for k := 0; k < rm.At(i).ScopeLogs().At(j).LogRecords().Len(); k++ {
				smetric := rm.At(i).ScopeLogs().At(j).LogRecords().At(k)
				smetric.SetTimestamp(now)
			}
		}
	}

	return nil
}
