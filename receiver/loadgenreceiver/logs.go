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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

//go:embed testdata/logs.jsonl
var demoLogs []byte

type receiverLogs struct {
	logs     plog.Logs
	jsonSize int
}

type logsGenerator struct {
	cfg    *Config
	logger *zap.Logger

	sampleLogs      []receiverLogs
	lastSampleIndex int
	consumer        consumer.Logs

	cancelFn context.CancelFunc
}

func createLogsReceiver(
	ctx context.Context,
	set receiver.Settings,
	config component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	genConfig := config.(*Config)
	recv := logsGenerator{
		cfg:             genConfig,
		logger:          set.Logger,
		consumer:        consumer,
		sampleLogs:      make([]receiverLogs, 0),
		lastSampleIndex: 0,
	}

	parser := plog.JSONUnmarshaler{}
	var err error
	sampleLogs := demoLogs

	if genConfig.Logs.JsonlFile != "" {
		sampleLogs, err = os.ReadFile(string(genConfig.Logs.JsonlFile))
		if err != nil {
			return nil, err
		}
	}

	scanner := bufio.NewScanner(bytes.NewReader(sampleLogs))
	for scanner.Scan() {
		logBytes := scanner.Bytes()
		lineLogs, err := parser.UnmarshalLogs(logBytes)
		if err != nil {
			return nil, err
		}
		recv.sampleLogs = append(recv.sampleLogs, receiverLogs{
			logs:     lineLogs,
			jsonSize: len(logBytes),
		})
	}

	return &recv, nil
}

func (ar *logsGenerator) Start(ctx context.Context, _ component.Host) error {
	startCtx, cancelFn := context.WithCancel(ctx)
	ar.cancelFn = cancelFn

	go func() {
		for {
			select {
			case <-startCtx.Done():
				return
			default:
				nMetrics, _, err := ar.nextLogs()
				if err != nil {
					ar.logger.Error(err.Error())
					continue
				}
				err = ar.consumer.ConsumeLogs(startCtx, nMetrics)
				if err != nil {
					ar.logger.Error(err.Error())
					continue
				}
			}
		}
	}()
	return nil
}

func (ar *logsGenerator) Shutdown(context.Context) error {
	if ar.cancelFn != nil {
		ar.cancelFn()
	}
	return nil
}

func (ar *logsGenerator) nextLogs() (plog.Logs, int, error) {
	now := pcommon.NewTimestampFromTime(time.Now())

	nextLogs := plog.NewLogs()

	ar.sampleLogs[ar.lastSampleIndex].logs.CopyTo(nextLogs)
	sampledSize := ar.sampleLogs[ar.lastSampleIndex].jsonSize

	rm := nextLogs.ResourceLogs()
	for i := 0; i < rm.Len(); i++ {
		for j := 0; j < rm.At(i).ScopeLogs().Len(); j++ {
			for k := 0; k < rm.At(i).ScopeLogs().At(j).LogRecords().Len(); k++ {
				smetric := rm.At(i).ScopeLogs().At(j).LogRecords().At(k)
				smetric.SetTimestamp(now)
			}
		}
	}

	ar.lastSampleIndex = (ar.lastSampleIndex + 1) % len(ar.sampleLogs)

	return nextLogs, sampledSize, nil
}
