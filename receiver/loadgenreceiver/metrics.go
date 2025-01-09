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

package loadgenreceiver

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
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

//go:embed testdata/metrics.jsonl
var demoMetrics []byte

type receiverMetrics struct {
	metrics  pmetric.Metrics
	jsonSize int
}

type metricsGenerator struct {
	cfg    *Config
	logger *zap.Logger

	sampleMetrics   []receiverMetrics
	lastSampleIndex int
	consumer        consumer.Metrics

	cancelFn context.CancelFunc
}

func createMetricsReceiver(
	ctx context.Context,
	set receiver.Settings,
	config component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	genConfig := config.(*Config)
	recv := metricsGenerator{
		cfg:             genConfig,
		logger:          set.Logger,
		consumer:        consumer,
		sampleMetrics:   make([]receiverMetrics, 0),
		lastSampleIndex: 0,
	}

	parser := pmetric.JSONUnmarshaler{}
	var err error
	sampleMetrics := demoMetrics

	if genConfig.Metrics.JsonFile != "" {
		sampleMetrics, err = os.ReadFile(string(genConfig.Metrics.JsonFile))
		if err != nil {
			return nil, err
		}
	}

	scanner := bufio.NewScanner(bytes.NewReader(sampleMetrics))
	for scanner.Scan() {
		metricBytes := scanner.Bytes()
		lineMetrics, err := parser.UnmarshalMetrics(metricBytes)
		if err != nil {
			return nil, err
		}
		recv.sampleMetrics = append(recv.sampleMetrics, receiverMetrics{
			metrics:  lineMetrics,
			jsonSize: len(metricBytes),
		})
	}

	return &recv, nil
}

func (ar *metricsGenerator) Start(ctx context.Context, _ component.Host) error {
	startCtx, cancelFn := context.WithCancel(ctx)
	ar.cancelFn = cancelFn

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		var throughput, totalSeconds, totalSendBytes float64
		for {
			select {
			case <-startCtx.Done():
				return
			case <-ticker.C:
				totalSeconds += 1
				throughput = totalSendBytes / totalSeconds
				for throughput < float64(ar.cfg.Metrics.Throughput) {
					nMetrics, nSize, err := ar.nextMetrics()
					if err != nil {
						ar.logger.Error(err.Error())
						continue
					}
					err = ar.consumer.ConsumeMetrics(startCtx, nMetrics)
					if err != nil {
						ar.logger.Error(err.Error())
						continue
					}

					totalSendBytes += float64(nSize)
					throughput = totalSendBytes / totalSeconds
				}
				ar.logger.Info("Consumed metrics", zap.Float64("bytes", totalSendBytes))
			}
		}
	}()
	return nil
}

func (ar *metricsGenerator) Shutdown(context.Context) error {
	if ar.cancelFn != nil {
		ar.cancelFn()
	}
	return nil
}

func (ar *metricsGenerator) nextMetrics() (pmetric.Metrics, int, error) {
	now := pcommon.NewTimestampFromTime(time.Now())

	nextMetrics := pmetric.NewMetrics()

	ar.sampleMetrics[ar.lastSampleIndex].metrics.CopyTo(nextMetrics)
	sampledSize := ar.sampleMetrics[ar.lastSampleIndex].jsonSize

	rm := nextMetrics.ResourceMetrics()
	for i := 0; i < rm.Len(); i++ {
		for j := 0; j < rm.At(i).ScopeMetrics().Len(); j++ {
			for k := 0; k < rm.At(i).ScopeMetrics().At(j).Metrics().Len(); k++ {
				smetric := rm.At(i).ScopeMetrics().At(j).Metrics().At(k)
				switch smetric.Type() {
				case pmetric.MetricTypeGauge:
					dps := smetric.Gauge().DataPoints()
					for i := 0; i < dps.Len(); i++ {
						dps.At(i).SetTimestamp(now)
						dps.At(i).SetStartTimestamp(now)
					}
				case pmetric.MetricTypeSum:
					dps := smetric.Sum().DataPoints()
					for i := 0; i < dps.Len(); i++ {
						dps.At(i).SetTimestamp(now)
						dps.At(i).SetStartTimestamp(now)
					}
				case pmetric.MetricTypeHistogram:
					dps := smetric.Histogram().DataPoints()
					for i := 0; i < dps.Len(); i++ {
						dps.At(i).SetTimestamp(now)
						dps.At(i).SetStartTimestamp(now)
					}
				case pmetric.MetricTypeExponentialHistogram:
					dps := smetric.ExponentialHistogram().DataPoints()
					for i := 0; i < dps.Len(); i++ {
						dps.At(i).SetTimestamp(now)
						dps.At(i).SetStartTimestamp(now)
					}
				case pmetric.MetricTypeSummary:
					dps := smetric.Summary().DataPoints()
					for i := 0; i < dps.Len(); i++ {
						dps.At(i).SetTimestamp(now)
						dps.At(i).SetStartTimestamp(now)
					}
				case pmetric.MetricTypeEmpty:
				}
			}
		}
	}

	ar.lastSampleIndex = (ar.lastSampleIndex + 1) % len(ar.sampleMetrics)

	return nextMetrics, sampledSize, nil
}
