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
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver/internal"
)

//go:embed testdata/metrics.jsonl
var demoMetrics []byte

type metricsGenerator struct {
	cfg    *Config
	logger *zap.Logger

	samples internal.LoopingList[pmetric.Metrics]

	stats TelemetryStats

	consumer consumer.Metrics

	cancelFn context.CancelFunc
}

func createMetricsReceiver(
	ctx context.Context,
	set receiver.Settings,
	config component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	genConfig := config.(*Config)

	parser := pmetric.JSONUnmarshaler{}
	var err error
	sampleMetrics := demoMetrics

	if genConfig.Metrics.JsonlFile != "" {
		sampleMetrics, err = os.ReadFile(string(genConfig.Metrics.JsonlFile))
		if err != nil {
			return nil, err
		}
	}

	var samples []pmetric.Metrics
	scanner := bufio.NewScanner(bytes.NewReader(sampleMetrics))
	for scanner.Scan() {
		metricBytes := scanner.Bytes()
		lineMetrics, err := parser.UnmarshalMetrics(metricBytes)
		if err != nil {
			return nil, err
		}
		samples = append(samples, lineMetrics)
	}

	return &metricsGenerator{
		cfg:      genConfig,
		logger:   set.Logger,
		consumer: consumer,
		samples:  internal.NewLoopingList(samples),
	}, nil
}

func (ar *metricsGenerator) Start(ctx context.Context, _ component.Host) error {
	startCtx, cancelFn := context.WithCancel(ctx)
	ar.cancelFn = cancelFn

	go func() {
		for {
			select {
			case <-startCtx.Done():
				return
			default:
			}
			m := ar.nextMetrics()
			if err := ar.consumer.ConsumeMetrics(startCtx, m); err != nil {
				ar.logger.Error(err.Error())
				ar.stats.FailedRequests++
				ar.stats.FailedMetricDataPoints += m.DataPointCount()
			} else {
				ar.stats.Requests++
				ar.stats.MetricDataPoints += m.DataPointCount()
			}
			if ar.isDone() {
				if ar.cfg.Metrics.doneCh != nil {
					ar.cfg.Metrics.doneCh <- ar.stats
				}
				return
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

func (ar *metricsGenerator) nextMetrics() pmetric.Metrics {
	now := pcommon.NewTimestampFromTime(time.Now())

	nextMetrics := pmetric.NewMetrics()
	ar.samples.Next().CopyTo(nextMetrics)

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

	return nextMetrics
}

func (ar *metricsGenerator) isDone() bool {
	return ar.samples.LoopCount() > 0 && ar.samples.LoopCount() > ar.cfg.Metrics.MaxReplay
}
