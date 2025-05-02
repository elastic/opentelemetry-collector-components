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

package ratelimitprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor"
import (
	"context"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor/internal/metadata"
	"go.opentelemetry.io/otel/metric"
)

type ratelimitProcessorTelemetry struct {
	exportCtx context.Context

	processorAttr    metric.MeasurementOption
	telemetryBuilder *metadata.TelemetryBuilder
}

func newRatelimitProcessorTelemetry(set processor.Settings) (*ratelimitProcessorTelemetry, error) {
	attrs := metric.WithAttributeSet(attribute.NewSet(attribute.String("processor_id", set.ID.String())))
	telemetryBuilder, err := metadata.NewTelemetryBuilder(
		set.TelemetrySettings,
	)

	if err != nil {
		return nil, err
	}

	return &ratelimitProcessorTelemetry{
		exportCtx:        context.Background(),
		telemetryBuilder: telemetryBuilder,
		processorAttr:    attrs,
	}, nil
}

func (rpt *ratelimitProcessorTelemetry) record(attrs ...attribute.KeyValue) {
	rpt.telemetryBuilder.ProcessorRatelimitRequests.Add(rpt.exportCtx, 1, metric.WithAttributeSet(attribute.NewSet(attrs...)))
}
