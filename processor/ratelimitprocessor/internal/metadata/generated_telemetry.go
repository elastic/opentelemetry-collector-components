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

// Code generated by mdatagen. DO NOT EDIT.

package metadata

import (
	"errors"
	"sync"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/collector/component"
)

func Meter(settings component.TelemetrySettings) metric.Meter {
	return settings.MeterProvider.Meter("github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor")
}

func Tracer(settings component.TelemetrySettings) trace.Tracer {
	return settings.TracerProvider.Tracer("github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor")
}

// TelemetryBuilder provides an interface for components to report telemetry
// as defined in metadata and user config.
type TelemetryBuilder struct {
	meter                       metric.Meter
	mu                          sync.Mutex
	registrations               []metric.Registration
	RatelimitConcurrentRequests metric.Int64Gauge
	RatelimitRequestDuration    metric.Float64Histogram
	RatelimitRequests           metric.Int64Counter
}

// TelemetryBuilderOption applies changes to default builder.
type TelemetryBuilderOption interface {
	apply(*TelemetryBuilder)
}

type telemetryBuilderOptionFunc func(mb *TelemetryBuilder)

func (tbof telemetryBuilderOptionFunc) apply(mb *TelemetryBuilder) {
	tbof(mb)
}

// Shutdown unregister all registered callbacks for async instruments.
func (builder *TelemetryBuilder) Shutdown() {
	builder.mu.Lock()
	defer builder.mu.Unlock()
	for _, reg := range builder.registrations {
		reg.Unregister()
	}
}

// NewTelemetryBuilder provides a struct with methods to update all internal telemetry
// for a component
func NewTelemetryBuilder(settings component.TelemetrySettings, options ...TelemetryBuilderOption) (*TelemetryBuilder, error) {
	builder := TelemetryBuilder{}
	for _, op := range options {
		op.apply(&builder)
	}
	builder.meter = Meter(settings)
	var err, errs error
	builder.RatelimitConcurrentRequests, err = builder.meter.Int64Gauge(
		"otelcol_ratelimit.concurrent_requests",
		metric.WithDescription("Number of in-flight requests at any given time"),
		metric.WithUnit("{requests}"),
	)
	errs = errors.Join(errs, err)
	builder.RatelimitRequestDuration, err = builder.meter.Float64Histogram(
		"otelcol_ratelimit.request_duration",
		metric.WithDescription("Time(in seconds) taken to process a rate limit request"),
		metric.WithUnit("{seconds}"),
		metric.WithExplicitBucketBoundaries([]float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1, 5, 10}...),
	)
	errs = errors.Join(errs, err)
	builder.RatelimitRequests, err = builder.meter.Int64Counter(
		"otelcol_ratelimit.requests",
		metric.WithDescription("Number of rate-limiting requests"),
		metric.WithUnit("{requests}"),
	)
	errs = errors.Join(errs, err)
	return &builder, errs
}
