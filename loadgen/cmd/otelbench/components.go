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

package main

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/exporter/nopexporter"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/nopreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"

	"github.com/elastic/opentelemetry-collector-components/processor/ratelimitprocessor"
	"github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver"
)

func components(logsDone, metricsDone, tracesDone, profilesDone chan loadgenreceiver.Stats) (otelcol.Factories, error) {
	var err error
	factories := otelcol.Factories{}

	// Receivers
	factories.Receivers, err = otelcol.MakeFactoryMap[receiver.Factory](
		loadgenreceiver.NewFactoryWithDone(logsDone, metricsDone, tracesDone, profilesDone),
		nopreceiver.NewFactory(),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}

	// Processors
	factories.Processors, err = otelcol.MakeFactoryMap[processor.Factory](
		ratelimitprocessor.NewFactory(),
		transformprocessor.NewFactory(),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}

	// Exporters
	factories.Exporters, err = otelcol.MakeFactoryMap(
		otlpexporter.NewFactory(),
		otlphttpexporter.NewFactory(),
		debugexporter.NewFactory(),
		nopexporter.NewFactory(),
	)
	if err != nil {
		return otelcol.Factories{}, err
	}

	factories.Connectors, err = otelcol.MakeFactoryMap[connector.Factory]()
	if err != nil {
		return otelcol.Factories{}, err
	}

	factories.Extensions, err = otelcol.MakeFactoryMap[extension.Factory]()
	if err != nil {
		return otelcol.Factories{}, err
	}

	// Telemetry
	factories.Telemetry = otelconftelemetry.NewFactory()

	return factories, err
}
