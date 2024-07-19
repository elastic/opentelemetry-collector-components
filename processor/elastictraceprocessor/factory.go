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

package elastictraceprocessor // import "github.com/elastic/opentelemetry-collector-components/processor/elastictraceprocessor"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"

	"github.com/elastic/opentelemetry-collector-components/processor/elastictraceprocessor/internal/metadata"
)

// NewFactory returns a processor.Factory that constructs elastic
// trace processor instances.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		func() component.Config { return &struct{}{} },
		processor.WithTraces(createTraces, metadata.TracesStability),
	)
}

func createTraces(
	_ context.Context, set processor.Settings, _ component.Config, next consumer.Traces,
) (processor.Traces, error) {
	return newProcessor(set.Logger, next), nil
}
