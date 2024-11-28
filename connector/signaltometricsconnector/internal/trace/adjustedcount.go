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

package trace // import "github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/trace"

import (
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling"
)

// CalculateAdjustedCount calculates the adjusted count which represents
// the number of spans in the population that are represented by the
// individually sampled span. If the span is not-sampled OR if a non-
// probability sampler is used then adjusted count defaults to 1.
// https://github.com/open-telemetry/oteps/blob/main/text/trace/0235-sampling-threshold-in-trace-state.md
func CalculateAdjustedCount(tracestate string) float64 {
	w3cTraceState, err := sampling.NewW3CTraceState(tracestate)
	if err != nil {
		return 1
	}
	otTraceState := w3cTraceState.OTelValue()
	if otTraceState == nil {
		return 1
	}
	if len(otTraceState.TValue()) == 0 {
		// For non-probabilistic sampler OR always sampling threshold, default to 1
		return 1
	}
	// TODO (lahsivjar): Handle fractional adjusted count. One way to do this
	// would be to scale the values in the histograms for some precision.
	return otTraceState.AdjustedCount()
}
