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

package customottl // import "github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/customottl"

import (
	"context"

	"github.com/elastic/opentelemetry-collector-components/connector/signaltometricsconnector/internal/trace"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
)

func NewAdjustedCountFactory() ottl.Factory[ottlspan.TransformContext] {
	return ottl.NewFactory("AdjustedCount", nil, createAdjustedCountFunction)
}

func createAdjustedCountFunction(_ ottl.FunctionContext, _ ottl.Arguments) (ottl.ExprFunc[ottlspan.TransformContext], error) {
	return func(_ context.Context, tCtx ottlspan.TransformContext) (any, error) {
		return trace.CalculateAdjustedCount(tCtx.GetSpan().TraceState().AsRaw()), nil
	}, nil
}
