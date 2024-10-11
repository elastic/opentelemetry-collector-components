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
	"fmt"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
)

type GetArguments[K any] struct {
	Value ottl.Getter[K]
}

func NewGetFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("get", &GetArguments[K]{}, createGetFunction[K])
}

func createGetFunction[K any](_ ottl.FunctionContext, oArgs ottl.Arguments) (ottl.ExprFunc[K], error) {
	args, ok := oArgs.(*GetArguments[K])

	if !ok {
		return nil, fmt.Errorf("GetFactory args must be of type *GetArguments[K]")
	}

	return get(args.Value), nil
}

func get[K any](value ottl.Getter[K]) ottl.ExprFunc[K] {
	return func(ctx context.Context, tCtx K) (any, error) {
		return value.Get(ctx, tCtx)
	}
}

// ConvertToStatement converts ottl.Value to an OTTL statement. To do
// this, it uses a custom `get` editor. This is expected to be a
// temporary measure until value parsing is allowed by the OTTL pkg.
func ConvertToStatement(s string) string {
	return fmt.Sprintf("get(%s)", s)
}
