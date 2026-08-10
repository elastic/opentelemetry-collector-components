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

package prometheusremotewriteexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter"

import (
	"context"
	"errors"

	"go.uber.org/zap"
)

type ctxKey int

const (
	loggerCtxKey ctxKey = iota
)

func contextWithLogger(ctx context.Context, log *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerCtxKey, log)
}

func loggerFromContext(ctx context.Context) (*zap.Logger, error) {
	v := ctx.Value(loggerCtxKey)
	if v == nil {
		return nil, errors.New("no logger found in context")
	}

	l, ok := v.(*zap.Logger)
	if !ok {
		return nil, errors.New("invalid logger found in context")
	}

	return l, nil
}
