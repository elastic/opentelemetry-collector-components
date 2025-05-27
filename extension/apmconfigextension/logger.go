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

package apmconfigextension // import "github.com/elastic/opentelemetry-collector-components/extension/apmconfigextension"

import (
	"context"

	"github.com/open-telemetry/opamp-go/client/types"
	"go.uber.org/zap"
)

var _ types.Logger = &opAMPLogger{}

type opAMPLogger struct {
	l *zap.SugaredLogger
}

// Debugf implements types.Logger.
func (o *opAMPLogger) Debugf(_ context.Context, format string, v ...any) {
	o.l.Debugf(format, v...)
}

// Errorf implements types.Logger.
func (o *opAMPLogger) Errorf(_ context.Context, format string, v ...any) {
	o.l.Errorf(format, v...)
}

func newLoggerFromZap(l *zap.Logger) types.Logger {
	return &opAMPLogger{
		l: l.Sugar(),
	}
}
