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

package vercel // import "github.com/elastic/opentelemetry-collector-components/internal/vercel"

import (
	"io"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// EncodingExtension parses Vercel request bodies into OpenTelemetry payloads.
//
// The parser streams the body: receivers call Next until io.EOF, so extensions
// can emit one payload or many without changing receiver routing logic.
//
// DecoderOptions let the receiver own the flush/batch policy while the extension
// owns the decoding mechanism. The Vercel decoder honors WithFlushItems and
// WithFlushBytes (whichever limit is hit first ends a payload); other options
// are accepted for forward compatibility but may be ignored.
type EncodingExtension interface {
	extension.Extension
	NewVercelParser(io.Reader, ...encoding.DecoderOption) (PayloadParser, error)
}

// PayloadParser returns Vercel payloads from a request body.
//
// Next returns io.EOF when the request body has been fully parsed.
type PayloadParser interface {
	Next() (Payload, error)
}

type Signal string

const (
	SignalLogs    Signal = "logs"
	SignalMetrics Signal = "metrics"
)

type Payload struct {
	Signal  Signal
	Logs    plog.Logs
	Metrics pmetric.Metrics
}
