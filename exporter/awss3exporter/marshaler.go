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

package awss3exporter // import "github.com/elastic/opentelemetry-collector-components/exporter/awss3exporter"

import (
	"errors"

	"go.opentelemetry.io/collector/pdata/plog"
)

// errUnknownFormat is returned when a record requests a format the exporter
// does not know how to encode.
var errUnknownFormat = errors.New("unknown format")

// logMarshaler encodes a batch of logs destined for a single S3 object and
// reports the file extension to use for the object key.
type logMarshaler struct {
	marshaler plog.Marshaler
	extension string
}

// marshalerForFormat resolves the marshaler for a format string coming from the
// format attribute (or the configured default). An empty string resolves to the
// default OTLP JSON encoding so that callers can treat "unset" and "otlp_json"
// identically.
func marshalerForFormat(format string) (logMarshaler, error) {
	switch format {
	case "", "json", "otlp_json":
		return logMarshaler{marshaler: &plog.JSONMarshaler{}, extension: "json"}, nil
	case "proto", "protobuf", "otlp_proto", "otlp_protobuf":
		return logMarshaler{marshaler: &plog.ProtoMarshaler{}, extension: "binpb"}, nil
	default:
		return logMarshaler{}, errUnknownFormat
	}
}
