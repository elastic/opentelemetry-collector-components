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

package hosttest // import "github.com/elastic/opentelemetry-collector-components/receiver/vercelreceiver/hosttest"

import (
	"context"
	"io"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
	"go.opentelemetry.io/collector/component"

	"github.com/elastic/opentelemetry-collector-components/internal/vercel"
)

const VercelEncodingExtensionID = "vercel"

type customHost struct {
	extensions map[component.ID]component.Component
}

func NewCustomHost() component.Host {
	return NewCustomHostWithExtensions(map[component.ID]component.Component{
		component.MustNewID(VercelEncodingExtensionID): &MockVercelEncodingExtension{},
	})
}

func NewCustomHostWithExtensions(extensions map[component.ID]component.Component) component.Host {
	return &customHost{
		extensions: extensions,
	}
}

func (m *customHost) GetExtensions() map[component.ID]component.Component {
	return m.extensions
}

func (*customHost) GetFactory(_ component.Kind, _ component.Type) component.Factory {
	return nil
}

type MockVercelEncodingExtension struct {
	component.Component
}

func (*MockVercelEncodingExtension) Start(context.Context, component.Host) error {
	return nil
}

func (*MockVercelEncodingExtension) Shutdown(context.Context) error {
	return nil
}

func (*MockVercelEncodingExtension) NewVercelParser(io.Reader, ...encoding.DecoderOption) (vercel.PayloadParser, error) {
	return &mockVercelPayloadParser{}, nil
}

type mockVercelPayloadParser struct{}

func (*mockVercelPayloadParser) Next() (vercel.Payload, error) {
	return vercel.Payload{}, io.EOF
}
