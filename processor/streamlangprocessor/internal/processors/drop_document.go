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

package processors // import "github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/internal/processors"

import (
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

type compiledDropDocument struct {
	ignoreFailure bool
}

func compileDropDocument(p *dsl.DropDocumentProcessor) (Compiled, error) {
	return &compiledDropDocument{ignoreFailure: p.IgnoreFailure}, nil
}

func (c *compiledDropDocument) Action() string      { return "drop_document" }
func (c *compiledDropDocument) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledDropDocument) Execute(d document.Document) error {
	d.Drop()
	return nil
}
