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
	"fmt"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

// Compile compiles a single dsl.Processor into a Compiled. Returns an error
// for unsupported actions in this phase (the parser already rejects them, so
// this is defense-in-depth).
func Compile(p dsl.Processor) (Compiled, error) {
	switch v := p.(type) {
	case *dsl.SetProcessor:
		return compileSet(v)
	case *dsl.AppendProcessor:
		return compileAppend(v)
	case *dsl.RemoveProcessor:
		return compileRemove(v)
	case *dsl.RemoveByPrefixProcessor:
		return compileRemoveByPrefix(v)
	case *dsl.RenameProcessor:
		return compileRename(v)
	case *dsl.DropDocumentProcessor:
		return compileDropDocument(v)
	case *dsl.UppercaseProcessor:
		return compileUppercase(v)
	case *dsl.LowercaseProcessor:
		return compileLowercase(v)
	case *dsl.TrimProcessor:
		return compileTrim(v)
	case *dsl.ReplaceProcessor:
		return compileReplace(v)
	case *dsl.ConvertProcessor:
		return compileConvert(v)
	case *dsl.GrokProcessor:
		return compileGrok(v)
	case *dsl.DissectProcessor:
		return compileDissect(v)
	case *dsl.DateProcessor:
		return compileDate(v)
	case *dsl.RedactProcessor:
		return compileRedact(v)
	case *dsl.MathProcessor:
		return compileMath(v)
	case *dsl.JSONExtractProcessor:
		return compileJSONExtract(v)
	case *dsl.NetworkDirectionProcessor:
		return compileNetworkDirection(v)
	case *dsl.SplitProcessor:
		return compileSplit(v)
	case *dsl.SortProcessor:
		return compileSort(v)
	case *dsl.JoinProcessor:
		return compileJoin(v)
	case *dsl.ConcatProcessor:
		return compileConcat(v)
	default:
		return nil, fmt.Errorf("streamlang: unsupported processor action %q", p.Action())
	}
}
