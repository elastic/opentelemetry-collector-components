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
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/document"
	"github.com/elastic/opentelemetry-collector-components/processor/streamlangprocessor/dsl"
)

// maxSafeIntegerInDouble is 2^53; floats above this lose integer precision.
const maxSafeIntegerInDouble = float64(1 << 53)

type compiledConvert struct {
	from          string
	to            string
	convertType   dsl.ConvertType
	ignoreMissing *bool
	ignoreFailure bool
}

func compileConvert(p *dsl.ConvertProcessor) (Compiled, error) {
	if p.From == "" {
		return nil, errors.New("streamlang/convert: 'from' is required")
	}
	switch p.Type {
	case dsl.ConvertTypeInteger, dsl.ConvertTypeLong, dsl.ConvertTypeDouble,
		dsl.ConvertTypeBoolean, dsl.ConvertTypeString:
	default:
		return nil, fmt.Errorf("streamlang/convert: unknown type %q", p.Type)
	}
	return &compiledConvert{
		from:          p.From,
		to:            p.To,
		convertType:   p.Type,
		ignoreMissing: p.IgnoreMissing,
		ignoreFailure: p.IgnoreFailure,
	}, nil
}

func (c *compiledConvert) Action() string      { return "convert" }
func (c *compiledConvert) IgnoreFailure() bool { return c.ignoreFailure }

func (c *compiledConvert) Execute(d document.Document) error {
	val, ok := d.Get(c.from)
	if !ok {
		if c.ignoreMissing != nil && *c.ignoreMissing {
			return &SkipError{Reason: "field missing"}
		}
		return fmt.Errorf("streamlang/convert: field %q not found", c.from)
	}
	out, err := convertValue(val, c.convertType)
	if err != nil {
		return fmt.Errorf("streamlang/convert: %w", err)
	}
	return d.Set(targetOrSelf(c.to, c.from), out)
}

// convertValue performs the per-type coercion. Note: ES "integer" is 32-bit
// and "long" is 64-bit at the storage layer; in this Go representation both
// are folded into int64 (matching Rust runtime behavior). Document this in
// comments for future tightening if needed.
func convertValue(v document.Value, t dsl.ConvertType) (document.Value, error) {
	switch t {
	case dsl.ConvertTypeString:
		return document.StringValue(convertToString(v)), nil
	case dsl.ConvertTypeBoolean:
		b, err := convertToBool(v)
		if err != nil {
			return document.Value{}, err
		}
		return document.BoolValue(b), nil
	case dsl.ConvertTypeInteger, dsl.ConvertTypeLong:
		i, err := convertToInt64(v)
		if err != nil {
			return document.Value{}, err
		}
		return document.IntValue(i), nil
	case dsl.ConvertTypeDouble:
		f, err := convertToFloat64(v)
		if err != nil {
			return document.Value{}, err
		}
		return document.DoubleValue(f), nil
	}
	return document.Value{}, fmt.Errorf("unknown type %q", t)
}

func convertToString(v document.Value) string {
	// Value.Str() handles primitive stringification already (incl. bool/numeric).
	switch v.Type() {
	case document.ValueTypeStr, document.ValueTypeBool,
		document.ValueTypeInt, document.ValueTypeDouble:
		return v.Str()
	default:
		// For composite values, rely on AsAny + fmt for a best-effort string.
		return fmt.Sprintf("%v", v.AsAny())
	}
}

func convertToBool(v document.Value) (bool, error) {
	switch v.Type() {
	case document.ValueTypeBool:
		return v.Bool(), nil
	case document.ValueTypeStr:
		s := strings.ToLower(strings.TrimSpace(v.Str()))
		switch s {
		case "true", "1":
			return true, nil
		case "false", "0", "":
			return false, nil
		}
		return false, fmt.Errorf("cannot convert %q to boolean", v.Str())
	case document.ValueTypeInt:
		return v.Int() != 0, nil
	case document.ValueTypeDouble:
		return v.Double() != 0, nil
	}
	return false, fmt.Errorf("cannot convert %v to boolean", v.Type())
}

func convertToInt64(v document.Value) (int64, error) {
	switch v.Type() {
	case document.ValueTypeInt:
		return v.Int(), nil
	case document.ValueTypeBool:
		if v.Bool() {
			return 1, nil
		}
		return 0, nil
	case document.ValueTypeDouble:
		f := v.Double()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, fmt.Errorf("cannot convert %v to integer", f)
		}
		if f == math.Trunc(f) && math.Abs(f) < maxSafeIntegerInDouble {
			return int64(f), nil
		}
		return 0, fmt.Errorf("cannot convert non-integer or out-of-range double %v to integer", f)
	case document.ValueTypeStr:
		s := strings.TrimSpace(v.Str())
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return i, nil
		}
		// Fall through: try float-then-truncate (matches Rust behavior).
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return 0, fmt.Errorf("cannot convert %q to integer", s)
			}
			if f == math.Trunc(f) && math.Abs(f) < maxSafeIntegerInDouble {
				return int64(f), nil
			}
		}
		return 0, fmt.Errorf("cannot convert %q to integer", s)
	}
	return 0, fmt.Errorf("cannot convert %v to integer", v.Type())
}

func convertToFloat64(v document.Value) (float64, error) {
	switch v.Type() {
	case document.ValueTypeDouble:
		return v.Double(), nil
	case document.ValueTypeInt:
		return float64(v.Int()), nil
	case document.ValueTypeBool:
		if v.Bool() {
			return 1, nil
		}
		return 0, nil
	case document.ValueTypeStr:
		s := strings.TrimSpace(v.Str())
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %q to double", s)
		}
		return f, nil
	}
	return 0, fmt.Errorf("cannot convert %v to double", v.Type())
}
