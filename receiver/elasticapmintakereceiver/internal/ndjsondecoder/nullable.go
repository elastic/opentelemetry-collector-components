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

// Adapted from github.com/elastic/apm-data/input/elasticapm/internal/modeldecoder/nullable.
// Package name changed so jsoniter registration keys match this package's type names.

package ndjsondecoder // import "github.com/elastic/opentelemetry-collector-components/receiver/elasticapmintakereceiver/internal/ndjsondecoder"

import (
	"errors"
	"fmt"
	"net/http"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
)

// supportedTSFormats lists variations of RFC3339 for supporting
// different formats for the timezone offset.
var supportedTSFormats = []string{
	"2006-01-02T15:04:05Z07:00", // RFC3339
	"2006-01-02T15:04:05Z0700",
	"2006-01-02T15:04:05Z07",
}

func init() {
	jsoniter.RegisterTypeDecoderFunc("ndjsondecoder.String", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		switch iter.WhatIsNext() {
		case jsoniter.NilValue:
			iter.ReadNil()
		default:
			(*((*String)(ptr))).Val = iter.ReadString()
			(*((*String)(ptr))).isSet = true
		}
	})
	jsoniter.RegisterTypeDecoderFunc("ndjsondecoder.Int", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		switch iter.WhatIsNext() {
		case jsoniter.NilValue:
			iter.ReadNil()
		default:
			(*((*Int)(ptr))).Val = iter.ReadInt()
			(*((*Int)(ptr))).isSet = true
		}
	})
	jsoniter.RegisterTypeDecoderFunc("ndjsondecoder.Int64", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		switch iter.WhatIsNext() {
		case jsoniter.NilValue:
			iter.ReadNil()
		default:
			(*((*Int64)(ptr))).Val = iter.ReadInt64()
			(*((*Int64)(ptr))).isSet = true
		}
	})
	jsoniter.RegisterTypeDecoderFunc("ndjsondecoder.Float64", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		switch iter.WhatIsNext() {
		case jsoniter.NilValue:
			iter.ReadNil()
		default:
			(*((*Float64)(ptr))).Val = iter.ReadFloat64()
			(*((*Float64)(ptr))).isSet = true
		}
	})
	jsoniter.RegisterTypeDecoderFunc("ndjsondecoder.Bool", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		switch iter.WhatIsNext() {
		case jsoniter.NilValue:
			iter.ReadNil()
		default:
			(*((*Bool)(ptr))).Val = iter.ReadBool()
			(*((*Bool)(ptr))).isSet = true
		}
	})
	jsoniter.RegisterTypeDecoderFunc("ndjsondecoder.Interface", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		switch iter.WhatIsNext() {
		case jsoniter.NilValue:
			iter.ReadNil()
		default:
			v := iter.Read()
			(*((*Interface)(ptr))).Val = v
			(*((*Interface)(ptr))).isSet = true
		}
	})
	jsoniter.RegisterTypeDecoderFunc("ndjsondecoder.TimeMicrosUnix", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		switch iter.WhatIsNext() {
		case jsoniter.NilValue:
			iter.ReadNil()
		case jsoniter.NumberValue:
			us := iter.ReadInt64()
			s := us / 1000000
			ns := (us - (s * 1000000)) * 1000
			(*((*TimeMicrosUnix)(ptr))).Val = time.Unix(s, ns).UTC()
			(*((*TimeMicrosUnix)(ptr))).isSet = true
		case jsoniter.StringValue:
			tstr := iter.ReadString()
			for _, f := range supportedTSFormats {
				if t, err := time.Parse(f, tstr); err == nil {
					(*((*TimeMicrosUnix)(ptr))).Val = t.UTC()
					(*((*TimeMicrosUnix)(ptr))).isSet = true
					return
				}
			}
			iter.Error = errors.New("failed to parse the provided time string")
		default:
			iter.Error = errors.New("invalid input type for timestamp")
		}
	})
	jsoniter.RegisterTypeDecoderFunc("ndjsondecoder.HTTPHeader", func(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
		switch iter.WhatIsNext() {
		case jsoniter.NilValue:
			iter.ReadNil()
		default:
			m, ok := iter.Read().(map[string]interface{})
			if !ok {
				iter.Error = errors.New("invalid input type for HTTPHeader")
				return
			}
			h := make(http.Header, len(m))
			for key, val := range m {
				switch v := val.(type) {
				case string:
					h.Add(key, v)
				case []interface{}:
					for _, entry := range v {
						switch entry := entry.(type) {
						case string:
							h.Add(key, entry)
						default:
							iter.Error = fmt.Errorf("invalid input for HTTPHeader slice value type: %T", entry)
							return
						}
					}
				default:
					iter.Error = fmt.Errorf("invalid input for HTTPHeader value type: %T", v)
					return
				}
			}
			(*((*HTTPHeader)(ptr))).Val = h
			(*((*HTTPHeader)(ptr))).isSet = true
		}
	})
}

// String stores a string value and the
// information if the value has been set
type String struct {
	Val   string
	isSet bool
}

// Set sets the value
func (v *String) Set(val string) {
	v.Val = val
	v.isSet = true
}

// IsSet is true when decode was called
func (v *String) IsSet() bool {
	return v.isSet
}

// Int stores an int value and the
// information if the value has been set
type Int struct {
	Val   int
	isSet bool
}

// Set sets the value
func (v *Int) Set(val int) {
	v.Val = val
	v.isSet = true
}

// IsSet is true when decode was called
func (v *Int) IsSet() bool {
	return v.isSet
}

// Int64 stores an int64 value and the
// information if the value has been set
type Int64 struct {
	Val   int64
	isSet bool
}

// Set sets the value
func (v *Int64) Set(val int64) {
	v.Val = val
	v.isSet = true
}

// IsSet is true when decode was called
func (v *Int64) IsSet() bool {
	return v.isSet
}

// Float64 stores a float64 value and the
// information if the value has been set
type Float64 struct {
	Val   float64
	isSet bool
}

// Set sets the value
func (v *Float64) Set(val float64) {
	v.Val = val
	v.isSet = true
}

// IsSet is true when decode was called
func (v *Float64) IsSet() bool {
	return v.isSet
}

// Bool stores a bool value and the
// information if the value has been set
type Bool struct {
	Val   bool
	isSet bool
}

// Set sets the value
func (v *Bool) Set(val bool) {
	v.Val = val
	v.isSet = true
}

// IsSet is true when decode was called
func (v *Bool) IsSet() bool {
	return v.isSet
}

// Interface stores an interface{} value and the
// information if the value has been set
type Interface struct {
	Val   interface{} `json:"val,omitempty"`
	isSet bool
}

// Set sets the value
func (v *Interface) Set(val interface{}) {
	v.Val = val
	v.isSet = true
}

// IsSet is true when decode was called
func (v *Interface) IsSet() bool {
	return v.isSet
}

type TimeMicrosUnix struct {
	Val   time.Time
	isSet bool
}

// Set sets the value
func (v *TimeMicrosUnix) Set(val time.Time) {
	v.Val = val
	v.isSet = true
}

// IsSet is true when decode was called
func (v *TimeMicrosUnix) IsSet() bool {
	return v.isSet
}

type HTTPHeader struct {
	Val   http.Header
	isSet bool
}

// Set sets the value
func (v *HTTPHeader) Set(val http.Header) {
	v.Val = val
	v.isSet = true
}

// IsSet is true when decode was called
func (v *HTTPHeader) IsSet() bool {
	return v.isSet
}
