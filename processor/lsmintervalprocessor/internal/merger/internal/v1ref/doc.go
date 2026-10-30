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

// Package v1ref holds the pmetric-based value implementation that wrote the
// v1 format, including the deterministic cumulative tie policy from #1483. It is used only by tests, as the reference that the
// v2 value must behave identically to, and to produce v1 encoded values.
package v1ref // import "github.com/elastic/opentelemetry-collector-components/processor/lsmintervalprocessor/internal/merger/internal/v1ref"
