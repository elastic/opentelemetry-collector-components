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

package internal // import "github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver/internal"

type LoopingList[T any] struct {
	items []T
	idx   int
}

func NewLoopingList[T any](items []T) LoopingList[T] {
	return LoopingList[T]{
		items: items,
	}
}

func (s *LoopingList[T]) Next() T {
	defer func() {
		s.idx++
	}()
	return s.items[s.idx]
}
