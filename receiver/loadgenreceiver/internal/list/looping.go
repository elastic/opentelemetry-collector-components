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

package list // import "github.com/elastic/opentelemetry-collector-components/receiver/loadgenreceiver/internal/list"

// LoopingList is a list that infinitely loops over the provided list of items.
type LoopingList[T any] struct {
	items   []T
	idx     int
	loopCnt int
}

func NewLoopingList[T any](items []T) LoopingList[T] {
	return LoopingList[T]{
		items: items,
	}
}

// Next returns the next item in list.
// Unsafe for concurrent calls.
func (s *LoopingList[T]) Next() T {
	defer func() {
		s.idx = (s.idx + 1) % len(s.items)
		if s.idx == 0 {
			s.loopCnt++
		}
	}()
	return s.items[s.idx]
}

func (s *LoopingList[T]) LoopCount() int {
	return s.loopCnt
}
