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

import (
	"errors"
	"sync"
)

var ErrLoopLimitReached = errors.New("loop limit reached")

// LoopingList is a list that loops over the provided list of items with an optional loop limit.
type LoopingList[T any] struct {
	items     []T
	idx       int
	loopCnt   int
	loopLimit int
	mu        sync.Mutex
}

// NewLoopingList returns a LoopingList over items for an optional loopLimit.
// Setting loopLimit to 0 causes the list to loop infinitely.
func NewLoopingList[T any](items []T, loopLimit int) *LoopingList[T] {
	return &LoopingList[T]{
		items:     items,
		loopLimit: loopLimit,
	}
}

// Next returns the next item in list with a nil error.
// Safe for concurrent use.
func (s *LoopingList[T]) Next() (T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.loopCnt > 0 && s.loopCnt >= s.loopLimit {
		var zero T
		return zero, ErrLoopLimitReached
	}

	item := s.items[s.idx]

	s.idx = (s.idx + 1) % len(s.items)
	if s.idx == 0 {
		s.loopCnt++
	}

	return item, nil
}
