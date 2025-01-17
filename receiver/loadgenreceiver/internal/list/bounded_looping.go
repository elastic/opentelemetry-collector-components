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

// BoundedLoopingList is a list that can optionally have a limit on the number of loops performed.
type BoundedLoopingList[T any] struct {
	list      LoopingList[T]
	loopLimit int
	mu        sync.Mutex
}

func NewBoundedLoopingList[T any](items []T, loopLimit int) BoundedLoopingList[T] {
	return BoundedLoopingList[T]{
		list:      LoopingList[T]{items: items},
		loopLimit: loopLimit,
	}
}

// Next returns the next element in the looping list.
// When loop limit is reached, it returns a zero value of T and ErrLoopLimitReached.
// Safe for concurrent use.
func (s *BoundedLoopingList[T]) Next() (T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loopLimit > 0 && s.list.LoopCount() >= s.loopLimit {
		var zero T
		return zero, ErrLoopLimitReached
	}
	return s.list.Next(), nil
}
