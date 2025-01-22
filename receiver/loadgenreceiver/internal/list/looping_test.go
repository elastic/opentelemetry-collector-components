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

package list

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNext_loopLimit(t *testing.T) {
	for _, loopLimit := range []int{0, 10} {
		t.Run(fmt.Sprintf("loopLimit=%d", loopLimit), func(t *testing.T) {
			items := []int{0, 1, 2}
			l := NewLoopingList(items, loopLimit)
			for i := 0; ; i++ {
				item, err := l.Next()
				if loopLimit == 0 {
					if i > 10 {
						// terminate after some point if infinitely looping
						return
					}
				} else {
					if i >= len(items)*loopLimit {
						assert.ErrorIs(t, err, ErrLoopLimitReached)
						return
					}
				}
				assert.NoError(t, err)
				assert.Equal(t, i%3, item)
			}
		})
	}
}
