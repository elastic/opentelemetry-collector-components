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
