package limits

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTracker(t *testing.T) {
	for _, tc := range []struct {
		name             string
		maxCardinality   int64
		inputHashes      []uint64
		expectedOverflow uint64
	}{
		{
			name: "empty",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tracker := NewTracker(tc.maxCardinality)
			assert.False(t, tracker.HasOverflow())
			assert.Zero(t, tracker.EstimateOverflow())
			for _, h := range tc.inputHashes {
				tracker.CheckOverflow(h)
			}
			if tc.expectedOverflow > 0 {
				assert.True(t, tracker.HasOverflow())
				assert.Equal(t, tc.expectedOverflow, tracker.EstimateOverflow())
			} else {
				assert.False(t, tracker.HasOverflow())
				assert.Zero(t, tracker.EstimateOverflow())
			}
		})
	}
}
