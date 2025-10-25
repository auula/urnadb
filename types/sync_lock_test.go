package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSyncLock(t *testing.T) {
	// Create new sync lock
	sl := NewSyncLock()

	// Check if sync lock is not nil
	assert.NotNil(t, sl)

	// Check if LockID is not empty
	assert.NotEmpty(t, sl.LockID)

	// Check if LockID length is 64 characters
	assert.Equal(t, 64, len(sl.LockID))

	// Create another lock to verify unique IDs
	assert.NotEqual(t, sl.LockID, NewSyncLock())
}
