package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLeaseLock(t *testing.T) {
	// Create new lease lock
	ll := NewLeaseLock()

	// Check if lease lock is not nil
	assert.NotNil(t, ll)

	// Check if LockID is not empty
	assert.NotEmpty(t, ll.LockID)

	// Check if LockID length is 64 characters
	assert.Equal(t, 64, len(ll.LockID))

	// Create another lock to verify unique IDs
	assert.NotEqual(t, ll.LockID, NewLeaseLock().LockID)
}
