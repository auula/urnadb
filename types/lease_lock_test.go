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

	// Check if Token is not empty
	assert.NotEmpty(t, ll.Token)

	// Check if Token length is 64 characters
	assert.Equal(t, 64, len(ll.Token))

	// Create another lock to verify unique IDs
	assert.NotEqual(t, ll.Token, NewLeaseLock().Token)
}
