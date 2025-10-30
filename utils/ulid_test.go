package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateULID(t *testing.T) {
	ulid := NewULID()
	assert.NotEmpty(t, ulid)
	assert.NotEqual(t, ulid, NewULID())
}
