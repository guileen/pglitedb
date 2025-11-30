package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEngineBasic(t *testing.T) {
	t.Run("FactoryFunctionExists", func(t *testing.T) {
		// This test verifies that the factory function exists and compiles
		_ = NewStorageEngine
		assert.True(t, true)
	})
}