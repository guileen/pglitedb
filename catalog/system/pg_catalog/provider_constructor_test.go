package pgcatalog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewProvider(t *testing.T) {
	t.Run("CreateProviderWithManager", func(t *testing.T) {
		mockManager := new(MockTableManager)
		provider := NewProvider(mockManager)

		assert.NotNil(t, provider)
		assert.Equal(t, mockManager, provider.manager)
	})

	t.Run("CreateProviderWithNilManager", func(t *testing.T) {
		provider := NewProvider(nil)

		assert.NotNil(t, provider)
		assert.Nil(t, provider.manager)
	})
}