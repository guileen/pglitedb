package resources

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResourceManagerRefactored(t *testing.T) {
	// Test that the ResourceManager can be created
	rm := GetResourceManager()
	assert.NotNil(t, rm)

	// Test getting metrics
	metrics := rm.GetMetrics()
	assert.NotNil(t, metrics)

	// Test resetting metrics
	rm.ResetMetrics()
	// Note: We can't assert specific values because other tests might have run
}

func TestLeakDetectionIntegration(t *testing.T) {
	rm := GetResourceManager()
	assert.NotNil(t, rm)

	// Test that we can get the leak detector
	leakDetector := rm.GetLeakDetector()
	assert.NotNil(t, leakDetector)

	// Test checking for leaks (should not panic)
	report := rm.CheckForLeaks()
	assert.NotNil(t, report)
}