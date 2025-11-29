package resources

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResourceManagerRefactored(t *testing.T) {
	// Test that the ResourceManager can be created
	rm := GetResourceManager()
	assert.NotNil(t, rm)

	// Test getting metrics via new interface
	metrics := rm.Metrics()
	assert.NotNil(t, metrics)
	
	// Test getting metrics via backward compatibility method
	metricsData := rm.GetMetrics()
	assert.NotNil(t, metricsData)

	// Test resetting metrics
	rm.ResetMetrics()
	// Note: We can't assert specific values because other tests might have run
}

func TestResourceManagerNewInterface(t *testing.T) {
	// Test that the ResourceManager can be created
	rm := GetResourceManager()
	assert.NotNil(t, rm)

	// Test accessing specialized components
	assert.NotNil(t, rm.PoolManager())
	assert.NotNil(t, rm.Metrics())
	assert.NotNil(t, rm.LeakDetector())
	assert.NotNil(t, rm.SizingManager())
}

func TestLeakDetectionIntegration(t *testing.T) {
	rm := GetResourceManager()
	assert.NotNil(t, rm)

	// Test that we can get the leak detector via new interface
	leakDetector := rm.LeakDetector()
	assert.NotNil(t, leakDetector)
	
	// Test that we can get the leak detector via backward compatibility method
	leakDetectorCompat := rm.GetLeakDetector()
	assert.NotNil(t, leakDetectorCompat)

	// Test checking for leaks (should not panic)
	report := rm.CheckForLeaks()
	assert.NotNil(t, report)
}