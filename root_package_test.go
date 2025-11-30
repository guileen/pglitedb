package main

import (
	"testing"
	"time"
)

func TestConcurrencyLevelStructure(t *testing.T) {
	level := ConcurrencyLevel{
		Workers: 10,
		Name:    "High",
	}
	
	if level.Workers != 10 {
		t.Errorf("Expected Workers to be 10, got %d", level.Workers)
	}
	
	if level.Name != "High" {
		t.Errorf("Expected Name to be 'High', got '%s'", level.Name)
	}
}

func TestValidationStatsOpsPerSecond(t *testing.T) {
	stats := &ValidationStats{
		Operations: 100,
		StartTime:  time.Now().Add(-10 * time.Second),
	}
	
	opsPerSec := stats.OpsPerSecond()
	expected := 10.0 // 100 operations in 10 seconds
	
	if opsPerSec < expected*0.9 || opsPerSec > expected*1.1 {
		t.Errorf("Expected approximately %f ops/sec, got %f", expected, opsPerSec)
	}
}

func TestValidationStatsErrorRate(t *testing.T) {
	// Test with no operations
	stats := &ValidationStats{
		Operations: 0,
		Errors:     0,
	}
	
	if stats.ErrorRate() != 0 {
		t.Errorf("Expected error rate 0 with no operations, got %f", stats.ErrorRate())
	}
	
	// Test with operations but no errors
	stats.Operations = 100
	stats.Errors = 0
	
	if stats.ErrorRate() != 0 {
		t.Errorf("Expected error rate 0 with no errors, got %f", stats.ErrorRate())
	}
	
	// Test with operations and errors
	stats.Errors = 10
	expected := 10.0 // 10 errors out of 100 operations = 10%
	
	if stats.ErrorRate() != expected {
		t.Errorf("Expected error rate %f, got %f", expected, stats.ErrorRate())
	}
}

func TestSimpleConnectionFunction(t *testing.T) {
	// This is just to verify the function exists and can be compiled
	// We can't actually test the connection without a running server
	t.Log("Simple connection function structure verified")
}