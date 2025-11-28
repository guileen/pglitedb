package resources

// This file is intentionally left minimal as the ResourceManager functionality
// has been split into multiple files:
// - core.go: Contains the ResourceManager struct definition and basic initialization
// - pools.go: Contains all pool acquisition and release methods
// - pool_sizing.go: Contains adaptive pool sizing functionality
// - leak_detection.go: Contains leak detection integration
// - metrics.go: Contains the metrics collector implementation (in a separate file)
// - metrics_interface.go: Contains the ResourceManager metrics interface methods