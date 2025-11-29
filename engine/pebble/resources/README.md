# Resource Management

The resource management system has been refactored to reduce the god object anti-pattern in the `ResourceManager` class.

## Architecture

The system is now organized into specialized components:

```
resources/
├── core.go              # Main resource manager (coordinator only)
├── pools/               # Pool-specific implementations
│   ├── manager.go       # Pool manager
│   ├── iterator.go      # Iterator pool
│   ├── batch.go         # Batch pool
│   └── ...              # Other specialized pools
├── leak/                # Leak detection
│   └── detector.go      # Leak detector implementation
├── metrics/             # Metrics collection
│   └── collector.go     # Metrics collector
└── sizing/              # Adaptive sizing
    └── manager.go       # Sizing manager
```

## Refactored Interface

The `ResourceManager` now acts as a coordinator that provides access to specialized components rather than exposing all functionality directly.

### Old Interface (Deprecated for new code)
```go
rm := resources.GetResourceManager()
iter := rm.AcquireIterator()
rm.ReleaseIterator(iter)
metrics := rm.GetMetrics()
```

### New Interface (Preferred)
```go
rm := resources.GetResourceManager()
iter := rm.PoolManager().AcquireIterator()
rm.PoolManager().ReleaseIterator(iter)
metrics := rm.Metrics().GetMetrics()
```

### Component Access Methods

- `PoolManager()` - Access to all resource pools
- `Metrics()` - Access to metrics collection
- `LeakDetector()` - Access to leak detection functionality
- `SizingManager()` - Access to adaptive sizing logic

### Backward Compatibility

All previous methods remain available for backward compatibility but are implemented as delegations to the specialized components.