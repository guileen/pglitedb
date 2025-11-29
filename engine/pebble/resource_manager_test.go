package pebble

import (
	"testing"

	"github.com/guileen/pglitedb/engine/pebble/resources"
)

func TestTieredBufferPools(t *testing.T) {
	rm := resources.GetResourceManager()
	
	// Test small buffer pool (≤ 64 bytes) - using new interface
	buf := rm.PoolManager().AcquireTieredBuffer(32)
	if len(buf) != 32 {
		t.Errorf("Expected buffer length 32, got %d", len(buf))
	}
	rm.PoolManager().ReleaseTieredBuffer(buf)
	
	// Test medium buffer pool (≤ 256 bytes) - using backward compatibility
	buf = rm.AcquireTieredBuffer(128)
	if len(buf) != 128 {
		t.Errorf("Expected buffer length 128, got %d", len(buf))
	}
	rm.ReleaseTieredBuffer(buf)
	
	// Test large buffer pool (≤ 1024 bytes) - using new interface
	buf = rm.PoolManager().AcquireTieredBuffer(512)
	if len(buf) != 512 {
		t.Errorf("Expected buffer length 512, got %d", len(buf))
	}
	rm.PoolManager().ReleaseTieredBuffer(buf)
	
	// Test huge buffer pool (≤ 4096 bytes) - using backward compatibility
	buf = rm.AcquireTieredBuffer(2048)
	if len(buf) != 2048 {
		t.Errorf("Expected buffer length 2048, got %d", len(buf))
	}
	rm.ReleaseTieredBuffer(buf)
	
	// Test general buffer pool (> 4096 bytes) - using new interface
	buf = rm.PoolManager().AcquireTieredBuffer(8192)
	if len(buf) != 8192 {
		t.Errorf("Expected buffer length 8192, got %d", len(buf))
	}
	rm.PoolManager().ReleaseTieredBuffer(buf)
}

func TestKeyBufferPools(t *testing.T) {
	rm := resources.GetResourceManager()
	
	// Test index key buffers - using new interface
	indexBuf := rm.PoolManager().AcquireIndexKeyBuffer(50)
	if len(indexBuf) != 50 {
		t.Errorf("Expected index buffer length 50, got %d", len(indexBuf))
	}
	rm.PoolManager().ReleaseIndexKeyBuffer(indexBuf)
	
	// Test table key buffers - using backward compatibility
	tableBuf := rm.AcquireTableKeyBuffer(20)
	if len(tableBuf) != 20 {
		t.Errorf("Expected table buffer length 20, got %d", len(tableBuf))
	}
	rm.ReleaseTableKeyBuffer(tableBuf)
}

func TestScanResultPool(t *testing.T) {
	rm := resources.GetResourceManager()
	
	// Test scan result pools - using new interface
	result := rm.PoolManager().AcquireScanResult()
	if result == nil {
		t.Error("Expected non-nil scan result")
	}
	rm.PoolManager().ReleaseScanResult(result)
}