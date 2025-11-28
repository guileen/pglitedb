package idgen

import (
	"context"
	"sync"
	"testing"
)

func TestIDGenerator(t *testing.T) {
	gen := NewIDGenerator()

	t.Run("NextRowID", func(t *testing.T) {
		ctx := context.Background()
		id1, err := gen.NextRowID(ctx, 1, 1)
		if err != nil {
			t.Fatalf("failed to generate row id: %v", err)
		}

		id2, err := gen.NextRowID(ctx, 1, 1)
		if err != nil {
			t.Fatalf("failed to generate row id: %v", err)
		}

		if id1 == id2 {
			t.Errorf("generated duplicate row IDs: %d", id1)
		}
	})

	t.Run("NextTableID", func(t *testing.T) {
		ctx := context.Background()
		id1, err := gen.NextTableID(ctx, 1)
		if err != nil {
			t.Fatalf("failed to generate table id: %v", err)
		}

		id2, err := gen.NextTableID(ctx, 1)
		if err != nil {
			t.Fatalf("failed to generate table id: %v", err)
		}

		if id1 >= id2 {
			t.Errorf("table IDs not incrementing: %d >= %d", id1, id2)
		}
	})

	t.Run("NextIndexID", func(t *testing.T) {
		ctx := context.Background()
		id1, err := gen.NextIndexID(ctx, 1, 1)
		if err != nil {
			t.Fatalf("failed to generate index id: %v", err)
		}

		id2, err := gen.NextIndexID(ctx, 1, 1)
		if err != nil {
			t.Fatalf("failed to generate index id: %v", err)
		}

		if id1 >= id2 {
			t.Errorf("index IDs not incrementing: %d >= %d", id1, id2)
		}
	})

	t.Run("ConcurrentSafety", func(t *testing.T) {
		const goroutines = 10
		const iterations = 100
		
		var wg sync.WaitGroup
		errors := make(chan error, goroutines*iterations*3)
		
		// Test concurrent row ID generation
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				ctx := context.Background()
				for j := 0; j < iterations; j++ {
					_, err := gen.NextRowID(ctx, 1, int64(j))
					if err != nil {
						errors <- err
					}
				}
			}()
		}
		
		// Test concurrent table ID generation
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				ctx := context.Background()
				for j := 0; j < iterations; j++ {
					_, err := gen.NextTableID(ctx, int64(j%10))
					if err != nil {
						errors <- err
					}
				}
			}()
		}
		
		// Test concurrent index ID generation
		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				ctx := context.Background()
				for j := 0; j < iterations; j++ {
					_, err := gen.NextIndexID(ctx, int64(j%10), int64(j%5))
					if err != nil {
						errors <- err
					}
				}
			}()
		}
		
		wg.Wait()
		close(errors)
		
		// Check for any errors
		for err := range errors {
			t.Errorf("concurrent test error: %v", err)
		}
	})
}