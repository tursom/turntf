package clock

import (
	"slices"
	"sync"
	"testing"
)

func TestIDGeneratorConcurrentUnique(t *testing.T) {
	t.Parallel()

	const (
		goroutines        = 32
		idsPerGoroutine   = 512
		totalGeneratedIDs = goroutines * idsPerGoroutine
	)

	gen := NewIDGenerator()
	ids := make([]int64, totalGeneratedIDs)

	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			offset := i * idsPerGoroutine
			for j := range idsPerGoroutine {
				ids[offset+j] = gen.Next()
			}
		}(i)
	}

	wg.Wait()
	slices.Sort(ids)

	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("ids must be unique and increasing after sort: ids[%d]=%d ids[%d]=%d", i-1, ids[i-1], i, ids[i])
		}
	}
}
