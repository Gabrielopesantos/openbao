package lru

import (
	"testing"
)

func TestLRU(t *testing.T) {
	// Test creation with valid size
	lru, err := NewLRU[string, int](10)
	if err != nil {
		t.Fatalf("Failed to create LRU: %v", err)
	}
	if lru.Size() != 10 {
		t.Errorf("Expected size 10, got %d", lru.Size())
	}

	// Test creation with invalid size
	_, err = NewLRU[string, int](-1)
	if err == nil {
		t.Error("Expected error for negative size, got nil")
	}

	// Test basic operations
	t.Run("Basic Operations", func(t *testing.T) {
		lru, _ := NewLRU[string, int](5)

		// Test Store and Load
		lru.Store("key1", 1)
		val, ok := lru.Load("key1")
		if !ok || val != 1 {
			t.Errorf("Expected (1, true), got (%d, %t)", val, ok)
		}

		// Test missing key
		_, ok = lru.Load("missing")
		if ok {
			t.Error("Expected missing key to return ok=false")
		}

		// Test Delete
		lru.Store("key2", 2)
		lru.Delete("key2")
		_, ok = lru.Load("key2")
		if ok {
			t.Error("Expected key to be deleted")
		}
	})

	// Test eviction
	t.Run("Eviction", func(t *testing.T) {
		lru, _ := NewLRU[string, int](3)

		// Fill the cache
		lru.Store("key1", 1)
		lru.Store("key2", 2)
		lru.Store("key3", 3)

		// Add one more to trigger eviction
		lru.Store("key4", 4)

		// One of the keys should be evicted (likely key1 with TwoQueueCache)
		// But we can't guarantee which one, so just check total keys
		var count int
		for _, k := range []string{"key1", "key2", "key3", "key4"} {
			if _, ok := lru.Load(k); ok {
				count++
			}
		}

		if count != 3 {
			t.Errorf("Expected 3 keys in cache, found %d", count)
		}
	})

	// Test with different types
	t.Run("Different Types", func(t *testing.T) {
		lru, _ := NewLRU[int, string](5)
		lru.Store(1, "one")
		lru.Store(2, "two")

		val, ok := lru.Load(1)
		if !ok || val != "one" {
			t.Errorf("Expected (\"one\", true), got (%s, %t)", val, ok)
		}
	})
}
