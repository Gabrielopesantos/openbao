package bptree

import (
	"context"
	"fmt"
	"testing"

	"github.com/openbao/openbao/sdk/v2/logical"
	"github.com/stretchr/testify/require"
)

// stringLess is a comparison function for strings
func stringLess(a, b string) bool {
	return a < b
}

// intLess is a comparison function for integers
func intLess(a, b int) bool {
	return a < b
}

func TestNewBPlusTree(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}

	t.Run("DefaultOrder", func(t *testing.T) {
		adapter := NewStorageAdapter[string, string](ctx, "bptree_default", s, nil)
		tree, err := NewBPlusTree(0, stringLess, adapter)
		require.Error(t, err, "Order must be at least 2")
		require.Nil(t, tree)
	})

	t.Run("CustomOrder", func(t *testing.T) {
		adapter := NewStorageAdapter[string, string](ctx, "bptree_custom", s, nil)
		tree, err := NewBPlusTree(4, stringLess, adapter)
		require.NoError(t, err, "Failed to create B+ tree with custom order")
		require.Equal(t, 4, tree.order)
	})

	t.Run("NilComparisonFunction", func(t *testing.T) {
		adapter := NewStorageAdapter[string, string](ctx, "bptree_nil_comp", s, nil)
		tree, err := NewBPlusTree(4, nil, adapter)
		require.Error(t, err, "Should fail with nil comparison function")
		require.Nil(t, tree)
	})
}

func TestBPlusTreeBasicOperations(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter := NewStorageAdapter[string, string](ctx, "bptree_basic", s, nil)

	tree, err := NewBPlusTree(4, stringLess, adapter)
	require.NoError(t, err, "Failed to create B+ tree")

	t.Run("EmptyTree", func(t *testing.T) {
		val, found, err := tree.Get("key1")
		require.NoError(t, err, "Should not error when getting from empty tree")
		require.False(t, found, "Should not find key in empty tree")
		require.Equal(t, "", val, "Value should be empty")
	})

	t.Run("InsertAndGet", func(t *testing.T) {
		// Insert a key
		err = tree.Insert("key1", "value1")
		require.NoError(t, err, "Failed to insert key")

		// Get the key
		val, found, err := tree.Get("key1")
		require.NoError(t, err, "Error when getting key")
		require.True(t, found, "Should find inserted key")
		require.Equal(t, "value1", val, "Retrieved value should match inserted value")
	})

	t.Run("Delete", func(t *testing.T) {
		// Delete key
		err = tree.Delete("key1")
		require.NoError(t, err, "Failed to delete key")

		// Verify key was deleted
		val, found, err := tree.Get("key1")
		require.NoError(t, err, "Should not error when getting a deleted key")
		require.False(t, found, "Should not find deleted key")
		require.Equal(t, "", val, "Value should be empty after deletion")
	})

	t.Run("DeleteNonExistentKey", func(t *testing.T) {
		err = tree.Delete("nonexistent")
		require.Error(t, err, "Deleting non-existent key should return error")
		require.Equal(t, ErrKeyNotFound, err, "Should return ErrKeyNotFound")
	})
}

func TestBPlusTreeInsertionWithSplitting(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter := NewStorageAdapter[int, string](ctx, "bptree_split", s, nil)

	// Create a tree with small order to test splitting
	tree, err := NewBPlusTree(2, intLess, adapter)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert keys that will cause leaf splitting
	err = tree.Insert(10, "value10")
	require.NoError(t, err, "Failed to insert key 10")

	err = tree.Insert(20, "value20")
	require.NoError(t, err, "Failed to insert key 20")

	// This should cause a leaf split
	err = tree.Insert(30, "value30")
	require.NoError(t, err, "Failed to insert key 30")

	// Verify all values are accessible
	testCases := []struct {
		key   int
		value string
	}{
		{10, "value10"},
		{20, "value20"},
		{30, "value30"},
	}

	for _, tc := range testCases {
		val, found, err := tree.Get(tc.key)
		require.NoError(t, err, fmt.Sprintf("Error when getting key %d", tc.key))
		require.True(t, found, fmt.Sprintf("Should find inserted key %d", tc.key))
		require.Equal(t, tc.value, val, fmt.Sprintf("Retrieved value should match inserted value for key %d", tc.key))
	}

	// Continue inserting to create internal node splits
	err = tree.Insert(40, "value40")
	require.NoError(t, err, "Failed to insert key 40")

	err = tree.Insert(50, "value50")
	require.NoError(t, err, "Failed to insert key 50")

	err = tree.Insert(60, "value60")
	require.NoError(t, err, "Failed to insert key 60")

	err = tree.Insert(70, "value70")
	require.NoError(t, err, "Failed to insert key 70")

	err = tree.Insert(80, "value80")
	require.NoError(t, err, "Failed to insert key 80")

	// Verify all values after more complex splitting
	for _, tc := range []struct {
		key   int
		value string
	}{
		{10, "value10"},
		{20, "value20"},
		{30, "value30"},
		{40, "value40"},
		{50, "value50"},
		{60, "value60"},
		{70, "value70"},
		{80, "value80"},
	} {
		val, found, err := tree.Get(tc.key)
		require.NoError(t, err, fmt.Sprintf("Error when getting key %d", tc.key))
		require.True(t, found, fmt.Sprintf("Should find inserted key %d", tc.key))
		require.Equal(t, tc.value, val, fmt.Sprintf("Retrieved value should match inserted value for key %d", tc.key))
	}
}

func TestBPlusTreeDelete(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter := NewStorageAdapter[string, int](ctx, "bptree_delete", s, nil)

	tree, err := NewBPlusTree(4, stringLess, adapter)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert keys
	keys := []string{"a", "b", "c", "d", "e"}
	for i, key := range keys {
		err = tree.Insert(key, i+1)
		require.NoError(t, err, "Failed to insert key")
	}

	// Test deleting from the middle
	err = tree.Delete("c")
	require.NoError(t, err, "Failed to delete key")

	// Verify deletion
	_, found, err := tree.Get("c")
	require.NoError(t, err, "Error when getting deleted key")
	require.False(t, found, "Should not find deleted key")

	// Verify remaining keys
	for _, key := range []string{"a", "b", "d", "e"} {
		val, found, err := tree.Get(key)
		require.NoError(t, err, "Error when getting key")
		require.True(t, found, "Should find remaining key")

		// Original index in the keys slice
		expectedVal := 0
		if key == "a" {
			expectedVal = 1
		} else if key == "b" {
			expectedVal = 2
		} else if key == "d" {
			expectedVal = 4
		} else if key == "e" {
			expectedVal = 5
		}

		require.Equal(t, expectedVal, val, "Retrieved value should match expected")
	}

	// Delete first key
	err = tree.Delete("a")
	require.NoError(t, err, "Failed to delete first key")

	// Delete last key
	err = tree.Delete("e")
	require.NoError(t, err, "Failed to delete last key")

	// Verify only "b" and "d" remain
	for _, key := range []string{"b", "d"} {
		_, found, err := tree.Get(key)
		require.NoError(t, err, "Error when getting key")
		require.True(t, found, "Should find remaining key")
	}

	// Verify "a" and "e" are gone
	for _, key := range []string{"a", "e"} {
		_, found, err := tree.Get(key)
		require.NoError(t, err, "Error when getting deleted key")
		require.False(t, found, "Should not find deleted key")
	}
}

func TestBPlusTreeLargeDataSet(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter := NewStorageAdapter[int, string](ctx, "bptree_large", s, nil)

	tree, err := NewBPlusTree(8, intLess, adapter)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert 100 keys
	for i := 1; i <= 100; i++ {
		err = tree.Insert(i, fmt.Sprintf("value%d", i))
		require.NoError(t, err, "Failed to insert key %d", i)
	}

	// Verify all keys exist
	for i := 1; i <= 100; i++ {
		val, found, err := tree.Get(i)
		require.NoError(t, err, "Error when getting key %d", i)
		require.True(t, found, "Should find key %d", i)
		require.Equal(t, fmt.Sprintf("value%d", i), val, "Retrieved value should match for key %d", i)
	}

	// Delete every other key
	for i := 2; i <= 100; i += 2 {
		err = tree.Delete(i)
		require.NoError(t, err, "Failed to delete key %d", i)
	}

	// Verify odd keys exist and even keys don't
	for i := 1; i <= 100; i++ {
		val, found, err := tree.Get(i)
		require.NoError(t, err, "Error when getting key %d", i)

		if i%2 == 1 {
			// Odd keys should exist
			require.True(t, found, "Should find odd key %d", i)
			require.Equal(t, fmt.Sprintf("value%d", i), val, "Retrieved value should match for key %d", i)
		} else {
			// Even keys should be deleted
			require.False(t, found, "Should not find even key %d", i)
			require.Equal(t, "", val, "Value should be empty for deleted key %d", i)
		}
	}
}

func TestBPlusTreeConcurrency(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter := NewStorageAdapter[int, string](ctx, "bptree_concurrent", s, nil)

	tree, err := NewBPlusTree(4, intLess, adapter)
	require.NoError(t, err, "Failed to create B+ tree")

	// Test concurrent reads
	t.Run("ConcurrentReads", func(t *testing.T) {
		// Insert some test data
		err = tree.Insert(1, "value1")
		require.NoError(t, err)

		// Launch multiple goroutines to read concurrently
		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func() {
				val, found, err := tree.Get(1)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, "value1", val)
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < 10; i++ {
			<-done
		}
	})

	// Test concurrent writes
	t.Run("ConcurrentWrites", func(t *testing.T) {
		// Launch multiple goroutines to write concurrently
		done := make(chan bool)
		for i := 0; i < 10; i++ {
			go func(i int) {
				err := tree.Insert(i, fmt.Sprintf("value%d", i))
				require.NoError(t, err)
				done <- true
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < 10; i++ {
			<-done
		}

		// Verify all values were inserted
		for i := 0; i < 10; i++ {
			val, found, err := tree.Get(i)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, fmt.Sprintf("value%d", i), val)
		}
	})
}

func TestBPlusTreeEdgeCases(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter := NewStorageAdapter[int, string](ctx, "bptree_edge", s, nil)

	tree, err := NewBPlusTree(2, intLess, adapter) // Small order to test splits
	require.NoError(t, err, "Failed to create B+ tree")

	t.Run("SplitAtRoot", func(t *testing.T) {
		// Insert keys that will cause root split
		err = tree.Insert(1, "value1")
		require.NoError(t, err)
		err = tree.Insert(2, "value2")
		require.NoError(t, err)
		err = tree.Insert(3, "value3") // This should cause a split
		require.NoError(t, err)

		// Verify all values are accessible
		for i := 1; i <= 3; i++ {
			val, found, err := tree.Get(i)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, fmt.Sprintf("value%d", i), val)
		}
	})

	t.Run("SplitAtLeaf", func(t *testing.T) {
		// Insert more keys to cause leaf splits
		err = tree.Insert(4, "value4")
		require.NoError(t, err)
		err = tree.Insert(5, "value5")
		require.NoError(t, err)
		err = tree.Insert(6, "value6") // This should cause a leaf split
		require.NoError(t, err)

		// Verify all values are accessible
		for i := 1; i <= 6; i++ {
			val, found, err := tree.Get(i)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, fmt.Sprintf("value%d", i), val)
		}
	})
}

// MockStorageAdapter simulates storage errors
type MockStorageAdapter[K comparable, V any] struct {
	*StorageAdapter[K, V]
	shouldFail bool
}

func (m *MockStorageAdapter[K, V]) SaveNode(node *Node[K, V]) error {
	if m.shouldFail {
		return fmt.Errorf("simulated storage error")
	}
	return m.StorageAdapter.SaveNode(node)
}

func (m *MockStorageAdapter[K, V]) LoadNode(id string) (*Node[K, V], error) {
	if m.shouldFail {
		return nil, fmt.Errorf("simulated storage error")
	}
	return m.StorageAdapter.LoadNode(id)
}

func TestBPlusTreeStorageErrors(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	baseAdapter := NewStorageAdapter[string, string](ctx, "bptree_storage", s, nil)
	mockAdapter := &MockStorageAdapter[string, string]{
		StorageAdapter: baseAdapter,
		shouldFail:     false,
	}

	tree, err := NewBPlusTree(4, stringLess, mockAdapter)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert some test data
	err = tree.Insert("key1", "value1")
	require.NoError(t, err)

	t.Run("StorageFailureDuringGet", func(t *testing.T) {
		mockAdapter.shouldFail = true
		_, _, err := tree.Get("key1")
		require.Error(t, err, "Should error when storage fails")
		require.Contains(t, err.Error(), "simulated storage error")
		mockAdapter.shouldFail = false
	})

	t.Run("StorageFailureDuringInsert", func(t *testing.T) {
		mockAdapter.shouldFail = true
		err := tree.Insert("key2", "value2")
		require.Error(t, err, "Should error when storage fails")
		require.Contains(t, err.Error(), "simulated storage error")
		mockAdapter.shouldFail = false
	})

	t.Run("StorageFailureDuringDelete", func(t *testing.T) {
		mockAdapter.shouldFail = true
		err := tree.Delete("key1")
		require.Error(t, err, "Should error when storage fails")
		require.Contains(t, err.Error(), "simulated storage error")
		mockAdapter.shouldFail = false
	})

	t.Run("RecoveryAfterStorageFailure", func(t *testing.T) {
		// Verify tree is still usable after storage errors
		val, found, err := tree.Get("key1")
		require.NoError(t, err, "Should work after storage recovers")
		require.True(t, found, "Should find key after storage recovers")
		require.Equal(t, "value1", val, "Value should be correct after storage recovers")
	})
}
