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

// stringEqual is a comparison function for strings
func stringEqual(a, b string) bool {
	return a == b
}

// intEqual is a comparison function for integers
func intEqual(a, b int) bool {
	return a == b
}

func TestNewBPlusTree(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}

	t.Run("DefaultOrder", func(t *testing.T) {
		adapter, err := NewStorageAdapter[string, string]("bptree_default", s, nil, 100)
		require.NoError(t, err, "Failed to create storage adapter")
		tree, err := NewBPlusTree(ctx, 0, stringLess, stringEqual, adapter)
		require.Error(t, err, "Order must be at least 2")
		require.Nil(t, tree)
	})

	t.Run("CustomOrder", func(t *testing.T) {
		adapter, err := NewStorageAdapter[string, string]("bptree_custom", s, nil, 100)
		require.NoError(t, err, "Failed to create storage adapter")
		tree, err := NewBPlusTree(ctx, 4, stringLess, stringEqual, adapter)
		require.NoError(t, err, "Failed to create B+ tree with custom order")
		require.Equal(t, 4, tree.order)
	})

	t.Run("NilComparisonFunction", func(t *testing.T) {
		adapter, err := NewStorageAdapter[string, string]("bptree_nil_comp", s, nil, 100)
		require.NoError(t, err, "Failed to create storage adapter")
		tree, err := NewBPlusTree(ctx, 4, nil, stringEqual, adapter)
		require.Error(t, err, "Should fail with nil comparison function")
		require.Nil(t, tree)
	})

	t.Run("NilEqualityFunction", func(t *testing.T) {
		adapter, err := NewStorageAdapter[string, string]("bptree_nil_equal", s, nil, 100)
		require.NoError(t, err, "Failed to create storage adapter")
		tree, err := NewBPlusTree(ctx, 4, stringLess, nil, adapter)
		require.Error(t, err, "Should fail with nil equality function")
		require.Nil(t, tree)
	})
}

func TestBPlusTreeBasicOperations(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter, err := NewStorageAdapter[string, string]("bptree_basic", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	tree, err := NewBPlusTree(ctx, 4, stringLess, stringEqual, adapter)
	require.NoError(t, err, "Failed to create B+ tree")

	t.Run("EmptyTree", func(t *testing.T) {
		val, found, err := tree.Get(ctx, "key1")
		require.NoError(t, err, "Should not error when getting from empty tree")
		require.False(t, found, "Should not find key in empty tree")
		require.Empty(t, val, "Value should be empty")
	})

	t.Run("InsertAndGet", func(t *testing.T) {
		// Insert a key
		err = tree.Insert(ctx, "key1", "value1")
		require.NoError(t, err, "Failed to insert key")

		// Get the key
		val, found, err := tree.Get(ctx, "key1")
		require.NoError(t, err, "Error when getting key")
		require.True(t, found, "Should find inserted key")
		require.Equal(t, []string{"value1"}, val, "Retrieved value should match inserted value")
	})

	t.Run("Delete", func(t *testing.T) {
		// Delete key
		err = tree.Delete(ctx, "key1")
		require.NoError(t, err, "Failed to delete key")

		// Verify key was deleted
		val, found, err := tree.Get(ctx, "key1")
		require.NoError(t, err, "Should not error when getting a deleted key")
		require.False(t, found, "Should not find deleted key")
		require.Empty(t, val, "Value should be empty after deletion")
	})

	t.Run("DeleteNonExistentKey", func(t *testing.T) {
		err = tree.Delete(ctx, "nonexistent")
		require.Error(t, err, "Deleting non-existent key should return error")
		require.Equal(t, ErrKeyNotFound, err, "Should return ErrKeyNotFound")
	})
}

func TestBPlusTreeInsertionWithSplitting(t *testing.T) {
	s := &logical.InmemStorage{}
	adapter, err := NewStorageAdapter[int, string]("bptree_split", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	// Create a tree with small order to test splitting
	ctx := context.Background()
	tree, err := NewBPlusTree(ctx, 2, intLess, stringEqual, adapter)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert keys that will cause leaf splitting
	err = tree.Insert(ctx, 10, "value10")
	require.NoError(t, err, "Failed to insert key 10")

	err = tree.Insert(ctx, 20, "value20")
	require.NoError(t, err, "Failed to insert key 20")

	// This should cause a leaf split
	err = tree.Insert(ctx, 30, "value30")
	require.NoError(t, err, "Failed to insert key 30")

	// Verify all values are accessible
	testCases := []struct {
		key   int
		value []string
	}{
		{10, []string{"value10"}},
		{20, []string{"value20"}},
		{30, []string{"value30"}},
	}

	for _, tc := range testCases {
		val, found, err := tree.Get(ctx, tc.key)
		require.NoError(t, err, fmt.Sprintf("Error when getting key %d", tc.key))
		require.True(t, found, fmt.Sprintf("Should find inserted key %d", tc.key))
		require.Equal(t, tc.value, val, fmt.Sprintf("Retrieved value should match inserted value for key %d", tc.key))
	}

	// Continue inserting to create internal node splits
	err = tree.Insert(ctx, 40, "value40")
	require.NoError(t, err, "Failed to insert key 40")

	err = tree.Insert(ctx, 50, "value50")
	require.NoError(t, err, "Failed to insert key 50")

	err = tree.Insert(ctx, 60, "value60")
	require.NoError(t, err, "Failed to insert key 60")

	err = tree.Insert(ctx, 70, "value70")
	require.NoError(t, err, "Failed to insert key 70")

	err = tree.Insert(ctx, 80, "value80")
	require.NoError(t, err, "Failed to insert key 80")

	// Verify all values after more complex splitting
	for _, tc := range []struct {
		key   int
		value []string
	}{
		{10, []string{"value10"}},
		{20, []string{"value20"}},
		{30, []string{"value30"}},
		{40, []string{"value40"}},
		{50, []string{"value50"}},
		{60, []string{"value60"}},
		{70, []string{"value70"}},
		{80, []string{"value80"}},
	} {
		val, found, err := tree.Get(ctx, tc.key)
		require.NoError(t, err, fmt.Sprintf("Error when getting key %d", tc.key))
		require.True(t, found, fmt.Sprintf("Should find inserted key %d", tc.key))
		require.Equal(t, tc.value, val, fmt.Sprintf("Retrieved value should match inserted value for key %d", tc.key))
	}
}

func TestBPlusTreeDelete(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter, err := NewStorageAdapter[string, int]("bptree_delete", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	tree, err := NewBPlusTree(ctx, 4, stringLess, intEqual, adapter)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert keys
	keys := []string{"a", "b", "c", "d", "e"}
	for i, key := range keys {
		err = tree.Insert(ctx, key, i+1)
		require.NoError(t, err, "Failed to insert key")
	}

	// Test deleting from the middle
	err = tree.Delete(ctx, "c")
	require.NoError(t, err, "Failed to delete key")

	// Verify deletion
	_, found, err := tree.Get(ctx, "c")
	require.NoError(t, err, "Error when getting deleted key")
	require.False(t, found, "Should not find deleted key")

	// Verify remaining keys
	for _, key := range []string{"a", "b", "d", "e"} {
		val, found, err := tree.Get(ctx, key)
		require.NoError(t, err, "Error when getting key")
		require.True(t, found, "Should find remaining key")

		// Original index in the keys slice
		expectedVal := []int{}
		if key == "a" {
			expectedVal = []int{1}
		} else if key == "b" {
			expectedVal = []int{2}
		} else if key == "d" {
			expectedVal = []int{4}
		} else if key == "e" {
			expectedVal = []int{5}
		}

		require.Equal(t, expectedVal, val, "Retrieved value should match expected")
	}

	// Delete first key
	err = tree.Delete(ctx, "a")
	require.NoError(t, err, "Failed to delete first key")

	// Delete last key
	err = tree.Delete(ctx, "e")
	require.NoError(t, err, "Failed to delete last key")

	// Verify only "b" and "d" remain
	for _, key := range []string{"b", "d"} {
		_, found, err := tree.Get(ctx, key)
		require.NoError(t, err, "Error when getting key")
		require.True(t, found, "Should find remaining key")
	}

	// Verify "a" and "e" are gone
	for _, key := range []string{"a", "e"} {
		_, found, err := tree.Get(ctx, key)
		require.NoError(t, err, "Error when getting deleted key")
		require.False(t, found, "Should not find deleted key")
	}
}

func TestBPlusTreeLargeDataSet(t *testing.T) {
	s := &logical.InmemStorage{}
	adapter, err := NewStorageAdapter[int, string]("bptree_large", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	ctx := context.Background()
	tree, err := NewBPlusTree(ctx, 8, intLess, stringEqual, adapter)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert 100 keys
	for i := 1; i <= 100; i++ {
		err = tree.Insert(ctx, i, fmt.Sprintf("value%d", i))
		require.NoError(t, err, "Failed to insert key %d", i)
	}

	// Verify all keys exist
	for i := 1; i <= 100; i++ {
		val, found, err := tree.Get(ctx, i)
		require.NoError(t, err, "Error when getting key %d", i)
		require.True(t, found, "Should find key %d", i)
		require.Equal(t, []string{fmt.Sprintf("value%d", i)}, val, "Retrieved value should match for key %d", i)
	}

	// Delete every other key
	for i := 2; i <= 100; i += 2 {
		err = tree.Delete(ctx, i)
		require.NoError(t, err, "Failed to delete key %d", i)
	}

	// Verify odd keys exist and even keys don't
	for i := 1; i <= 100; i++ {
		val, found, err := tree.Get(ctx, i)
		require.NoError(t, err, "Error when getting key %d", i)

		if i%2 == 1 {
			// Odd keys should exist
			require.True(t, found, "Should find odd key %d", i)
			require.Equal(t, []string{fmt.Sprintf("value%d", i)}, val, "Retrieved value should match for key %d", i)
		} else {
			// Even keys should be deleted
			require.False(t, found, "Should not find even key %d", i)
			require.Empty(t, val, "Value should be empty for deleted key %d", i)
		}
	}
}

// func TestBPlusTreeConcurrency(t *testing.T) {
// 	s := &logical.InmemStorage{}
// 	adapter, err := NewStorageAdapter[int, string]("bptree_concurrent", s, nil, 100)
// 	require.NoError(t, err, "Failed to create storage adapter")

// 	ctx := context.Background()
// 	tree, err := NewBPlusTree(ctx, 4, intLess, adapter)
// 	require.NoError(t, err, "Failed to create B+ tree")

// 	// Test concurrent reads
// 	t.Run("ConcurrentReads", func(t *testing.T) {
// 		// Insert some test data
// 		err = tree.Insert(ctx, 1, "value1")
// 		require.NoError(t, err)

// 		// Launch multiple goroutines to read concurrently
// 		done := make(chan bool, 10)
// 		for i := 0; i < 10; i++ {
// 			go func() {
// 				val, found, err := tree.Get(ctx, 1)
// 				require.NoError(t, err)
// 				require.True(t, found)
// 				require.Equal(t, "value1", val)
// 				done <- true
// 			}()
// 		}

// 		// Wait for all goroutines to complete
// 		for i := 0; i < 10; i++ {
// 			<-done
// 		}
// 	})

// 	// Test concurrent writes
// 	t.Run("ConcurrentWrites", func(t *testing.T) {
// 		// Launch multiple goroutines to write concurrently
// 		done := make(chan bool, 10)
// 		for i := 0; i < 10; i++ {
// 			go func(i int) {
// 				err := tree.Insert(ctx, i, fmt.Sprintf("value%d", i))
// 				require.NoError(t, err)
// 				done <- true
// 			}(i)
// 		}

// 		// Wait for all goroutines to complete
// 		for i := 0; i < 10; i++ {
// 			<-done
// 		}

// 		// Verify all values were inserted
// 		for i := 0; i < 10; i++ {
// 			val, found, err := tree.Get(ctx, i)
// 			require.NoError(t, err, "Error when getting key %d", i)
// 			require.True(t, found, "Should find key %d", i)
// 			require.Equal(t, fmt.Sprintf("value%d", i), val, "Retrieved value should match for key %d", i)
// 		}
// 	})

// 	// Test concurrent DeleteValue operations
// 	t.Run("ConcurrentDeleteValue", func(t *testing.T) {
// 		// Insert a key with multiple values
// 		for i := 0; i < 5; i++ {
// 			err = tree.Insert(ctx, 100, fmt.Sprintf("value%d", i))
// 			require.NoError(t, err, "Failed to insert value %d", i)
// 		}

// 		// Verify all values are accessible
// 		values, found, err := tree.Get(ctx, 100)
// 		require.NoError(t, err, "Error when getting key")
// 		require.True(t, found, "Should find inserted key")
// 		require.Len(t, values, 5, "Should have 5 values")

// 		// Launch multiple goroutines to delete values concurrently
// 		done := make(chan bool, 5)
// 		for i := 0; i < 5; i++ {
// 			go func(i int) {
// 				err := tree.DeleteValue(ctx, 100, fmt.Sprintf("value%d", i))
// 				require.NoError(t, err, "Failed to delete value %d", i)
// 				done <- true
// 			}(i)
// 		}

// 		// Wait for all goroutines to complete
// 		for i := 0; i < 5; i++ {
// 			<-done
// 		}

// 		// Verify the key is no longer accessible
// 		_, found, err = tree.Get(ctx, 100)
// 		require.NoError(t, err, "Error when getting key after all values deleted")
// 		require.False(t, found, "Should not find key after all values deleted")
// 	})
// }

func TestBPlusTreeEdgeCases(t *testing.T) {
	s := &logical.InmemStorage{}
	adapter, err := NewStorageAdapter[int, string]("bptree_edge", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	ctx := context.Background()
	tree, err := NewBPlusTree(ctx, 2, intLess, stringEqual, adapter) // Small order to test splits
	require.NoError(t, err, "Failed to create B+ tree")

	t.Run("SplitAtRoot", func(t *testing.T) {
		// Insert keys that will cause root split
		err = tree.Insert(ctx, 1, "value1")
		require.NoError(t, err)
		err = tree.Insert(ctx, 2, "value2")
		require.NoError(t, err)
		err = tree.Insert(ctx, 3, "value3") // This should cause a split
		require.NoError(t, err)

		// Verify all values are accessible
		for i := 1; i <= 3; i++ {
			val, found, err := tree.Get(ctx, i)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []string{fmt.Sprintf("value%d", i)}, val)
		}
	})

	t.Run("SplitAtLeaf", func(t *testing.T) {
		// Insert more keys to cause leaf splits
		err = tree.Insert(ctx, 4, "value4")
		require.NoError(t, err)
		err = tree.Insert(ctx, 5, "value5")
		require.NoError(t, err)
		err = tree.Insert(ctx, 6, "value6") // This should cause a leaf split
		require.NoError(t, err)

		// Verify all values are accessible
		for i := 1; i <= 6; i++ {
			val, found, err := tree.Get(ctx, i)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []string{fmt.Sprintf("value%d", i)}, val)
		}
	})
}

// MockStorageAdapter simulates storage errors
type MockStorageAdapter[K comparable, V any] struct {
	*StorageAdapter[K, V]
	shouldFail bool
}

func (m *MockStorageAdapter[K, V]) SaveNode(ctx context.Context, node *Node[K, V]) error {
	if m.shouldFail {
		return fmt.Errorf("simulated storage error")
	}
	return m.StorageAdapter.SaveNode(ctx, node)
}

func (m *MockStorageAdapter[K, V]) LoadNode(ctx context.Context, id string) (*Node[K, V], error) {
	if m.shouldFail {
		return nil, fmt.Errorf("simulated storage error")
	}
	return m.StorageAdapter.LoadNode(ctx, id)
}

func TestBPlusTreeStorageErrors(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	baseAdapter, err := NewStorageAdapter[string, string]("bptree_storage", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")
	mockAdapter := &MockStorageAdapter[string, string]{
		StorageAdapter: baseAdapter,
		shouldFail:     false,
	}

	tree, err := NewBPlusTree(ctx, 4, stringLess, stringEqual, mockAdapter)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert some test data
	err = tree.Insert(ctx, "key1", "value1")
	require.NoError(t, err)

	t.Run("StorageFailureDuringGet", func(t *testing.T) {
		mockAdapter.shouldFail = true
		_, _, err := tree.Get(ctx, "key1")
		require.Error(t, err, "Should error when storage fails")
		require.Contains(t, err.Error(), "simulated storage error")
		mockAdapter.shouldFail = false
	})

	t.Run("StorageFailureDuringInsert", func(t *testing.T) {
		mockAdapter.shouldFail = true
		err := tree.Insert(ctx, "key2", "value2")
		require.Error(t, err, "Should error when storage fails")
		require.Contains(t, err.Error(), "simulated storage error")
		mockAdapter.shouldFail = false
	})

	t.Run("StorageFailureDuringDelete", func(t *testing.T) {
		mockAdapter.shouldFail = true
		err := tree.Delete(ctx, "key1")
		require.Error(t, err, "Should error when storage fails")
		require.Contains(t, err.Error(), "simulated storage error")
		mockAdapter.shouldFail = false
	})

	t.Run("StorageFailureDuringDeleteValue", func(t *testing.T) {
		// Insert a key with multiple values
		err = tree.Insert(ctx, "key3", "value1")
		require.NoError(t, err)
		err = tree.Insert(ctx, "key3", "value2")
		require.NoError(t, err)

		mockAdapter.shouldFail = true
		err = tree.DeleteValue(ctx, "key3", "value1")
		require.Error(t, err, "Should error when storage fails")
		require.Contains(t, err.Error(), "simulated storage error")
		mockAdapter.shouldFail = false
	})

	t.Run("RecoveryAfterStorageFailure", func(t *testing.T) {
		// Verify tree is still usable after storage errors
		val, found, err := tree.Get(ctx, "key1")
		require.NoError(t, err, "Should work after storage recovers")
		require.True(t, found, "Should find key after storage recovers")
		require.Equal(t, []string{"value1"}, val, "Value should be correct after storage recovers")
	})
}

func TestBPlusTreeWithTransaction(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter, err := NewStorageAdapter[string, string]("bptree_with_transaction", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	tree, err := NewBPlusTree(ctx, 4, stringLess, stringEqual, adapter)
	require.NoError(t, err, "Failed to create B+ tree")

	t.Run("TransactionalInsert", func(t *testing.T) {
		err := tree.WithTransaction(ctx, func() error {
			err := tree.Insert(ctx, "key1", "value1")
			require.NoError(t, err, "Failed to insert key")

			err = tree.Insert(ctx, "key2", "value2")
			require.NoError(t, err, "Failed to insert key")

			err = tree.Insert(ctx, "key3", "value3")
			require.NoError(t, err, "Failed to insert key")
			return nil
		})
		require.NoError(t, err, "Failed to insert key")
	})

	t.Run("TransactionalDeleteValue", func(t *testing.T) {
		// First insert a key with multiple values
		err = tree.Insert(ctx, "key4", "value1")
		require.NoError(t, err, "Failed to insert first value")
		err = tree.Insert(ctx, "key4", "value2")
		require.NoError(t, err, "Failed to insert second value")
		err = tree.Insert(ctx, "key4", "value3")
		require.NoError(t, err, "Failed to insert third value")

		// Verify all values are accessible
		values, found, err := tree.Get(ctx, "key4")
		require.NoError(t, err, "Error when getting key")
		require.True(t, found, "Should find inserted key")
		require.Equal(t, []string{"value1", "value2", "value3"}, values, "Retrieved values should match inserted values")

		// Delete a value within a transaction
		err = tree.WithTransaction(ctx, func() error {
			err := tree.DeleteValue(ctx, "key4", "value2")
			require.NoError(t, err, "Failed to delete value within transaction")
			return nil
		})
		require.NoError(t, err, "Transaction should complete successfully")

		// Verify the value was deleted
		values, found, err = tree.Get(ctx, "key4")
		require.NoError(t, err, "Error when getting key after transaction")
		require.True(t, found, "Should still find key after value deletion")
		require.Equal(t, []string{"value1", "value3"}, values, "Retrieved values should not include deleted value")
	})
}

func TestBPlusTreeDeleteValue(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter, err := NewStorageAdapter[string, string]("bptree_delete_value", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	tree, err := NewBPlusTree(ctx, 4, stringLess, stringEqual, adapter)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert a key with multiple values
	err = tree.Insert(ctx, "key1", "value1")
	require.NoError(t, err, "Failed to insert first value")
	err = tree.Insert(ctx, "key1", "value2")
	require.NoError(t, err, "Failed to insert second value")
	err = tree.Insert(ctx, "key1", "value3")
	require.NoError(t, err, "Failed to insert third value")

	// Verify all values are accessible
	values, found, err := tree.Get(ctx, "key1")
	require.NoError(t, err, "Error when getting key")
	require.True(t, found, "Should find inserted key")
	require.Equal(t, []string{"value1", "value2", "value3"}, values, "Retrieved values should match inserted values")

	// Delete a specific value
	err = tree.DeleteValue(ctx, "key1", "value2")
	require.NoError(t, err, "Failed to delete value")

	// Verify the value was deleted
	values, found, err = tree.Get(ctx, "key1")
	require.NoError(t, err, "Error when getting key after deletion")
	require.True(t, found, "Should still find key after value deletion")
	require.Equal(t, []string{"value1", "value3"}, values, "Retrieved values should not include deleted value")

	// Delete another value
	err = tree.DeleteValue(ctx, "key1", "value1")
	require.NoError(t, err, "Failed to delete second value")

	// Verify the value was deleted
	values, found, err = tree.Get(ctx, "key1")
	require.NoError(t, err, "Error when getting key after second deletion")
	require.True(t, found, "Should still find key after second value deletion")
	require.Equal(t, []string{"value3"}, values, "Retrieved values should only include remaining value")

	// Delete the last value
	err = tree.DeleteValue(ctx, "key1", "value3")
	require.NoError(t, err, "Failed to delete last value")

	// Verify the key is no longer accessible
	_, found, err = tree.Get(ctx, "key1")
	require.NoError(t, err, "Error when getting key after all values deleted")
	require.False(t, found, "Should not find key after all values deleted")

	// Try to delete a non-existent value
	err = tree.DeleteValue(ctx, "key1", "nonexistent")
	require.Error(t, err, "Deleting non-existent value should return error")
	require.Equal(t, ErrKeyNotFound, err, "Should return ErrKeyNotFound")

	// Try to delete a value from a non-existent key
	err = tree.DeleteValue(ctx, "nonexistent", "value1")
	require.Error(t, err, "Deleting value from non-existent key should return error")
	require.Equal(t, ErrKeyNotFound, err, "Should return ErrKeyNotFound")
}

func TestBPlusTreeDuplicateValues(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter, err := NewStorageAdapter[string, string]("bptree_duplicate", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	tree, err := NewBPlusTree(ctx, 4, stringLess, stringEqual, adapter)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert initial values
	err = tree.Insert(ctx, "key1", "value1")
	require.NoError(t, err)
	err = tree.Insert(ctx, "key1", "value2")
	require.NoError(t, err)

	// Try to insert duplicate values
	err = tree.Insert(ctx, "key1", "value1")
	require.NoError(t, err) // Should not error, but should not add duplicate
	err = tree.Insert(ctx, "key1", "value2")
	require.NoError(t, err) // Should not error, but should not add duplicate

	// Verify values
	values, exists, err := tree.Get(ctx, "key1")
	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, values, 2)
	require.Contains(t, values, "value1")
	require.Contains(t, values, "value2")

	// Insert a new value
	err = tree.Insert(ctx, "key1", "value3")
	require.NoError(t, err)

	// Verify values again
	values, exists, err = tree.Get(ctx, "key1")
	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, values, 3)
	require.Contains(t, values, "value1")
	require.Contains(t, values, "value2")
	require.Contains(t, values, "value3")
}
