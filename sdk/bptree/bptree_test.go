package bptree

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/openbao/openbao/sdk/v2/logical"
	"github.com/stretchr/testify/require"
)

func TestNewBPlusTree(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}

	t.Run("DefaultOrder", func(t *testing.T) {
		storage, err := NewNodeStorage("bptree_default", s, nil, 100)
		require.NoError(t, err, "Failed to create storage storage")
		tree, err := NewBPlusTree(ctx, 0, storage)
		require.Error(t, err, "Order must be at least 2")
		require.Nil(t, tree)
	})

	t.Run("CustomOrder", func(t *testing.T) {
		storage, err := NewNodeStorage("bptree_custom", s, nil, 100)
		require.NoError(t, err, "Failed to create storage storage")
		tree, err := NewBPlusTree(ctx, 4, storage)
		require.NoError(t, err, "Failed to create B+ tree with custom order")
		require.Equal(t, 4, tree.order)
	})
}

func TestBPlusTreeBasicOperations(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_basic", s, nil, 100)
	require.NoError(t, err, "Failed to create storage storage")

	tree, err := NewBPlusTree(ctx, 4, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	t.Run("EmptyTree", func(t *testing.T) {
		val, found, err := tree.Get(ctx, storage, "key1")
		require.NoError(t, err, "Should not error when getting from empty tree")
		require.False(t, found, "Should not find key in empty tree")
		require.Empty(t, val, "Value should be empty")
	})

	t.Run("InsertAndGet", func(t *testing.T) {
		// Insert a key
		err = tree.Insert(ctx, storage, "key1", "value1")
		require.NoError(t, err, "Failed to insert key")

		// Get the key
		val, found, err := tree.Get(ctx, storage, "key1")
		require.NoError(t, err, "Error when getting key")
		require.True(t, found, "Should find inserted key")
		require.Equal(t, []string{"value1"}, val, "Retrieved value should match inserted value")
	})

	t.Run("Delete", func(t *testing.T) {
		// Delete key
		err = tree.Delete(ctx, storage, "key1")
		require.NoError(t, err, "Failed to delete key")

		// Verify key was deleted
		val, found, err := tree.Get(ctx, storage, "key1")
		require.NoError(t, err, "Should not error when getting a deleted key")
		require.False(t, found, "Should not find deleted key")
		require.Empty(t, val, "Value should be empty after deletion")
	})

	t.Run("DeleteNonExistentKey", func(t *testing.T) {
		err = tree.Delete(ctx, storage, "nonexistent")
		require.Error(t, err, "Deleting non-existent key should return error")
		require.Equal(t, ErrKeyNotFound, err, "Should return ErrKeyNotFound")
	})
}

func TestBPlusTreeInsertionWithSplitting(t *testing.T) {
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_split", s, nil, 100)
	require.NoError(t, err, "Failed to create storage storage")

	// Create a tree with small order to test splitting
	ctx := context.Background()
	tree, err := NewBPlusTree(ctx, 2, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert keys that will cause leaf splitting
	err = tree.Insert(ctx, storage, "10", "value10")
	require.NoError(t, err, "Failed to insert key 10")

	err = tree.Insert(ctx, storage, "20", "value20")
	require.NoError(t, err, "Failed to insert key 20")

	// This should cause a leaf split
	err = tree.Insert(ctx, storage, "30", "value30")
	require.NoError(t, err, "Failed to insert key 30")

	// Verify all values are accessible
	testCases := []struct {
		key   string
		value []string
	}{
		{"10", []string{"value10"}},
		{"20", []string{"value20"}},
		{"30", []string{"value30"}},
	}

	for _, tc := range testCases {
		val, found, err := tree.Get(ctx, storage, tc.key)
		require.NoError(t, err, fmt.Sprintf("Error when getting key %v", tc.key))
		require.True(t, found, fmt.Sprintf("Should find inserted key %v", tc.key))
		require.Equal(t, tc.value, val, fmt.Sprintf("Retrieved value should match inserted value for key %v", tc.key))
	}

	// Continue inserting to create internal node splits
	err = tree.Insert(ctx, storage, "40", "value40")
	require.NoError(t, err, "Failed to insert key 40")

	err = tree.Insert(ctx, storage, "50", "value50")
	require.NoError(t, err, "Failed to insert key 50")

	err = tree.Insert(ctx, storage, "60", "value60")
	require.NoError(t, err, "Failed to insert key 60")

	err = tree.Insert(ctx, storage, "70", "value70")
	require.NoError(t, err, "Failed to insert key 70")

	err = tree.Insert(ctx, storage, "80", "value80")
	require.NoError(t, err, "Failed to insert key 80")

	// Verify all values after more complex splitting
	for _, tc := range []struct {
		key   string
		value []string
	}{
		{"10", []string{"value10"}},
		{"20", []string{"value20"}},
		{"30", []string{"value30"}},
		{"40", []string{"value40"}},
		{"50", []string{"value50"}},
		{"60", []string{"value60"}},
		{"70", []string{"value70"}},
		{"80", []string{"value80"}},
	} {
		val, found, err := tree.Get(ctx, storage, tc.key)
		require.NoError(t, err, fmt.Sprintf("Error when getting key %v", tc.key))
		require.True(t, found, fmt.Sprintf("Should find inserted key %v", tc.key))
		require.Equal(t, tc.value, val, fmt.Sprintf("Retrieved value should match inserted value for key %v", tc.key))
	}
}

func TestBPlusTreeDelete(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_delete", s, nil, 100)
	require.NoError(t, err, "Failed to create storage storage")

	tree, err := NewBPlusTree(ctx, 4, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert keys
	keys := []string{"a", "b", "c", "d", "e"}
	for i, key := range keys {
		err = tree.Insert(ctx, storage, key, strconv.Itoa(i+1))
		require.NoError(t, err, "Failed to insert key")
	}

	// Test deleting from the middle
	err = tree.Delete(ctx, storage, "c")
	require.NoError(t, err, "Failed to delete key")

	// Verify deletion
	_, found, err := tree.Get(ctx, storage, "c")
	require.NoError(t, err, "Error when getting deleted key")
	require.False(t, found, "Should not find deleted key")

	// Verify remaining keys
	for _, key := range []string{"a", "b", "d", "e"} {
		val, found, err := tree.Get(ctx, storage, key)
		require.NoError(t, err, "Error when getting key")
		require.True(t, found, "Should find remaining key")

		// Original index in the keys slice
		expectedVal := []string{}
		if key == "a" {
			expectedVal = []string{"1"}
		} else if key == "b" {
			expectedVal = []string{"2"}
		} else if key == "d" {
			expectedVal = []string{"4"}
		} else if key == "e" {
			expectedVal = []string{"5"}
		}

		require.Equal(t, expectedVal, val, "Retrieved value should match expected")
	}

	// Delete first key
	err = tree.Delete(ctx, storage, "a")
	require.NoError(t, err, "Failed to delete first key")

	// Delete last key
	err = tree.Delete(ctx, storage, "e")
	require.NoError(t, err, "Failed to delete last key")

	// Verify only "b" and "d" remain
	for _, key := range []string{"b", "d"} {
		_, found, err := tree.Get(ctx, storage, key)
		require.NoError(t, err, "Error when getting key")
		require.True(t, found, "Should find remaining key")
	}

	// Verify "a" and "e" are gone
	for _, key := range []string{"a", "e"} {
		_, found, err := tree.Get(ctx, storage, key)
		require.NoError(t, err, "Error when getting deleted key")
		require.False(t, found, "Should not find deleted key")
	}
}

func TestBPlusTreeLargeDataSet(t *testing.T) {
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_large", s, nil, 1000)
	require.NoError(t, err, "Failed to create storage storage")

	ctx := context.Background()
	tree, err := NewBPlusTree(ctx, 32, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	const numKeys = 10000

	// Generate a pseudo-random but deterministic sequence of keys using a simple hash
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		// Use a simple permutation: (i*7919 + 104729) % 1000003
		k := (i*7919 + 104729) % 1000003
		keys[i] = strconv.Itoa(k)
	}

	// Insert all keys
	for i, key := range keys {
		err = tree.Insert(ctx, storage, key, fmt.Sprintf("value%d", i))
		require.NoError(t, err, "Failed to insert key %s", key)
	}

	// Verify all keys exist
	for i, key := range keys {
		val, found, err := tree.Get(ctx, storage, key)
		require.NoError(t, err, "Error when getting key %s", key)
		require.True(t, found, "Should find key %s", key)
		require.Equal(t, []string{fmt.Sprintf("value%d", i)}, val, "Retrieved value should match for key %s", key)
	}

	// Delete every other key (even indices)
	for i := 0; i < numKeys; i += 2 {
		err = tree.Delete(ctx, storage, keys[i])
		require.NoError(t, err, "Failed to delete key %s", keys[i])
	}

	// Verify odd-indexed keys exist and even-indexed keys don't
	for i, key := range keys {
		val, found, err := tree.Get(ctx, storage, key)
		require.NoError(t, err, "Error when getting key %s", key)

		if i%2 == 1 {
			// Odd-indexed keys should exist
			require.True(t, found, "Should find odd-indexed key %s", key)
			require.Equal(t, []string{fmt.Sprintf("value%d", i)}, val, "Retrieved value should match for key %s", key)
		} else {
			// Even-indexed keys should be deleted
			require.False(t, found, "Should not find even-indexed key %s", key)
			require.Empty(t, val, "Value should be empty for deleted key %s", key)
		}
	}
}

func TestBPlusTreeConcurrency(t *testing.T) {
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_concurrent", s, nil, 100)
	require.NoError(t, err, "Failed to create storage storage")

	ctx := context.Background()
	tree, err := NewBPlusTree(ctx, 4, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	// Test concurrent reads
	t.Run("ConcurrentReads", func(t *testing.T) {
		// Insert some test data
		err = tree.Insert(ctx, storage, "1", "value1")
		require.NoError(t, err)

		var wg sync.WaitGroup
		errChan := make(chan error, 10)

		// Launch multiple goroutines to read concurrently
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()

				val, found, err := tree.Get(ctx, storage, "1")
				if err != nil {
					errChan <- fmt.Errorf("error getting value: %w", err)
					return
				}
				if !found {
					errChan <- fmt.Errorf("value not found")
					return
				}
				if !reflect.DeepEqual(val, []string{"value1"}) {
					errChan <- fmt.Errorf("expected value %v, got %v", []string{"value1"}, val)
					return
				}
			}()
		}

		// Wait for all goroutines to complete with a timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// All goroutines completed
		case <-time.After(5 * time.Second):
			t.Fatal("test timed out waiting for goroutines to complete")
		}

		close(errChan)

		// Check for errors
		for err := range errChan {
			t.Error(err)
		}
	})

	// Test concurrent writes
	t.Run("ConcurrentWrites", func(t *testing.T) {
		var wg sync.WaitGroup
		errChan := make(chan error, 10)

		// Launch multiple goroutines to write concurrently
		for i := range 10 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				err := tree.Insert(ctx, storage, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
				if err != nil {
					errChan <- fmt.Errorf("error inserting key %d: %w", i, err)
					return
				}
			}(i)
		}

		// Wait for all goroutines to complete with a timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// All goroutines completed
		case <-time.After(5 * time.Second):
			t.Fatal("test timed out waiting for goroutines to complete")
		}

		close(errChan)

		// Check for errors
		for err := range errChan {
			t.Error(err)
		}

		// Verify all values were inserted
		for i := range 10 {
			val, found, err := tree.Get(ctx, storage, fmt.Sprintf("key%d", i))
			require.NoError(t, err, "Error when getting key %d", i)
			require.True(t, found, "Should find key %d", i)
			require.Equal(t, []string{fmt.Sprintf("value%d", i)}, val, "Retrieved value should match for key %d", i)
		}
	})

	// Test concurrent DeleteValue operations
	t.Run("ConcurrentDeleteValue", func(t *testing.T) {
		// Insert a key with multiple values
		for i := range 5 {
			err = tree.Insert(ctx, storage, "100", fmt.Sprintf("value%d", i))
			require.NoError(t, err, "Failed to insert value %d", i)
		}

		// Verify all values are accessible
		values, found, err := tree.Get(ctx, storage, "100")
		require.NoError(t, err, "Error when getting key")
		require.True(t, found, "Should find inserted key")
		require.Len(t, values, 5, "Should have 5 values")

		var wg sync.WaitGroup
		errChan := make(chan error, 5)

		// Launch multiple goroutines to delete values concurrently
		for i := range 5 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				err := tree.DeleteValue(ctx, storage, "100", fmt.Sprintf("value%d", i))
				if err != nil {
					errChan <- fmt.Errorf("error deleting value %d: %w", i, err)
					return
				}
			}(i)
		}

		// Wait for all goroutines to complete with a timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// All goroutines completed
		case <-time.After(5 * time.Second):
			t.Fatal("test timed out waiting for goroutines to complete")
		}

		close(errChan)

		// Check for errors
		for err := range errChan {
			t.Error(err)
		}

		// Verify the key is no longer accessible
		_, found, err = tree.Get(ctx, storage, "100")
		require.NoError(t, err, "Error when getting key after all values deleted")
		require.False(t, found, "Should not find key after all values deleted")
	})
}

func TestBPlusTreeEdgeCases(t *testing.T) {
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_edge", s, nil, 100)
	require.NoError(t, err, "Failed to create storage storage")

	ctx := context.Background()
	tree, err := NewBPlusTree(ctx, 2, storage) // Small order to test splits
	require.NoError(t, err, "Failed to create B+ tree")

	t.Run("SplitAtRoot", func(t *testing.T) {
		// Insert keys that will cause root split
		err = tree.Insert(ctx, storage, "1", "value1")
		require.NoError(t, err)
		err = tree.Insert(ctx, storage, "2", "value2")
		require.NoError(t, err)
		err = tree.Insert(ctx, storage, "3", "value3") // This should cause a split
		require.NoError(t, err)

		// Verify all values are accessible
		for i := 1; i <= 3; i++ {
			val, found, err := tree.Get(ctx, storage, strconv.Itoa(i))
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []string{fmt.Sprintf("value%d", i)}, val)
		}
	})

	t.Run("SplitAtLeaf", func(t *testing.T) {
		// Insert more keys to cause leaf splits
		err = tree.Insert(ctx, storage, "4", "value4")
		require.NoError(t, err)
		err = tree.Insert(ctx, storage, "5", "value5")
		require.NoError(t, err)
		err = tree.Insert(ctx, storage, "6", "value6") // This should cause a leaf split
		require.NoError(t, err)

		// Verify all values are accessible
		for i := 1; i <= 6; i++ {
			val, found, err := tree.Get(ctx, storage, strconv.Itoa(i))
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []string{fmt.Sprintf("value%d", i)}, val)
		}
	})
}

// MockStoragestorage simulates storage errors
type MockStoragestorage struct {
	*NodeStorage
	shouldFail bool
}

func (m *MockStoragestorage) SaveNode(ctx context.Context, node *Node) error {
	if m.shouldFail {
		return fmt.Errorf("simulated storage error")
	}
	return m.NodeStorage.SaveNode(ctx, node)
}

func (m *MockStoragestorage) LoadNode(ctx context.Context, id string) (*Node, error) {
	if m.shouldFail {
		return nil, fmt.Errorf("simulated storage error")
	}
	return m.NodeStorage.LoadNode(ctx, id)
}

func TestBPlusTreeStorageErrors(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	basestorage, err := NewNodeStorage("bptree_storage", s, nil, 100)
	require.NoError(t, err, "Failed to create storage storage")
	mockstorage := &MockStoragestorage{
		NodeStorage: basestorage,
		shouldFail:  false,
	}

	tree, err := NewBPlusTree(ctx, 4, mockstorage)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert some test data
	err = tree.Insert(ctx, mockstorage, "key1", "value1")
	require.NoError(t, err)

	t.Run("StorageFailureDuringGet", func(t *testing.T) {
		mockstorage.shouldFail = true
		_, _, err := tree.Get(ctx, mockstorage, "key1")
		require.Error(t, err, "Should error when storage fails")
		require.Contains(t, err.Error(), "simulated storage error")
		mockstorage.shouldFail = false
	})

	t.Run("StorageFailureDuringInsert", func(t *testing.T) {
		mockstorage.shouldFail = true
		err := tree.Insert(ctx, mockstorage, "key2", "value2")
		require.Error(t, err, "Should error when storage fails")
		require.Contains(t, err.Error(), "simulated storage error")
		mockstorage.shouldFail = false
	})

	t.Run("StorageFailureDuringDelete", func(t *testing.T) {
		mockstorage.shouldFail = true
		err := tree.Delete(ctx, mockstorage, "key1")
		require.Error(t, err, "Should error when storage fails")
		require.Contains(t, err.Error(), "simulated storage error")
		mockstorage.shouldFail = false
	})

	t.Run("StorageFailureDuringDeleteValue", func(t *testing.T) {
		// Insert a key with multiple values
		err = tree.Insert(ctx, mockstorage, "key3", "value1")
		require.NoError(t, err)
		err = tree.Insert(ctx, mockstorage, "key3", "value2")
		require.NoError(t, err)

		mockstorage.shouldFail = true
		err = tree.DeleteValue(ctx, mockstorage, "key3", "value1")
		require.Error(t, err, "Should error when storage fails")
		require.Contains(t, err.Error(), "simulated storage error")
		mockstorage.shouldFail = false
	})

	t.Run("RecoveryAfterStorageFailure", func(t *testing.T) {
		// Verify tree is still usable after storage errors
		val, found, err := tree.Get(ctx, mockstorage, "key1")
		require.NoError(t, err, "Should work after storage recovers")
		require.True(t, found, "Should find key after storage recovers")
		require.Equal(t, []string{"value1"}, val, "Value should be correct after storage recovers")
	})
}

func TestBPlusTreeDeleteValue(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_delete_value", s, nil, 100)
	require.NoError(t, err, "Failed to create storage storage")

	tree, err := NewBPlusTree(ctx, 4, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert a key with multiple values
	err = tree.Insert(ctx, storage, "key1", "value1")
	require.NoError(t, err, "Failed to insert first value")
	err = tree.Insert(ctx, storage, "key1", "value2")
	require.NoError(t, err, "Failed to insert second value")
	err = tree.Insert(ctx, storage, "key1", "value3")
	require.NoError(t, err, "Failed to insert third value")

	// Verify all values are accessible
	values, found, err := tree.Get(ctx, storage, "key1")
	require.NoError(t, err, "Error when getting key")
	require.True(t, found, "Should find inserted key")
	require.Equal(t, []string{"value1", "value2", "value3"}, values, "Retrieved values should match inserted values")

	// Delete a specific value
	err = tree.DeleteValue(ctx, storage, "key1", "value2")
	require.NoError(t, err, "Failed to delete value")

	// Verify the value was deleted
	values, found, err = tree.Get(ctx, storage, "key1")
	require.NoError(t, err, "Error when getting key after deletion")
	require.True(t, found, "Should still find key after value deletion")
	require.Equal(t, []string{"value1", "value3"}, values, "Retrieved values should not include deleted value")

	// Delete another value
	err = tree.DeleteValue(ctx, storage, "key1", "value1")
	require.NoError(t, err, "Failed to delete second value")

	// Verify the value was deleted
	values, found, err = tree.Get(ctx, storage, "key1")
	require.NoError(t, err, "Error when getting key after second deletion")
	require.True(t, found, "Should still find key after second value deletion")
	require.Equal(t, []string{"value3"}, values, "Retrieved values should only include remaining value")

	// Delete the last value
	err = tree.DeleteValue(ctx, storage, "key1", "value3")
	require.NoError(t, err, "Failed to delete last value")

	// Verify the key is no longer accessible
	_, found, err = tree.Get(ctx, storage, "key1")
	require.NoError(t, err, "Error when getting key after all values deleted")
	require.False(t, found, "Should not find key after all values deleted")

	// Try to delete a non-existent value
	err = tree.DeleteValue(ctx, storage, "key1", "nonexistent")
	require.Error(t, err, "Deleting non-existent value should return error")
	require.Equal(t, ErrKeyNotFound, err, "Should return ErrKeyNotFound")

	// Try to delete a value from a non-existent key
	err = tree.DeleteValue(ctx, storage, "nonexistent", "value1")
	require.Error(t, err, "Deleting value from non-existent key should return error")
	require.Equal(t, ErrKeyNotFound, err, "Should return ErrKeyNotFound")
}

func TestBPlusTreeDuplicateValues(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_duplicate", s, nil, 100)
	require.NoError(t, err, "Failed to create storage storage")

	tree, err := NewBPlusTree(ctx, 4, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert initial values
	err = tree.Insert(ctx, storage, "key1", "value1")
	require.NoError(t, err)
	err = tree.Insert(ctx, storage, "key1", "value2")
	require.NoError(t, err)

	// Try to insert duplicate values
	err = tree.Insert(ctx, storage, "key1", "value1")
	require.NoError(t, err) // Should not error, but should not add duplicate
	err = tree.Insert(ctx, storage, "key1", "value2")
	require.NoError(t, err) // Should not error, but should not add duplicate

	// Verify values
	values, exists, err := tree.Get(ctx, storage, "key1")
	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, values, 2)
	require.Contains(t, values, "value1")
	require.Contains(t, values, "value2")

	// Insert a new value
	err = tree.Insert(ctx, storage, "key1", "value3")
	require.NoError(t, err)

	// Verify values again
	values, exists, err = tree.Get(ctx, storage, "key1")
	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, values, 3)
	require.Contains(t, values, "value1")
	require.Contains(t, values, "value2")
	require.Contains(t, values, "value3")
}

// TestNextIDLinking tests that the NextID field is properly set during leaf operations
func TestNextIDLinking(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_nextid", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	// Use a small order to force splits
	tree, err := NewBPlusTree(ctx, 2, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	t.Run("SingleLeafNode", func(t *testing.T) {
		// Insert into root leaf - should have empty NextID initially
		err = tree.Insert(ctx, storage, "key1", "value1")
		require.NoError(t, err, "Failed to insert key1")

		root, err := tree.getRoot(ctx, storage)
		require.NoError(t, err, "Failed to get root")
		require.True(t, root.IsLeaf, "Root should be a leaf")
		require.Empty(t, root.NextID, "Single leaf should have empty NextID")
	})

	t.Run("LeafSplitCreatesTwoLinkedLeaves", func(t *testing.T) {
		// Insert more keys to force a leaf split
		err = tree.Insert(ctx, storage, "key2", "value2")
		require.NoError(t, err, "Failed to insert key2")

		// This should cause a leaf split (order=2, so max 2 keys per leaf)
		err = tree.Insert(ctx, storage, "key3", "value3")
		require.NoError(t, err, "Failed to insert key3")

		// Find the leftmost leaf
		leftmost, err := tree.findLeftmostLeaf(ctx, storage)
		require.NoError(t, err, "Failed to find leftmost leaf")

		// Verify the leaf has a NextID
		require.NotEmpty(t, leftmost.NextID, "Leftmost leaf should have NextID after split")

		// Load the next leaf
		rightLeaf, err := storage.LoadNode(ctx, leftmost.NextID)
		require.NoError(t, err, "Failed to load right leaf")
		require.True(t, rightLeaf.IsLeaf, "Next node should be a leaf")

		// The rightmost leaf should have empty NextID
		require.Empty(t, rightLeaf.NextID, "Rightmost leaf should have empty NextID")

		// Verify keys are properly distributed
		allKeys := append(leftmost.Keys, rightLeaf.Keys...)
		require.ElementsMatch(t, []string{"key1", "key2", "key3"}, allKeys, "All keys should be present across leaves")
	})

	t.Run("SequentialTraversalWorks", func(t *testing.T) {
		// Insert more keys to create multiple leaf splits
		for i := 4; i <= 10; i++ {
			err = tree.Insert(ctx, storage, "key"+string(rune('0'+i)), "value"+string(rune('0'+i)))
			require.NoError(t, err, "Failed to insert key%d", i)
		}

		// Traverse through all leaves using NextID
		var allKeys []string
		current, err := tree.findLeftmostLeaf(ctx, storage)
		require.NoError(t, err, "Failed to find leftmost leaf")

		for current != nil {
			allKeys = append(allKeys, current.Keys...)
			if current.NextID == "" {
				break
			}
			current, err = storage.LoadNode(ctx, current.NextID)
			require.NoError(t, err, "Failed to load next leaf")
		}

		// Verify all keys are present and in order
		expectedKeys := []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key:"}
		require.Equal(t, expectedKeys, allKeys, "Keys should be in sorted order through leaf traversal")
	})
}

// TestSearchPrefix tests the SearchPrefix functionality
func TestSearchPrefix(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_prefix", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	tree, err := NewBPlusTree(ctx, 4, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert keys with various prefixes
	keys := []string{
		"app/config/db",
		"app/config/redis",
		"app/secrets/api_key",
		"app/secrets/jwt_secret",
		"auth/users/alice",
		"auth/users/bob",
		"auth/roles/admin",
		"system/health",
		"system/version",
	}

	for _, key := range keys {
		err = tree.Insert(ctx, storage, key, "value_"+key)
		require.NoError(t, err, "Failed to insert %s", key)
	}

	// Test basic prefix search
	results, err := tree.SearchPrefix(ctx, storage, "app/")
	require.NoError(t, err, "SearchPrefix failed")
	require.Len(t, results, 4, "Should find 4 keys with 'app/' prefix")
	require.Contains(t, results, "app/config/db")
	require.Contains(t, results, "app/config/redis")
	require.Contains(t, results, "app/secrets/api_key")
	require.Contains(t, results, "app/secrets/jwt_secret")

	// Test more specific prefix
	results, err = tree.SearchPrefix(ctx, storage, "app/config/")
	require.NoError(t, err, "SearchPrefix failed")
	require.Len(t, results, 2, "Should find 2 keys with 'app/config/' prefix")
	require.Contains(t, results, "app/config/db")
	require.Contains(t, results, "app/config/redis")

	// Test prefix with no matches
	results, err = tree.SearchPrefix(ctx, storage, "nonexistent/")
	require.NoError(t, err, "SearchPrefix should not fail on non-existent prefix")
	require.Empty(t, results, "Non-existent prefix should return empty results")

	// Test exact key as prefix
	results, err = tree.SearchPrefix(ctx, storage, "system")
	require.NoError(t, err, "SearchPrefix failed")
	require.Len(t, results, 2, "Should find keys starting with 'system'")
	require.Contains(t, results, "system/health")
	require.Contains(t, results, "system/version")
}

// TestSearchPrefixEdgeCases tests various edge cases for the SearchPrefix functionality
func TestSearchPrefixEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("EmptyTree", func(t *testing.T) {
		s := &logical.InmemStorage{}
		storage, err := NewNodeStorage("bptree_empty", s, nil, 100)
		require.NoError(t, err)

		tree, err := NewBPlusTree(ctx, 4, storage)
		require.NoError(t, err)

		// Search in empty tree
		results, err := tree.SearchPrefix(ctx, storage, "any/prefix")
		require.NoError(t, err, "SearchPrefix should not fail on empty tree")
		require.Empty(t, results, "Empty tree should return empty results")

		// Empty prefix on empty tree
		results, err = tree.SearchPrefix(ctx, storage, "")
		require.NoError(t, err, "Empty prefix on empty tree should not fail")
		require.Empty(t, results, "Empty prefix should return empty results")
	})

	t.Run("EmptyPrefixOnNonEmptyTree", func(t *testing.T) {
		s := &logical.InmemStorage{}
		storage, err := NewNodeStorage("bptree_empty_prefix", s, nil, 100)
		require.NoError(t, err)

		tree, err := NewBPlusTree(ctx, 4, storage)
		require.NoError(t, err)

		// Insert some keys
		keys := []string{"app/config", "auth/users", "system/health"}
		for _, key := range keys {
			err = tree.Insert(ctx, storage, key, "value_"+key)
			require.NoError(t, err)
		}

		// Empty prefix should return empty results, not all keys
		results, err := tree.SearchPrefix(ctx, storage, "")
		require.NoError(t, err, "Empty prefix should not fail")
		require.Empty(t, results, "Empty prefix should return empty results for security/performance reasons")
	})

	t.Run("SpecialCharactersInPrefix", func(t *testing.T) {
		s := &logical.InmemStorage{}
		storage, err := NewNodeStorage("bptree_special_chars", s, nil, 100)
		require.NoError(t, err)

		tree, err := NewBPlusTree(ctx, 4, storage)
		require.NoError(t, err)

		// Insert keys with special characters
		specialKeys := []string{
			"app/config-dev",
			"app/config_prod",
			"app/config.test",
			"app/config@staging",
			"app/config+backup",
			"app/config (legacy)",
			"app/config/with spaces",
			"app/config/with/unicode/🔑",
		}

		for _, key := range specialKeys {
			err = tree.Insert(ctx, storage, key, "value_"+key)
			require.NoError(t, err, "Failed to insert key with special chars: %s", key)
		}

		// Test prefix search with special characters
		results, err := tree.SearchPrefix(ctx, storage, "app/config-")
		require.NoError(t, err)
		require.Contains(t, results, "app/config-dev")
		require.NotContains(t, results, "app/config_prod")

		results, err = tree.SearchPrefix(ctx, storage, "app/config_")
		require.NoError(t, err)
		require.Contains(t, results, "app/config_prod")
		require.NotContains(t, results, "app/config-dev")

		// Test with Unicode
		results, err = tree.SearchPrefix(ctx, storage, "app/config/with/unicode/")
		require.NoError(t, err)
		require.Contains(t, results, "app/config/with/unicode/🔑")
	})

	t.Run("PrefixLongerThanAnyKey", func(t *testing.T) {
		s := &logical.InmemStorage{}
		storage, err := NewNodeStorage("bptree_long_prefix_no_match", s, nil, 100)
		require.NoError(t, err)

		tree, err := NewBPlusTree(ctx, 4, storage)
		require.NoError(t, err)

		// Insert short keys
		err = tree.Insert(ctx, storage, "a", "value1")
		require.NoError(t, err)
		err = tree.Insert(ctx, storage, "ab", "value2")
		require.NoError(t, err)
		err = tree.Insert(ctx, storage, "abc", "value3")
		require.NoError(t, err)

		// Search with prefix longer than any key
		results, err := tree.SearchPrefix(ctx, storage, "abcdefghijklmnop")
		require.NoError(t, err)
		require.Empty(t, results, "Prefix longer than any key should return empty results")
	})

	t.Run("PrefixEqualsExactKey", func(t *testing.T) {
		s := &logical.InmemStorage{}
		storage, err := NewNodeStorage("bptree_exact_match", s, nil, 100)
		require.NoError(t, err)

		tree, err := NewBPlusTree(ctx, 4, storage)
		require.NoError(t, err)

		// Insert keys where one exactly matches our search prefix
		err = tree.Insert(ctx, storage, "app/config", "exact_match")
		require.NoError(t, err)
		err = tree.Insert(ctx, storage, "app/config/db", "sub_key1")
		require.NoError(t, err)
		err = tree.Insert(ctx, storage, "app/config/redis", "sub_key2")
		require.NoError(t, err)
		err = tree.Insert(ctx, storage, "app/configure", "different_key")
		require.NoError(t, err)

		// Search with prefix that exactly matches one key
		results, err := tree.SearchPrefix(ctx, storage, "app/config")
		require.NoError(t, err)
		require.Len(t, results, 4, "Should find exact match and keys with same prefix")
		require.Contains(t, results, "app/config")
		require.Contains(t, results, "app/config/db")
		require.Contains(t, results, "app/config/redis")
		require.Contains(t, results, "app/configure")
	})

	t.Run("CaseSensitivity", func(t *testing.T) {
		s := &logical.InmemStorage{}
		storage, err := NewNodeStorage("bptree_case_sensitive", s, nil, 100)
		require.NoError(t, err)

		tree, err := NewBPlusTree(ctx, 4, storage)
		require.NoError(t, err)

		// Insert keys with different cases
		err = tree.Insert(ctx, storage, "App/Config", "mixed_case")
		require.NoError(t, err)
		err = tree.Insert(ctx, storage, "app/config", "lower_case")
		require.NoError(t, err)
		err = tree.Insert(ctx, storage, "APP/CONFIG", "upper_case")
		require.NoError(t, err)

		// Search should be case sensitive
		results, err := tree.SearchPrefix(ctx, storage, "app/")
		require.NoError(t, err)
		require.Contains(t, results, "app/config")
		require.NotContains(t, results, "App/Config")
		require.NotContains(t, results, "APP/CONFIG")

		results, err = tree.SearchPrefix(ctx, storage, "App/")
		require.NoError(t, err)
		require.Contains(t, results, "App/Config")
		require.NotContains(t, results, "app/config")
		require.NotContains(t, results, "APP/CONFIG")
	})
}

// TestSearchPrefixOptimizations tests the optimization cases where we avoid tree traversal
func TestSearchPrefixOptimizations(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_optimizations", s, nil, 100)
	require.NoError(t, err)

	tree, err := NewBPlusTree(ctx, 4, storage)
	require.NoError(t, err)

	// Insert test data in sorted order
	keys := []string{
		"app/config/db",
		"app/config/redis",
		"app/secrets/api",
		"auth/users/alice",
		"auth/users/bob",
		"system/health",
		"system/version",
	}

	for _, key := range keys {
		err = tree.Insert(ctx, storage, key, "value_"+key)
		require.NoError(t, err)
	}

	t.Run("PrefixLargerThanLargestKey", func(t *testing.T) {
		// Prefix "zzz" is larger than "system/version" (largest key)
		results, err := tree.SearchPrefix(ctx, storage, "zzz")
		require.NoError(t, err)
		require.Empty(t, results, "Prefix larger than largest key should return empty")
	})

	t.Run("PrefixSmallerThanSmallestKeyNoMatch", func(t *testing.T) {
		// Prefix "aaa" where calculatePrefixLimit("aaa") = "aab"
		// "aab" < "app/config/db" and "app/config/db" doesn't start with "aaa"
		results, err := tree.SearchPrefix(ctx, storage, "aaa")
		require.NoError(t, err)
		require.Empty(t, results, "Prefix smaller than smallest key with no match should return empty")
	})

	t.Run("EarlyTerminationOptimization", func(t *testing.T) {
		// Search for "app/" should find matches and stop at "auth/" keys
		// because "auth/" >= calculatePrefixLimit("app/") = "apq"
		results, err := tree.SearchPrefix(ctx, storage, "app/")
		require.NoError(t, err)
		require.Len(t, results, 3, "Should find exactly 3 'app/' matches")
		require.Contains(t, results, "app/config/db")
		require.Contains(t, results, "app/config/redis")
		require.Contains(t, results, "app/secrets/api")
		require.NotContains(t, results, "auth/users/alice")
	})

	t.Run("EmptyTreeOptimization", func(t *testing.T) {
		emptyStorage, err := NewNodeStorage("bptree_empty_opt", s, nil, 100)
		require.NoError(t, err)

		emptyTree, err := NewBPlusTree(ctx, 4, emptyStorage)
		require.NoError(t, err)

		results, err := emptyTree.SearchPrefix(ctx, emptyStorage, "any/prefix")
		require.NoError(t, err)
		require.Empty(t, results, "Empty tree should return empty results immediately")
	})

	t.Run("PrefixEqualsLargestKey", func(t *testing.T) {
		// Edge case: prefix equals the largest key
		results, err := tree.SearchPrefix(ctx, storage, "system/version")
		require.NoError(t, err)
		require.Contains(t, results, "system/version")
	})

	t.Run("SingleCharacterOptimizations", func(t *testing.T) {
		// Single character prefixes should work efficiently
		results, err := tree.SearchPrefix(ctx, storage, "s")
		require.NoError(t, err)
		require.Len(t, results, 2, "Should find 'system/' keys")
		require.Contains(t, results, "system/health")
		require.Contains(t, results, "system/version")

		results, err = tree.SearchPrefix(ctx, storage, "a")
		require.NoError(t, err)
		require.Len(t, results, 5, "Should find 'app/' and 'auth/' keys")
	})
}

// TestSearchPrefixWithNextIDTraversal tests that SearchPrefix properly uses NextID for traversal
func TestSearchPrefixWithNextIDTraversal(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_nextid_traversal", s, nil, 100)
	require.NoError(t, err)

	// Use very small order to force many splits and multiple leaf nodes
	tree, err := NewBPlusTree(ctx, 3, storage)
	require.NoError(t, err)

	// Insert many keys to create multiple leaf nodes
	numKeys := 20
	for i := 1; i <= numKeys; i++ {
		key := fmt.Sprintf("key%02d", i) // key01, key02, ..., key20
		value := fmt.Sprintf("value%02d", i)
		err = tree.Insert(ctx, storage, key, value)
		require.NoError(t, err, "Failed to insert %s", key)
	}

	// Verify we have multiple leaves by checking NextID chain
	var leafCount int
	current, err := tree.findLeftmostLeaf(ctx, storage)
	require.NoError(t, err)

	for current != nil {
		leafCount++
		if current.NextID == "" {
			break
		}
		current, err = storage.LoadNode(ctx, current.NextID)
		require.NoError(t, err)
	}

	require.GreaterOrEqual(t, leafCount, 2, "Should have created multiple leaves")

	// Test prefix search that spans multiple leaves
	results, err := tree.SearchPrefix(ctx, storage, "key")
	require.NoError(t, err)
	require.Len(t, results, numKeys, "Should find all keys with 'key' prefix")

	// Test more specific prefix that might span leaves
	results, err = tree.SearchPrefix(ctx, storage, "key1")
	require.NoError(t, err)
	expectedKey1Results := []string{"key10", "key11", "key12", "key13", "key14", "key15", "key16", "key17", "key18", "key19"}
	require.Len(t, results, len(expectedKey1Results), "Should find all 'key1*' matches")

	for _, expectedKey := range expectedKey1Results {
		require.Contains(t, results, expectedKey, "Should contain %s", expectedKey)
	}
}

// TestSearchPrefixComprehensive is a comprehensive test that demonstrates all functionality
func TestSearchPrefixComprehensive(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_comprehensive", s, nil, 100)
	require.NoError(t, err)

	tree, err := NewBPlusTree(ctx, 4, storage)
	require.NoError(t, err)

	// Insert comprehensive test data
	testData := map[string]string{
		"app/config/db":       "database_config",
		"app/config/redis":    "redis_config",
		"app/secrets/api_key": "secret_api_key",
		"app/secrets/jwt":     "jwt_secret",
		"auth/users/alice":    "user_alice",
		"auth/users/bob":      "user_bob",
		"auth/roles/admin":    "admin_role",
		"auth/roles/user":     "user_role",
		"system/health":       "health_check",
		"system/version":      "version_info",
		"logs/app/error":      "error_logs",
		"logs/app/info":       "info_logs",
		"logs/system/debug":   "debug_logs",
	}

	for key, value := range testData {
		err = tree.Insert(ctx, storage, key, value)
		require.NoError(t, err, "Failed to insert %s", key)
	}

	// Test various prefix searches
	testCases := []struct {
		prefix           string
		expectedCount    int
		shouldContain    []string
		shouldNotContain []string
	}{
		{
			prefix:        "app/",
			expectedCount: 4,
			shouldContain: []string{"app/config/db", "app/config/redis", "app/secrets/api_key", "app/secrets/jwt"},
		},
		{
			prefix:           "app/config/",
			expectedCount:    2,
			shouldContain:    []string{"app/config/db", "app/config/redis"},
			shouldNotContain: []string{"app/secrets/api_key"},
		},
		{
			prefix:        "auth/",
			expectedCount: 4,
			shouldContain: []string{"auth/users/alice", "auth/users/bob", "auth/roles/admin", "auth/roles/user"},
		},
		{
			prefix:        "logs/",
			expectedCount: 3,
			shouldContain: []string{"logs/app/error", "logs/app/info", "logs/system/debug"},
		},
		{
			prefix:        "nonexistent/",
			expectedCount: 0,
		},
		{
			prefix:        "",
			expectedCount: 0, // Empty prefix returns empty results
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Prefix_%s", tc.prefix), func(t *testing.T) {
			results, err := tree.SearchPrefix(ctx, storage, tc.prefix)
			require.NoError(t, err, "SearchPrefix failed for prefix: %s", tc.prefix)
			require.Len(t, results, tc.expectedCount, "Wrong result count for prefix: %s", tc.prefix)

			for _, key := range tc.shouldContain {
				require.Contains(t, results, key, "Should contain %s for prefix %s", key, tc.prefix)
				require.Equal(t, testData[key], results[key][0], "Wrong value for key %s", key)
			}

			for _, key := range tc.shouldNotContain {
				require.NotContains(t, results, key, "Should not contain %s for prefix %s", key, tc.prefix)
			}
		})
	}
}
