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
	storage, err := NewNodeStorage("bptree_large", s, nil, 100)
	require.NoError(t, err, "Failed to create storage storage")

	ctx := context.Background()
	tree, err := NewBPlusTree(ctx, 8, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert 100 keys
	for i := 1; i <= 100; i++ {
		err = tree.Insert(ctx, storage, strconv.Itoa(i), fmt.Sprintf("value%d", i))
		require.NoError(t, err, "Failed to insert key %d", i)
	}

	// Verify all keys exist
	for i := 1; i <= 100; i++ {
		val, found, err := tree.Get(ctx, storage, strconv.Itoa(i))
		require.NoError(t, err, "Error when getting key %d", i)
		require.True(t, found, "Should find key %d", i)
		require.Equal(t, []string{fmt.Sprintf("value%d", i)}, val, "Retrieved value should match for key %d", i)
	}

	// Delete every other key
	for i := 2; i <= 100; i += 2 {
		err = tree.Delete(ctx, storage, strconv.Itoa(i))
		require.NoError(t, err, "Failed to delete key %d", i)
	}

	// Verify odd keys exist and even keys don't
	for i := 1; i <= 100; i++ {
		val, found, err := tree.Get(ctx, storage, strconv.Itoa(i))
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
