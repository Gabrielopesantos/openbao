package bptree

import (
	"context"
	"testing"

	"github.com/openbao/openbao/sdk/v2/logical"
	"github.com/stretchr/testify/require"
)

func TestUnderflowHandling(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage(s, nil, 100)
	require.NoError(t, err, "Failed to create storage")

	// Create a tree with small order to force underflow scenarios
	tree, err := InitializeBPlusTree(ctx, storage, &BPlusTreeConfig{Order: 3}) // order=3 means max 2 keys, min 1 key
	require.NoError(t, err, "Failed to create B+ tree")

	t.Run("LeafUnderflowWithBorrowing", func(t *testing.T) {
		// Insert keys to create a tree structure that will require borrowing
		keys := []string{"10", "20", "30", "40", "50"}
		for _, key := range keys {
			err = tree.Insert(ctx, storage, key, "value_"+key)
			require.NoError(t, err, "Failed to insert key %s", key)
		}

		// Verify all keys exist
		for _, key := range keys {
			val, found, err := tree.Search(ctx, storage, key)
			require.NoError(t, err, "Error getting key %s", key)
			require.True(t, found, "Key %s should exist", key)
			require.Equal(t, []string{"value_" + key}, val, "Value mismatch for key %s", key)
		}

		// Delete a key that should cause underflow and borrowing
		deleted, err := tree.Delete(ctx, storage, "20")
		require.NoError(t, err, "Failed to delete key 20")
		require.True(t, deleted, "Key 20 should have been deleted")

		// Verify the key was deleted
		_, found, err := tree.Search(ctx, storage, "20")
		require.NoError(t, err, "Error checking deleted key")
		require.False(t, found, "Deleted key should not exist")

		// Verify remaining keys still exist
		remainingKeys := []string{"10", "30", "40", "50"}
		for _, key := range remainingKeys {
			val, found, err := tree.Search(ctx, storage, key)
			require.NoError(t, err, "Error getting remaining key %s", key)
			require.True(t, found, "Remaining key %s should exist", key)
			require.Equal(t, []string{"value_" + key}, val, "Value mismatch for remaining key %s", key)
		}
	})

	t.Run("LeafUnderflowWithMerging", func(t *testing.T) {
		// Clear and start fresh
		s := &logical.InmemStorage{}
		storage, err := NewNodeStorage(s, nil, 100)
		require.NoError(t, err, "Failed to create storage")

		tree, err := InitializeBPlusTree(ctx, storage, &BPlusTreeConfig{Order: 3}) // order=3 means max 2 keys, min 1 key
		require.NoError(t, err, "Failed to create B+ tree")

		// Insert fewer keys to force merging scenario
		keys := []string{"10", "20", "30"}
		for _, key := range keys {
			err = tree.Insert(ctx, storage, key, "value_"+key)
			require.NoError(t, err, "Failed to insert key %s", key)
		}

		// Delete keys to force underflow and merging
		deleted, err := tree.Delete(ctx, storage, "20")
		require.NoError(t, err, "Failed to delete key 20")
		require.True(t, deleted, "Key 20 should have been deleted")

		deleted, err = tree.Delete(ctx, storage, "30")
		require.NoError(t, err, "Failed to delete key 30")
		require.True(t, deleted, "Key 30 should have been deleted")

		// Verify remaining key still exists
		val, found, err := tree.Search(ctx, storage, "10")
		require.NoError(t, err, "Error getting remaining key")
		require.True(t, found, "Remaining key should exist")
		require.Equal(t, []string{"value_10"}, val, "Value mismatch for remaining key")

		// Verify deleted keys don't exist
		for _, key := range []string{"20", "30"} {
			_, found, err := tree.Search(ctx, storage, key)
			require.NoError(t, err, "Error checking deleted key %s", key)
			require.False(t, found, "Deleted key %s should not exist", key)
		}
	})

	t.Run("DeleteValueUnderflow", func(t *testing.T) {
		// Clear and start fresh
		s := &logical.InmemStorage{}
		storage, err := NewNodeStorage(s, nil, 100)
		require.NoError(t, err, "Failed to create storage")

		tree, err := InitializeBPlusTree(ctx, storage, &BPlusTreeConfig{Order: 3}) // order=3 means max 2 keys, min 1 key
		require.NoError(t, err, "Failed to create B+ tree")

		// Insert keys with multiple values
		keys := []string{"10", "20", "30", "40"}
		for _, key := range keys {
			err = tree.Insert(ctx, storage, key, "value1_"+key)
			require.NoError(t, err, "Failed to insert key %s", key)
			err = tree.Insert(ctx, storage, key, "value2_"+key)
			require.NoError(t, err, "Failed to insert second value for key %s", key)
		}

		// Remove all values for a key by removing them individually
		deleted, err := tree.DeleteValue(ctx, storage, "20", "value1_20")
		require.NoError(t, err, "Failed to delete value1 for key 20")
		require.True(t, deleted, "Value1 for key 20 should have been deleted")

		deleted, err = tree.DeleteValue(ctx, storage, "20", "value2_20")
		require.NoError(t, err, "Failed to delete value2 for key 20")
		require.True(t, deleted, "Value2 for key 20 should have been deleted")

		// Verify the key was completely removed
		_, found, err := tree.Search(ctx, storage, "20")
		require.NoError(t, err, "Error checking key after all values deleted")
		require.False(t, found, "Key should not exist after all values deleted")

		// Verify other keys still exist with both values
		for _, key := range []string{"10", "30", "40"} {
			val, found, err := tree.Search(ctx, storage, key)
			require.NoError(t, err, "Error getting key %s", key)
			require.True(t, found, "Key %s should exist", key)
			require.Len(t, val, 2, "Key %s should have 2 values", key)
		}
	})

	t.Run("RootNodeSpecialCase", func(t *testing.T) {
		// Clear and start fresh
		s := &logical.InmemStorage{}
		storage, err := NewNodeStorage(s, nil, 100)
		require.NoError(t, err, "Failed to create storage")

		tree, err := InitializeBPlusTree(ctx, storage, &BPlusTreeConfig{Order: 3}) // order=3 means max 2 keys, min 1 key
		require.NoError(t, err, "Failed to create B+ tree")

		// Insert and delete to test root underflow handling
		err = tree.Insert(ctx, storage, "10", "value10")
		require.NoError(t, err, "Failed to insert key")

		deleted, err := tree.Delete(ctx, storage, "10")
		require.NoError(t, err, "Failed to delete key from root")
		require.True(t, deleted, "Key 10 should have been deleted from root")

		// Tree should still be valid (empty root)
		_, found, err := tree.Search(ctx, storage, "10")
		require.NoError(t, err, "Error checking deleted key")
		require.False(t, found, "Deleted key should not exist")

		// Should be able to insert again
		err = tree.Insert(ctx, storage, "20", "value20")
		require.NoError(t, err, "Failed to insert after root deletion")

		val, found, err := tree.Search(ctx, storage, "20")
		require.NoError(t, err, "Error getting newly inserted key")
		require.True(t, found, "Newly inserted key should exist")
		require.Equal(t, []string{"value20"}, val, "Value mismatch")
	})
}
