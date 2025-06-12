package bptree

import (
	"context"
	"testing"

	"github.com/openbao/openbao/sdk/v2/logical"
	"github.com/stretchr/testify/require"
)

// TestTreeConfigPersistence tests that tree metadata is properly stored and loaded
func TestTreeConfigPersistence(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage(s, nil, 100)
	require.NoError(t, err, "Failed to create storage")

	t.Run("ConfigStorageAndRetrieval", func(t *testing.T) {
		// Create a tree with specific configuration
		config, err := NewBPlusTreeConfig("metadata_test", 6)
		require.NoError(t, err)

		tree, err := InitializeBPlusTree(ctx, storage, config)
		require.NoError(t, err, "Should create new tree")

		// Verify metadata was stored
		treeCtx := tree.contextWithTreeID(ctx)
		metadata, err := storage.GetTreeConfig(treeCtx)
		require.NoError(t, err, "Should retrieve stored metadata")
		require.NotNil(t, metadata, "Config should exist")
		require.Equal(t, "metadata_test", metadata.TreeID)
		require.Equal(t, 6, metadata.Order)
		require.Equal(t, 1, metadata.Version)
	})

	t.Run("LoadingWithCorrectConfig", func(t *testing.T) {
		// Create a tree
		config, err := NewBPlusTreeConfig("load_test", 8)
		require.NoError(t, err)

		tree1, err := InitializeBPlusTree(ctx, storage, config)
		require.NoError(t, err)

		// Add some data
		err = tree1.Insert(ctx, storage, "key1", "value1")
		require.NoError(t, err)

		// Load with same config - should work
		tree2, err := LoadExistingBPlusTree(ctx, storage, config.TreeID)
		require.NoError(t, err, "Should load existing tree with matching config")

		// Verify data is accessible
		values, found, err := tree2.Search(ctx, storage, "key1")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []string{"value1"}, values)
	})

	t.Run("LoadExistingTreeFunction", func(t *testing.T) {
		// Create a tree with specific order
		config, err := NewBPlusTreeConfig("explicit_load", 12)
		require.NoError(t, err)

		tree1, err := NewBPlusTree(ctx, storage, config)
		require.NoError(t, err)

		err = tree1.Insert(ctx, storage, "key1", "value1")
		require.NoError(t, err)

		// Load using LoadExistingTree with just TreeID
		tree2, err := LoadExistingBPlusTree(ctx, storage, "explicit_load")
		require.NoError(t, err, "Should load existing tree using stored config")

		// Verify the loaded tree has the correct order from storage
		require.Equal(t, "explicit_load", tree2.config.TreeID)
		require.Equal(t, 12, tree2.config.Order, "Should use stored order, not placeholder")

		// Verify data is accessible
		values, found, err := tree2.Search(ctx, storage, "key1")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []string{"value1"}, values)
	})

	t.Run("LoadExistingTreeWrongID", func(t *testing.T) {
		// Try to load non-existent tree
		_, err = LoadExistingBPlusTree(ctx, storage, "nonexistent")
		require.Error(t, err, "Should fail to load non-existent tree")
		require.Contains(t, err.Error(), "does not exist")
	})

	t.Run("LoadExistingTreeIDMismatch", func(t *testing.T) {
		// Create a tree
		config1, err := NewBPlusTreeConfig("mismatch_test", 4)
		require.NoError(t, err)

		_, err = NewBPlusTree(ctx, storage, config1)
		require.NoError(t, err)

		// Try to load with different TreeID in config
		// Override the context to use the correct tree (simulating internal mismatch)
		mismatchCtx := WithTreeID(ctx, "mismatch_test")
		_, err = LoadExistingBPlusTree(mismatchCtx, storage, "wrong_id")
		require.Error(t, err, "Should fail when TreeID doesn't match")
		// require.Contains(t, err.Error(), "tree ID mismatch")
	})
}
