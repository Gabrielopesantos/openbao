package bptree

import (
	"context"
	"fmt"
	"testing"

	"github.com/openbao/openbao/sdk/v2/logical"
	"github.com/stretchr/testify/require"
)

// TestStorageAdapter tests the storage adapter
// NOTE: We probably do not want to test the Node's methods here, but only the storage adapter
func TestStorageAdapter(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter, err := NewStorageAdapter[string, string]("bptree", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	// Test SetRootID and GetRootID
	rootID := "root-1"
	err = adapter.SetRootID(ctx, rootID)
	require.NoError(t, err, "Failed to set root ID")

	retrievedRootID, err := adapter.GetRootID(ctx)
	require.NoError(t, err, "Failed to get root ID")

	require.Equal(t, rootID, retrievedRootID, "Expected root ID %s, got %s", rootID, retrievedRootID)

	// Test SaveNode and LoadNode
	nodeID := "node-1"
	node := NewLeafNode[string, string](nodeID)
	node.Keys = append(node.Keys, "key1", "key2")
	node.Values = append(node.Values, "value1", "value2")

	// Save the node
	err = adapter.SaveNode(ctx, node)
	require.NoError(t, err, "Failed to save node")

	// Load the node
	loadedNode, err := adapter.LoadNode(ctx, nodeID)
	require.NoError(t, err, "Failed to load node")
	require.NotNil(t, loadedNode, "Loaded node is nil")

	require.Equal(t, node.ID, loadedNode.ID, "Expected node ID %s, got %s", node.ID, loadedNode.ID)
	require.Equal(t, node.Keys, loadedNode.Keys, "Expected node keys %v, got %v", node.Keys, loadedNode.Keys)
	require.Equal(t, node.Values, loadedNode.Values, "Expected node values %v, got %v", node.Values, loadedNode.Values)

	for i, key := range node.Keys {
		require.Equal(t, loadedNode.Keys[i], key, "Expected key %s at index %d, got %s", key, i, loadedNode.Keys[i])
	}

	for i, value := range node.Values {
		require.Equal(t, loadedNode.Values[i], value, "Expected value %s at index %d, got %s", value, i, loadedNode.Values[i])
	}

	// Test DeleteNode
	err = adapter.DeleteNode(ctx, nodeID)
	require.NoError(t, err, "Failed to delete node")

	deletedNode, err := adapter.LoadNode(ctx, nodeID)
	require.NoError(t, err, "Error when loading deleted node")

	require.Nil(t, deletedNode, "Node should have been deleted")
}

// TestStorageAdapterCache tests the caching behavior of the storage adapter
func TestStorageAdapterCache(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter, err := NewStorageAdapter[string, string]("bptree_cache", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	t.Run("CacheHit", func(t *testing.T) {
		// Create and save a node
		node := NewLeafNode[string, string]("node-1")
		node.Keys = []string{"key1"}
		node.Values = []string{"value1"}

		err := adapter.SaveNode(ctx, node)
		require.NoError(t, err, "Failed to save node")

		// First load should come from storage
		loadedNode, err := adapter.LoadNode(ctx, node.ID)
		require.NoError(t, err, "Failed to load node")
		require.Equal(t, node, loadedNode, "Loaded node should match original")

		// Second load should come from cache
		cachedNode, ok := adapter.cache.Get(node.ID)
		require.True(t, ok, "Node should be in cache")
		require.Equal(t, node, cachedNode, "Cached node should match original")
	})

	t.Run("CacheUpdate", func(t *testing.T) {
		// Create and save a node
		node := NewLeafNode[string, string]("node-2")
		node.Keys = []string{"key1"}
		node.Values = []string{"value1"}

		err := adapter.SaveNode(ctx, node)
		require.NoError(t, err, "Failed to save node")

		// Update the node
		node.Values[0] = "value1_updated"
		err = adapter.SaveNode(ctx, node)
		require.NoError(t, err, "Failed to update node")

		// Verify the updated node is in cache
		cachedNode, ok := adapter.cache.Get(node.ID)
		require.True(t, ok, "Updated node should be in cache")
		require.Equal(t, node, cachedNode, "Cached node should match updated version")
	})

	t.Run("CacheEviction", func(t *testing.T) {
		// Create and save many nodes to force eviction
		for i := 0; i < 150; i++ {
			node := NewLeafNode[string, string](fmt.Sprintf("node-%d", i))
			node.Keys = []string{fmt.Sprintf("key%d", i)}
			node.Values = []string{fmt.Sprintf("value%d", i)}

			err := adapter.SaveNode(ctx, node)
			require.NoError(t, err, "Failed to save node")
		}

		// Verify some nodes are in cache
		recentNode := NewLeafNode[string, string]("node-149")
		cachedNode, ok := adapter.cache.Get(recentNode.ID)
		require.True(t, ok, "Recent node should be in cache")
		require.Equal(t, recentNode.ID, cachedNode.ID, "Cached node should match recent node")

		// Verify older nodes might have been evicted but are still retrievable
		oldNode := NewLeafNode[string, string]("node-0")
		loadedNode, err := adapter.LoadNode(ctx, oldNode.ID)
		require.NoError(t, err, "Failed to load old node")
		require.Equal(t, oldNode.ID, loadedNode.ID, "Loaded node should match old node")
	})

	t.Run("CacheDeletion", func(t *testing.T) {
		// Create and save a node
		node := NewLeafNode[string, string]("node-3")
		node.Keys = []string{"key1"}
		node.Values = []string{"value1"}

		err := adapter.SaveNode(ctx, node)
		require.NoError(t, err, "Failed to save node")

		// Verify node is in cache
		cachedNode, ok := adapter.cache.Get(node.ID)
		require.True(t, ok, "Node should be in cache")
		require.Equal(t, node, cachedNode, "Cached node should match original")

		// Delete the node
		err = adapter.DeleteNode(ctx, node.ID)
		require.NoError(t, err, "Failed to delete node")

		// Verify node is removed from cache
		_, ok = adapter.cache.Get(node.ID)
		require.False(t, ok, "Deleted node should not be in cache")
	})

	// TODO (gabrielopesantos): Fix test
	// t.Run("CacheConcurrentAccess", func(t *testing.T) {
	// 	// Create and save a node
	// 	node := NewLeafNode[string, string]("node-4")
	// 	node.Keys = []string{"key1"}
	// 	node.Values = []string{"value1"}

	// 	err := adapter.SaveNode(ctx, node)
	// 	require.NoError(t, err, "Failed to save node")

	// 	// Launch multiple goroutines to read and write concurrently
	// 	done := make(chan bool, 10)
	// 	for i := 0; i < 10; i++ {
	// 		go func(i int) {
	// 			// Read existing node
	// 			loadedNode, err := adapter.LoadNode(ctx, node.ID)
	// 			require.NoError(t, err, "Failed to load node")
	// 			require.Equal(t, node, loadedNode, "Loaded node should match original")

	// 			// Create and save new node
	// 			newNode := NewLeafNode[string, string](fmt.Sprintf("node-%d", i))
	// 			newNode.Keys = []string{fmt.Sprintf("key%d", i)}
	// 			newNode.Values = []string{fmt.Sprintf("value%d", i)}

	// 			err = adapter.SaveNode(ctx, newNode)
	// 			require.NoError(t, err, "Failed to save new node")

	// 			// Verify new node is in cache
	// 			cachedNode, ok := adapter.cache.Get(newNode.ID)
	// 			require.True(t, ok, "New node should be in cache")
	// 			require.Equal(t, newNode, cachedNode, "Cached node should match new node")

	// 			done <- true
	// 		}(i)
	// 	}

	// 	// Wait for all goroutines to complete
	// 	for i := 0; i < 10; i++ {
	// 		<-done
	// 	}
	// })
}
