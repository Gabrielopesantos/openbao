package bptree

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/openbao/openbao/sdk/v2/logical"
	"github.com/stretchr/testify/require"
)

// NOTE (gsantos): We probably do not want to test the Node's methods here, but only the storage nodeStorage
func TestStorageOperations(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	nodeStorage, err := NewNodeStorage("bptree", s, nil, 100)
	require.NoError(t, err, "Failed to create storage nodeStorage")

	// Test SetRootID and GetRootID
	rootID := "root-1"
	err = nodeStorage.SetRootID(ctx, rootID)
	require.NoError(t, err, "Failed to set root ID")

	retrievedRootID, err := nodeStorage.GetRootID(ctx)
	require.NoError(t, err, "Failed to get root ID")

	require.Equal(t, rootID, retrievedRootID, "Expected root ID %s, got %s", rootID, retrievedRootID)

	// Test SaveNode and LoadNode
	nodeID := "node-1"
	node := NewLeafNode(nodeID)
	node.Keys = append(node.Keys, "key1", "key2")
	node.Values = append(node.Values, []string{"value1", "value2"})

	// Save the node
	err = nodeStorage.SaveNode(ctx, node)
	require.NoError(t, err, "Failed to save node")

	// Load the node
	loadedNode, err := nodeStorage.LoadNode(ctx, nodeID)
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
	err = nodeStorage.DeleteNode(ctx, nodeID)
	require.NoError(t, err, "Failed to delete node")

	deletedNode, err := nodeStorage.LoadNode(ctx, nodeID)
	require.NoError(t, err, "Error when loading deleted node")

	require.Nil(t, deletedNode, "Node should have been deleted")
}

// TestStoragenodeStorageCache tests the caching behavior of the storage nodeStorage
func TestStoragenodeStorageCache(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	nodeStorage, err := NewNodeStorage("bptree_cache", s, nil, 100)
	require.NoError(t, err, "Failed to create storage nodeStorage")

	t.Run("CacheHit", func(t *testing.T) {
		// Create and save a node
		node := NewLeafNode("node-1")
		node.Keys = []string{"key1"}
		node.Values = [][]string{{"value1"}}

		err := nodeStorage.SaveNode(ctx, node)
		require.NoError(t, err, "Failed to save node")

		// Try to load the node from cache
		cachedNode, ok := nodeStorage.cache.Get(node.ID)
		require.True(t, ok, "Node should be in cache")
		require.NotNil(t, cachedNode, "Cached node should not be empty")
	})

	t.Run("CacheUpdate", func(t *testing.T) {
		// Create and save a node
		node := NewLeafNode("node-2")
		node.Keys = []string{"key1"}
		node.Values = [][]string{{"value1"}}

		err := nodeStorage.SaveNode(ctx, node)
		require.NoError(t, err, "Failed to save node")

		// Verify the node is in cache
		cachedNode, ok := nodeStorage.cache.Get(node.ID)
		require.True(t, ok, "Node should be in cache")
		require.Equal(t, node, cachedNode, "Cached node should match original")

		// Update the node
		node.Values[0] = []string{"value1_updated"}
		err = nodeStorage.SaveNode(ctx, node)
		require.NoError(t, err, "Failed to update node")

		// Verify the updated node is in cache
		cachedNode, ok = nodeStorage.cache.Get(node.ID)
		require.True(t, ok, "Updated node should be in cache")
		require.Equal(t, node, cachedNode, "Cached node should match updated version")
	})

	t.Run("CacheEviction", func(t *testing.T) {
		// Create and save many nodes to force eviction
		for i := 0; i < 150; i++ {
			node := NewLeafNode(fmt.Sprintf("node-%d", i))
			node.Keys = []string{fmt.Sprintf("key%d", i)}
			node.Values = [][]string{{fmt.Sprintf("value%d", i)}}

			err := nodeStorage.SaveNode(ctx, node)
			require.NoError(t, err, "Failed to save node")
		}

		// Verify some nodes are in cache
		recentNode := NewLeafNode("node-149")
		cachedNode, ok := nodeStorage.cache.Get(recentNode.ID)
		require.True(t, ok, "Recent node should be in cache")
		require.Equal(t, recentNode.ID, cachedNode.ID, "Cached node should match recent node")

		// Verify older nodes might have been evicted but are still retrievable
		oldNode := NewLeafNode("node-0")
		loadedNode, err := nodeStorage.LoadNode(ctx, oldNode.ID)
		require.NoError(t, err, "Failed to load old node")
		require.Equal(t, oldNode.ID, loadedNode.ID, "Loaded node should match old node")
	})

	t.Run("CacheDeletion", func(t *testing.T) {
		// Create and save a node
		node := NewLeafNode("node-3")
		node.Keys = []string{"key1"}
		node.Values = [][]string{{"value1"}}

		err := nodeStorage.SaveNode(ctx, node)
		require.NoError(t, err, "Failed to save node")

		// Verify node is in cache
		cachedNode, ok := nodeStorage.cache.Get(node.ID)
		require.True(t, ok, "Node should be in cache")
		require.Equal(t, node, cachedNode, "Cached node should match original")

		// Delete the node
		err = nodeStorage.DeleteNode(ctx, node.ID)
		require.NoError(t, err, "Failed to delete node")

		// Verify node is removed from cache
		_, ok = nodeStorage.cache.Get(node.ID)
		require.False(t, ok, "Deleted node should not be in cache")
	})

	t.Run("CacheConcurrentAccess", func(t *testing.T) {
		// Create and save a node
		node := NewLeafNode("node-1")
		node.Keys = []string{"key1"}
		node.Values = [][]string{{"value1"}}

		err := nodeStorage.SaveNode(ctx, node)
		require.NoError(t, err, "Failed to save node")

		// Launch multiple goroutines to read and write concurrently
		var wg sync.WaitGroup
		errChan := make(chan error, 10)

		for i := range 10 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				// Read existing node
				loadedNode, err := nodeStorage.LoadNode(ctx, node.ID)
				if err != nil {
					errChan <- fmt.Errorf("error loading node: %w", err)
					return
				}
				if !reflect.DeepEqual(node, loadedNode) {
					errChan <- fmt.Errorf("loaded node does not match original: expected %v, got %v", node, loadedNode)
					return
				}

				// Create and save new node
				newNode := NewLeafNode(fmt.Sprintf("node-%d", i))
				newNode.Keys = []string{fmt.Sprintf("key%d", i)}
				newNode.Values = [][]string{{fmt.Sprintf("value%d", i)}}

				err = nodeStorage.SaveNode(ctx, newNode)
				if err != nil {
					errChan <- fmt.Errorf("error saving new node: %w", err)
					return
				}

				// Verify new node is in cache
				cachedNode, ok := nodeStorage.cache.Get(newNode.ID)
				if !ok {
					errChan <- fmt.Errorf("new node not found in cache")
					return
				}
				if !reflect.DeepEqual(newNode, cachedNode) {
					errChan <- fmt.Errorf("cached node does not match new node: expected %v, got %v", newNode, cachedNode)
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
	})
}
