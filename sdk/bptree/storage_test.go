package bptree

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	ctx, nodeStorage, _ := initTest(t, nil)

	t.Run("Storage Operations", func(t *testing.T) {
		// Test SetRootID and GetRootID
		rootID := "root-1"
		err := nodeStorage.SetRootID(ctx, rootID)
		require.NoError(t, err, "Failed to set root ID")

		retrievedRootID, err := nodeStorage.GetRootID(ctx)
		require.NoError(t, err, "Failed to get root ID")

		require.Equal(t, rootID, retrievedRootID, "Expected root ID %s, got %s", rootID, retrievedRootID)

		// Test SaveNode and LoadNode
		nodeID := "node-1"
		node := NewLeafNode(nodeID)
		err = node.InsertKeyValue("key1", "value1")
		require.NoError(t, err, "Failed to insert key-value pair into node")
		err = node.InsertKeyValue("key2", "value2")
		require.NoError(t, err, "Failed to insert key-value pair into node")

		// Save the node
		err = nodeStorage.SaveNode(ctx, node)
		require.NoError(t, err, "Failed to save node")

		// Try to load the node from cache using the proper cache key
		cachedNode, ok := nodeStorage.cache.Get(cacheKey(ctx, node.ID))
		require.True(t, ok, "Node should be in cache")
		require.NotNil(t, cachedNode, "Cached node should not be empty")
		require.Equal(t, node, cachedNode, "Cached node should match saved node")

		// Load the node
		loadedNode, err := nodeStorage.LoadNode(ctx, nodeID)
		require.NoError(t, err, "Failed to load node")
		require.NotNil(t, loadedNode, "Loaded node is nil")
		require.Equal(t, node, loadedNode, "Loaded node should match saved node")

		// Update the node
		err = node.InsertKeyValue("key3", "value3")
		require.NoError(t, err, "Failed to insert key-value pair into node")

		err = nodeStorage.SaveNode(ctx, node)
		require.NoError(t, err, "Failed to update node")

		// Verify if the cache has been updated
		cachedNode, ok = nodeStorage.cache.Get(cacheKey(ctx, node.ID))
		require.True(t, ok, "Updated node should be in cache")
		require.NotNil(t, cachedNode, "Cached node should not be empty after update")
		require.Equal(t, node, cachedNode, "Cached node should match updated node")

		// Load the updated node
		updatedNode, err := nodeStorage.LoadNode(ctx, nodeID)
		require.NoError(t, err, "Failed to load updated node")
		require.NotNil(t, updatedNode, "Updated node is nil")
		require.Equal(t, node, updatedNode, "Updated node should match saved node after update")

		// Try to load a non-existent node
		err = nodeStorage.DeleteNode(ctx, nodeID)
		require.NoError(t, err, "Failed to delete node")

		deletedNode, err := nodeStorage.LoadNode(ctx, nodeID)
		require.NoError(t, err, "Error when loading deleted node")
		require.Nil(t, deletedNode, "Node should have been deleted")

		// Verify if the cache has been cleared
		_, ok = nodeStorage.cache.Get(cacheKey(ctx, node.ID))
		require.False(t, ok, "Deleted node should not be in cache")
	})

	t.Run("Concurrent Access", func(t *testing.T) {
		// Create and save a node
		node := NewLeafNode("node-1")
		node.InsertKeyValue("key1", "value1")

		err := nodeStorage.SaveNode(ctx, node)
		require.NoError(t, err, "Failed to save node")

		var wg sync.WaitGroup
		errChan := make(chan error, 10)

		for i := range 10 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				// Load the existing node
				loadedNode, err := nodeStorage.LoadNode(ctx, node.ID)
				if err != nil {
					errChan <- fmt.Errorf("error loading node: %w", err)
					return
				}
				if !reflect.DeepEqual(node, loadedNode) {
					errChan <- fmt.Errorf("loaded node does not match original: expected %v, got %v", node, loadedNode)
					return
				}

				// Create and save a new node
				newNode := NewLeafNode(fmt.Sprintf("node-%d", i))
				err = newNode.InsertKeyValue(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
				require.NoError(t, err, "Failed to insert key-value pair into new node")

				err = nodeStorage.SaveNode(ctx, newNode)
				if err != nil {
					errChan <- fmt.Errorf("error saving new node: %w", err)
					return
				}

				// Verify new node is in cache using the proper cache key
				cachedNode, ok := nodeStorage.cache.Get(cacheKey(ctx, newNode.ID))
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

		wg.Wait()
		close(errChan)

		for err := range errChan {
			t.Error(err)
		}
	})
}
