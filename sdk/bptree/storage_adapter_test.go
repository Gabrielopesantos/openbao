package bptree

import (
	"context"
	"testing"

	"github.com/openbao/openbao/sdk/v2/logical"
	"github.com/stretchr/testify/require"
)

// TestStorageAdapter tests the storage adapter
// NOTE: We probably do not want to test the Node's methods here, but only the storage adapter
func TestStorageAdapter(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter := NewStorageAdapter[string, string](ctx, "bptree", s, nil)

	// Test SetRootID and GetRootID
	rootID := "root-1"
	err := adapter.SetRootID(rootID)
	require.NoError(t, err, "Failed to set root ID")

	retrievedRootID, err := adapter.GetRootID()
	require.NoError(t, err, "Failed to get root ID")

	require.Equal(t, rootID, retrievedRootID, "Expected root ID %s, got %s", rootID, retrievedRootID)

	// Test SaveNode and LoadNode
	nodeID := "node-1"
	node := NewLeafNode[string, string](nodeID)
	node.Keys = append(node.Keys, "key1", "key2")
	node.Values = append(node.Values, "value1", "value2")

	// Save the node
	err = adapter.SaveNode(node)
	require.NoError(t, err, "Failed to save node")

	// Load the node
	loadedNode, err := adapter.LoadNode(nodeID)
	require.NoError(t, err, "Failed to load node")
	require.NotNil(t, loadedNode, "Loaded node is nil")

	require.Equal(t, node.Id, loadedNode.Id, "Expected node ID %s, got %s", node.Id, loadedNode.Id)
	require.Equal(t, node.Keys, loadedNode.Keys, "Expected node keys %v, got %v", node.Keys, loadedNode.Keys)
	require.Equal(t, node.Values, loadedNode.Values, "Expected node values %v, got %v", node.Values, loadedNode.Values)

	for i, key := range node.Keys {
		require.Equal(t, loadedNode.Keys[i], key, "Expected key %s at index %d, got %s", key, i, loadedNode.Keys[i])
	}

	for i, value := range node.Values {
		require.Equal(t, loadedNode.Values[i], value, "Expected value %s at index %d, got %s", value, i, loadedNode.Values[i])
	}

	// Test DeleteNode
	err = adapter.DeleteNode(nodeID)
	require.NoError(t, err, "Failed to delete node")

	deletedNode, err := adapter.LoadNode(nodeID)
	require.NoError(t, err, "Error when loading deleted node")

	require.Nil(t, deletedNode, "Node should have been deleted")
}
