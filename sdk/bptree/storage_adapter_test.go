package bptree

import (
	"context"
	"testing"

	"github.com/openbao/openbao/sdk/v2/logical"
)

// TestStorageAdapter tests the storage adapter
func TestStorageAdapter(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter := NewStorageAdapter[string, string](ctx, s, "bptree", nil)

	// Test SetRootID and GetRootID
	rootID := "root-1"
	if err := adapter.SetRootID(rootID); err != nil {
		t.Fatalf("Failed to set root ID: %v", err)
	}

	retrievedRootID, err := adapter.GetRootID()
	if err != nil {
		t.Fatalf("Failed to get root ID: %v", err)
	}

	if retrievedRootID != rootID {
		t.Fatalf("Expected root ID %s, got %s", rootID, retrievedRootID)
	}

	// Test SaveNode and LoadNode
	node := NewLeafNode[string, string]("node-1")
	node.Keys = append(node.Keys, "key1", "key2")
	node.Values = append(node.Values, "value1", "value2")

	if err := adapter.SaveNode(node); err != nil {
		t.Fatalf("Failed to save node: %v", err)
	}

	loadedNode, err := adapter.LoadNode("node-1")
	if err != nil {
		t.Fatalf("Failed to load node: %v", err)
	}

	if loadedNode == nil {
		t.Fatal("Loaded node is nil")
	}

	if loadedNode.Id != node.Id {
		t.Fatalf("Expected node ID %s, got %s", node.Id, loadedNode.Id)
	}

	if len(loadedNode.Keys) != len(node.Keys) {
		t.Fatalf("Expected %d keys, got %d", len(node.Keys), len(loadedNode.Keys))
	}

	for i, key := range node.Keys {
		if loadedNode.Keys[i] != key {
			t.Fatalf("Expected key %s at index %d, got %s", key, i, loadedNode.Keys[i])
		}
	}

	for i, value := range node.Values {
		if loadedNode.Values[i] != value {
			t.Fatalf("Expected value %s at index %d, got %s", value, i, loadedNode.Values[i])
		}
	}

	// Test DeleteNode
	if err := adapter.DeleteNode("node-1"); err != nil {
		t.Fatalf("Failed to delete node: %v", err)
	}

	deletedNode, err := adapter.LoadNode("node-1")
	if err != nil {
		t.Fatalf("Error when loading deleted node: %v", err)
	}

	if deletedNode != nil {
		t.Fatal("Node should have been deleted")
	}
}

// TODO (gabrielopesantos): This should be covered in bptree_test.go (Can be deleted)
// TestBPlusTreeWithStorageAdapter tests the B+ tree with the storage adapter
func TestBPlusTreeWithStorageAdapter(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter := NewStorageAdapter[string, string](ctx, s, "bptree", nil)

	tree, err := NewBPlusTree(4, stringLess, adapter)
	if err != nil {
		t.Fatalf("Failed to create B+ tree: %v", err)
	}

	// Insert some key-value pairs
	testData := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"cherry": "red",
	}

	for k, v := range testData {
		if err := tree.Put(k, v); err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	// Verify the inserted data
	for k, v := range testData {
		value, found, err := tree.Get(k)
		if err != nil {
			t.Fatalf("Error getting %s: %v", k, err)
		}
		if !found {
			t.Fatalf("Key %s not found", k)
		}
		if value != v {
			t.Fatalf("Expected value %s for key %s, got %s", v, k, value)
		}
	}

	// Create a new tree with the same storage to test persistence
	newTree, err := NewBPlusTree(4, stringLess, adapter)
	if err != nil {
		t.Fatalf("Failed to create new B+ tree: %v", err)
	}

	// Verify the data is still accessible
	for k, v := range testData {
		value, found, err := newTree.Get(k)
		if err != nil {
			t.Fatalf("Error getting %s from new tree: %v", k, err)
		}
		if !found {
			t.Fatalf("Key %s not found in new tree", k)
		}
		if value != v {
			t.Fatalf("Expected value %s for key %s in new tree, got %s", v, k, value)
		}
	}
}
