package bptree

import (
	"context"
	"testing"

	"github.com/openbao/openbao/sdk/v2/logical"
)

// stringLess is a comparison function for strings
func stringLess(a, b string) bool {
	return a < b
}

// intLess is a comparison function for integers
func intLess(a, b int) bool {
	return a < b
}

// TestNewBPlusTree tests the creation of a new B+ tree
func TestNewBPlusTree(t *testing.T) {
	s := &logical.InmemStorage{}
	storage := NewStorageAdapter[string, string](context.Background(), s, "bptree", nil)

	tree, err := NewBPlusTree(4, stringLess, storage)
	if err != nil {
		t.Fatalf("Failed to create B+ tree: %v", err)
	}

	if tree.root == nil {
		t.Fatal("Root node is nil")
	}

	if !tree.root.IsLeaf {
		t.Fatal("Root node should be a leaf in a new tree")
	}

	if len(tree.root.Keys) != 0 {
		t.Fatal("Root node should be empty in a new tree")
	}
}

// TestBPlusTreeBasic tests basic operations of the B+ tree
func TestBPlusTreeBasic(t *testing.T) {
	s := &logical.InmemStorage{}
	storage := NewStorageAdapter[string, string](context.Background(), s, "bptree", nil)
	tree, _ := NewBPlusTree(4, stringLess, storage)

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

	// Test updating an existing key
	if err := tree.Put("apple", "green"); err != nil {
		t.Fatalf("Failed to update apple: %v", err)
	}

	value, found, err := tree.Get("apple")
	if err != nil {
		t.Fatalf("Error getting apple: %v", err)
	}
	if !found {
		t.Fatalf("Key apple not found after update")
	}
	if value != "green" {
		t.Fatalf("Expected value green for key apple, got %s", value)
	}

	// Test deleting a key
	if err := tree.Delete("banana"); err != nil {
		t.Fatalf("Failed to delete banana: %v", err)
	}

	_, found, err = tree.Get("banana")
	if err != nil {
		t.Fatalf("Error getting banana: %v", err)
	}
	if found {
		t.Fatalf("Key banana should have been deleted")
	}

	// Test range query
	results := make(map[string]string)
	err = tree.Range("apple", "cherry", func(k, v string) bool {
		results[k] = v
		return true
	})

	if err != nil {
		t.Fatalf("Range query failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	if results["apple"] != "green" {
		t.Fatalf("Expected value green for key apple, got %s", results["apple"])
	}

	if results["cherry"] != "red" {
		t.Fatalf("Expected value red for key cherry, got %s", results["cherry"])
	}
}

// TestBPlusTreeRange tests range queries in the B+ tree
func TestBPlusTreeRange(t *testing.T) {
	s := &logical.InmemStorage{}
	storage := NewStorageAdapter[string, string](context.Background(), s, "bptree", nil)
	tree, _ := NewBPlusTree(4, stringLess, storage)

	// Insert some key-value pairs in non-sequential order
	testData := map[string]string{
		"apple":  "red",
		"fig":    "purple",
		"banana": "yellow",
		"grape":  "purple",
		"cherry": "red",
		"date":   "brown",
		"kiwi":   "green",
		"lemon":  "yellow",
		"mango":  "orange",
	}

	for k, v := range testData {
		if err := tree.Put(k, v); err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	// Test range query
	results := make(map[string]string)
	err := tree.Range("banana", "kiwi", func(k, v string) bool {
		results[k] = v
		return true
	})

	if err != nil {
		t.Fatalf("Range query failed: %v", err)
	}

	// Expected keys in range [banana, kiwi]
	expectedKeys := []string{"banana", "cherry", "date", "fig", "grape", "kiwi"}

	if len(results) != len(expectedKeys) {
		t.Fatalf("Expected %d results, got %d", len(expectedKeys), len(results))
	}

	for _, k := range expectedKeys {
		v, ok := results[k]
		if !ok {
			t.Fatalf("Expected key %s in range results", k)
		}
		if v != testData[k] {
			t.Fatalf("Expected value %s for key %s, got %s", testData[k], k, v)
		}
	}

	// Test early termination
	count := 0
	err = tree.Range("apple", "mango", func(k, v string) bool {
		count++
		return count < 3 // Stop after 3 items
	})

	if err != nil {
		t.Fatalf("Range query with early termination failed: %v", err)
	}

	if count != 3 {
		t.Fatalf("Expected 3 results before termination, got %d", count)
	}
}

// TestBPlusTreeSplitting tests node splitting when a node becomes full
// func TestBPlusTreeSplitting(t *testing.T) {
// 	// Create a tree with a small order to force splits
// 	s := &logical.InmemStorage{}
// 	storage := NewStorageAdapter[int, string](context.Background(), s, "bptree", nil)
// 	tree, _ := NewBPlusTree(3, intLess, storage)

// 	// Insert keys in order
// 	for i := 1; i <= 5; i++ {
// 		t.Logf("Inserting key: %d, value: value-%d", i, i)
// 		if err := tree.Put(i, fmt.Sprintf("value-%d", i)); err != nil {
// 			t.Fatalf("Failed to put %d: %v", i, err)
// 		}
// 	}

// 	// Debug: print all nodes in storage
// 	t.Logf("Root ID: %s", storage.rootID)
// 	t.Logf("Nodes in storage:")
// 	for id, node := range storage.nodes {
// 		t.Logf("  Node %s: isLeaf=%v, keys=%v", id, node.isLeaf, node.keys)
// 		if len(node.children) > 0 {
// 			childIDs := make([]string, 0, len(node.children))
// 			for _, child := range node.children {
// 				if child != nil {
// 					childIDs = append(childIDs, child.id)
// 				} else {
// 					childIDs = append(childIDs, "nil")
// 				}
// 			}
// 			t.Logf("    Children: %v", childIDs)
// 		}
// 	}

// 	// Verify all keys can be retrieved
// 	for i := 1; i <= 5; i++ {
// 		t.Logf("Retrieving key: %d, expected value: value-%d", i, i)
// 		value, found, err := tree.Get(i)
// 		if err != nil {
// 			t.Fatalf("Error getting %d: %v", i, err)
// 		}
// 		if !found {
// 			t.Fatalf("Key %d not found", i)
// 		}
// 		if value != fmt.Sprintf("value-%d", i) {
// 			t.Fatalf("Expected value-%d, got %s", i, value)
// 		}
// 	}

// 	// Verify the tree structure
// 	if tree.root.isLeaf {
// 		t.Fatal("Root should not be a leaf after multiple splits")
// 	}

// 	// Count the number of nodes in the tree
// 	nodeCount := 0
// 	for range storage.nodes {
// 		nodeCount++
// 	}

// 	// A tree with order 3 and 5 keys should have multiple nodes
// 	if nodeCount < 3 {
// 		t.Fatalf("Expected at least 3 nodes, got %d", nodeCount)
// 	}
// }

// // TestConcurrentAccess tests concurrent access to the B+ tree
// func TestConcurrentAccess(t *testing.T) {
// 	s := &logical.InmemStorage{}
// 	storage := NewStorageAdapter[int, int](context.Background(), s, "bptree", nil)
// 	tree, _ := NewBPlusTree(8, intLess, storage)

// 	// Number of concurrent operations
// 	const numOps = 10

// 	// Insert initial data
// 	for i := 0; i < numOps; i++ {
// 		if err := tree.Put(i, i*10); err != nil {
// 			t.Fatalf("Failed to put %d: %v", i, err)
// 		}
// 	}

// 	// Debug: print all nodes in storage
// 	t.Logf("Root ID: %s", storage.rootID)
// 	t.Logf("Nodes in storage:")
// 	for id, node := range storage.nodes {
// 		t.Logf("  Node %s: isLeaf=%v, keys=%v", id, node.isLeaf, node.keys)
// 	}

// 	var wg sync.WaitGroup
// 	wg.Add(2) // 2 goroutines

// 	// Goroutine 1: Read existing keys
// 	go func() {
// 		defer wg.Done()
// 		for i := 0; i < numOps; i++ {
// 			value, found, err := tree.Get(i)
// 			if err != nil {
// 				t.Errorf("Error getting %d: %v", i, err)
// 				return
// 			}
// 			if !found {
// 				t.Errorf("Key %d not found", i)
// 				return
// 			}
// 			if value != i*10 {
// 				t.Errorf("Expected %d, got %d", i*10, value)
// 				return
// 			}
// 		}
// 	}()

// 	// Goroutine 2: Update existing keys
// 	go func() {
// 		defer wg.Done()
// 		for i := 0; i < numOps; i += 2 {
// 			if err := tree.Put(i, i*20); err != nil {
// 				t.Errorf("Failed to update %d: %v", i, err)
// 				return
// 			}
// 		}
// 	}()

// 	wg.Wait()

// 	// Verify the data
// 	for i := 0; i < numOps; i++ {
// 		expected := i * 10
// 		if i%2 == 0 {
// 			expected = i * 20 // Updated values
// 		}

// 		value, found, err := tree.Get(i)
// 		if err != nil {
// 			t.Fatalf("Error getting %d: %v", i, err)
// 		}
// 		if !found {
// 			t.Fatalf("Key %d not found", i)
// 		}
// 		if value != expected {
// 			t.Fatalf("Expected %d, got %d", expected, value)
// 		}
// 	}
// }
