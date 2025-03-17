package bptree

import (
	"errors"
	"fmt"
	"sync"
)

// DefaultOrder is the default maximum number of children per B+ tree node
const DefaultOrder = 32

// ErrKeyNotFound is returned when a key is not found in the tree
var ErrKeyNotFound = errors.New("key not found")

// BPlusTree represents a B+ tree data structure
type BPlusTree[K comparable, V any] struct {
	root      *Node[K, V]
	order     int
	storage   NodeStorage[K, V]
	less      func(a, b K) bool
	idCounter int
	lock      sync.RWMutex
}

// NewBPlusTree creates a new B+ tree with the specified order and comparison function
func NewBPlusTree[K comparable, V any](order int, less func(a, b K) bool, storage NodeStorage[K, V]) (*BPlusTree[K, V], error) {
	if order <= 2 {
		order = DefaultOrder
	}

	if less == nil {
		return nil, errors.New("comparison function cannot be nil")
	}

	tree := &BPlusTree[K, V]{
		order:     order,
		less:      less,
		storage:   storage,
		idCounter: 0,
	}

	// Initialize the tree with a root node
	rootID, err := storage.GetRootID()
	if err != nil {
		return nil, fmt.Errorf("failed to get root ID: %w", err)
	}

	if rootID == "" {
		// Create a new root node
		rootNode := NewLeafNode[K, V](tree.nextNodeID())
		tree.root = rootNode

		// Save the root node
		if err := storage.SaveNode(rootNode); err != nil {
			return nil, fmt.Errorf("failed to save root node: %w", err)
		}

		// Set the root ID
		if err := storage.SetRootID(rootNode.Id); err != nil {
			return nil, fmt.Errorf("failed to set root ID: %w", err)
		}
	} else {
		// Load the existing root node
		rootNode, err := storage.LoadNode(rootID)
		if err != nil {
			return nil, fmt.Errorf("failed to load root node: %w", err)
		}
		if rootNode == nil {
			return nil, errors.New("root node not found")
		}
		tree.root = rootNode
	}

	return tree, nil
}

// nextNodeID generates a unique node ID
func (t *BPlusTree[K, V]) nextNodeID() string {
	t.idCounter++
	return fmt.Sprintf("node-%d", t.idCounter)
}

// Get retrieves a value by key
func (t *BPlusTree[K, V]) Get(key K) (V, bool, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	node := t.findLeaf(key)
	idx, found := node.findKeyIndex(key, t.less)

	var zero V
	if !found || idx >= len(node.Keys) || t.less(key, node.Keys[idx]) || t.less(node.Keys[idx], key) {
		return zero, false, nil
	}

	return node.Values[idx], true, nil
}

// Put inserts or updates a key-value pair
func (t *BPlusTree[K, V]) Put(key K, value V) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Find the leaf node where this key belongs
	leaf := t.findLeaf(key)

	// Check if the key already exists
	idx, found := leaf.findKeyIndex(key, t.less)
	if found {
		// Update existing key
		leaf.Values[idx] = value
		return t.storage.SaveNode(leaf)
	}

	// Insert new key-value pair
	leaf.insertKeyValue(idx, key, value)

	// Split if necessary
	if len(leaf.Keys) > t.order-1 {
		return t.splitLeaf(leaf)
	}

	return t.storage.SaveNode(leaf)
}

// Delete removes a key-value pair
func (t *BPlusTree[K, V]) Delete(key K) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Find the leaf node where this key belongs
	leaf := t.findLeaf(key)

	// Check if the key exists
	idx, found := leaf.findKeyIndex(key, t.less)
	if !found {
		return ErrKeyNotFound
	}

	// Remove the key-value pair
	leaf.removeKeyValue(idx)

	// Handle underflow (if needed)
	if len(leaf.Keys) < (t.order-1)/2 && leaf != t.root {
		// TODO: Implement node merging and redistribution
		// For now, we'll just save the node
	}

	return t.storage.SaveNode(leaf)
}

// findLeaf finds the leaf node where a key belongs
func (t *BPlusTree[K, V]) findLeaf(key K) *Node[K, V] {
	node := t.root
	if node == nil {
		return nil
	}

	for !node.IsLeaf {
		idx, _ := node.findKeyIndex(key, t.less)
		if idx >= len(node.Children) {
			idx = len(node.Children) - 1
		}

		// Check if children are nil or empty
		if len(node.Children) == 0 {
			return node // Return current node if no children
		}

		childNode := node.Children[idx]
		if childNode == nil {
			return node // Return current node if child is nil
		}

		// If we have a storage backend, load the child from storage
		if t.storage != nil && childNode.Id != "" {
			loadedNode, err := t.storage.LoadNode(childNode.Id)
			if err != nil || loadedNode == nil {
				// If we can't load the child, return the current node
				return node
			}
			childNode = loadedNode
		}

		node = childNode
	}

	return node
}

// splitLeaf splits a leaf node when it's full
func (t *BPlusTree[K, V]) splitLeaf(leaf *Node[K, V]) error {
	// Create a new leaf
	newLeaf := NewLeafNode[K, V](t.nextNodeID())

	// Determine split point
	splitIndex := t.order / 2

	// Move half of the keys and values to the new leaf
	newLeaf.Keys = append(newLeaf.Keys, leaf.Keys[splitIndex:]...)
	newLeaf.Values = append(newLeaf.Values, leaf.Values[splitIndex:]...)

	// Update the original leaf
	leaf.Keys = leaf.Keys[:splitIndex]
	leaf.Values = leaf.Values[:splitIndex]

	// Update the leaf node chain for range queries
	newLeaf.Next = leaf.Next
	leaf.Next = newLeaf

	// If this is the root node, create a new root
	if leaf == t.root {
		newRoot := NewInternalNode[K, V](t.nextNodeID())
		newRoot.Keys = append(newRoot.Keys, newLeaf.Keys[0])
		newRoot.Children = append(newRoot.Children, leaf, newLeaf)

		leaf.Parent = newRoot
		newLeaf.Parent = newRoot

		t.root = newRoot

		// Save all nodes
		if err := t.storage.SaveNode(leaf); err != nil {
			return err
		}
		if err := t.storage.SaveNode(newLeaf); err != nil {
			return err
		}
		if err := t.storage.SaveNode(newRoot); err != nil {
			return err
		}

		// Update root ID
		return t.storage.SetRootID(newRoot.Id)
	}

	// Otherwise, insert the new leaf into the parent
	parent := leaf.Parent

	// Find the position to insert in the parent
	var insertIdx int
	for insertIdx < len(parent.Children) && parent.Children[insertIdx] != leaf {
		insertIdx++
	}
	insertIdx++ // Insert after the current leaf

	// Insert the new leaf and its first key into the parent
	parent.Keys = append(parent.Keys, newLeaf.Keys[0])
	if insertIdx < len(parent.Keys) {
		copy(parent.Keys[insertIdx+1:], parent.Keys[insertIdx:])
		parent.Keys[insertIdx] = newLeaf.Keys[0]
	}

	parent.Children = append(parent.Children, nil)
	if insertIdx < len(parent.Children)-1 {
		copy(parent.Children[insertIdx+1:], parent.Children[insertIdx:])
		parent.Children[insertIdx] = newLeaf
	}

	newLeaf.Parent = parent

	// Save the nodes
	if err := t.storage.SaveNode(leaf); err != nil {
		return err
	}
	if err := t.storage.SaveNode(newLeaf); err != nil {
		return err
	}

	// Split the parent if necessary
	if len(parent.Keys) > t.order-1 {
		return t.splitInternal(parent)
	}

	return t.storage.SaveNode(parent)
}

// splitInternal splits an internal node when it's full
func (t *BPlusTree[K, V]) splitInternal(node *Node[K, V]) error {
	// Create a new internal node
	newNode := NewInternalNode[K, V](t.nextNodeID())

	// Determine split point
	splitIndex := t.order / 2

	// Get the middle key that will move up to the parent
	middleKey := node.Keys[splitIndex]

	// Move half of the keys and children to the new node
	newNode.Keys = append(newNode.Keys, node.Keys[splitIndex+1:]...)
	newNode.Children = append(newNode.Children, node.Children[splitIndex+1:]...)

	// Update parent pointers for all children
	for _, child := range newNode.Children {
		child.Parent = newNode
	}

	// Update the original node
	node.Keys = node.Keys[:splitIndex]
	node.Children = node.Children[:splitIndex+1]

	// If this is the root, create a new root
	if node == t.root {
		newRoot := NewInternalNode[K, V](t.nextNodeID())
		newRoot.Keys = append(newRoot.Keys, middleKey)
		newRoot.Children = append(newRoot.Children, node, newNode)

		node.Parent = newRoot
		newNode.Parent = newRoot

		t.root = newRoot

		// Save all nodes
		if err := t.storage.SaveNode(node); err != nil {
			return err
		}
		if err := t.storage.SaveNode(newNode); err != nil {
			return err
		}
		if err := t.storage.SaveNode(newRoot); err != nil {
			return err
		}

		// Update root ID
		return t.storage.SetRootID(newRoot.Id)
	}

	// Otherwise, insert the middle key into the parent
	parent := node.Parent

	// Find the position to insert in the parent
	var insertIdx int
	for insertIdx < len(parent.Children) && parent.Children[insertIdx] != node {
		insertIdx++
	}
	insertIdx++ // Insert after the current node

	// Insert the new node and the middle key into the parent
	parent.Keys = append(parent.Keys, middleKey)
	if insertIdx-1 < len(parent.Keys) {
		copy(parent.Keys[insertIdx:], parent.Keys[insertIdx-1:])
		parent.Keys[insertIdx-1] = middleKey
	}

	parent.Children = append(parent.Children, nil)
	if insertIdx < len(parent.Children)-1 {
		copy(parent.Children[insertIdx+1:], parent.Children[insertIdx:])
		parent.Children[insertIdx] = newNode
	}

	newNode.Parent = parent

	// Save the nodes
	if err := t.storage.SaveNode(node); err != nil {
		return err
	}
	if err := t.storage.SaveNode(newNode); err != nil {
		return err
	}

	// Split the parent if necessary
	if len(parent.Keys) > t.order-1 {
		return t.splitInternal(parent)
	}

	return t.storage.SaveNode(parent)
}

// Range iterates over key-value pairs in the given key range [startKey, endKey]
// The callback function is called for each key-value pair
// If the callback returns false, iteration stops
func (t *BPlusTree[K, V]) Range(startKey, endKey K, callback func(K, V) bool) error {
	t.lock.RLock()
	defer t.lock.RUnlock()

	// Find the leaf node where the start key would be
	node := t.findLeaf(startKey)
	if node == nil {
		return nil // Empty tree
	}

	// Find the starting position in the leaf
	startIdx, _ := node.findKeyIndex(startKey, t.less)

	// Iterate through the leaf nodes
	for node != nil {
		// Iterate through keys in the current leaf
		for i := startIdx; i < len(node.Keys); i++ {
			// If we've passed the end key, stop
			if endKey != *new(K) && t.less(endKey, node.Keys[i]) {
				return nil
			}

			// Call the callback with the current key-value pair
			if !callback(node.Keys[i], node.Values[i]) {
				return nil // Callback requested to stop
			}
		}

		// Move to the next leaf node
		if node.Next == nil {
			break
		}

		// Load the next node
		nextNode, err := t.storage.LoadNode(node.Next.Id)
		if err != nil {
			return fmt.Errorf("failed to load next node: %w", err)
		}

		node = nextNode
		startIdx = 0 // Start from the beginning of the next node
	}

	return nil
}
