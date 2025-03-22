package bptree

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/hashicorp/go-uuid"
)

// DefaultOrder is the default maximum number of children per B+ tree node
const DefaultOrder = 32

// ErrKeyNotFound is returned when a key is not found in the tree
var ErrKeyNotFound = errors.New("key not found")

// BPlusTree represents a B+ tree data structure
type BPlusTree[K comparable, V any] struct {
	root    *Node[K, V]
	order   int
	storage NodeStorage[K, V]
	less    func(a, b K) bool
	lock    sync.RWMutex
}

// NewBPlusTree creates a new B+ tree with the specified order and comparison function
func NewBPlusTree[K comparable, V any](order int, less func(a, b K) bool, storage NodeStorage[K, V]) (*BPlusTree[K, V], error) {
	if order < 2 {
		order = DefaultOrder
	}

	if less == nil {
		return nil, errors.New("comparison function cannot be nil")
	}

	tree := &BPlusTree[K, V]{
		order:   order,
		less:    less,
		storage: storage,
	}

	// Initialize the tree with a root node
	rootID, err := storage.GetRootID()
	if err != nil {
		return nil, fmt.Errorf("failed to get root ID: %w", err)
	}

	if rootID == "" {
		// Create a new root node
		rootNode := NewLeafNode[K, V](genUuid())
		tree.root = rootNode

		// Save the root node
		if err := storage.SaveNode(rootNode); err != nil {
			return nil, fmt.Errorf("failed to save root node: %w", err)
		}

		// Set the root ID
		if err := storage.SetRootID(rootNode.ID); err != nil {
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

// Get retrieves a value by key
func (t *BPlusTree[K, V]) Get(key K) (V, bool, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.get(key)
}

func (t *BPlusTree[K, V]) get(key K) (V, bool, error) {
	var zero V
	leaf, err := t.findLeaf(key)
	if err != nil {
		return zero, false, err
	}

	idx, found := leaf.findKeyIndex(key, t.less)
	if !found {
		return zero, false, ErrKeyNotFound
	}

	return leaf.Values[idx], true, nil
}

// Insert inserts a key-value pair
func (t *BPlusTree[K, V]) Insert(key K, value V) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	_, found, err := t.get(key)
	if err != nil && err != ErrKeyNotFound {
		return err
	}

	if found { // TODO: Update or probably support multiple values.
		return errors.New("key already exists")
	}

	leaf, err := t.findLeaf(key)
	if err != nil {
		return err
	}

	// If this insert operation doesn't cause the leaf
	// node to overflow, we can insert the key-value pair directly.
	log.Printf("Keys: %v | Leaf Keys: %d | Order: %d", leaf.Keys, len(leaf.Keys), t.order)
	if len(leaf.Keys) < 2*t.order {
		log.Printf("Inserting into leaf")
		return t.insertIntoLeaf(leaf, key, value)
	}

	log.Printf("Splitting leaf")
	return t.insertIntoLeafAfterSplitting(leaf, key, value)
}

// Delete removes a key-value pair
func (t *BPlusTree[K, V]) Delete(key K) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Find the leaf node containing the key
	leaf, err := t.findLeaf(key)
	if err != nil {
		return err
	}
	if leaf == nil {
		return ErrKeyNotFound
	}

	// Find the key in the leaf
	idx, found := leaf.findKeyIndex(key, t.less)
	if !found {
		return ErrKeyNotFound
	}

	// Remove the key-value pair from the leaf
	leaf.Keys = append(leaf.Keys[:idx], leaf.Keys[idx+1:]...)
	leaf.Values = append(leaf.Values[:idx], leaf.Values[idx+1:]...)

	// Save the updated leaf node
	if err := t.storage.SaveNode(leaf); err != nil {
		return err
	}

	// If the leaf is empty and it's not the root, we should handle that
	// (For a complete implementation, we would need to merge or redistribute keys)

	return nil
}

// findLeaf finds the leaf node where a key belongs
func (t *BPlusTree[K, V]) findLeaf(key K) (*Node[K, V], error) {
	curr := t.root
	for !curr.IsLeaf {
		idx, _ := curr.findKeyIndex(key, t.less)
		currID := curr.ChildrenIDs[idx]
		var err error
		curr, err = t.storage.LoadNode(currID)
		if err != nil {
			return nil, err
		}
	}

	return curr, nil
}

func (t *BPlusTree[K, V]) insertIntoLeaf(leaf *Node[K, V], key K, value V) error {
	idx, _ := leaf.findKeyIndex(key, t.less)
	leaf.insertKeyValue(idx, key, value)
	err := t.storage.SaveNode(leaf)
	if err != nil {
		return err
	}

	return nil
}

// insertIntoLeafAfterSplitting handles insertion when a leaf node is full
func (t *BPlusTree[K, V]) insertIntoLeafAfterSplitting(leaf *Node[K, V], key K, value V) error {
	// Create a new leaf node
	newLeaf := NewLeafNode[K, V](genUuid())

	// Make temporary slices to hold all keys and values including the new one
	tempKeys := make([]K, 0, len(leaf.Keys)+1)
	tempValues := make([]V, 0, len(leaf.Values)+1)

	// Find position where the new key should be inserted
	insertPos, _ := leaf.findKeyIndex(key, t.less)

	// Fill temporary slices in proper order
	for i := 0; i < len(leaf.Keys); i++ {
		if i == insertPos {
			tempKeys = append(tempKeys, key)
			tempValues = append(tempValues, value)
		}
		tempKeys = append(tempKeys, leaf.Keys[i])
		tempValues = append(tempValues, leaf.Values[i])
	}

	// If the key belongs at the end
	if insertPos == len(leaf.Keys) {
		tempKeys = append(tempKeys, key)
		tempValues = append(tempValues, value)
	}

	// Determine split point so that order-1 elements are in the original leaf
	// and order elements are in the new leaf
	splitPoint := t.order - 1

	// Update original leaf with first half
	leaf.Keys = tempKeys[:splitPoint]
	leaf.Values = tempValues[:splitPoint]

	// Update new leaf with second half
	newLeaf.Keys = tempKeys[splitPoint:]
	newLeaf.Values = tempValues[splitPoint:]

	// Save both nodes
	if err := t.storage.SaveNode(leaf); err != nil {
		return err
	}
	if err := t.storage.SaveNode(newLeaf); err != nil {
		return err
	}

	// Insert the first key of the new leaf into the parent
	return t.insertIntoParent(leaf, newLeaf.Keys[0], newLeaf)
}

// insertIntoParent inserts a key and right node into the parent of left node
func (t *BPlusTree[K, V]) insertIntoParent(leftNode *Node[K, V], key K, rightNode *Node[K, V]) error {
	// If leftNode is the root, create a new root
	if leftNode.ID == t.root.ID {
		newRoot := NewInternalNode[K, V](genUuid())
		newRoot.Keys = []K{key}
		newRoot.ChildrenIDs = []string{leftNode.ID, rightNode.ID}

		// Update parent references
		leftNode.ParentID = newRoot.ID
		rightNode.ParentID = newRoot.ID

		// Update root
		t.root = newRoot

		// Save all nodes with updated parent references
		if err := t.storage.SaveNode(leftNode); err != nil {
			return fmt.Errorf("failed to save left node: %w", err)
		}
		if err := t.storage.SaveNode(rightNode); err != nil {
			return fmt.Errorf("failed to save right node: %w", err)
		}
		if err := t.storage.SaveNode(newRoot); err != nil {
			return fmt.Errorf("failed to save new root: %w", err)
		}

		// Update root ID in storage
		return t.storage.SetRootID(newRoot.ID)
	}

	// Load parent node
	parent, err := t.storage.LoadNode(leftNode.ParentID)
	if err != nil {
		return err
	}

	// Find position in parent to insert the new key and child
	var insertPos int
	for insertPos < len(parent.Keys) && t.less(parent.Keys[insertPos], leftNode.Keys[0]) {
		insertPos++
	}

	// If parent is not full, insert directly
	if len(parent.Keys) < t.order-1 {
		// Insert key
		parent.Keys = append(parent.Keys, key)
		copy(parent.Keys[insertPos+1:], parent.Keys[insertPos:len(parent.Keys)-1])
		parent.Keys[insertPos] = key

		// Insert child ID
		parent.ChildrenIDs = append(parent.ChildrenIDs, rightNode.ID)
		copy(parent.ChildrenIDs[insertPos+2:], parent.ChildrenIDs[insertPos+1:len(parent.ChildrenIDs)-1])
		parent.ChildrenIDs[insertPos+1] = rightNode.ID

		// Update right node's parent reference
		rightNode.ParentID = parent.ID

		// Save the updated nodes
		if err := t.storage.SaveNode(rightNode); err != nil {
			return err
		}
		return t.storage.SaveNode(parent)
	}

	// Parent is full, need to split it
	return t.splitInternalNode(parent, insertPos, key, rightNode)
}

// splitInternalNode splits an internal node when it's full
func (t *BPlusTree[K, V]) splitInternalNode(node *Node[K, V], insertPos int, key K, rightChild *Node[K, V]) error {
	// Create temporary slices for all keys and children
	tempKeys := make([]K, len(node.Keys)+1)
	tempChildren := make([]string, len(node.ChildrenIDs)+1)

	// Copy keys before insert position
	copy(tempKeys[:insertPos], node.Keys[:insertPos])

	// Insert the new key
	tempKeys[insertPos] = key

	// Copy remaining keys
	copy(tempKeys[insertPos+1:], node.Keys[insertPos:])

	// Copy children pointers before insert position (including the one at insert position)
	copy(tempChildren[:insertPos+1], node.ChildrenIDs[:insertPos+1])

	// Insert the new child pointer
	tempChildren[insertPos+1] = rightChild.ID

	// Copy remaining children pointers
	copy(tempChildren[insertPos+2:], node.ChildrenIDs[insertPos+1:])

	// Create a new internal node
	newNode := NewInternalNode[K, V](genUuid())

	// Split point is order-1 to maintain B+ tree properties
	splitPoint := t.order - 1

	// Key to move up to the parent
	promoteKey := tempKeys[splitPoint]

	// Update original node with entries before the split
	node.Keys = tempKeys[:splitPoint]
	node.ChildrenIDs = tempChildren[:splitPoint+1]

	// Add entries after split to new node (excluding the promoted key)
	newNode.Keys = tempKeys[splitPoint+1:]
	newNode.ChildrenIDs = tempChildren[splitPoint+1:]

	// Update parent references for children of the new node
	for _, childID := range newNode.ChildrenIDs {
		child, err := t.storage.LoadNode(childID)
		if err != nil {
			return err
		}

		child.ParentID = newNode.ID

		if err := t.storage.SaveNode(child); err != nil {
			return err
		}
	}

	// Set parent of right child to new node
	rightChild.ParentID = newNode.ID

	// Save all modified nodes
	if err := t.storage.SaveNode(rightChild); err != nil {
		return err
	}
	if err := t.storage.SaveNode(node); err != nil {
		return err
	}
	if err := t.storage.SaveNode(newNode); err != nil {
		return err
	}

	// Recursively insert the promoted key into the parent
	return t.insertIntoParent(node, promoteKey, newNode)
}

// TODO: Move to a better place and review if we need this
func genUuid() string {
	aUuid, err := uuid.GenerateUUID()
	if err != nil {
		panic(err)
	}
	return aUuid
}
