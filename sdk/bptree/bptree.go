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
		return nil, fmt.Errorf("order must be at least 2, got %d", order)
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

// minKeys returns the minimum number of keys a node must have
func (t *BPlusTree[K, V]) minKeys() int {
	return (t.order + 1) / 2 // ⌈m/2⌉
}

// maxKeys returns the maximum number of keys a node can have
func (t *BPlusTree[K, V]) maxKeys() int {
	return t.order // m
}

// isOverfull checks if a node has exceeded its maximum capacity
func (t *BPlusTree[K, V]) isOverfull(node *Node[K, V]) bool {
	return len(node.Keys) > t.maxKeys()
}

// isUnderfull checks if a node has fallen below its minimum capacity
func (t *BPlusTree[K, V]) isUnderfull(node *Node[K, V]) bool {
	return len(node.Keys) < t.minKeys()
}

// Get retrieves a value by key
func (t *BPlusTree[K, V]) Get(key K) (V, bool, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.get(key)
}

func (t *BPlusTree[K, V]) get(key K) (V, bool, error) {
	node := t.root
	var zero V
	for !node.IsLeaf {
		found := false
		for i, k := range node.Keys {
			// If search key is less than current key, go left
			if t.less(key, k) {
				var err error
				node, err = t.storage.LoadNode(node.ChildrenIDs[i])
				if err != nil {
					return zero, false, err
				}
				found = true
				break
			}
		}
		// If we didn't find a key greater than search key, go right
		if !found {
			var err error
			node, err = t.storage.LoadNode(node.ChildrenIDs[len(node.ChildrenIDs)-1])
			if err != nil {
				return zero, false, err
			}
		}
	}

	// If we get here, we are at a leaf node
	for i, k := range node.Keys {
		if t.less(k, key) {
			continue
		}
		if k == key {
			return node.Values[i], true, nil
		}
		break // If we find a key greater than search key, we can stop
	}

	return zero, false, ErrKeyNotFound
}

// Insert inserts a key-value pair
func (t *BPlusTree[K, V]) Insert(key K, value V) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	leaf, err := t.findLeafNode(key)
	if err != nil {
		return err
	}

	err = t.insertIntoLeaf(leaf, key, value)
	if err != nil {
		return err
	}

	// If the leaf is overfull, we need to split it
	if t.isOverfull(leaf) {
		newLeaf, splitKey := t.splitLeafNode(leaf)
		// Save both leaf nodes after splitting
		if err := t.storage.SaveNode(leaf); err != nil {
			return fmt.Errorf("failed to save original leaf node: %w", err)
		}
		if err := t.storage.SaveNode(newLeaf); err != nil {
			return fmt.Errorf("failed to save new leaf node: %w", err)
		}
		return t.insertIntoParent(leaf, newLeaf, splitKey)
	}

	// Save the leaf node after insertion
	if err := t.storage.SaveNode(leaf); err != nil {
		return fmt.Errorf("failed to save leaf node: %w", err)
	}

	return nil
}

// Delete removes a key-value pair
func (t *BPlusTree[K, V]) Delete(key K) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	leaf, err := t.findLeafNode(key)
	if err != nil {
		return err
	}

	// Check if the key exists in the leaf node
	idx, found := leaf.findKeyIndex(key, t.less)
	if !found {
		return ErrKeyNotFound
	}

	// Delete the key-value pair
	leaf.Keys = append(leaf.Keys[:idx], leaf.Keys[idx+1:]...)
	leaf.Values = append(leaf.Values[:idx], leaf.Values[idx+1:]...)

	// Save the leaf node after deletion
	if err := t.storage.SaveNode(leaf); err != nil {
		return fmt.Errorf("failed to save leaf node: %w", err)
	}

	// NOTE: Handle underflow

	return nil
}

// findLeaf finds the leaf node where a key belongs
// TODO (gabrielopesantos): Maybe accept a node (instead of always starting from the root)
func (t *BPlusTree[K, V]) findLeafNode(key K) (*Node[K, V], error) {
	curr := t.root
	for !curr.IsLeaf {
		for i, k := range curr.Keys {
			if t.less(k, key) {
				continue
			}

			var err error
			curr, err = t.storage.LoadNode(curr.ChildrenIDs[i])
			if err != nil {
				return nil, err
			}
			break
		}
	}

	return curr, nil
}

func (t *BPlusTree[K, V]) splitLeafNode(leaf *Node[K, V]) (*Node[K, V], K) {
	// Create a new leaf node
	newLeaf := NewLeafNode[K, V](genUuid())

	// Determine split point so that both nodes meet minimum occupancy
	// For leaf nodes, we want to ensure both nodes have at least ⌈m/2⌉ keys
	splitIndex := t.minKeys()

	// Move second half keys/values to new leaf
	newLeaf.Keys = leaf.Keys[splitIndex:]
	newLeaf.Values = leaf.Values[splitIndex:]

	// Update original leaf with first half
	leaf.Keys = leaf.Keys[:splitIndex]
	leaf.Values = leaf.Values[:splitIndex]

	// Return new leaf and split key to be copied into the parent
	return newLeaf, newLeaf.Keys[0]
}

func (t *BPlusTree[K, V]) insertIntoLeaf(leaf *Node[K, V], key K, value V) error {
	var position int
	for position < len(leaf.Keys) && t.less(leaf.Keys[position], key) {
		position++
	}

	// Append to make room for the new element
	leaf.Keys = append(leaf.Keys, key)
	leaf.Values = append(leaf.Values, value)

	// If we're not appending to the end, shift elements to make room
	if position < len(leaf.Keys)-1 {
		copy(leaf.Keys[position+1:], leaf.Keys[position:len(leaf.Keys)-1])
		copy(leaf.Values[position+1:], leaf.Values[position:len(leaf.Values)-1])
		leaf.Keys[position] = key
		leaf.Values[position] = value
	}

	return nil
}

// insertIntoParent inserts a key and right node into the parent of left node
func (t *BPlusTree[K, V]) insertIntoParent(leftNode *Node[K, V], rightNode *Node[K, V], splitKey K) error {
	// If leftNode is the root, create a new root
	if leftNode.ID == t.root.ID {
		newRoot := NewInternalNode[K, V](genUuid())
		newRoot.Keys = []K{splitKey}
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
	for insertPos < len(parent.Keys) && t.less(parent.Keys[insertPos], splitKey) {
		insertPos++
	}

	// Insert the split key and right node into the parent
	parent.Keys = append(parent.Keys, splitKey)
	copy(parent.Keys[insertPos+1:], parent.Keys[insertPos:len(parent.Keys)-1])
	parent.Keys[insertPos] = splitKey
	parent.ChildrenIDs = append(parent.ChildrenIDs, rightNode.ID)
	copy(parent.ChildrenIDs[insertPos+2:], parent.ChildrenIDs[insertPos+1:len(parent.ChildrenIDs)-1])
	parent.ChildrenIDs[insertPos+1] = rightNode.ID

	rightNode.ParentID = parent.ID

	// Save the updated nodes
	if err := t.storage.SaveNode(rightNode); err != nil {
		return fmt.Errorf("failed to save right node: %w", err)
	}
	if err := t.storage.SaveNode(parent); err != nil {
		return fmt.Errorf("failed to save parent node: %w", err)
	}

	// If the internal node is overfull, we need to split it
	if t.isOverfull(parent) {
		newInternal, splitKey := t.splitInternalNode(parent)
		// Save both internal nodes after splitting
		if err := t.storage.SaveNode(parent); err != nil {
			return fmt.Errorf("failed to save original internal node: %w", err)
		}
		if err := t.storage.SaveNode(newInternal); err != nil {
			return fmt.Errorf("failed to save new internal node: %w", err)
		}
		return t.insertIntoParent(parent, newInternal, splitKey)
	}

	return nil
}

func (t *BPlusTree[K, V]) splitInternalNode(node *Node[K, V]) (*Node[K, V], K) {
	// Create a new internal node
	newInternal := NewInternalNode[K, V](genUuid())

	// Determine split point so that both nodes meet minimum occupancy
	// For internal nodes, we want to ensure both nodes have at least ⌈m/2⌉ children
	splitIndex := t.minKeys()

	// The key at splitIndex is promoted, so do not copy it to any node
	newSplitKey := node.Keys[splitIndex]

	// Copy keys and children after splitIndex to newInternal
	newInternal.Keys = node.Keys[splitIndex+1:]
	newInternal.ChildrenIDs = node.ChildrenIDs[splitIndex+1:]

	// Update original node with first half
	node.Keys = node.Keys[:splitIndex]
	node.ChildrenIDs = node.ChildrenIDs[:splitIndex]

	// Update parent references of newInternal's children
	for _, childID := range newInternal.ChildrenIDs {
		child, err := t.storage.LoadNode(childID)
		if err != nil {
			// Log error but continue since we can't fail here
			continue
		}
		child.ParentID = newInternal.ID
		if err := t.storage.SaveNode(child); err != nil {
			// Log error but continue since we can't fail here
			continue
		}
	}

	return newInternal, newSplitKey
}

// TODO: Move to a better place and review if we need this
func genUuid() string {
	aUuid, err := uuid.GenerateUUID()
	if err != nil {
		panic(err)
	}
	return aUuid
}

// PrintTree prints a visual representation of the B+ tree
func (t *BPlusTree[K, V]) PrintTree() error {
	t.lock.RLock()
	defer t.lock.RUnlock()

	if t.root == nil {
		fmt.Println("Empty tree")
		return nil
	}

	return t.printNode(t.root, "", true)
}

// printNode recursively prints a node and its children
func (t *BPlusTree[K, V]) printNode(node *Node[K, V], prefix string, isLast bool) error {
	// Print the current node
	if node.IsLeaf {
		log.Printf("%s%s Leaf Node (ID: %s)\n", prefix, getPrefix(isLast), node.ID)
		log.Printf("%s%s Keys: %v\n", prefix, getPrefix(isLast), node.Keys)
		log.Printf("%s%s Values: %v\n", prefix, getPrefix(isLast), node.Values)
	} else {
		log.Printf("%s%s Internal Node (ID: %s)\n", prefix, getPrefix(isLast), node.ID)
		log.Printf("%s%s Keys: %v\n", prefix, getPrefix(isLast), node.Keys)
	}

	// Print children for internal nodes
	if !node.IsLeaf {
		for i, childID := range node.ChildrenIDs {
			child, err := t.storage.LoadNode(childID)
			if err != nil {
				return fmt.Errorf("failed to load child node %s: %w", childID, err)
			}

			// Determine if this is the last child
			isLastChild := i == len(node.ChildrenIDs)-1

			// Create the prefix for the next level
			nextPrefix := prefix
			if isLast {
				nextPrefix += "    "
			} else {
				nextPrefix += "│   "
			}

			// Recursively print the child
			if err := t.printNode(child, nextPrefix, isLastChild); err != nil {
				return err
			}
		}
	}

	return nil
}

// getPrefix returns the appropriate prefix character for tree visualization
func getPrefix(isLast bool) string {
	if isLast {
		return "└── "
	}
	return "├── "
}
