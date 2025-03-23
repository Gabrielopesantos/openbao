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

		// Save the root node
		if err := storage.SaveNode(rootNode); err != nil {
			return nil, fmt.Errorf("failed to save root node: %w", err)
		}

		// Set the root ID
		if err := storage.SetRootID(rootNode.ID); err != nil {
			return nil, fmt.Errorf("failed to set root ID: %w", err)
		}
	}

	return tree, nil
}

// getRoot loads the root node from storage
func (t *BPlusTree[K, V]) getRoot() (*Node[K, V], error) {
	rootID, err := t.storage.GetRootID()
	if err != nil {
		return nil, fmt.Errorf("failed to get root ID: %w", err)
	}
	if rootID == "" {
		return nil, errors.New("root node not found")
	}
	root, err := t.storage.LoadNode(rootID)
	if err != nil {
		return nil, fmt.Errorf("failed to load root node: %w", err)
	}
	return root, nil
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
	var zero V
	leaf, err := t.findLeafNode(key)
	if err != nil {
		return zero, false, fmt.Errorf("failed to find leaf node: %w", err)
	}

	// If we get here, we are at a leaf node
	idx, found := leaf.findKeyIndex(key, t.less)
	if found {
		return leaf.Values[idx], true, nil
	}

	// Key not found is a valid state, not an error
	return zero, false, nil
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
	leaf.removeKeyValue(idx)

	// Save the leaf node after deletion
	if err := t.storage.SaveNode(leaf); err != nil {
		return fmt.Errorf("failed to save leaf node: %w", err)
	}

	// TODO (gabrielopesantos): Handle underflow

	return nil
}

// findLeafNode finds the leaf node where a key belongs
func (t *BPlusTree[K, V]) findLeafNode(key K) (*Node[K, V], error) {
	node, err := t.getRoot()
	if err != nil {
		return nil, err
	}

	for !node.IsLeaf {
		found := false
		for i, k := range node.Keys {
			// If search key is less than current key, go left
			if t.less(key, k) {
				var err error
				node, err = t.storage.LoadNode(node.ChildrenIDs[i])
				if err != nil {
					return nil, err
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
				return nil, err
			}
		}
	}
	return node, nil
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
	idx, _ := leaf.findKeyIndex(key, t.less)
	leaf.insertKeyValue(idx, key, value)
	return nil
}

// insertIntoParent inserts a key and right node into the parent of left node
func (t *BPlusTree[K, V]) insertIntoParent(leftNode *Node[K, V], rightNode *Node[K, V], splitKey K) error {
	// If leftNode is the root, create a new root
	rootID, err := t.storage.GetRootID()
	if err != nil {
		return fmt.Errorf("failed to get root ID: %w", err)
	}
	if leftNode.ID == rootID {
		newRoot := NewInternalNode[K, V](genUuid())
		newRoot.Keys = []K{splitKey}
		newRoot.ChildrenIDs = []string{leftNode.ID, rightNode.ID}

		// Update parent references
		leftNode.ParentID = newRoot.ID
		rightNode.ParentID = newRoot.ID

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
	insertPos, _ := parent.findKeyIndex(splitKey, t.less)

	// Insert the split key and right node into the parent
	parent.insertKey(insertPos, splitKey)
	parent.insertChild(insertPos+1, rightNode.ID)

	rightNode.ParentID = parent.ID

	// Save the updated nodes
	if err := t.storage.SaveNode(leftNode); err != nil {
		return fmt.Errorf("failed to save left node: %w", err)
	}
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
	node.Keys = node.Keys[:splitIndex+1]
	node.ChildrenIDs = node.ChildrenIDs[:splitIndex+1] // Keep one extra child for the split key

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

// Print prints a visual representation of the B+ tree
func (t *BPlusTree[K, V]) Print() error {
	root, err := t.getRoot()
	if err != nil {
		return err
	}

	return t.printNode(root, "", true, true)
}

// printNode recursively prints a node and its children
func (t *BPlusTree[K, V]) printNode(node *Node[K, V], prefix string, isLast bool, isRoot bool) error {
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
			if err := t.printNode(child, nextPrefix, isLastChild, false); err != nil {
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
