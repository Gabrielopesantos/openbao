package bptree

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
)

// DefaultOrder is the default maximum number of children per B+ tree node
const DefaultOrder = 32

// ErrKeyNotFound is returned when a key is not found in the tree
var ErrKeyNotFound = errors.New("key not found")

// ErrValueNotFound is returned when a value is not found in the tree
var ErrValueNotFound = errors.New("value not found")

// BPlusTree represents a B+ tree data structure
type BPlusTree struct {
	order int
	lock  sync.RWMutex
}

// NewBPlusTree creates a new B+ tree with the specified order and comparison functions
func NewBPlusTree(
	ctx context.Context,
	order int,
	storage Storage,
) (*BPlusTree, error) {
	if order < 2 {
		return nil, fmt.Errorf("order must be at least 2, got %d", order)
	}

	tree := &BPlusTree{
		order: order,
	}

	// Initialize the tree with a root node
	rootID, err := storage.GetRootID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get root ID: %w", err)
	}

	if rootID == "" {
		// Create a new root node
		rootNode := NewLeafNode(genUUID())

		// Save the root node
		if err := storage.SaveNode(ctx, rootNode); err != nil {
			return nil, fmt.Errorf("failed to save root node: %w", err)
		}

		// Set the root ID
		if err := storage.SetRootID(ctx, rootNode.ID); err != nil {
			return nil, fmt.Errorf("failed to set root ID: %w", err)
		}
	}

	return tree, nil
}

// getRoot loads the root node from storage
func (t *BPlusTree) getRoot(ctx context.Context, storage Storage) (*Node, error) {
	rootID, err := storage.GetRootID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get root ID: %w", err)
	}
	if rootID == "" {
		return nil, errors.New("root node not found")
	}

	return storage.LoadNode(ctx, rootID)
}

// minKeys returns the minimum number of keys a node must have
func (t *BPlusTree) minKeys() int {
	return (t.order + 1) / 2 // ⌈m/2⌉ -- Review these values
}

// maxKeys returns the maximum number of keys a node can have
func (t *BPlusTree) maxKeys() int {
	return t.order // m -- Review these values
}

// isOverfull checks if a node has exceeded its maximum capacity
func (t *BPlusTree) isOverfull(node *Node) bool {
	return len(node.Keys) > t.maxKeys()
}

// isUnderfull checks if a node has fallen below its minimum capacity
// func (t *BPlusTree) isUnderfull(node *Node) bool {
// 	return len(node.Keys) < t.minKeys()
// }

// Get retrieves all values for a key
func (t *BPlusTree) Get(ctx context.Context, storage Storage, key string) ([]string, bool, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.get(ctx, storage, key)
}

// SearchPrefix returns all key-value pairs that start with the given prefix
// This function leverages the NextID linking to efficiently traverse leaf nodes sequentially
func (t *BPlusTree) SearchPrefix(ctx context.Context, storage Storage, prefix string) (map[string][]string, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	results := make(map[string][]string)

	// Handle empty prefix - we don't allow this as it would return all keys which is expensive
	if prefix == "" {
		return results, nil // Return empty results for empty prefix
	}

	// Check if prefix is larger than any possible key
	// by comparing with the rightmost (largest) key in the tree
	rightmostLeaf, err := t.findRightmostLeaf(ctx, storage)
	if err != nil {
		return nil, fmt.Errorf("failed to find rightmost leaf: %w", err)
	}

	// If tree is empty (rightmost leaf has no keys), return empty result
	if len(rightmostLeaf.Keys) == 0 {
		return results, nil
	}

	// If prefix is lexicographically greater than the largest key, no matches possible
	largestKey := rightmostLeaf.Keys[len(rightmostLeaf.Keys)-1]
	if prefix > largestKey {
		return results, nil
	}

	// If prefix is lexicographically smaller than any key in the tree,
	// we still need to search, but we can optimize by checking if the prefix could
	// possibly match anything by comparing with the leftmost key
	leftmostLeaf, err := t.findLeftmostLeaf(ctx, storage)
	if err != nil {
		return nil, fmt.Errorf("failed to find leftmost leaf: %w", err)
	}

	// Calculate the "limit" string - the smallest string that's larger than any possible match
	// For prefix "app", the limit would be "aq" (increment last character)
	prefixLimit := calculatePrefixLimit(prefix)

	if len(leftmostLeaf.Keys) > 0 {
		smallestKey := leftmostLeaf.Keys[0]
		if prefixLimit <= smallestKey && !strings.HasPrefix(smallestKey, prefix) {
			// The prefix is so small that even after incrementing it,
			// it's still smaller than the smallest key, and the smallest key
			// doesn't match the prefix, so no matches are possible
			return results, nil
		}
	}

	// Find the first leaf that might contain our prefix
	startLeaf, err := t.findLeafNode(ctx, storage, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to find leaf node for prefix %s: %w", prefix, err)
	}

	// Traverse leaves using NextID to find all matching keys
	current := startLeaf
	for current != nil {
		// Check all keys in the current leaf
		for i, key := range current.Keys {
			if strings.HasPrefix(key, prefix) {
				// This key matches our prefix
				results[key] = current.Values[i]
			} else if key >= prefixLimit {
				// We've reached keys that are definitely beyond our prefix range
				// Since keys are sorted, we can stop here
				return results, nil
			}
		}

		// Move to the next leaf using NextID
		if current.NextID == "" {
			break
		}
		current, err = storage.LoadNode(ctx, current.NextID)
		if err != nil {
			return nil, fmt.Errorf("failed to load next leaf node: %w", err)
		}
	}

	return results, nil
}

func (t *BPlusTree) get(ctx context.Context, storage Storage, key string) ([]string, bool, error) {
	// Load the root node
	leaf, err := t.findLeafNode(ctx, storage, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to find leaf node: %w", err)
	}

	// If we get here, we are at a leaf node
	idx, indexFound := leaf.findKeyIndex(key)
	if indexFound {
		// If the key is found, return the values
		return leaf.Values[idx], true, nil
	}

	// Key not found is a valid state, not an error
	return nil, false, nil
}

// Insert inserts a key-value pair
func (t *BPlusTree) Insert(ctx context.Context, storage Storage, key string, value string) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	leaf, err := t.findLeafNode(ctx, storage, key)
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
		if err := storage.SaveNode(ctx, leaf); err != nil { // NOTE:  We do not necessarily need to save them, just cache them somewhere
			return fmt.Errorf("failed to save original leaf node: %w", err)
		}
		if err := storage.SaveNode(ctx, newLeaf); err != nil {
			return fmt.Errorf("failed to save new leaf node: %w", err)
		}
		return t.insertIntoParent(ctx, storage, leaf, newLeaf, splitKey)
	}

	// Save the leaf node after insertion
	if err := storage.SaveNode(ctx, leaf); err != nil {
		return fmt.Errorf("failed to save leaf node: %w", err)
	}

	return nil
}

// Delete removes all values for a key
func (t *BPlusTree) Delete(ctx context.Context, storage Storage, key string) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Find the leaf node where the key belongs
	leaf, err := t.findLeafNode(ctx, storage, key)
	if err != nil {
		return err
	}

	// Check if the key exists in the leaf node
	idx, found := leaf.findKeyIndex(key)
	if !found {
		return ErrKeyNotFound
	}

	// Delete the key-value pair
	leaf.removeKeyValue(idx)

	// Save the leaf node after deletion
	if err := storage.SaveNode(ctx, leaf); err != nil {
		return fmt.Errorf("failed to save leaf node: %w", err)
	}

	// TODO (gabrielopesantos): Do we really need to handle underflow?

	return nil
}

// Purge removes all keys and values from the tree
// func (t *BPlusTree[K, V]) Purge(ctx context.Context) error {
// 	return nil
// }

// DeleteValue removes a specific value for a key
func (t *BPlusTree) DeleteValue(ctx context.Context, storage Storage, key string, value string) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	leaf, err := t.findLeafNode(ctx, storage, key)
	if err != nil {
		return err
	}

	// Check if the key exists in the leaf node
	idx, found := leaf.findKeyIndex(key)
	if !found {
		return ErrKeyNotFound
	}

	// Find the value in the slice of values
	values := leaf.Values[idx]
	for i, v := range values {
		if v == value {
			// Remove the value from the slice
			values = slices.Delete(values, i, i+1)
			if len(values) == 0 {
				// If no values left, remove the key
				leaf.removeKeyValue(idx)
			} else {
				// Otherwise, update the values
				leaf.Values[idx] = values
			}

			// Save the leaf node after deletion
			if err := storage.SaveNode(ctx, leaf); err != nil {
				return fmt.Errorf("failed to save leaf node: %w", err)
			}

			return nil
		}
	}

	// Value not found
	return ErrValueNotFound
}

// findLeafNode finds the leaf node where a key belongs
func (t *BPlusTree) findLeafNode(ctx context.Context, storage Storage, key string) (*Node, error) {
	node, err := t.getRoot(ctx, storage)
	if err != nil {
		return nil, err
	}

	for !node.IsLeaf {
		found := false
		for i, k := range node.Keys {
			// If search key is less than current key, go left
			if key < k {
				var err error
				node, err = storage.LoadNode(ctx, node.ChildrenIDs[i])
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
			node, err = storage.LoadNode(ctx, node.ChildrenIDs[len(node.ChildrenIDs)-1])
			if err != nil {
				return nil, err
			}
		}
	}
	return node, nil
}

func (t *BPlusTree) splitLeafNode(leaf *Node) (*Node, string) {
	// Create a new leaf node
	newLeaf := NewLeafNode(genUUID())

	// Determine split point so that both nodes meet minimum occupancy
	// For leaf nodes, we want to ensure both nodes have at least ⌈m/2⌉ keys
	splitIndex := t.minKeys()

	// Move second half keys/values to new leaf
	newLeaf.Keys = append(newLeaf.Keys, leaf.Keys[splitIndex:]...)
	newLeaf.Values = append(newLeaf.Values, leaf.Values[splitIndex:]...)

	// Update original leaf with first half
	leaf.Keys = leaf.Keys[:splitIndex]
	leaf.Values = leaf.Values[:splitIndex]

	// Set up NextID linking: newLeaf should point to what the original leaf was pointing to
	newLeaf.NextID = leaf.NextID
	// The original leaf should now point to the new leaf
	leaf.NextID = newLeaf.ID

	// Set parent reference for the new leaf
	newLeaf.ParentID = leaf.ParentID

	// Return new leaf and split key to be copied into the parent
	return newLeaf, newLeaf.Keys[0]
}

func (t *BPlusTree) insertIntoLeaf(leaf *Node, key string, value string) error {
	leaf.insertKeyValue(key, value)
	return nil
}

// insertIntoParent inserts a key and right node into the parent of left node
func (t *BPlusTree) insertIntoParent(ctx context.Context, storage Storage, leftNode *Node, rightNode *Node, splitKey string) error {
	// If leftNode is the root, create a new root
	rootID, err := storage.GetRootID(ctx)
	if err != nil {
		return fmt.Errorf("failed to get root ID: %w", err)
	}
	if leftNode.ID == rootID {
		newRoot := NewInternalNode(genUUID())
		newRoot.Keys = []string{splitKey}
		newRoot.ChildrenIDs = []string{leftNode.ID, rightNode.ID}

		// Update parent references
		leftNode.ParentID = newRoot.ID
		rightNode.ParentID = newRoot.ID

		// Save all nodes with updated parent references
		if err := storage.SaveNode(ctx, leftNode); err != nil {
			return fmt.Errorf("failed to save left node: %w", err)
		}
		if err := storage.SaveNode(ctx, rightNode); err != nil {
			return fmt.Errorf("failed to save right node: %w", err)
		}
		if err := storage.SaveNode(ctx, newRoot); err != nil {
			return fmt.Errorf("failed to save new root: %w", err)
		}

		// Update root ID in storage
		return storage.SetRootID(ctx, newRoot.ID)
	}

	// Load parent node
	parent, err := storage.LoadNode(ctx, leftNode.ParentID)
	if err != nil {
		return err
	}

	// Find position in parent to insert the new key and child
	insertPos, _ := parent.findKeyIndex(splitKey)

	// Insert the split key and right node into the parent
	parent.insertKey(insertPos, splitKey)
	parent.insertChild(insertPos+1, rightNode.ID)

	rightNode.ParentID = parent.ID

	// Save the updated nodes
	if err := storage.SaveNode(ctx, leftNode); err != nil {
		return fmt.Errorf("failed to save left node: %w", err)
	}
	if err := storage.SaveNode(ctx, rightNode); err != nil {
		return fmt.Errorf("failed to save right node: %w", err)
	}
	if err := storage.SaveNode(ctx, parent); err != nil {
		return fmt.Errorf("failed to save parent node: %w", err)
	}

	// If the internal node is overfull, we need to split it
	if t.isOverfull(parent) {
		newInternal, splitKey := t.splitInternalNode(ctx, storage, parent)
		// Save both internal nodes after splitting
		if err := storage.SaveNode(ctx, parent); err != nil {
			return fmt.Errorf("failed to save original internal node: %w", err)
		}
		if err := storage.SaveNode(ctx, newInternal); err != nil {
			return fmt.Errorf("failed to save new internal node: %w", err)
		}
		return t.insertIntoParent(ctx, storage, parent, newInternal, splitKey)
	}

	return nil
}

func (t *BPlusTree) splitInternalNode(ctx context.Context, storage Storage, node *Node) (*Node, string) {
	// Create a new internal node
	newInternal := NewInternalNode(genUUID())

	// Determine split point so that both nodes meet minimum occupancy
	// For internal nodes, we want to ensure both nodes have at least ⌈m/2⌉ children
	splitIndex := t.minKeys()

	// The key at splitIndex is promoted, so do not copy it to any node
	newSplitKey := node.Keys[splitIndex]

	// Copy keys and children after splitIndex to newInternal
	newInternal.Keys = append(newInternal.Keys, node.Keys[splitIndex+1:]...)
	newInternal.ChildrenIDs = append(newInternal.ChildrenIDs, node.ChildrenIDs[splitIndex+1:]...)

	// Update original node with first half
	node.Keys = node.Keys[:splitIndex+1]
	node.ChildrenIDs = node.ChildrenIDs[:splitIndex+1] // Keep one extra child for the split key

	// Update parent references of newInternal's children
	for _, childID := range newInternal.ChildrenIDs {
		child, err := storage.LoadNode(ctx, childID)
		if err != nil {
			// Log error but continue since we can't fail here
			continue
		}
		child.ParentID = newInternal.ID
		if err := storage.SaveNode(ctx, child); err != nil {
			// Log error but continue since we can't fail here
			continue
		}
	}

	return newInternal, newSplitKey
}

// findLeftmostLeaf finds the leftmost leaf node in the tree
func (t *BPlusTree) findLeftmostLeaf(ctx context.Context, storage Storage) (*Node, error) {
	root, err := t.getRoot(ctx, storage)
	if err != nil {
		return nil, err
	}

	current := root
	for !current.IsLeaf {
		// Always go to the leftmost child
		if len(current.ChildrenIDs) == 0 {
			return nil, errors.New("internal node has no children")
		}
		current, err = storage.LoadNode(ctx, current.ChildrenIDs[0])
		if err != nil {
			return nil, fmt.Errorf("failed to load child node: %w", err)
		}
	}

	return current, nil
}

// findRightmostLeaf finds the rightmost leaf node in the tree
func (t *BPlusTree) findRightmostLeaf(ctx context.Context, storage Storage) (*Node, error) {
	root, err := t.getRoot(ctx, storage)
	if err != nil {
		return nil, err
	}

	current := root
	for !current.IsLeaf {
		// Always go to the rightmost child
		if len(current.ChildrenIDs) == 0 {
			return nil, errors.New("internal node has no children")
		}
		rightmostIndex := len(current.ChildrenIDs) - 1
		current, err = storage.LoadNode(ctx, current.ChildrenIDs[rightmostIndex])
		if err != nil {
			return nil, fmt.Errorf("failed to load child node: %w", err)
		}
	}

	return current, nil
}

// findPreviousLeaf finds the leaf node that should point to the given leaf in the NextID chain
func (t *BPlusTree) findPreviousLeaf(ctx context.Context, storage Storage, targetLeafID string) (*Node, error) {
	leftmost, err := t.findLeftmostLeaf(ctx, storage)
	if err != nil {
		return nil, err
	}

	current := leftmost
	for current != nil {
		if current.NextID == targetLeafID {
			return current, nil
		}
		if current.NextID == "" {
			break
		}
		current, err = storage.LoadNode(ctx, current.NextID)
		if err != nil {
			return nil, fmt.Errorf("failed to load next leaf: %w", err)
		}
	}

	return nil, nil // Not found
}

// TODO (gsantos): Delete
// Print prints a visual representation of the B+ tree
func (t *BPlusTree) Print(storage Storage) error {
	root, err := t.getRoot(context.Background(), storage)
	if err != nil {
		return err
	}

	return t.printNode(storage, root, "", true)
}

// TODO (gsantos): Delete
// printNode recursively prints a node and its children
func (t *BPlusTree) printNode(storage Storage, node *Node, prefix string, isLast bool) error {
	getPrefix := func(isLast bool) string {
		if isLast {
			return "└── "
		}
		return "├── "
	}

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
			child, err := storage.LoadNode(context.Background(), childID)
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
			if err := t.printNode(storage, child, nextPrefix, isLastChild); err != nil {
				return err
			}
		}
	}

	return nil
}
