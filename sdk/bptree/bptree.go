package bptree

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
)

// BPlusTree represents a B+ tree data structure
type BPlusTree struct {
	config       *BPlusTreeConfig // B+Tree configuration
	lock         sync.RWMutex     // Mutex to protect concurrent access
	cachedRootID string           // Cached root ID
}

// InitializeBPlusTree initializes a tree, creating it if it doesn't exist or loading it if it does.
// For new trees, the provided config is used. For existing trees, stored config is used and only the TreeID is used.
func InitializeBPlusTree(
	ctx context.Context,
	storage Storage,
	config *BPlusTreeConfig,
) (*BPlusTree, error) {
	if config == nil {
		config = NewDefaultBPlusTreeConfig()
	} else if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Try to load existing tree first
	existingTree, err := LoadExistingBPlusTree(ctx, storage, config.TreeID)
	if err == nil {
		return existingTree, nil
	}

	// If tree doesn't exist, create it
	return NewBPlusTree(ctx, storage, config)
}

// NewBPlusTree creates a new B+ tree with the given configuration.
// Fails if a tree with the same ID already exists.
func NewBPlusTree(
	ctx context.Context,
	storage Storage,
	config *BPlusTreeConfig,
) (*BPlusTree, error) {
	if config == nil {
		return nil, fmt.Errorf("config is required for tree creation")
	}
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Add the treeID to the context
	ctx = config.contextWithTreeID(ctx)

	// Check if tree already exists
	existingConfig, err := storage.GetTreeConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check for existing tree: %w", err)
	}
	if existingConfig != nil {
		return nil, fmt.Errorf("tree (%s) already exists", config.TreeID)
	}

	tree := &BPlusTree{
		config: config,
	}

	// Create new leaf root
	root := NewLeafNode(generateUUID())
	if err := storage.SaveNode(ctx, root); err != nil {
		return nil, fmt.Errorf("failed to save root node: %w", err)
	}

	// Set root ID
	if err := tree.setRootID(ctx, storage, root.ID); err != nil {
		return nil, fmt.Errorf("failed to set root ID: %w", err)
	}

	// Store configuration
	if err := storage.SetTreeConfig(ctx, config); err != nil {
		return nil, fmt.Errorf("failed to store tree configuration: %w", err)
	}

	return tree, nil
}

// LoadExistingBPlusTree loads an existing B+ tree from storage using the stored configuration
// as the source of truth. If the tree doesn't exist, returns an error.
func LoadExistingBPlusTree(
	ctx context.Context,
	storage Storage,
	treeID string,
) (*BPlusTree, error) {
	if treeID == "" {
		return nil, fmt.Errorf("treeID cannot be empty")
	}
	// Add the TreeID to the context
	ctx = withTreeID(ctx, treeID)

	// Get stored configuration - this is the source of truth
	storedConfig, err := storage.GetTreeConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load tree configuration: %w", err)
	}
	if storedConfig == nil {
		return nil, fmt.Errorf("tree '%s' does not exist", treeID)
	}

	// Create tree with stored configuration
	tree := &BPlusTree{
		config: storedConfig,
	}

	// TODO (gabrielopesantos): Validate tree structure
	// We need to be careful here because a full validation might be too expensive...
	ctx = tree.contextWithTreeID(ctx)
	rootID, err := tree.getRootID(ctx, storage)
	if err != nil {
		return nil, fmt.Errorf("failed to get root ID: %w", err)
	}
	if rootID == "" {
		return nil, fmt.Errorf("tree metadata exists but no root node found - tree may be corrupted")
	}

	// Validate root node exists
	root, err := storage.LoadNode(ctx, rootID)
	if err != nil {
		return nil, fmt.Errorf("failed to load root node: %w", err)
	}
	if root == nil || root.ID != rootID {
		return nil, fmt.Errorf("root node validation failed")
	}

	return tree, nil
}

// getRoot loads the root node from storage
func (t *BPlusTree) getRoot(ctx context.Context, storage Storage) (*Node, error) {
	rootID, err := t.getRootID(ctx, storage)
	if err != nil {
		return nil, fmt.Errorf("failed to get root ID: %w", err)
	}
	if rootID == "" {
		return nil, errors.New("root node not found")
	}

	return storage.LoadNode(ctx, rootID)
}

// getRootID returns the root ID, using cache when possible
func (t *BPlusTree) getRootID(ctx context.Context, storage Storage) (string, error) {
	// Check if we have a valid cached root ID
	if t.cachedRootID != "" {
		return t.cachedRootID, nil
	}

	// Load from storage and cache
	rootID, err := storage.GetRootID(ctx)
	if err != nil {
		return "", err
	}

	// Update cache
	t.cachedRootID = rootID

	return rootID, nil
}

// setRootID updates both storage and cache
func (t *BPlusTree) setRootID(ctx context.Context, storage Storage, newRootID string) error {
	// Update storage first
	if err := storage.SetRootID(ctx, newRootID); err != nil {
		return err
	}

	// Update cache on successful storage update
	t.cachedRootID = newRootID

	return nil
}

// contextWithTreeID returns a context with the tree's ID added, enabling multi-tree storage
func (t *BPlusTree) contextWithTreeID(ctx context.Context) context.Context {
	return t.config.contextWithTreeID(ctx)
}

// maxChildrenNodes returns the maximum number of children an internal node can have
func (t *BPlusTree) maxChildrenNodes() int {
	return t.config.Order
}

// maxKeys returns the maxium number of keys an internal node can have
func (t *BPlusTree) maxKeys() int {
	return t.maxChildrenNodes() - 1
}

// minChildrenNodes returns the minimum number of children an internal node can have
func (t *BPlusTree) minChildrenNodes() int {
	return int(math.Ceil(float64(t.config.Order) / float64(2)))
}

// minKeys returns the minimum number of keys a node must have
func (t *BPlusTree) minKeys() int {
	return t.minChildrenNodes() - 1
}

// nodeOverflows checks if a node has exceeded its maximum capacity
func (t *BPlusTree) nodeOverflows(node *Node) bool {
	return len(node.Keys) > t.maxKeys()
}

// nodeUnderflows checks if a node has fallen below its minimum capacity
func (t *BPlusTree) nodeUnderflows(node *Node) bool {
	return len(node.Keys) < t.minKeys()
}

// Search retrieves all values for a key
// If the key is not found, it returns an empty slice and false
func (t *BPlusTree) Search(ctx context.Context, storage Storage, key string) ([]string, bool, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	ctx = t.contextWithTreeID(ctx)
	return t.search(ctx, storage, key)
}

func (t *BPlusTree) search(ctx context.Context, storage Storage, key string) ([]string, bool, error) {
	leaf, err := t.findLeafNode(ctx, storage, key)
	if err != nil {
		return nil, false, fmt.Errorf("failed to find leaf node: %w", err)
	}

	// If we get here, we are at a leaf node
	idx, found := leaf.FindKeyIndex(key)
	if found {
		// If the key is found, return the values
		return leaf.Values[idx], true, nil
	}

	// Key not found is a valid state, not an error
	return nil, false, nil
}

// SearchPrefix returns all key-value pairs that start with the given prefix
// This function leverages the NextID linking to efficiently traverse leaf nodes sequentially
// No wildcards searches are supported - only exact prefix matches
func (t *BPlusTree) SearchPrefix(ctx context.Context, storage Storage, prefix string) (map[string][]string, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	ctx = t.contextWithTreeID(ctx)
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
		return nil, fmt.Errorf("failed to find leaf node for prefix (%s): %w", prefix, err)
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

// Insert inserts a key-value pair
func (t *BPlusTree) Insert(ctx context.Context, storage Storage, key string, value string) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	ctx = t.contextWithTreeID(ctx)
	// Find the leaf node where the key should be inserted
	leaf, err := t.findLeafNode(ctx, storage, key)
	if err != nil {
		return err
	}

	_ = leaf.InsertKeyValue(key, value)

	// If the leaf has overflow, we need to split it
	if t.nodeOverflows(leaf) {
		newLeaf, splitKey := t.splitLeafNode(leaf)
		// Save both leaf nodes after splitting
		if err := storage.SaveNode(ctx, leaf); err != nil { // NOTE (gabrielopesantos):  We do not necessarily need to save them, just cache them somewhere
			return fmt.Errorf("failed to save original leaf node: %w", err)
		}
		if err := storage.SaveNode(ctx, newLeaf); err != nil {
			return fmt.Errorf("failed to save new leaf node: %w", err)
		}

		return t.insertIntoParent(ctx, storage, leaf, newLeaf, splitKey)
	} else {
		// Save the leaf node after insertion
		if err := storage.SaveNode(ctx, leaf); err != nil {
			return fmt.Errorf("failed to save leaf node: %w", err)
		}
	}

	return nil
}

// Delete removes all values for a key, if the key exists.
// NOTE (gabrielopesantos): This implementation is not removing separator keys that were removed from the leaf node.
func (t *BPlusTree) Delete(ctx context.Context, storage Storage, key string) (bool, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	var entryDeleted bool
	ctx = t.contextWithTreeID(ctx)
	// Find the leaf node where the key belongs
	leaf, err := t.findLeafNode(ctx, storage, key)
	if err != nil {
		return entryDeleted, err
	}

	// Check if the key exists in the leaf node
	idx, found := leaf.FindKeyIndex(key)
	if !found {
		return entryDeleted, nil // Key not found, nothing to delete
	}

	// Delete the key-value pair
	err = leaf.RemoveKeyAt(idx)
	if err != nil {
		return entryDeleted, fmt.Errorf("failed to remove key: %w", err)
	}
	err = leaf.RemoveValueAt(idx)
	if err != nil {
		return entryDeleted, fmt.Errorf("failed to remove value: %w", err)
	}

	// Save the leaf node after deletion
	if err := storage.SaveNode(ctx, leaf); err != nil {
		return entryDeleted, fmt.Errorf("failed to save leaf node: %w", err)
	}
	entryDeleted = true // Mark that we deleted an entry

	// Handle underflow if the node is not the root and has too few keys
	rootID, err := t.getRootID(ctx, storage)
	if err != nil {
		return entryDeleted, fmt.Errorf("failed to get root ID: %w", err)
	}
	if leaf.ID != rootID && t.nodeUnderflows(leaf) {
		return entryDeleted, t.handleUnderflow(ctx, storage, leaf)
	}

	return entryDeleted, nil
}

// DeleteValue removes a specific value for a key
// NOTE (gabrielopesantos): This implementation is not removing separator keys that were removed from the leaf node.
func (t *BPlusTree) DeleteValue(ctx context.Context, storage Storage, key string, value string) (bool, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	var valueDeleted bool
	ctx = t.contextWithTreeID(ctx)
	// Find the leaf node where the key belongs
	leaf, err := t.findLeafNode(ctx, storage, key)
	if err != nil {
		return valueDeleted, err
	}

	// Check if the key exists in the leaf node
	if !leaf.HasKey(key) {
		return valueDeleted, nil
	}

	result, _ := leaf.RemoveValueFromKey(key, value)
	if result == KeyNotFound {
		return valueDeleted, nil
	}

	// Save the leaf node after deletion
	if err := storage.SaveNode(ctx, leaf); err != nil {
		return valueDeleted, fmt.Errorf("failed to save leaf node: %w", err)
	}
	valueDeleted = true

	// Handle underflow if the node is not the root and has too few keys
	rootID, err := t.getRootID(ctx, storage)
	if err != nil {
		return valueDeleted, fmt.Errorf("failed to get root ID to verify if the node is the root: %w", err)
	}
	if leaf.ID != rootID && t.nodeUnderflows(leaf) {
		return valueDeleted, t.handleUnderflow(ctx, storage, leaf)
	}

	// Value not found
	return valueDeleted, nil
}

// Purge removes all keys and values from the tree
// func (t *BPlusTree[K, V]) Purge(ctx context.Context) error {
// 	return nil
// }

// findLeafNode finds the leaf node where the key should be located.
func (t *BPlusTree) findLeafNode(ctx context.Context, storage Storage, key string) (*Node, error) {
	// Load the root node
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
	newLeaf := NewLeafNode(generateUUID())

	// Split at the median index
	splitIndex := int(math.Floor(float64(len(leaf.Keys)) / float64(2)))

	// Move second half keys/values to new leaf (includes the split key)
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

// insertIntoParent inserts a key and right node into the parent of left node
func (t *BPlusTree) insertIntoParent(ctx context.Context, storage Storage, leftNode *Node, rightNode *Node, splitKey string) error {
	// If leftNode was the root, we need to create a new root
	// and make the leftNode and rightNode its children
	rootID, err := t.getRootID(ctx, storage)
	if err != nil {
		return fmt.Errorf("failed to get root ID: %w", err)
	}
	if leftNode.ID == rootID {
		newRoot := NewInternalNode(generateUUID())
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
			return fmt.Errorf("failed to save new root node: %w", err)
		}

		// Update root ID in storage
		return t.setRootID(ctx, storage, newRoot.ID)
	}

	// Otherwise, we need to insert into the existing parent node
	// Load parent node
	parent, err := storage.LoadNode(ctx, leftNode.ParentID)
	if err != nil {
		return err
	}

	// Insert the split key and right node into the parent
	err = parent.InsertKeyChild(splitKey, rightNode.ID)
	if err != nil {
		return fmt.Errorf("failed to insert key-child into parent: %w", err)
	}

	rightNode.ParentID = parent.ID

	// Save the updated nodes
	if err := storage.SaveNode(ctx, leftNode); err != nil {
		return fmt.Errorf("failed to save left node: %w", err)
	}
	if err := storage.SaveNode(ctx, rightNode); err != nil {
		return fmt.Errorf("failed to save right node: %w", err)
	}
	if err := storage.SaveNode(ctx, parent); err != nil {
		return fmt.Errorf("failed to save parent: %w", err)
	}

	// If the internal node is overflows, we need to split it
	if t.nodeOverflows(parent) {
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
	newInternal := NewInternalNode(generateUUID())

	// Split at the median index
	splitIndex := int(math.Floor(float64(len(node.Keys)) / float64(2)))

	// The key at splitIndex is promoted, so do not copy it to any node
	splitKey := node.Keys[splitIndex]

	// Copy keys and children after splitIndex to newInternal
	newInternal.Keys = append(newInternal.Keys, node.Keys[splitIndex+1:]...)
	newInternal.ChildrenIDs = append(newInternal.ChildrenIDs, node.ChildrenIDs[splitIndex+1:]...)

	// Update original node with first half
	node.Keys = node.Keys[:splitIndex]                 // Keep only keys before the split key
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

	return newInternal, splitKey
}

// handleUnderflow handles underflow in a node by trying to borrow from siblings or merge
func (t *BPlusTree) handleUnderflow(ctx context.Context, storage Storage, node *Node) error {
	// If it's the root node, no underflow handling needed
	rootID, err := t.getRootID(ctx, storage)
	if err != nil {
		return fmt.Errorf("failed to get root ID: %w", err)
	}
	if node.ID == rootID {
		// Root can have fewer keys, but if it's empty and has children, promote the only child
		if !node.IsLeaf && len(node.Keys) == 0 && len(node.ChildrenIDs) == 1 {
			return t.setRootID(ctx, storage, node.ChildrenIDs[0])
		}
		return nil
	}

	// Load parent and find node's position
	parent, err := storage.LoadNode(ctx, node.ParentID)
	if err != nil {
		return fmt.Errorf("failed to load parent node: %w", err)
	}

	nodeIndex := -1
	for i, childID := range parent.ChildrenIDs {
		if childID == node.ID {
			nodeIndex = i
			break
		}
	}
	// Sanity check: node must be a child of the parent
	if nodeIndex == -1 {
		return errors.New("node not found in parent's children")
	}

	// Try to borrow from left sibling first
	if nodeIndex > 0 {
		leftSibling, err := storage.LoadNode(ctx, parent.ChildrenIDs[nodeIndex-1])
		if err != nil {
			return fmt.Errorf("failed to load left sibling: %w", err)
		}
		if len(leftSibling.Keys) > t.minKeys() {
			return t.borrowFromSibling(ctx, storage, node, leftSibling, parent, nodeIndex, true)
		}
	}

	// Try to borrow from right sibling
	if nodeIndex < len(parent.ChildrenIDs)-1 {
		rightSibling, err := storage.LoadNode(ctx, parent.ChildrenIDs[nodeIndex+1])
		if err != nil {
			return fmt.Errorf("failed to load right sibling: %w", err)
		}
		if len(rightSibling.Keys) > t.minKeys() {
			return t.borrowFromSibling(ctx, storage, node, rightSibling, parent, nodeIndex, false)
		}
	}

	// Can't borrow, need to merge
	// Prefer merging with left sibling
	if nodeIndex > 0 {
		leftSibling, err := storage.LoadNode(ctx, parent.ChildrenIDs[nodeIndex-1])
		if err != nil {
			return fmt.Errorf("failed to load left sibling for merge: %w", err)
		}
		return t.mergeWithSibling(ctx, storage, node, leftSibling, parent, nodeIndex, true)
	} else {
		// Merge with right sibling
		rightSibling, err := storage.LoadNode(ctx, parent.ChildrenIDs[nodeIndex+1])
		if err != nil {
			return fmt.Errorf("failed to load right sibling for merge: %w", err)
		}
		return t.mergeWithSibling(ctx, storage, node, rightSibling, parent, nodeIndex, false)
	}
}

// borrowFromSibling borrows a key from a sibling node
func (t *BPlusTree) borrowFromSibling(ctx context.Context, storage Storage, node *Node, sibling *Node, parent *Node, nodeIndex int, borrowFromLeft bool) error {
	if node.IsLeaf {
		return t.borrowFromLeafSibling(ctx, storage, node, sibling, parent, nodeIndex, borrowFromLeft)
	} else {
		return t.borrowFromInternalSibling(ctx, storage, node, sibling, parent, nodeIndex, borrowFromLeft)
	}
}

// borrowFromLeafSibling borrows a key-value pair from a leaf sibling
func (t *BPlusTree) borrowFromLeafSibling(ctx context.Context, storage Storage, node *Node, sibling *Node, parent *Node, nodeIndex int, borrowFromLeft bool) error {
	if borrowFromLeft {
		// Borrow the rightmost key-value from left sibling
		borrowedKey := sibling.Keys[len(sibling.Keys)-1]
		borrowedValue := sibling.Values[len(sibling.Values)-1]

		// Remove from sibling
		err := sibling.RemoveKeyAt(len(sibling.Keys) - 1)
		if err != nil {
			return fmt.Errorf("failed to remove key from sibling: %w", err)
		}
		err = sibling.RemoveValueAt(len(sibling.Values) - 1)
		if err != nil {
			return fmt.Errorf("failed to remove value from sibling: %w", err)
		}

		// Insert at beginning of node
		node.Keys = slices.Insert(node.Keys, 0, borrowedKey)
		node.Values = slices.Insert(node.Values, 0, borrowedValue)

		// Update parent's separator key (key between left sibling and node)
		parent.Keys[nodeIndex-1] = node.Keys[0]
	} else {
		// Borrow the leftmost key-value from right sibling
		borrowedKey := sibling.Keys[0]
		borrowedValue := sibling.Values[0]

		// Remove from sibling
		err := sibling.RemoveKeyAt(0)
		if err != nil {
			return fmt.Errorf("failed to remove key from sibling: %w", err)
		}
		err = sibling.RemoveValueAt(0)
		if err != nil {
			return fmt.Errorf("failed to remove value from sibling: %w", err)
		}

		// Insert at end of node
		node.Keys = append(node.Keys, borrowedKey)
		node.Values = append(node.Values, borrowedValue)

		// Update parent's separator key (key between node and right sibling)
		parent.Keys[nodeIndex] = sibling.Keys[0]
	}

	// Save all modified nodes
	if err := storage.SaveNode(ctx, node); err != nil {
		return fmt.Errorf("failed to save node: %w", err)
	}
	if err := storage.SaveNode(ctx, sibling); err != nil {
		return fmt.Errorf("failed to save sibling: %w", err)
	}
	if err := storage.SaveNode(ctx, parent); err != nil {
		return fmt.Errorf("failed to save parent: %w", err)
	}

	return nil
}

// borrowFromInternalSibling borrows a key and child from an internal sibling
func (t *BPlusTree) borrowFromInternalSibling(ctx context.Context, storage Storage, node *Node, sibling *Node, parent *Node, nodeIndex int, borrowFromLeft bool) error {
	if borrowFromLeft {
		// Move separator key from parent to node
		separatorKey := parent.Keys[nodeIndex-1]
		node.Keys = slices.Insert(node.Keys, 0, separatorKey)

		// Move rightmost child from sibling to node
		borrowedChild := sibling.ChildrenIDs[len(sibling.ChildrenIDs)-1]
		sibling.ChildrenIDs = sibling.ChildrenIDs[:len(sibling.ChildrenIDs)-1]
		node.ChildrenIDs = slices.Insert(node.ChildrenIDs, 0, borrowedChild)

		// Move rightmost key from sibling to parent (becomes new separator)
		parent.Keys[nodeIndex-1] = sibling.Keys[len(sibling.Keys)-1]
		sibling.Keys = sibling.Keys[:len(sibling.Keys)-1]

		// Update borrowed child's parent
		child, err := storage.LoadNode(ctx, borrowedChild)
		if err != nil {
			return fmt.Errorf("failed to load borrowed child: %w", err)
		}
		child.ParentID = node.ID
		if err := storage.SaveNode(ctx, child); err != nil {
			return fmt.Errorf("failed to save borrowed child: %w", err)
		}
	} else {
		// Move separator key from parent to node
		separatorKey := parent.Keys[nodeIndex]
		node.Keys = append(node.Keys, separatorKey)

		// Move leftmost child from sibling to node
		borrowedChild := sibling.ChildrenIDs[0]
		sibling.ChildrenIDs = sibling.ChildrenIDs[1:]
		node.ChildrenIDs = append(node.ChildrenIDs, borrowedChild)

		// Move leftmost key from sibling to parent (becomes new separator)
		parent.Keys[nodeIndex] = sibling.Keys[0]
		sibling.Keys = sibling.Keys[1:]

		// Update borrowed child's parent
		child, err := storage.LoadNode(ctx, borrowedChild)
		if err != nil {
			return fmt.Errorf("failed to load borrowed child: %w", err)
		}
		child.ParentID = node.ID
		if err := storage.SaveNode(ctx, child); err != nil {
			return fmt.Errorf("failed to save borrowed child: %w", err)
		}
	}

	// Save all modified nodes
	if err := storage.SaveNode(ctx, node); err != nil {
		return fmt.Errorf("failed to save node: %w", err)
	}
	if err := storage.SaveNode(ctx, sibling); err != nil {
		return fmt.Errorf("failed to save sibling: %w", err)
	}
	if err := storage.SaveNode(ctx, parent); err != nil {
		return fmt.Errorf("failed to save parent: %w", err)
	}

	return nil
}

// mergeWithSibling merges a node with its sibling
func (t *BPlusTree) mergeWithSibling(ctx context.Context, storage Storage, node *Node, sibling *Node, parent *Node, nodeIndex int, mergeWithLeft bool) error {
	if node.IsLeaf {
		return t.mergeLeafNodes(ctx, storage, node, sibling, parent, nodeIndex, mergeWithLeft)
	} else {
		return t.mergeInternalNodes(ctx, storage, node, sibling, parent, nodeIndex, mergeWithLeft)
	}
}

// mergeLeafNodes merges two leaf nodes
func (t *BPlusTree) mergeLeafNodes(ctx context.Context, storage Storage, node *Node, sibling *Node, parent *Node, nodeIndex int, mergeWithLeft bool) error {
	var separatorIndex int
	var nodeToKeep, nodeToRemove *Node

	if mergeWithLeft {
		// Merge node into left sibling
		nodeToKeep = sibling
		nodeToRemove = node
		separatorIndex = nodeIndex - 1

		// Move all keys/values from node to sibling
		nodeToKeep.Keys = append(nodeToKeep.Keys, nodeToRemove.Keys...)
		nodeToKeep.Values = append(nodeToKeep.Values, nodeToRemove.Values...)

		// Update NextID linking
		nodeToKeep.NextID = nodeToRemove.NextID
	} else {
		// Merge right sibling into node
		nodeToKeep = node
		nodeToRemove = sibling
		separatorIndex = nodeIndex

		// Move all keys/values from sibling to node
		nodeToKeep.Keys = append(nodeToKeep.Keys, nodeToRemove.Keys...)
		nodeToKeep.Values = append(nodeToKeep.Values, nodeToRemove.Values...)

		// Update NextID linking
		nodeToKeep.NextID = nodeToRemove.NextID
	}

	// Remove separator key from parent
	parent.Keys = slices.Delete(parent.Keys, separatorIndex, separatorIndex+1)
	// Remove child pointer from parent
	childIndexToRemove := separatorIndex + 1
	if mergeWithLeft {
		childIndexToRemove = nodeIndex
	}
	parent.ChildrenIDs = slices.Delete(parent.ChildrenIDs, childIndexToRemove, childIndexToRemove+1)

	// Save the merged node and parent
	if err := storage.SaveNode(ctx, nodeToKeep); err != nil {
		return fmt.Errorf("failed to save merged node: %w", err)
	}
	if err := storage.SaveNode(ctx, parent); err != nil {
		return fmt.Errorf("failed to save parent after merge: %w", err)
	}

	// Delete the removed node
	if err := storage.DeleteNode(ctx, nodeToRemove.ID); err != nil {
		return fmt.Errorf("failed to delete merged node: %w", err)
	}

	// Check if parent underflows
	rootID, err := t.getRootID(ctx, storage)
	if err != nil {
		return fmt.Errorf("failed to get root ID: %w", err)
	}
	if parent.ID != rootID && t.nodeUnderflows(parent) {
		return t.handleUnderflow(ctx, storage, parent)
	}

	return nil
}

// mergeInternalNodes merges two internal nodes
func (t *BPlusTree) mergeInternalNodes(ctx context.Context, storage Storage, node *Node, sibling *Node, parent *Node, nodeIndex int, mergeWithLeft bool) error {
	var separatorIndex int
	var nodeToKeep, nodeToRemove *Node
	var separatorKey string

	if mergeWithLeft {
		// Merge node into left sibling
		nodeToKeep = sibling
		nodeToRemove = node
		separatorIndex = nodeIndex - 1
		separatorKey = parent.Keys[separatorIndex]

		// Add separator key and merge keys/children
		nodeToKeep.Keys = append(nodeToKeep.Keys, separatorKey)
		nodeToKeep.Keys = append(nodeToKeep.Keys, nodeToRemove.Keys...)
		nodeToKeep.ChildrenIDs = append(nodeToKeep.ChildrenIDs, nodeToRemove.ChildrenIDs...)
	} else {
		// Merge right sibling into node
		nodeToKeep = node
		nodeToRemove = sibling
		separatorIndex = nodeIndex
		separatorKey = parent.Keys[separatorIndex]

		// Add separator key and merge keys/children
		nodeToKeep.Keys = append(nodeToKeep.Keys, separatorKey)
		nodeToKeep.Keys = append(nodeToKeep.Keys, nodeToRemove.Keys...)
		nodeToKeep.ChildrenIDs = append(nodeToKeep.ChildrenIDs, nodeToRemove.ChildrenIDs...)
	}

	// Update parent references for all children of the merged node
	for _, childID := range nodeToRemove.ChildrenIDs {
		child, err := storage.LoadNode(ctx, childID)
		if err != nil {
			continue // Log error but continue
		}
		child.ParentID = nodeToKeep.ID
		if err := storage.SaveNode(ctx, child); err != nil {
			continue // Log error but continue
		}
	}

	// Remove separator key from parent
	parent.Keys = slices.Delete(parent.Keys, separatorIndex, separatorIndex+1)
	// Remove child pointer from parent
	childIndexToRemove := separatorIndex + 1
	if mergeWithLeft {
		childIndexToRemove = nodeIndex
	}
	parent.ChildrenIDs = slices.Delete(parent.ChildrenIDs, childIndexToRemove, childIndexToRemove+1)

	// Save the merged node and parent
	if err := storage.SaveNode(ctx, nodeToKeep); err != nil {
		return fmt.Errorf("failed to save merged node: %w", err)
	}
	if err := storage.SaveNode(ctx, parent); err != nil {
		return fmt.Errorf("failed to save parent after merge: %w", err)
	}

	// Delete the removed node
	if err := storage.DeleteNode(ctx, nodeToRemove.ID); err != nil {
		return fmt.Errorf("failed to delete merged node: %w", err)
	}

	// Check if parent underflows
	rootID, err := t.getRootID(ctx, storage)
	if err != nil {
		return fmt.Errorf("failed to get root ID: %w", err)
	}
	if parent.ID != rootID && t.nodeUnderflows(parent) {
		return t.handleUnderflow(ctx, storage, parent)
	}

	return nil
}

// findLeftmostLeaf finds the leftmost leaf node in the tree
func (t *BPlusTree) findLeftmostLeaf(ctx context.Context, storage Storage) (*Node, error) {
	// Load the root node
	node, err := t.getRoot(ctx, storage)
	if err != nil {
		return nil, err
	}

	for !node.IsLeaf {
		// Always go to the leftmost child
		if len(node.ChildrenIDs) == 0 {
			return nil, errors.New("internal node has no children")
		}
		node, err = storage.LoadNode(ctx, node.ChildrenIDs[0])
		if err != nil {
			return nil, fmt.Errorf("failed to load child node: %w", err)
		}
	}

	return node, nil
}

// findRightmostLeaf finds the rightmost leaf node in the tree
func (t *BPlusTree) findRightmostLeaf(ctx context.Context, storage Storage) (*Node, error) {
	// Load the root node
	node, err := t.getRoot(ctx, storage)
	if err != nil {
		return nil, err
	}

	for !node.IsLeaf {
		// Always go to the rightmost child
		if len(node.ChildrenIDs) == 0 {
			return nil, errors.New("internal node has no children")
		}
		rightmostIndex := len(node.ChildrenIDs) - 1
		node, err = storage.LoadNode(ctx, node.ChildrenIDs[rightmostIndex])
		if err != nil {
			return nil, fmt.Errorf("failed to load child node: %w", err)
		}
	}

	return node, nil
}
