package bptree

import "slices"

// Node represents a node in the B+ tree
type Node struct {
	ID          string     `json:"id"`
	IsLeaf      bool       `json:"isLeaf"`
	Keys        []string   `json:"keys"`
	Values      [][]string `json:"values"`      // Only for leaf nodes
	ChildrenIDs []string   `json:"childrenIDs"` // Only for internal nodes
	ParentID    string     `json:"parentID"`
	NextID      string     `json:"nextID"` // ID of the next leaf node
}

// NewLeafNode creates a new leaf node
func NewLeafNode(id string) *Node {
	return &Node{
		ID:     id,
		IsLeaf: true,
		Keys:   make([]string, 0),
		Values: make([][]string, 0),
	}
}

// NewInternalNode creates a new internal node
func NewInternalNode(id string) *Node {
	return &Node{
		ID:          id,
		IsLeaf:      false,
		Keys:        make([]string, 0),
		ChildrenIDs: make([]string, 0),
	}
}

// findKeyIndex finds the index where a key should be inserted or is located
// Returns the index and whether the key was found
func (n *Node) findKeyIndex(key string) (int, bool) {
	for i, k := range n.Keys {
		if k == key {
			return i, true
		}
		if key < k {
			return i, false
		}
	}
	return len(n.Keys), false
}

// insertKey inserts a key at the specified index
func (n *Node) insertKey(idx int, key string) {
	if idx < 0 || idx > len(n.Keys) {
		// NOTE (gsantos): ?
		idx = len(n.Keys) // Append if index is out of bounds
	}

	// Insert new key
	n.Keys = slices.Insert(n.Keys, idx, key)
}

// insertKeyValue inserts a key-value pair at the specified index (for leaf nodes only)
// NOTE (gabrielopesantos): Return something?
func (n *Node) insertKeyValue(key string, value string) {
	if n.IsLeaf {
		// Key index is the index where the key should be inserted
		idx, found := n.findKeyIndex(key)
		if !found {
			// Insert the key if it doesn't exist
			n.insertKey(idx, key)
		}

		// If we're inserting at an existing key, check for duplicates
		if found {
			// Check if the value already exists and return early if it does
			if slices.Contains(n.Values[idx], value) {
				return
			}
			n.Values[idx] = append(n.Values[idx], value)
			return
		}

		// Insert new value slices
		n.Values = slices.Insert(n.Values, idx, []string{value})
	}
}

// insertChild inserts a child node ID at the specified index
func (n *Node) insertChild(idx int, childID string) {
	if idx < 0 || idx > len(n.ChildrenIDs) {
		//.NOTE (gsantos): Are we sure about this?
		idx = len(n.ChildrenIDs) // Append if index is out of bounds
	}
	// Insert new childID
	n.ChildrenIDs = slices.Insert(n.ChildrenIDs, idx, childID)
}

// removeKeyValue removes a key-value pair at the specified index
func (n *Node) removeKeyValue(idx int) {
	n.Keys = slices.Delete(n.Keys, idx, idx+1)
	n.Values = slices.Delete(n.Values, idx, idx+1)
}

// removeChild removes a child at the specified index
func (n *Node) removeChild(idx int) {
	n.ChildrenIDs = slices.Delete(n.ChildrenIDs, idx, idx+1)
}
