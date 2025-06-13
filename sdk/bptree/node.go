package bptree

import (
	"errors"
	"slices"
)

var (
	// ErrKeyIndexOutOfBounds is returned when an index for a key is out of bounds
	ErrKeyIndexOutOfBounds = errors.New("key index out of bounds")

	// ErrValueIndexOutOfBounds is returned when an index for a value is out of bounds
	ErrValueIndexOutOfBounds = errors.New("value index out of bounds")

	// ErrChildIndexOutOfBounds is returned when an index for a child node is out of bounds
	ErrChildIndexOutOfBounds = errors.New("child index out of bounds")

	// ErrNotALeafNode is returned when an operation is attempted on a non-leaf node
	ErrNotALeafNode = errors.New("operation not allowed on a non-leaf node")

	// ErrNotAnInternalNode is returned when an operation is attempted on a leaf node that requires an internal node
	ErrNotAnInternalNode = errors.New("operation not allowed on a leaf node")
)

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

	// NOTE (gabrielopesantos): Review returning the length of Keys if not found
	return len(n.Keys), false
}

// insertKeyAt inserts a key at the specified index
func (n *Node) insertKeyAt(idx int, key string) error {
	if idx < 0 || idx > len(n.Keys) {
		return ErrKeyIndexOutOfBounds
	}

	// log.Printf("Inserting key %s at index %d in node %s", key, idx, n.ID)
	// Insert new key
	n.Keys = slices.Insert(n.Keys, idx, key)
	return nil
}

// appendIfNotExists appends a value to a slice if it does not already exist
func appendIfNotExists(slice []string, value string) []string {
	if !slices.Contains(slice, value) {
		return append(slice, value)
	}

	return slice
}

// insertValueAt inserts a value at the specified index
// If the key already exists, it appends the value to the existing slice
// NOTE (gabrielopesantos): I don't like the fact that `keyExists` is passed in
// couldn't couldn't make it work otherwise (REVIEW)
func (n *Node) insertValueAt(idx int, keyExists bool, value string) error {
	// Sanity check: ensure index is within bounds
	if idx < 0 || idx > len(n.Values) {
		return ErrValueIndexOutOfBounds
	}

	// If we are inserting the value for an existing key, it is possible
	// that is duplicated so we don't want to insert it again
	if keyExists {
		n.Values[idx] = appendIfNotExists(n.Values[idx], value)
	} else {
		// Insert new value slice
		n.Values = slices.Insert(n.Values, idx, []string{value})
	}

	return nil
}

// insertChildAt inserts a child node ID at the specified index
func (n *Node) insertChildAt(idx int, childID string) error {
	// Sanity check: ensure index is within bounds
	// log.Printf("Idx is %d, ChildrenIDs length is %d", idx, len(n.ChildrenIDs))
	if idx < 0 || idx > len(n.ChildrenIDs) {
		// NOTE (gabrielopesantos): Review...
		idx = len(n.ChildrenIDs) // If out of bounds, append to the end
	}

	// log.Printf("Inserting childID %s at index %d in node %s", childID, idx, n.ID)
	// Insert new childID
	n.ChildrenIDs = slices.Insert(n.ChildrenIDs, idx, childID)
	return nil
}

// insertKeyValueAt inserts a key-value pair at the specified index (for leaf nodes only)
func (n *Node) insertKeyValueAt(key string, value string) error {
	// Sanity check: ensure this is a leaf node
	if !n.IsLeaf {
		return ErrNotALeafNode
	}

	// Fetch the index where the key should be inserted
	insertIdx, keyExists := n.findKeyIndex(key)
	if !keyExists {
		// Insert the key if it doesn't exist
		n.insertKeyAt(insertIdx, key)
	}

	// Insert the value at the same index
	n.insertValueAt(insertIdx, keyExists, value)
	return nil
}

// insertKeyChildAt inserts a key and a child node ID at the specified index
func (n *Node) insertKeyChildAt(idx int, key string, childID string) error {
	if n.IsLeaf {
		return ErrNotAnInternalNode
	}

	// Insert new key
	err := n.insertKeyAt(idx, key)
	if err != nil {
		return err
	}

	// Insert new childID
	return n.insertChildAt(idx+1, childID)
}

func (n *Node) removeKeyAt(idx int) error {
	if idx < 0 || idx >= len(n.Keys) {
		return ErrKeyIndexOutOfBounds
	}

	// Remove the key at the specified index
	n.Keys = slices.Delete(n.Keys, idx, idx+1)
	return nil
}

func (n *Node) removeValueAt(idx int) error {
	if idx < 0 || idx >= len(n.Values) {
		return ErrValueIndexOutOfBounds
	}

	// Remove the value at the specified index
	n.Values = slices.Delete(n.Values, idx, idx+1)
	return nil
}

// removeKeyEntryAt removes a key-value pair at the specified index
// This methods is currently specific to leaf nodes
func (n *Node) removeKeyEntryAt(idx int) error {
	if !n.IsLeaf {
		return ErrNotALeafNode
	}

	err := n.removeKeyAt(idx)
	if err != nil {
		return err
	}

	err = n.removeValueAt(idx)
	if err != nil {
		return err
	}

	// if len(n.Keys) == 0 {
	// 	n.Values = nil // Clear values if no keys left
	// }

	return nil
}

// removeKeyValueAt removes a key and the associated value at the specified index
func (n *Node) removeKeyValueAt(idx int, value string) error {
	if !n.IsLeaf {
		return ErrNotALeafNode
	}

	values := n.Values[idx]
	if slices.Contains(values, value) {
		// Remove the value from the slice
		values = slices.Delete(values, slices.Index(values, value), slices.Index(values, value)+1)
		if len(values) == 0 {
			// If no values left, remove the key
			n.removeKeyEntryAt(idx)
		} else {
			// Otherwise, update the values
			n.Values[idx] = values
		}
	}

	return nil
}

// TODO (gabrielopesantos): Remove if it is not being used;
// removeChild removes a child at the specified index
func (n *Node) removeChild(idx int) {
	n.ChildrenIDs = slices.Delete(n.ChildrenIDs, idx, idx+1)
}
