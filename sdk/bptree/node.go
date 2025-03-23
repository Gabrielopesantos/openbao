package bptree

// Node represents a node in the B+ tree
type Node[K comparable, V any] struct {
	ID          string   `json:"id"`
	IsLeaf      bool     `json:"isLeaf"`
	Keys        []K      `json:"keys"`
	Values      []V      `json:"values"`
	ChildrenIDs []string `json:"childrenIDs"`
	ParentID    string   `json:"parentID"`
	NextID      string   `json:"nextID"` // ID of the next leaf node
}

// NewLeafNode creates a new leaf node
func NewLeafNode[K comparable, V any](id string) *Node[K, V] {
	return &Node[K, V]{
		ID:     id,
		IsLeaf: true,
	}
}

// NewInternalNode creates a new internal node
func NewInternalNode[K comparable, V any](id string) *Node[K, V] {
	return &Node[K, V]{
		ID:     id,
		IsLeaf: false,
	}
}

// findKeyIndex finds the index where a key should be inserted or is located
// Returns the index and whether the key was found
func (n *Node[K, V]) findKeyIndex(key K, less func(a, b K) bool) (int, bool) {
	for i, k := range n.Keys {
		if k == key {
			return i, true
		}
		if less(key, k) {
			return i, false
		}
	}
	return len(n.Keys), false
}

// insertKey inserts a key at the specified index
func (n *Node[K, V]) insertKey(idx int, key K) {
	if idx < 0 || idx > len(n.Keys) {
		idx = len(n.Keys) // Append if index is out of bounds
	}

	// Append to make room for the new key
	n.Keys = append(n.Keys, key)

	// If we're not appending to the end, shift elements to make room
	if idx < len(n.Keys)-1 {
		copy(n.Keys[idx+1:], n.Keys[idx:len(n.Keys)-1])
		n.Keys[idx] = key
	}
}

// insertKeyValue inserts a key-value pair at the specified index (for leaf nodes only)
func (n *Node[K, V]) insertKeyValue(idx int, key K, value V) {
	if n.IsLeaf {
		n.insertKey(idx, key)
		// Handle values array separately since it's only for leaf nodes
		n.Values = append(n.Values, value)
		if idx < len(n.Values)-1 {
			copy(n.Values[idx+1:], n.Values[idx:len(n.Values)-1])
			n.Values[idx] = value
		}
	}
}

// insertChild inserts a child node ID at the specified index
func (n *Node[K, V]) insertChild(idx int, childID string) {
	if idx < 0 || idx > len(n.ChildrenIDs) {
		idx = len(n.ChildrenIDs) // Append if index is out of bounds
	}

	// Append to make room for the new child
	n.ChildrenIDs = append(n.ChildrenIDs, childID)

	// If we're not appending to the end, shift elements to make room
	if idx < len(n.ChildrenIDs)-1 {
		copy(n.ChildrenIDs[idx+1:], n.ChildrenIDs[idx:len(n.ChildrenIDs)-1])
		n.ChildrenIDs[idx] = childID
	}
}

// removeKeyValue removes a key-value pair at the specified index
func (n *Node[K, V]) removeKeyValue(idx int) {
	n.Keys = append(n.Keys[:idx], n.Keys[idx+1:]...)
	n.Values = append(n.Values[:idx], n.Values[idx+1:]...)
}

// removeChild removes a child at the specified index
func (n *Node[K, V]) removeChild(idx int) {
	n.ChildrenIDs = append(n.ChildrenIDs[:idx], n.ChildrenIDs[idx+1:]...)
}
