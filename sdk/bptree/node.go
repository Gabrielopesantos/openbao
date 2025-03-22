package bptree

// Node represents a node in the B+ tree
type Node[K comparable, V any] struct {
	ID          string   `json:"id"`
	IsLeaf      bool     `json:"isLeaf"`
	Keys        []K      `json:"keys"`
	Values      []V      `json:"values"`
	ChildrenIDs []string `json:"childrenIDs"`
	children    []*Node[K, V]
	ParentID    string `json:"parentID"`
	parent      *Node[K, V]
	next        *Node[K, V] // Next leaf node (for range queries)
	NextID      string      `json:"nextID"` // ID of the next leaf node
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
		if less(k, key) {
			continue
		}
		return i, true
	}
	return len(n.Keys), false
}

// insertKeyValue inserts a key-value pair at the specified index
func (n *Node[K, V]) insertKeyValue(idx int, key K, value V) {
	if idx < 0 || idx > len(n.Keys) {
		idx = len(n.Keys) // Append if index is out of bounds
	}

	// Append to make room for the new element
	n.Keys = append(n.Keys, key)
	n.Values = append(n.Values, value)

	// If we're not appending to the end, shift elements to make room
	if idx < len(n.Keys)-1 {
		copy(n.Keys[idx+1:], n.Keys[idx:len(n.Keys)-1])
		copy(n.Values[idx+1:], n.Values[idx:len(n.Values)-1])
		n.Keys[idx] = key
		n.Values[idx] = value
	}
}

// insertChild inserts a child node at the specified index
func (n *Node[K, V]) insertChild(idx int, child *Node[K, V]) {
	n.children = append(n.children, nil)
	n.ChildrenIDs = append(n.ChildrenIDs, child.ID)
	// if idx < len(n.Children)-1 {
	// Shift children to make room for the new child
	copy(n.children[idx+1:], n.children[idx:len(n.children)-1])
	n.children[idx] = child

	// Shift children keys to make room for the new child key
	copy(n.ChildrenIDs[idx+1:], n.ChildrenIDs[idx:len(n.ChildrenIDs)-1])
	n.ChildrenIDs[idx] = child.ID
	// }
	child.parent = n
	child.ParentID = n.ID
}

// removeKeyValue removes a key-value pair at the specified index
func (n *Node[K, V]) removeKeyValue(idx int) {
	n.Keys = append(n.Keys[:idx], n.Keys[idx+1:]...)
	n.Values = append(n.Values[:idx], n.Values[idx+1:]...)
}

// removeChild removes a child at the specified index
func (n *Node[K, V]) removeChild(idx int) {
	n.children = append(n.children[:idx], n.children[idx+1:]...)
	n.ChildrenIDs = append(n.ChildrenIDs[:idx], n.ChildrenIDs[idx+1:]...)
}
