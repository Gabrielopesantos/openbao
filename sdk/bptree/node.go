package bptree

// Node represents a node in the B+ tree
type Node[K comparable, V any] struct {
	Id       string        `json:"id"`
	Keys     []K           `json:"keys"`
	Values   []V           `json:"values"`
	Children []*Node[K, V] `json:"children"`
	Parent   *Node[K, V]   `json:"parent"`
	IsLeaf   bool          `json:"isLeaf"`
	Next     *Node[K, V]   `json:"next"` // Next leaf node (for range queries)
	// ChildrenKeys []K
}

// NewLeafNode creates a new leaf node
func NewLeafNode[K comparable, V any](id string) *Node[K, V] {
	return &Node[K, V]{
		Id:     id,
		IsLeaf: true,
	}
}

// NewInternalNode creates a new internal node
func NewInternalNode[K comparable, V any](id string) *Node[K, V] {
	return &Node[K, V]{
		Id:     id,
		IsLeaf: false,
	}
}

// findKeyIndex finds the index where a key should be inserted or is located
// Returns the index and whether the key was found
func (n *Node[K, V]) findKeyIndex(key K, less func(a, b K) bool) (int, bool) {
	// Binary search for the key
	low, high := 0, len(n.Keys)-1
	for low <= high {
		mid := (low + high) / 2
		if less(n.Keys[mid], key) {
			low = mid + 1
		} else if less(key, n.Keys[mid]) {
			high = mid - 1
		} else {
			return mid, true // Key found
		}
	}
	return low, false // Key not found, return insertion point
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
	n.Children = append(n.Children, nil)
	if idx < len(n.Children)-1 {
		copy(n.Children[idx+1:], n.Children[idx:len(n.Children)-1])
		n.Children[idx] = child
	}
	child.Parent = n
}

// removeKeyValue removes a key-value pair at the specified index
func (n *Node[K, V]) removeKeyValue(idx int) {
	n.Keys = append(n.Keys[:idx], n.Keys[idx+1:]...)
	n.Values = append(n.Values[:idx], n.Values[idx+1:]...)
}

// removeChild removes a child at the specified index
func (n *Node[K, V]) removeChild(idx int) {
	n.Children = append(n.Children[:idx], n.Children[idx+1:]...)
}
