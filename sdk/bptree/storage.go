package bptree

type NodeStorage[K comparable, V any] interface {
	// LoadNode loads a node from storage
	LoadNode(id string) (*Node[K, V], error)
	// SaveNode saves a node to storage
	SaveNode(node *Node[K, V]) error
	// DeleteNode deletes a node from storage
	DeleteNode(id string) error
	// GetRootID gets the ID of the root node
	GetRootID() (string, error)
	// SetRootID sets the ID of the root node
	SetRootID(id string) error
}
