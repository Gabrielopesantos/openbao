package bptree

type NodeStorage[K comparable, V any] interface {
	LoadNode(id string) (*Node[K, V], error)
	SaveNode(node *Node[K, V]) error
	DeleteNode(id string) error
	GetRootID() (string, error)
	SetRootID(id string) error
}
