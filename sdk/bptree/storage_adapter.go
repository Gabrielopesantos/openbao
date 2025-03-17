package bptree

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/openbao/openbao/sdk/v2/logical"
)

const (
	NodePath      = "nodes"
	RootPath      = "root"
	MetadataPath  = "metadata"
	BPlusTreePath = "bptree"
)

// StorageAdapter adapts the logical.Storage interface to the NodeStorage interface
type StorageAdapter[K comparable, V any] struct {
	ctx        context.Context
	prefix     string
	storage    logical.Storage
	serializer NodeSerializer[K, V]
}

// NodeSerializer defines how to serialize and deserialize nodes
type NodeSerializer[K comparable, V any] interface {
	Serialize(node *Node[K, V]) ([]byte, error)
	Deserialize(data []byte) (*Node[K, V], error)
}

// JSONSerializer is a simple JSON-based serializer for nodes
type JSONSerializer[K comparable, V any] struct{}

// Serialize converts a node to JSON
func (s *JSONSerializer[K, V]) Serialize(node *Node[K, V]) ([]byte, error) {
	return json.Marshal(node)
}

// Deserialize converts JSON to a node
func (s *JSONSerializer[K, V]) Deserialize(data []byte) (*Node[K, V], error) {
	var node Node[K, V]
	if err := json.Unmarshal(data, &node); err != nil {
		return nil, err
	}
	return &node, nil
}

// NewStorageAdapter creates a new adapter for the logical.Storage interface
func NewStorageAdapter[K comparable, V any](
	ctx context.Context,
	prefix string,
	storage logical.Storage,
	serializer NodeSerializer[K, V],
) *StorageAdapter[K, V] {
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	if serializer == nil {
		serializer = &JSONSerializer[K, V]{}
	}

	return &StorageAdapter[K, V]{
		ctx:        ctx,
		prefix:     prefix,
		storage:    storage,
		serializer: serializer,
	}
}

// LoadNode loads a node from storage
func (a *StorageAdapter[K, V]) LoadNode(id string) (*Node[K, V], error) {
	path := a.prefix + NodePath + "/" + id
	entry, err := a.storage.Get(a.ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to load node %s: %w", id, err)
	}

	if entry == nil {
		return nil, nil
	}

	node, err := a.serializer.Deserialize(entry.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize node %s: %w", id, err)
	}

	return node, nil
}

// SaveNode saves a node to storage
func (a *StorageAdapter[K, V]) SaveNode(node *Node[K, V]) error {
	if node == nil {
		return fmt.Errorf("cannot save nil node")
	}

	data, err := a.serializer.Serialize(node)
	if err != nil {
		return fmt.Errorf("failed to serialize node %s: %w", node.Id, err)
	}

	path := a.prefix + NodePath + "/" + node.Id
	entry := &logical.StorageEntry{
		Key:   path,
		Value: data,
	}

	if err := a.storage.Put(a.ctx, entry); err != nil {
		return fmt.Errorf("failed to save node %s: %w", node.Id, err)
	}

	return nil
}

// DeleteNode deletes a node from storage
func (a *StorageAdapter[K, V]) DeleteNode(id string) error {
	path := a.prefix + NodePath + "/" + id
	if err := a.storage.Delete(a.ctx, path); err != nil {
		return fmt.Errorf("failed to delete node %s: %w", id, err)
	}

	return nil
}

// GetRootID gets the ID of the root node
func (a *StorageAdapter[K, V]) GetRootID() (string, error) {
	path := a.prefix + MetadataPath + "/" + RootPath
	entry, err := a.storage.Get(a.ctx, path)
	if err != nil {
		return "", fmt.Errorf("failed to get root ID: %w", err)
	}

	if entry == nil {
		return "", nil
	}

	return string(entry.Value), nil
}

// SetRootID sets the ID of the root node
func (a *StorageAdapter[K, V]) SetRootID(id string) error {
	path := a.prefix + MetadataPath + "/" + RootPath
	entry := &logical.StorageEntry{
		Key:   path,
		Value: []byte(id),
	}

	if err := a.storage.Put(a.ctx, entry); err != nil {
		return fmt.Errorf("failed to set root ID: %w", err)
	}

	return nil
}
