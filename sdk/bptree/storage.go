package bptree

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/openbao/openbao/sdk/v2/logical"
	"github.com/openbao/openbao/sdk/v2/lru"
)

type NodeStorage[K comparable, V any] interface {
	// LoadNode loads a node from storage
	LoadNode(ctx context.Context, id string) (*Node[K, V], error)
	// SaveNode saves a node to storage
	SaveNode(ctx context.Context, node *Node[K, V]) error
	// DeleteNode deletes a node from storage
	DeleteNode(ctx context.Context, id string) error
	// GetRootID gets the ID of the root node
	GetRootID(ctx context.Context) (string, error)
	// SetRootID sets the ID of the root node
	SetRootID(ctx context.Context, id string) error
}

const (
	NodePath      = "nodes"
	RootPath      = "root"
	MetadataPath  = "metadata"
	BPlusTreePath = "bptree"
)

// StorageAdapter adapts the logical.Storage interface to the NodeStorage interface
type StorageAdapter[K comparable, V any] struct {
	prefix     string
	storage    logical.Storage
	serializer NodeSerializer[K, V]
	cache      *lru.LRU[string, *Node[K, V]]
	nodesLock  sync.RWMutex
	rootLock   sync.RWMutex
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
	prefix string,
	storage logical.Storage,
	serializer NodeSerializer[K, V],
	cacheSize int,
) (*StorageAdapter[K, V], error) {
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	if serializer == nil {
		serializer = &JSONSerializer[K, V]{}
	}

	cache, err := lru.NewLRU[string, *Node[K, V]](cacheSize)
	if err != nil {
		return nil, err
	}

	return &StorageAdapter[K, V]{
		prefix:     prefix,
		storage:    storage,
		serializer: serializer,
		cache:      cache,
	}, nil
}

// LoadNode loads a node from storage
func (s *StorageAdapter[K, V]) LoadNode(ctx context.Context, id string) (*Node[K, V], error) {
	// Lock the nodes
	s.nodesLock.RLock()
	defer s.nodesLock.RUnlock()

	// Try to get from cache first
	if node, ok := s.cache.Get(id); ok {
		return node, nil
	}

	// Load from storage
	path := s.prefix + NodePath + "/" + id
	entry, err := s.storage.Get(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to load node %s: %w", id, err)
	}

	if entry == nil {
		return nil, nil
	}

	node, err := s.serializer.Deserialize(entry.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize node %s: %w", id, err)
	}

	// Cache the loaded node
	s.cache.Add(id, node)

	return node, nil
}

// SaveNode saves a node to storage
func (s *StorageAdapter[K, V]) SaveNode(ctx context.Context, node *Node[K, V]) error {
	// Check if the node is nil
	if node == nil {
		return fmt.Errorf("cannot save nil node")
	}

	// Lock storage if the node is not nil
	s.nodesLock.Lock()
	defer s.nodesLock.Unlock()

	data, err := s.serializer.Serialize(node)
	if err != nil {
		return fmt.Errorf("failed to serialize node %s: %w", node.ID, err)
	}

	path := s.prefix + NodePath + "/" + node.ID
	entry := &logical.StorageEntry{
		Key:   path,
		Value: data,
	}

	if err := s.storage.Put(ctx, entry); err != nil {
		return fmt.Errorf("failed to save node %s: %w", node.ID, err)
	}

	// Update cache
	s.cache.Add(node.ID, node)

	return nil
}

// DeleteNode deletes a node from storage
func (s *StorageAdapter[K, V]) DeleteNode(ctx context.Context, id string) error {
	// Lock the nodes
	s.nodesLock.Lock()
	defer s.nodesLock.Unlock()

	path := s.prefix + NodePath + "/" + id
	if err := s.storage.Delete(ctx, path); err != nil {
		return fmt.Errorf("failed to delete node %s: %w", id, err)
	}

	// Remove from cache
	s.cache.Delete(id)

	return nil
}

// GetRootID gets the ID of the root node
func (s *StorageAdapter[K, V]) GetRootID(ctx context.Context) (string, error) {
	// Lock the root
	s.rootLock.RLock()
	defer s.rootLock.RUnlock()

	path := s.prefix + MetadataPath + "/" + RootPath
	entry, err := s.storage.Get(ctx, path)
	if err != nil {
		return "", fmt.Errorf("failed to get root ID: %w", err)
	}

	if entry == nil {
		return "", nil
	}

	return string(entry.Value), nil
}

// SetRootID sets the ID of the root node
func (s *StorageAdapter[K, V]) SetRootID(ctx context.Context, id string) error {
	// Lock the root
	s.rootLock.Lock()
	defer s.rootLock.Unlock()

	path := s.prefix + MetadataPath + "/" + RootPath
	entry := &logical.StorageEntry{
		Key:   path,
		Value: []byte(id),
	}

	if err := s.storage.Put(ctx, entry); err != nil {
		return fmt.Errorf("failed to set root ID: %w", err)
	}

	return nil
}
