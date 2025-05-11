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

type Storage[K comparable, V any] interface {
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
	// WithTransaction wraps the storage in a transaction
	WithTransaction(ctx context.Context, fn func() error) error
}

const (
	NodePath      = "nodes"
	RootPath      = "root"
	MetadataPath  = "metadata"
	BPlusTreePath = "bptree"
)

// NodeStorage adapts the logical.Storage interface to the bptree.Storage interface
type NodeStorage[K comparable, V any] struct {
	prefix         string
	serializer     NodeSerializer[K, V]
	storage        logical.Storage
	cache          *lru.LRU[string, *Node[K, V]]
	skipCache      bool
	nodesLock      sync.RWMutex
	rootLock       sync.RWMutex
	operationQueue []cacheOperation[K, V]
	queueLock      sync.Mutex
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

// NewNodeStorage creates a new adapter for the logical.Storage interface
func NewNodeStorage[K comparable, V any](
	prefix string,
	storage logical.Storage,
	serializer NodeSerializer[K, V],
	cacheSize int,
) (*NodeStorage[K, V], error) {
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

	return &NodeStorage[K, V]{
		prefix:     prefix,
		storage:    storage,
		serializer: serializer,
		cache:      cache,
	}, nil
}

// LoadNode loads a node from storage
func (s *NodeStorage[K, V]) LoadNode(ctx context.Context, id string) (*Node[K, V], error) {
	// Lock the nodes
	s.nodesLock.RLock()
	defer s.nodesLock.RUnlock()

	// Try to get from cache first
	if node, ok := s.cache.Get(id); ok && !s.skipCache {
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

	// Queue the operation to add to cache
	s.queueCacheOperation(CacheOpAdd, id, node)

	return node, nil
}

// SaveNode saves a node to storage
func (s *NodeStorage[K, V]) SaveNode(ctx context.Context, node *Node[K, V]) error {
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

	// Queue the operation to add to cache
	s.queueCacheOperation(CacheOpAdd, node.ID, node)

	return err
}

// DeleteNode deletes a node from storage
func (s *NodeStorage[K, V]) DeleteNode(ctx context.Context, id string) error {
	// Lock the nodes
	s.nodesLock.Lock()
	defer s.nodesLock.Unlock()

	path := s.prefix + NodePath + "/" + id
	if err := s.storage.Delete(ctx, path); err != nil {
		return fmt.Errorf("failed to delete node %s: %w", id, err)
	}

	// Queue the operation to delete from cache
	s.queueCacheOperation(CacheOpDelete, id, nil)

	return nil
}

// GetRootID gets the ID of the root node
func (s *NodeStorage[K, V]) GetRootID(ctx context.Context) (string, error) {
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
func (s *NodeStorage[K, V]) SetRootID(ctx context.Context, id string) error {
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

func (s *NodeStorage[K, V]) WithTransaction(ctx context.Context, function func() error) error {
	// TODO (gabrielopesantos): This is actually all wrong;
	err := logical.WithTransaction(ctx, s.storage, func(storage logical.Storage) error {
		return function()
	})
	s.flushCacheOperations(err == nil)
	return err
}

type cacheOp string

const (
	CacheOpAdd    cacheOp = "add"
	CacheOpDelete cacheOp = "delete"
)

// cacheOperation is a struct to hold the operation type, key, and value
type cacheOperation[K comparable, V any] struct {
	opType cacheOp
	key    string
	value  *Node[K, V]
}

// Add an operation to the queue
func (s *NodeStorage[K, V]) queueCacheOperation(opType cacheOp, key string, value *Node[K, V]) {
	s.queueLock.Lock()
	defer s.queueLock.Unlock()

	s.operationQueue = append(s.operationQueue, cacheOperation[K, V]{
		opType: opType,
		key:    key,
		value:  value,
	})
}

// Add a method to flush queued operations
func (s *NodeStorage[K, V]) flushCacheOperations(apply bool) error {
	s.queueLock.Lock()
	defer s.queueLock.Unlock()

	if apply {
		for _, op := range s.operationQueue {
			switch op.opType {
			case CacheOpAdd:
				s.cache.Add(op.key, op.value)
			case CacheOpDelete:
				s.cache.Delete(op.key)
			}
		}
	}

	// Clear the queue after flushing
	s.operationQueue = nil
	return nil
}
