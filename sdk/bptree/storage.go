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

const (
	nodesPath    = "nodes"
	rootPath     = "root"
	metadataPath = "metadata"
)

type Storage interface {
	// LoadNode loads a node from storage
	LoadNode(ctx context.Context, id string) (*Node, error)
	// SaveNode saves a node to storage
	SaveNode(ctx context.Context, node *Node) error
	// DeleteNode deletes a node from storage
	DeleteNode(ctx context.Context, id string) error
	// GetRootID gets the ID of the root node
	GetRootID(ctx context.Context) (string, error)
	// SetRootID sets the ID of the root node
	SetRootID(ctx context.Context, id string) error
}

var _ Storage = &NodeStorage{}

// NodeStorage adapts the logical.Storage interface to the bptree.Storage interface
type NodeStorage struct {
	prefix         string
	serializer     NodeSerializer
	storage        logical.Storage
	cache          *lru.LRU[string, *Node]
	skipCache      bool
	nodesLock      sync.RWMutex
	rootLock       sync.RWMutex
	operationQueue []cacheOperation
	queueLock      sync.Mutex
}

// NodeSerializer defines how to serialize and deserialize nodes
type NodeSerializer interface {
	Serialize(node *Node) ([]byte, error)
	Deserialize(data []byte) (*Node, error)
}

// JSONSerializer is a simple JSON-based serializer for nodes
type JSONSerializer struct{}

// Serialize converts a node to JSON
func (s *JSONSerializer) Serialize(node *Node) ([]byte, error) {
	return json.Marshal(node)
}

// Deserialize converts JSON to a node
func (s *JSONSerializer) Deserialize(data []byte) (*Node, error) {
	var node Node
	if err := json.Unmarshal(data, &node); err != nil {
		return nil, err
	}
	return &node, nil
}

// NewNodeStorage creates a new adapter for the logical.Storage interface
func NewNodeStorage(
	prefix string,
	storage logical.Storage,
	serializer NodeSerializer,
	cacheSize int,
) (*NodeStorage, error) {
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	if serializer == nil {
		serializer = &JSONSerializer{}
	}

	cache, err := lru.NewLRU[string, *Node](cacheSize)
	if err != nil {
		return nil, err
	}

	return &NodeStorage{
		prefix:     prefix,
		storage:    storage,
		serializer: serializer,
		cache:      cache,
	}, nil
}

// LoadNode loads a node from storage
func (s *NodeStorage) LoadNode(ctx context.Context, id string) (*Node, error) {
	// Lock the nodes
	s.nodesLock.RLock()
	defer s.nodesLock.RUnlock()

	// Try to get from cache first
	if node, ok := s.cache.Get(id); ok && !s.skipCache {
		return node, nil
	}

	// Load from storage
	path := s.prefix + nodesPath + "/" + id
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
func (s *NodeStorage) SaveNode(ctx context.Context, node *Node) error {
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

	path := s.prefix + nodesPath + "/" + node.ID
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
func (s *NodeStorage) DeleteNode(ctx context.Context, id string) error {
	// Lock the nodes
	s.nodesLock.Lock()
	defer s.nodesLock.Unlock()

	path := s.prefix + nodesPath + "/" + id
	if err := s.storage.Delete(ctx, path); err != nil {
		return fmt.Errorf("failed to delete node %s: %w", id, err)
	}

	// Queue the operation to delete from cache
	s.queueCacheOperation(CacheOpDelete, id, nil)

	return nil
}

// GetRootID gets the ID of the root node
func (s *NodeStorage) GetRootID(ctx context.Context) (string, error) {
	// Lock the root
	s.rootLock.RLock()
	defer s.rootLock.RUnlock()

	path := s.prefix + metadataPath + "/" + rootPath
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
func (s *NodeStorage) SetRootID(ctx context.Context, id string) error {
	// Lock the root
	s.rootLock.Lock()
	defer s.rootLock.Unlock()

	path := s.prefix + metadataPath + "/" + rootPath
	entry := &logical.StorageEntry{
		Key:   path,
		Value: []byte(id),
	}

	if err := s.storage.Put(ctx, entry); err != nil {
		return fmt.Errorf("failed to set root ID: %w", err)
	}

	return nil
}

type cacheOp string

const (
	CacheOpAdd    cacheOp = "add"
	CacheOpDelete cacheOp = "delete"
)

// cacheOperation is a struct to hold the operation type, key, and value
type cacheOperation struct {
	opType cacheOp
	key    string
	value  *Node
}

// Add an operation to the queue
func (s *NodeStorage) queueCacheOperation(opType cacheOp, key string, value *Node) {
	s.queueLock.Lock()
	defer s.queueLock.Unlock()

	s.operationQueue = append(s.operationQueue, cacheOperation{
		opType: opType,
		key:    key,
		value:  value,
	})
}

// Add a method to flush queued operations
func (s *NodeStorage) flushCacheOperations(apply bool) error {
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

// type TransactionalNodeStorage struct {
// 	NodeStorage
// }

// var _ TransactionalStorage = &TransactionalNodeStorage{}

// type NodeTransaction struct {
// 	NodeStorage
// }

// var _ Transaction = &NodeTransaction{}

// func (s *TransactionalNodeStorage) BeginReadOnlyTx(ctx context.Context) (Transaction, error) {
// 	tx, err := s.storage.(logical.TransactionalStorage).BeginReadOnlyTx(ctx)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &NodeTransaction{
// 		NodeStorage: NodeStorage{
// 			storage: tx,
// 		},
// 	}, nil
// }

// func (s *TransactionalNodeStorage) BeginTx(ctx context.Context) (Transaction, error) {
// 	tx, err := s.storage.(logical.TransactionalStorage).BeginReadOnlyTx(ctx)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &NodeTransaction{
// 		NodeStorage: NodeStorage{
// 			storage: tx,
// 		},
// 	}, nil
// }

// func (s *NodeTransaction) Commit(ctx context.Context) error {
// 	return s.storage.(logical.Transaction).Commit(ctx)
// }

// func (s *NodeTransaction) Rollback(ctx context.Context) error {
// 	return s.storage.(logical.Transaction).Rollback(ctx)
// }

// WithTransaction will begin and end a transaction around the execution of the `callback` function.
// func WithTransaction(ctx context.Context, originalStorage Storage, callback func(Storage) error) error {
// 	if txnStorage, ok := originalStorage.(TransactionalStorage); ok {
// 		txn, err := txnStorage.BeginTx(ctx)
// 		if err != nil {
// 			return err
// 		}
// 		defer txn.Rollback(ctx)
// 		if err := callback(txnStorage); err != nil {
// 			return err
// 		}
// 		if err := txn.Commit(ctx); err != nil {
// 			return err
// 		}
// 	} else {
// 		return callback(originalStorage)
// 	}
// 	return nil
// }
