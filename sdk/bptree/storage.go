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
	// GetRootID gets the ID of the root node
	GetRootID(ctx context.Context) (string, error)
	// SetRootID sets the ID of the root node
	SetRootID(ctx context.Context, id string) error
	// LoadNode loads a node from storage
	LoadNode(ctx context.Context, id string) (*Node, error)
	// SaveNode saves a node to storage
	SaveNode(ctx context.Context, node *Node) error
	// DeleteNode deletes a node from storage
	DeleteNode(ctx context.Context, id string) error
	// PurgeNodes clears all nodes from storage starting with the prefix
	// PurgeNodes(ctx context.Context) error
}

var _ Storage = &NodeStorage{}

// NodeStorage adapts the logical.Storage interface to the bptree.Storage interface
type NodeStorage struct {
	prefix             string
	storage            logical.Storage
	serializer         NodeSerializer
	nodesLock          sync.RWMutex
	rootLock           sync.RWMutex
	skipCache          bool
	cache              *lru.LRU[string, *Node]
	pendingCacheOps    []cacheOperation // Operations to be applied on commit
	cachesOpsQueueLock sync.Mutex
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

// NewTransactionalNodeStorage creates a new transactional adapter for the logical.Storage interface
// if the underlying storage supports transactions. Otherwise, it returns a regular NodeStorage.
func NewTransactionalNodeStorage(
	prefix string,
	storage logical.Storage,
	serializer NodeSerializer,
	cacheSize int,
) (Storage, error) {
	nodeStorage, err := NewNodeStorage(prefix, storage, serializer, cacheSize)
	if err != nil {
		return nil, err
	}

	// Check if the underlying storage supports transactions
	if _, ok := storage.(logical.TransactionalStorage); ok {
		return &TransactionalNodeStorage{
			NodeStorage: nodeStorage,
		}, nil
	}

	// Return regular NodeStorage if transactions not supported
	return nodeStorage, nil
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

// LoadNode loads a node from storage
func (s *NodeStorage) LoadNode(ctx context.Context, id string) (*Node, error) {
	// Lock the nodes for reading
	s.nodesLock.RLock()
	defer s.nodesLock.RUnlock()

	// Try to get from cache first (unless cache is disabled)
	if !s.skipCache {
		if node, ok := s.cache.Get(id); ok {
			return node, nil
		}
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

	// Cache the loaded node (immediate for non-transactional, queued for transactional)
	if !s.skipCache {
		s.applyCacheOp(CacheOpAdd, id, node)
	}

	return node, nil
}

// SaveNode saves a node to storage
func (s *NodeStorage) SaveNode(ctx context.Context, node *Node) error {
	// Check if the node is nil
	if node == nil {
		return fmt.Errorf("cannot save nil node")
	}

	// Lock storage for writing
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

	// Cache the saved node (immediate for non-transactional, queued for transactional)
	if !s.skipCache {
		s.applyCacheOp(CacheOpAdd, node.ID, node)
	}

	return nil
}

// DeleteNode deletes a node from storage
func (s *NodeStorage) DeleteNode(ctx context.Context, id string) error {
	// Lock the nodes for writing
	s.nodesLock.Lock()
	defer s.nodesLock.Unlock()

	path := s.prefix + nodesPath + "/" + id
	if err := s.storage.Delete(ctx, path); err != nil {
		return fmt.Errorf("failed to delete node %s: %w", id, err)
	}

	// Remove from cache (immediate for non-transactional, queued for transactional)
	if !s.skipCache {
		s.applyCacheOp(CacheOpDelete, id, nil)
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

// queueCacheOp adds an operation to the pending cache operations queue.
// This allows batching cache operations and applying them only on successful commits.
func (s *NodeStorage) queueCacheOp(opType cacheOp, key string, value *Node) {
	s.cachesOpsQueueLock.Lock()
	defer s.cachesOpsQueueLock.Unlock()

	s.pendingCacheOps = append(s.pendingCacheOps, cacheOperation{
		opType: opType,
		key:    key,
		value:  value,
	})
}

// applyCacheOp applies cache operations immediately if not in a transaction,
// or queues them if in a transaction context.
func (s *NodeStorage) applyCacheOp(opType cacheOp, key string, value *Node) {
	// Check if this is a transaction by seeing if we have a transaction storage type
	if _, isTransaction := s.storage.(logical.Transaction); isTransaction {
		// We're in a transaction - queue the operation for later commit/rollback
		s.queueCacheOp(opType, key, value)
	} else {
		// Not in a transaction - apply immediately
		switch opType {
		case CacheOpAdd:
			if value != nil {
				s.cache.Add(key, value)
			}
		case CacheOpDelete:
			s.cache.Delete(key)
		}
	}
}

// flushCacheOps applies or discards pending cache operations.
// If apply is true, operations are applied to the cache.
// If apply is false, operations are discarded (rollback behavior).
func (s *NodeStorage) flushCacheOps(apply bool) error {
	s.cachesOpsQueueLock.Lock()
	defer func() {
		// Clear the queue after processing
		s.pendingCacheOps = s.pendingCacheOps[:0]
		s.cachesOpsQueueLock.Unlock()
	}()

	if !apply || len(s.pendingCacheOps) == 0 {
		// Rollback: just clear the queue without applying operations
		// Or nothing to apply
		return nil
	}

	// Apply operations to cache
	for _, op := range s.pendingCacheOps {
		switch op.opType {
		case CacheOpAdd:
			if op.value != nil {
				s.cache.Add(op.key, op.value)
			}
		case CacheOpDelete:
			s.cache.Delete(op.key)
		}
	}

	return nil
}

// CacheStats returns information about the current cache state
func (s *NodeStorage) CacheStats() (size int, pendingOps int) {
	s.cachesOpsQueueLock.Lock()
	pendingOps = len(s.pendingCacheOps)
	s.cachesOpsQueueLock.Unlock()

	// Return the configured cache size
	if s.cache != nil {
		return s.cache.Size(), pendingOps
	}
	return 0, pendingOps
}

// EnableCache enables or disables cache operations
func (s *NodeStorage) EnableCache(enabled bool) {
	s.skipCache = !enabled
}

// IsCacheEnabled returns whether cache operations are enabled
func (s *NodeStorage) IsCacheEnabled() bool {
	return !s.skipCache
}

// PurgeCache clears all entries from the cache
func (s *NodeStorage) PurgeCache() {
	s.cache.Purge()
}

type TransactionalNodeStorage struct {
	*NodeStorage
}

var _ TransactionalStorage = &TransactionalNodeStorage{}

type NodeTransaction struct {
	*NodeStorage
}

var _ Transaction = &NodeTransaction{}

func (s *TransactionalNodeStorage) BeginReadOnlyTx(ctx context.Context) (Transaction, error) {
	tx, err := s.storage.(logical.TransactionalStorage).BeginReadOnlyTx(ctx)
	if err != nil {
		return nil, err
	}

	return &NodeTransaction{
		NodeStorage: &NodeStorage{
			prefix:     s.prefix,
			storage:    tx,
			serializer: s.serializer,
			cache:      s.cache, // Share cache for read-only transactions
			skipCache:  false,   // Enable cache for read-only transactions
			// New mutex instances for the transaction (read-only still needs its own locks)
			nodesLock:          sync.RWMutex{},
			rootLock:           sync.RWMutex{},
			cachesOpsQueueLock: sync.Mutex{},
			pendingCacheOps:    nil, // No cache operations for read-only
		},
	}, nil
}

func (s *TransactionalNodeStorage) BeginTx(ctx context.Context) (Transaction, error) {
	tx, err := s.storage.(logical.TransactionalStorage).BeginTx(ctx) // Fix: was BeginReadOnlyTx
	if err != nil {
		return nil, err
	}

	return &NodeTransaction{
		NodeStorage: &NodeStorage{
			prefix:     s.prefix,
			storage:    tx,
			serializer: s.serializer,
			cache:      s.cache, // Share cache within transactions
			skipCache:  false,   // Enable cache within transactions
			// New mutex instances for the transaction
			nodesLock:          sync.RWMutex{},
			rootLock:           sync.RWMutex{},
			cachesOpsQueueLock: sync.Mutex{},
			pendingCacheOps:    make([]cacheOperation, 0),
		},
	}, nil
}

func (s *NodeTransaction) Commit(ctx context.Context) error {
	var err error
	defer s.flushCacheOps(err == nil) // Ensure cache operations are flushed on commit

	// Commit the underlying transaction first
	if err = s.storage.(logical.Transaction).Commit(ctx); err != nil {
		return err
	}

	return nil
}

func (s *NodeTransaction) Rollback(ctx context.Context) error {
	// Clear any pending cache operations (don't apply them)
	s.flushCacheOps(false)

	// Rollback the underlying transaction
	return s.storage.(logical.Transaction).Rollback(ctx)
}

// WithTransaction will begin and end a transaction around the execution of the `callback` function.
// If the storage supports transactions, it creates a transaction and passes it to the callback.
// On success, the transaction is committed; on failure, it's rolled back.
func WithTransaction(ctx context.Context, originalStorage Storage, callback func(Storage) error) error {
	if txnStorage, ok := originalStorage.(TransactionalStorage); ok {
		txn, err := txnStorage.BeginTx(ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}

		// Ensure rollback is called if commit is not reached
		defer func() {
			if rollbackErr := txn.Rollback(ctx); rollbackErr != nil {
				// Log rollback errors but don't override the main error
				// In production, you might want to use a proper logger here
			}
		}()

		// Execute the callback with the transaction storage
		if err := callback(txn); err != nil {
			return err
		}

		// Commit the transaction
		if err := txn.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		return nil
	} else {
		// If storage doesn't support transactions, execute directly
		return callback(originalStorage)
	}
}
