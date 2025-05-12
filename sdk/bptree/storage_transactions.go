// Copyright (c) 2024 OpenBao a Series of LF Projects, LLC
// SPDX-License-Identifier: MPL-2.0

package bptree

import (
	"context"
)

type Transactional[K comparable, V any] interface {
	BeginReadOnlyTx(context.Context) (Transaction[K, V], error)
	BeginTx(context.Context) (Transaction[K, V], error)
}

type Transaction[K comparable, V any] interface {
	Storage[K, V]
	Commit(context.Context) error

	// Rollback a transaction, preventing any changes from being persisted.
	// Either Commit or Rollback must be called to release resources.
	Rollback(context.Context) error
}

// TransactionalStorage is implemented if a storage backend implements
// Transactional as well.
type TransactionalStorage[K comparable, V any] interface {
	Storage[K, V]
	Transactional[K, V]
}
