// Copyright (c) 2024 OpenBao a Series of LF Projects, LLC
// SPDX-License-Identifier: MPL-2.0

package bptree

import (
	"context"
)

type Transactional[V any] interface {
	BeginReadOnlyTx(context.Context) (Transaction[V], error)
	BeginTx(context.Context) (Transaction[V], error)
}

type Transaction[V any] interface {
	Storage[V]
	Commit(context.Context) error

	// Rollback a transaction, preventing any changes from being persisted.
	// Either Commit or Rollback must be called to release resources.
	Rollback(context.Context) error
}

// TransactionalStorage is implemented if a storage backend implements
// Transactional as well.
type TransactionalStorage[V any] interface {
	Storage[V]
	Transactional[V]
}
