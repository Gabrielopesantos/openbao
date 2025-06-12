package bptree

import (
	"context"
	"testing"

	"github.com/openbao/openbao/sdk/v2/logical"
	"github.com/stretchr/testify/require"
)

// TestMultiTreeOperations tests that multiple trees can operate independently
func TestMultiTreeOperations(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage(s, nil, 100)
	require.NoError(t, err, "Failed to create storage")

	// Create two trees with different names
	config1, err := NewBPlusTreeConfig("tree1", 4)
	require.NoError(t, err)
	tree1, err := InitializeBPlusTree(ctx, storage, config1)
	require.NoError(t, err, "Failed to create tree1")

	config2, err := NewBPlusTreeConfig("tree2", 4)
	require.NoError(t, err)
	tree2, err := InitializeBPlusTree(ctx, storage, config2)
	require.NoError(t, err, "Failed to create tree2")

	// Insert data into tree1
	err = tree1.Insert(ctx, storage, "key1", "tree1_value1")
	require.NoError(t, err, "Failed to insert into tree1")
	err = tree1.Insert(ctx, storage, "key2", "tree1_value2")
	require.NoError(t, err, "Failed to insert into tree1")

	// Insert data into tree2
	err = tree2.Insert(ctx, storage, "key1", "tree2_value1")
	require.NoError(t, err, "Failed to insert into tree2")
	err = tree2.Insert(ctx, storage, "key3", "tree2_value3")
	require.NoError(t, err, "Failed to insert into tree2")

	// Verify tree1 data
	val, found, err := tree1.Get(ctx, storage, "key1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"tree1_value1"}, val)

	val, found, err = tree1.Get(ctx, storage, "key2")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"tree1_value2"}, val)

	// key3 should not exist in tree1
	_, found, err = tree1.Get(ctx, storage, "key3")
	require.NoError(t, err)
	require.False(t, found)

	// Verify tree2 data
	val, found, err = tree2.Get(ctx, storage, "key1")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"tree2_value1"}, val)

	val, found, err = tree2.Get(ctx, storage, "key3")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []string{"tree2_value3"}, val)

	// key2 should not exist in tree2
	_, found, err = tree2.Get(ctx, storage, "key2")
	require.NoError(t, err)
	require.False(t, found)
}
