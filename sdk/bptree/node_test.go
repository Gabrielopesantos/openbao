package bptree

import (
	"context"
	"testing"

	"github.com/openbao/openbao/sdk/v2/logical"
	"github.com/stretchr/testify/require"
)

func TestNewLeafNode(t *testing.T) {
	node := NewLeafNode[int, string]("leaf1")
	require.NotNil(t, node)
	require.Equal(t, "leaf1", node.ID)
	require.True(t, node.IsLeaf)
	require.Empty(t, node.Keys)
	require.Empty(t, node.Values)
	require.Empty(t, node.ChildrenIDs)
}

func TestNewInternalNode(t *testing.T) {
	node := NewInternalNode[int, string]("internal1")
	require.NotNil(t, node)
	require.Equal(t, "internal1", node.ID)
	require.False(t, node.IsLeaf)
	require.Empty(t, node.Keys)
	require.Empty(t, node.Values)
	require.Empty(t, node.ChildrenIDs)
}

func TestFindKeyIndex(t *testing.T) {
	t.Run("EmptyNode", func(t *testing.T) {
		node := NewLeafNode[int, string]("leaf")
		idx, found := node.findKeyIndex(5, intLess)
		require.False(t, found)
		require.Equal(t, 0, idx)
	})

	t.Run("KeyFound", func(t *testing.T) {
		node := NewLeafNode[int, string]("leaf")
		node.insertKeyValue(1, "one", intLess, stringEqual)
		node.insertKeyValue(3, "three", intLess, stringEqual)
		node.insertKeyValue(5, "five", intLess, stringEqual)
		node.insertKeyValue(7, "seven", intLess, stringEqual)
		node.insertKeyValue(9, "nine", intLess, stringEqual)

		idx, found := node.findKeyIndex(5, intLess)
		require.True(t, found)
		require.Equal(t, 2, idx)
	})

	t.Run("KeyNotFound", func(t *testing.T) {
		node := NewLeafNode[int, string]("leaf")
		node.insertKeyValue(1, "one", intLess, stringEqual)
		node.insertKeyValue(3, "three", intLess, stringEqual)
		node.insertKeyValue(5, "five", intLess, stringEqual)
		node.insertKeyValue(7, "seven", intLess, stringEqual)
		node.insertKeyValue(9, "nine", intLess, stringEqual)

		// Test insertion point for various values
		tests := []struct {
			key           int
			expectedIndex int
		}{
			{0, 0},  // Before all keys
			{2, 1},  // Between 1 and 3
			{4, 2},  // Between 3 and 5
			{6, 3},  // Between 5 and 7
			{8, 4},  // Between 7 and 9
			{10, 5}, // After all keys
		}

		for _, test := range tests {
			idx, found := node.findKeyIndex(test.key, intLess)
			require.False(t, found)
			require.Equal(t, test.expectedIndex, idx)
		}
	})

	t.Run("StringKeys", func(t *testing.T) {
		node := NewLeafNode[string, int]("leaf")
		node.insertKeyValue("a", 1, stringLess, intEqual)
		node.insertKeyValue("c", 3, stringLess, intEqual)
		node.insertKeyValue("e", 5, stringLess, intEqual)
		node.insertKeyValue("g", 7, stringLess, intEqual)
		node.insertKeyValue("i", 9, stringLess, intEqual)

		idx, found := node.findKeyIndex("e", stringLess)
		require.True(t, found)
		require.Equal(t, 2, idx)

		idx, found = node.findKeyIndex("d", stringLess)
		require.False(t, found)
		require.Equal(t, 2, idx) // Between "c" and "e"
	})
}

func TestInsertKeyValue(t *testing.T) {
	t.Run("EmptyNode", func(t *testing.T) {
		node := NewLeafNode[int, string]("leaf")
		node.insertKeyValue(5, "five", intLess, stringEqual)

		require.Equal(t, []int{5}, node.Keys)
		require.Equal(t, [][]string{{"five"}}, node.Values)
	})

	t.Run("AppendToEnd", func(t *testing.T) {
		node := NewLeafNode[int, string]("leaf")
		node.insertKeyValue(1, "one", intLess, stringEqual)
		node.insertKeyValue(3, "three", intLess, stringEqual)

		node.insertKeyValue(5, "five", intLess, stringEqual)

		require.Equal(t, []int{1, 3, 5}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"three"}, {"five"}}, node.Values)
	})

	t.Run("InsertInMiddle", func(t *testing.T) {
		node := NewLeafNode[int, string]("leaf")
		node.insertKeyValue(1, "one", intLess, stringEqual)
		node.insertKeyValue(5, "five", intLess, stringEqual)

		node.insertKeyValue(3, "three", intLess, stringEqual)

		require.Equal(t, []int{1, 3, 5}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"three"}, {"five"}}, node.Values)
	})

	t.Run("InsertAtBeginning", func(t *testing.T) {
		node := NewLeafNode[int, string]("leaf")
		node.insertKeyValue(3, "three", intLess, stringEqual)
		node.insertKeyValue(5, "five", intLess, stringEqual)

		node.insertKeyValue(1, "one", intLess, stringEqual)

		require.Equal(t, []int{1, 3, 5}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"three"}, {"five"}}, node.Values)
	})

	t.Run("OutOfBoundsIndex", func(t *testing.T) {
		node := NewLeafNode[int, string]("leaf")
		node.insertKeyValue(1, "one", intLess, stringEqual)
		node.insertKeyValue(3, "three", intLess, stringEqual)

		// Index beyond bounds should append to the end
		node.insertKeyValue(5, "five", intLess, stringEqual)

		require.Equal(t, []int{1, 3, 5}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"three"}, {"five"}}, node.Values)
	})

	t.Run("InsertExistingKey", func(t *testing.T) {
		node := NewLeafNode[string, string]("leaf")
		node.insertKeyValue("color", "red", stringLess, stringEqual)
		node.insertKeyValue("size", "small", stringLess, stringEqual)
		node.insertKeyValue("color", "blue", stringLess, stringEqual)
		node.insertKeyValue("color", "blue", stringLess, stringEqual)
		node.insertKeyValue("size", "large", stringLess, stringEqual)

		require.Equal(t, []string{"color", "size"}, node.Keys)
		require.Equal(t, [][]string{{"red", "blue"}, {"small", "large"}}, node.Values)
	})
}

func TestInsertChild(t *testing.T) {
	t.Run("AppendChild", func(t *testing.T) {
		parent := NewInternalNode[int, string]("parent")
		child1 := NewLeafNode[int, string]("child1")

		parent.insertChild(0, child1.ID)
		require.Equal(t, 1, len(parent.ChildrenIDs))
		require.Equal(t, child1.ID, parent.ChildrenIDs[0])
	})

	t.Run("InsertMultipleChildren", func(t *testing.T) {
		parent := NewInternalNode[int, string]("parent")
		child1 := NewLeafNode[int, string]("child1")
		child2 := NewLeafNode[int, string]("child2")
		child3 := NewLeafNode[int, string]("child3")

		parent.insertChild(0, child1.ID)
		parent.insertChild(1, child3.ID) // Add at the end
		parent.insertChild(1, child2.ID) // Insert in the middle

		require.Equal(t, 3, len(parent.ChildrenIDs))
		require.Equal(t, []string{"child1", "child2", "child3"}, parent.ChildrenIDs)
	})
}

func TestRemoveKeyValue(t *testing.T) {
	t.Run("RemoveFromMiddle", func(t *testing.T) {
		node := NewLeafNode[int, string]("leaf")
		node.insertKeyValue(1, "one", intLess, stringEqual)
		node.insertKeyValue(3, "three", intLess, stringEqual)
		node.insertKeyValue(5, "five", intLess, stringEqual)

		node.removeKeyValue(1) // Remove 3

		require.Equal(t, []int{1, 5}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"five"}}, node.Values)
	})

	t.Run("RemoveFromBeginning", func(t *testing.T) {
		node := NewLeafNode[int, string]("leaf")
		node.insertKeyValue(1, "one", intLess, stringEqual)
		node.insertKeyValue(3, "three", intLess, stringEqual)
		node.insertKeyValue(5, "five", intLess, stringEqual)

		node.removeKeyValue(0) // Remove 1

		require.Equal(t, []int{3, 5}, node.Keys)
		require.Equal(t, [][]string{{"three"}, {"five"}}, node.Values)
	})

	t.Run("RemoveFromEnd", func(t *testing.T) {
		node := NewLeafNode[int, string]("leaf")
		node.insertKeyValue(1, "one", intLess, stringEqual)
		node.insertKeyValue(3, "three", intLess, stringEqual)
		node.insertKeyValue(5, "five", intLess, stringEqual)

		node.removeKeyValue(2) // Remove 5

		require.Equal(t, []int{1, 3}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"three"}}, node.Values)
	})

	t.Run("RemoveOnlyElement", func(t *testing.T) {
		node := NewLeafNode[int, string]("leaf")
		node.insertKeyValue(1, "one", intLess, stringEqual)

		node.removeKeyValue(0)

		require.Empty(t, node.Keys)
		require.Empty(t, node.Values)
	})
}

func TestRemoveChild(t *testing.T) {
	t.Run("RemoveFromMiddle", func(t *testing.T) {
		parent := NewInternalNode[int, string]("parent")
		child1 := NewLeafNode[int, string]("child1")
		child2 := NewLeafNode[int, string]("child2")
		child3 := NewLeafNode[int, string]("child3")

		parent.ChildrenIDs = []string{child1.ID, child2.ID, child3.ID}

		parent.removeChild(1) // Remove child2

		require.Equal(t, 2, len(parent.ChildrenIDs))
		require.Equal(t, []string{child1.ID, child3.ID}, parent.ChildrenIDs)
	})

	t.Run("RemoveFromBeginning", func(t *testing.T) {
		parent := NewInternalNode[int, string]("parent")
		child1 := NewLeafNode[int, string]("child1")
		child2 := NewLeafNode[int, string]("child2")
		child3 := NewLeafNode[int, string]("child3")

		parent.ChildrenIDs = []string{child1.ID, child2.ID, child3.ID}

		parent.removeChild(0) // Remove child1

		require.Equal(t, 2, len(parent.ChildrenIDs))
		require.Equal(t, []string{child2.ID, child3.ID}, parent.ChildrenIDs)
	})

	t.Run("RemoveFromEnd", func(t *testing.T) {
		parent := NewInternalNode[int, string]("parent")
		child1 := NewLeafNode[int, string]("child1")
		child2 := NewLeafNode[int, string]("child2")
		child3 := NewLeafNode[int, string]("child3")

		parent.ChildrenIDs = []string{child1.ID, child2.ID, child3.ID}

		parent.removeChild(2) // Remove child3

		require.Equal(t, 2, len(parent.ChildrenIDs))
		require.Equal(t, []string{child1.ID, child2.ID}, parent.ChildrenIDs)
	})

	t.Run("RemoveOnlyChild", func(t *testing.T) {
		parent := NewInternalNode[int, string]("parent")
		child := NewLeafNode[int, string]("child")

		parent.ChildrenIDs = []string{child.ID}

		parent.removeChild(0)

		require.Empty(t, parent.ChildrenIDs)
	})
}

func TestNodeInterfacingWithStorageAdapter(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	adapter, err := NewStorageAdapter[string, string]("bptree", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	// Test SaveNode and LoadNode
	leafID := "leaf-1"
	leaf := NewLeafNode[string, string](leafID)
	leaf.insertKeyValue("key1", "value1", stringLess, stringEqual)
	leaf.insertKeyValue("key2", "value2", stringLess, stringEqual)

	// Create an internal node
	internalID := "internal-1"
	internal := NewInternalNode[string, string](internalID)
	internal.insertChild(0, leaf.ID)

	// Save the leaf node (saved after being added to the internal node otherwise there is no reference to parent on it)
	err = adapter.SaveNode(ctx, leaf)
	require.NoError(t, err, "Failed to save node")

	// Load the node
	loadedLeaf, err := adapter.LoadNode(ctx, leafID)
	require.NoError(t, err, "Failed to load node")
	require.NotNil(t, loadedLeaf, "Loaded node is nil")

	require.Equal(t, leaf.ID, loadedLeaf.ID, "Expected node ID %s, got %s", leaf.ID, loadedLeaf.ID)
	require.Equal(t, leaf.Keys, loadedLeaf.Keys, "Expected node keys %v, got %v", leaf.Keys, loadedLeaf.Keys)
	require.Equal(t, leaf.Values, loadedLeaf.Values, "Expected node values %v, got %v", leaf.Values, loadedLeaf.Values)

	// Save the internal node
	err = adapter.SaveNode(ctx, internal)
	require.NoError(t, err, "Failed to save internal node")

	// Load the internal node and check that keys are set but references not
	loadedInternal, err := adapter.LoadNode(ctx, internalID)
	require.NoError(t, err, "Failed to load internal node")
	require.NotNil(t, loadedInternal, "Loaded internal node is nil")

	require.Equal(t, internal.ID, loadedInternal.ID, "Expected node ID %s, got %s", internal.ID, loadedInternal.ID)
	require.Equal(t, internal.ChildrenIDs, loadedInternal.ChildrenIDs, "Expected node children keys %v, got %v", internal.ChildrenIDs, loadedInternal.ChildrenIDs)
}
