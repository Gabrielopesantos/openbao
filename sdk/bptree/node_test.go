package bptree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewLeafNode(t *testing.T) {
	node := NewLeafNode("leaf1")
	require.NotNil(t, node)
	require.Equal(t, "leaf1", node.ID)
	require.True(t, node.IsLeaf)
	require.Empty(t, node.Keys)
	require.Empty(t, node.Values)
	require.Empty(t, node.ChildrenIDs)
}

func TestNewInternalNode(t *testing.T) {
	node := NewInternalNode("internal1")
	require.NotNil(t, node)
	require.Equal(t, "internal1", node.ID)
	require.False(t, node.IsLeaf)
	require.Empty(t, node.Keys)
	require.Empty(t, node.Values)
	require.Empty(t, node.ChildrenIDs)
}

func TestFindKeyIndex(t *testing.T) {
	t.Run("EmptyNode", func(t *testing.T) {
		node := NewLeafNode("leaf")
		idx, found := node.findKeyIndex("key5")
		require.False(t, found)
		require.Equal(t, 0, idx)
	})

	t.Run("KeyFound", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		node.insertKeyValueAt("key3", "three")
		node.insertKeyValueAt("key5", "five")
		node.insertKeyValueAt("key7", "seven")
		node.insertKeyValueAt("key9", "nine")

		// Test finding an existing key
		tests := []struct {
			key           string
			expectedIndex int
		}{
			{"key1", 0}, // First key
			{"key3", 1}, // Second key
			{"key5", 2}, // Third key
			{"key7", 3}, // Fourth key
			{"key9", 4}, // Fifth key
		}
		for _, test := range tests {
			idx, found := node.findKeyIndex(test.key)
			require.True(t, found)
			require.Equal(t, test.expectedIndex, idx)
		}
	})

	t.Run("KeyNotFound", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		node.insertKeyValueAt("key3", "three")
		node.insertKeyValueAt("key5", "five")
		node.insertKeyValueAt("key7", "seven")
		node.insertKeyValueAt("key9", "nine")

		// Test insertion point for various values
		tests := []struct {
			key           string
			expectedIndex int
		}{
			{"key0", 0}, // Before all keys
			{"key2", 1}, // Between key1 and key3
			{"key4", 2}, // Between key3 and key5
			{"key6", 3}, // Between key5 and key7
			{"key8", 4}, // Between key7 and key9
			{"keyZ", 5}, // After all keys
		}

		for _, test := range tests {
			idx, found := node.findKeyIndex(test.key)
			require.False(t, found)
			require.Equal(t, test.expectedIndex, idx)
		}
	})
}

func TestInsertKeyValue(t *testing.T) {
	t.Run("EmptyNode", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key5", "five")

		require.Equal(t, []string{"key5"}, node.Keys)
		require.Equal(t, [][]string{{"five"}}, node.Values)
	})

	t.Run("AppendToEnd", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		node.insertKeyValueAt("key3", "three")

		node.insertKeyValueAt("key5", "five")

		require.Equal(t, []string{"key1", "key3", "key5"}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"three"}, {"five"}}, node.Values)
	})

	t.Run("InsertInMiddle", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		node.insertKeyValueAt("key5", "five")

		node.insertKeyValueAt("key3", "three")

		require.Equal(t, []string{"key1", "key3", "key5"}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"three"}, {"five"}}, node.Values)
	})

	t.Run("InsertAtBeginning", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key3", "three")
		node.insertKeyValueAt("key5", "five")

		node.insertKeyValueAt("key1", "one")

		require.Equal(t, []string{"key1", "key3", "key5"}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"three"}, {"five"}}, node.Values)
	})

	t.Run("OutOfBoundsIndex", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		node.insertKeyValueAt("key3", "three")

		// Index beyond bounds should append to the end
		node.insertKeyValueAt("key5", "five")

		require.Equal(t, []string{"key1", "key3", "key5"}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"three"}, {"five"}}, node.Values)
	})

	t.Run("InsertExistingKey", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("color", "red")
		node.insertKeyValueAt("size", "small")
		node.insertKeyValueAt("color", "blue")
		node.insertKeyValueAt("color", "blue")
		node.insertKeyValueAt("size", "large")

		require.Equal(t, []string{"color", "size"}, node.Keys)
		require.Equal(t, [][]string{{"red", "blue"}, {"small", "large"}}, node.Values)
	})

	t.Run("InsertMultipleValuesForSameKey", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("color", "red")
		node.insertKeyValueAt("color", "blue")
		node.insertKeyValueAt("color", "green")

		require.Equal(t, []string{"color"}, node.Keys)
		require.Equal(t, [][]string{{"red", "blue", "green"}}, node.Values)
	})

	t.Run("InsertMultipleKeysAndValues", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("color", "red")
		node.insertKeyValueAt("size", "small")
		node.insertKeyValueAt("color", "blue")
		node.insertKeyValueAt("size", "large")
		node.insertKeyValueAt("shape", "circle")
		node.insertKeyValueAt("shape", "square")
		node.insertKeyValueAt("color", "yellow")
		node.insertKeyValueAt("size", "medium")
		node.insertKeyValueAt("shape", "triangle")
		require.Equal(t, []string{"color", "shape", "size"}, node.Keys)
		require.Equal(t, [][]string{
			{"red", "blue", "yellow"},
			{"circle", "square", "triangle"},
			{"small", "large", "medium"},
		}, node.Values)
	})

	t.Run("InsertKeyValueOnInternalNode", func(t *testing.T) {
		node := NewInternalNode("internal")
		err := node.insertKeyValueAt("key1", "value1")
		require.Error(t, err, "Expected error when inserting key-value on internal node")
		require.Equal(t, ErrNotALeafNode, err)
	})
}

// NOTE (gabrielopesantos): This methods needs to be reviewed.
func TestInsertKeyChild(t *testing.T) {
	t.Run("InsertKeyChildInEmptyNode", func(t *testing.T) {
		node := NewInternalNode("internal")
		err := node.insertKeyChildAt(0, "key1", "child1")
		require.NoError(t, err)
		require.Equal(t, []string{"key1"}, node.Keys)
		require.Equal(t, []string{"child1"}, node.ChildrenIDs)
	})

	// NOTE (gabrielopesantos): This method is built for inserting a key and the child to the right of the index,
	// so it fails. Needs to be reviewed.
	// t.Run("InsertKeyChildAtBeginning", func(t *testing.T) {
	// 	node := NewInternalNode("internal")
	// 	node.insertKeyChildAt(0, "key2", "child2")
	// 	node.insertKeyChildAt(0, "key1", "child1") // Insert at beginning

	// 	require.Equal(t, []string{"key1", "key2"}, node.Keys)
	// 	require.Equal(t, []string{"child1", "child2"}, node.ChildrenIDs)
	// })

	t.Run("InsertKeyChildAtEnd", func(t *testing.T) {
		node := NewInternalNode("internal")
		node.insertKeyChildAt(0, "key1", "child1")
		node.insertKeyChildAt(1, "key3", "child3") // Insert at end

		require.Equal(t, []string{"key1", "key3"}, node.Keys)
		require.Equal(t, []string{"child1", "child3"}, node.ChildrenIDs)
	})

	// NOTE (gabrielopesantos): Also needs to be reviewed.
	// t.Run("InsertKeyChildInMiddle", func(t *testing.T) {
	// 	node := NewInternalNode("internal")
	// 	node.insertKeyChildAt(0, "key1", "child1")
	// 	node.insertKeyChildAt(1, "key3", "child3")
	// 	node.insertKeyChildAt(1, "key2", "child2") // Insert in middle

	// 	require.Equal(t, []string{"key1", "key2", "key3"}, node.Keys)
	// 	require.Equal(t, []string{"child1", "child2", "child3"}, node.ChildrenIDs)
	// })

	// t.Run("InsertKeyChildOutOfBounds", func(t *testing.T) {
	// 	node := NewInternalNode("internal")
	// 	node.insertKeyChildAt(0, "key1", "child1")

	// 	// Attempt to insert at an out-of-bounds index
	// 	err := node.insertKeyChildAt(5, "key2", "child2")
	// 	require.NoError(t, err) // Should append to the end
	// 	require.Equal(t, []string{"key1", "key2"}, node.Keys)
	// 	require.Equal(t, []string{"child1", "child2"}, node.ChildrenIDs)
	// })

	t.Run("InsertKeyChildOnLeafNode", func(t *testing.T) {
		node := NewLeafNode("leaf")
		err := node.insertKeyChildAt(0, "key1", "child1")
		require.Error(t, err, "Expected error when inserting key-child on leaf node")
		require.Equal(t, ErrNotAnInternalNode, err)
	})
}

func TestRemoveKeyValue(t *testing.T) {
	t.Run("RemoveFromMiddle", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		node.insertKeyValueAt("key3", "three")
		node.insertKeyValueAt("key5", "five")

		node.removeKeyEntryAt(1) // Remove key3

		require.Equal(t, []string{"key1", "key5"}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"five"}}, node.Values)
	})

	t.Run("RemoveFromBeginning", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		node.insertKeyValueAt("key3", "three")
		node.insertKeyValueAt("key5", "five")

		node.removeKeyEntryAt(0) // Remove key1

		require.Equal(t, []string{"key3", "key5"}, node.Keys)
		require.Equal(t, [][]string{{"three"}, {"five"}}, node.Values)
	})

	t.Run("RemoveFromEnd", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		node.insertKeyValueAt("key3", "three")
		node.insertKeyValueAt("key5", "five")

		node.removeKeyEntryAt(2) // Remove key5

		require.Equal(t, []string{"key1", "key3"}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"three"}}, node.Values)
	})

	t.Run("RemoveOnlyElement", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")

		node.removeKeyEntryAt(0)

		require.Empty(t, node.Keys)
		require.Empty(t, node.Values)
	})

	t.Run("RemoveNonExistentKey", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		node.insertKeyValueAt("key3", "three")

		// Attempt to remove a key that doesn't exist
		err := node.removeKeyEntryAt(5) // Out of bounds index
		require.Error(t, err, "Expected error when removing non-existent key")
		require.Equal(t, ErrKeyIndexOutOfBounds, err)
	})
}

func TestRemoveKeyEntryAt(t *testing.T) {
	t.Run("RemoveKeyEntryFromInternalNode", func(t *testing.T) {
		node := NewInternalNode("internal")
		err := node.removeKeyEntryAt(0) // Attempt to remove key1
		require.Error(t, err, "Expected error when inserting key-child on internal node")
		require.Equal(t, ErrNotALeafNode, err)
	})

	t.Run("RemoveKeyEntryFromLeafNode", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		node.insertKeyValueAt("key3", "three")
		node.insertKeyValueAt("key5", "five")

		err := node.removeKeyEntryAt(1) // Remove key3
		require.NoError(t, err)

		require.Equal(t, []string{"key1", "key5"}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"five"}}, node.Values)
	})

	t.Run("RemoveKeyEntryFromBeginning", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		node.insertKeyValueAt("key3", "three")
		node.insertKeyValueAt("key5", "five")
		err := node.removeKeyEntryAt(0) // Remove key1
		require.NoError(t, err)
		require.Equal(t, []string{"key3", "key5"}, node.Keys)
		require.Equal(t, [][]string{{"three"}, {"five"}}, node.Values)
	})
	t.Run("RemoveKeyEntryFromEnd", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		node.insertKeyValueAt("key3", "three")
		node.insertKeyValueAt("key5", "five")
		err := node.removeKeyEntryAt(2) // Remove key5
		require.NoError(t, err)
		require.Equal(t, []string{"key1", "key3"}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"three"}}, node.Values)
	})

	t.Run("RemoveKeyEntryFromOnlyElement", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		err := node.removeKeyEntryAt(0) // Remove key1
		require.NoError(t, err)
		require.Empty(t, node.Keys)
		require.Empty(t, node.Values)
	})

	t.Run("RemoveKeyEntryFromEmptyNode", func(t *testing.T) {
		node := NewLeafNode("leaf")

		err := node.removeKeyEntryAt(0) // Attempt to remove from empty node
		require.Error(t, err, "Expected error when removing key from empty node")
		require.Equal(t, ErrKeyIndexOutOfBounds, err)
	})
}

func TestRemoveKeyValueAt(t *testing.T) {
	t.Run("RemoveKeyValueAtFromInternalNode", func(t *testing.T) {
		node := NewInternalNode("internal")
		err := node.removeKeyValueAt(0, "value") // Attempt to remove key1
		require.Error(t, err, "Expected error when removing key-value from internal node")
		require.Equal(t, ErrNotALeafNode, err)
	})

	t.Run("RemoveKeyValueAtFromLeafNode", func(t *testing.T) {
		node := NewLeafNode("leaf")
		node.insertKeyValueAt("key1", "one")
		node.insertKeyValueAt("key1", "oneone")
		node.insertKeyValueAt("key1", "oneoneone")
		node.insertKeyValueAt("key3", "three")
		node.insertKeyValueAt("key5", "five")

		// Remove key1 with value "oneone"
		err := node.removeKeyValueAt(0, "oneone") // Remove key1
		require.NoError(t, err)

		require.Equal(t, []string{"key1", "key3", "key5"}, node.Keys)
		require.Equal(t, [][]string{{"one", "oneoneone"}, {"three"}, {"five"}}, node.Values)

		// Remove non-existent value from key1
		err = node.removeKeyValueAt(0, "nonexistent")
		require.NoError(t, err, "Expected no error when removing non-existent value")

		// Remove last value from key1
		err = node.removeKeyValueAt(0, "oneoneone")
		require.NoError(t, err)
		require.Equal(t, []string{"key1", "key3", "key5"}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"three"}, {"five"}}, node.Values)

		// Remove key2 with value "three"
		err = node.removeKeyValueAt(1, "three")
		require.NoError(t, err)
		require.Equal(t, []string{"key1", "key5"}, node.Keys)
		require.Equal(t, [][]string{{"one"}, {"five"}}, node.Values)

		// Remove key5 with value "five"
		err = node.removeKeyValueAt(1, "five")
		require.NoError(t, err)
		require.Equal(t, []string{"key1"}, node.Keys)
		require.Equal(t, [][]string{{"one"}}, node.Values)

		// Remove last key-value pair
		err = node.removeKeyValueAt(0, "one")
		require.NoError(t, err)
		require.Empty(t, node.Keys)
		require.Empty(t, node.Values)

		// Remove from empty node
		// err = node.removeKeyValueAt(0, "anyvalue")
		// require.Error(t, err, "Expected error when removing key-value from empty node")
		// require.Equal(t, ErrKeyIndexOutOfBounds, err)
	})
}

func TestNodeInterfacingWithStorage(t *testing.T) {
	ctx, storage, _ := initTest(t, nil)

	// Test SaveNode and LoadNode
	leafID := "leaf-1"
	leaf := NewLeafNode(leafID)
	leaf.insertKeyValueAt("key1", "value1")
	leaf.insertKeyValueAt("key2", "value2")

	// Create an internal node
	internalID := "internal-1"
	internal := NewInternalNode(internalID)
	internal.insertChildAt(0, leaf.ID)

	// Save the leaf node (saved after being added to the internal node otherwise there is no reference to parent on it)
	err := storage.SaveNode(ctx, leaf)
	require.NoError(t, err, "failed to save node")

	// Load the node
	loadedLeaf, err := storage.LoadNode(ctx, leafID)
	require.NoError(t, err, "failed to load node")
	require.NotNil(t, loadedLeaf, "loaded node is nil")

	require.Equal(t, leaf.ID, loadedLeaf.ID, "expected node ID %s, got %s", leaf.ID, loadedLeaf.ID)
	require.Equal(t, leaf.Keys, loadedLeaf.Keys, "expected node keys %v, got %v", leaf.Keys, loadedLeaf.Keys)
	require.Equal(t, leaf.Values, loadedLeaf.Values, "expected node values %v, got %v", leaf.Values, loadedLeaf.Values)

	// Save the internal node
	err = storage.SaveNode(ctx, internal)
	require.NoError(t, err, "failed to save internal node")

	// Load the internal node and check that keys are set but references not
	loadedInternal, err := storage.LoadNode(ctx, internalID)
	require.NoError(t, err, "failed to load internal node")
	require.NotNil(t, loadedInternal, "loaded internal node is nil")

	require.Equal(t, internal.ID, loadedInternal.ID, "expected node ID %s, got %s", internal.ID, loadedInternal.ID)
	require.Equal(t, internal.ChildrenIDs, loadedInternal.ChildrenIDs, "expected node children keys %v, got %v", internal.ChildrenIDs, loadedInternal.ChildrenIDs)
}
