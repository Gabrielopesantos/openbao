package bptree

import (
	"context"
	"testing"

	"github.com/openbao/openbao/sdk/v2/logical"
	"github.com/stretchr/testify/require"
)

// TestNextIDLinking tests that the NextID field is properly set during leaf operations
func TestNextIDLinking(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_nextid", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	// Use a small order to force splits
	tree, err := NewBPlusTree(ctx, 2, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	t.Run("SingleLeafNode", func(t *testing.T) {
		// Insert into root leaf - should have empty NextID initially
		err = tree.Insert(ctx, storage, "key1", "value1")
		require.NoError(t, err, "Failed to insert key1")

		root, err := tree.getRoot(ctx, storage)
		require.NoError(t, err, "Failed to get root")
		require.True(t, root.IsLeaf, "Root should be a leaf")
		require.Empty(t, root.NextID, "Single leaf should have empty NextID")
	})

	t.Run("LeafSplitCreatesTwoLinkedLeaves", func(t *testing.T) {
		// Insert more keys to force a leaf split
		err = tree.Insert(ctx, storage, "key2", "value2")
		require.NoError(t, err, "Failed to insert key2")

		// This should cause a leaf split (order=2, so max 2 keys per leaf)
		err = tree.Insert(ctx, storage, "key3", "value3")
		require.NoError(t, err, "Failed to insert key3")

		// Find the leftmost leaf
		leftmost, err := tree.findLeftmostLeaf(ctx, storage)
		require.NoError(t, err, "Failed to find leftmost leaf")

		// Verify the leaf has a NextID
		require.NotEmpty(t, leftmost.NextID, "Leftmost leaf should have NextID after split")

		// Load the next leaf
		rightLeaf, err := storage.LoadNode(ctx, leftmost.NextID)
		require.NoError(t, err, "Failed to load right leaf")
		require.True(t, rightLeaf.IsLeaf, "Next node should be a leaf")

		// The rightmost leaf should have empty NextID
		require.Empty(t, rightLeaf.NextID, "Rightmost leaf should have empty NextID")

		// Verify keys are properly distributed
		allKeys := append(leftmost.Keys, rightLeaf.Keys...)
		require.ElementsMatch(t, []string{"key1", "key2", "key3"}, allKeys, "All keys should be present across leaves")
	})

	t.Run("SequentialTraversalWorks", func(t *testing.T) {
		// Insert more keys to create multiple leaf splits
		for i := 4; i <= 10; i++ {
			err = tree.Insert(ctx, storage, "key"+string(rune('0'+i)), "value"+string(rune('0'+i)))
			require.NoError(t, err, "Failed to insert key%d", i)
		}

		// Traverse through all leaves using NextID
		var allKeys []string
		current, err := tree.findLeftmostLeaf(ctx, storage)
		require.NoError(t, err, "Failed to find leftmost leaf")

		for current != nil {
			allKeys = append(allKeys, current.Keys...)
			if current.NextID == "" {
				break
			}
			current, err = storage.LoadNode(ctx, current.NextID)
			require.NoError(t, err, "Failed to load next leaf")
		}

		// Verify all keys are present and in order
		expectedKeys := []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key:"}
		require.Equal(t, expectedKeys, allKeys, "Keys should be in sorted order through leaf traversal")
	})

	t.Run("FindRightmostLeaf", func(t *testing.T) {
		rightmost, err := tree.findRightmostLeaf(ctx, storage)
		require.NoError(t, err, "Failed to find rightmost leaf")
		require.Empty(t, rightmost.NextID, "Rightmost leaf should have empty NextID")

		// Verify it contains the largest keys
		require.Contains(t, rightmost.Keys, "key:", "Rightmost leaf should contain largest key")
	})
}

// TestNextIDAfterMultipleSplits tests NextID linking behavior with multiple splits
func TestNextIDAfterMultipleSplits(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_multi_split", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	// Very small order to force many splits
	tree, err := NewBPlusTree(ctx, 3, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert many keys to force multiple splits
	numKeys := 20
	for i := 1; i <= numKeys; i++ {
		key := "key" + string(rune('A'+i-1)) // keyA, keyB, keyC, etc.
		value := "value" + string(rune('A'+i-1))
		err = tree.Insert(ctx, storage, key, value)
		require.NoError(t, err, "Failed to insert %s", key)
	}

	// Traverse all leaves and verify NextID chain is intact
	var leafCount int
	var totalKeys int
	current, err := tree.findLeftmostLeaf(ctx, storage)
	require.NoError(t, err, "Failed to find leftmost leaf")

	for current != nil {
		leafCount++
		totalKeys += len(current.Keys)

		if current.NextID == "" {
			// This should be the rightmost leaf
			rightmost, err := tree.findRightmostLeaf(ctx, storage)
			require.NoError(t, err, "Failed to find rightmost leaf")
			require.Equal(t, current.ID, rightmost.ID, "Last leaf in chain should be the rightmost")
			break
		}

		// Load next leaf
		next, err := storage.LoadNode(ctx, current.NextID)
		require.NoError(t, err, "Failed to load next leaf")
		require.True(t, next.IsLeaf, "NextID should point to a leaf")

		// Verify ordering: all keys in current should be <= all keys in next
		if len(current.Keys) > 0 && len(next.Keys) > 0 {
			lastKeyInCurrent := current.Keys[len(current.Keys)-1]
			firstKeyInNext := next.Keys[0]
			require.True(t, lastKeyInCurrent <= firstKeyInNext,
				"Keys should be ordered across leaf boundaries: %s <= %s", lastKeyInCurrent, firstKeyInNext)
		}

		current = next
	}

	require.Equal(t, numKeys, totalKeys, "All keys should be present in leaf chain")
	require.GreaterOrEqual(t, leafCount, 2, "Should have created multiple leaves")
}

// TestNextIDPrefixSearch simulates using NextID for prefix search functionality
func TestNextIDPrefixSearch(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_prefix", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	tree, err := NewBPlusTree(ctx, 4, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert keys with various prefixes
	keys := []string{
		"app/config/db",
		"app/config/redis",
		"app/secrets/api_key",
		"app/secrets/jwt_secret",
		"auth/users/alice",
		"auth/users/bob",
		"auth/roles/admin",
		"system/health",
		"system/version",
	}

	for _, key := range keys {
		err = tree.Insert(ctx, storage, key, "value_"+key)
		require.NoError(t, err, "Failed to insert %s", key)
	}

	// Simulate prefix search for "app/" using NextID traversal
	prefix := "app/"
	var prefixMatches []string

	// Find the first leaf that might contain our prefix
	startLeaf, err := tree.findLeafNode(ctx, storage, prefix)
	require.NoError(t, err, "Failed to find leaf for prefix")

	// Traverse leaves using NextID to find all matching keys
	current := startLeaf
	for current != nil {
		for _, key := range current.Keys {
			if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
				prefixMatches = append(prefixMatches, key)
			} else if len(prefixMatches) > 0 && key > prefix {
				// We've passed all possible matches since keys are sorted
				goto done
			}
		}

		if current.NextID == "" {
			break
		}
		current, err = storage.LoadNode(ctx, current.NextID)
		require.NoError(t, err, "Failed to load next leaf")
	}

done:
	expectedMatches := []string{
		"app/config/db",
		"app/config/redis",
		"app/secrets/api_key",
		"app/secrets/jwt_secret",
	}
	require.Equal(t, expectedMatches, prefixMatches, "Should find all keys with 'app/' prefix")
}

// TestFindPreviousLeaf tests the helper function for finding previous leaf
func TestFindPreviousLeaf(t *testing.T) {
	ctx := context.Background()
	s := &logical.InmemStorage{}
	storage, err := NewNodeStorage("bptree_previous", s, nil, 100)
	require.NoError(t, err, "Failed to create storage adapter")

	tree, err := NewBPlusTree(ctx, 3, storage)
	require.NoError(t, err, "Failed to create B+ tree")

	// Insert enough keys to create multiple leaves
	for i := 1; i <= 10; i++ {
		key := "key" + string(rune('0'+i))
		err = tree.Insert(ctx, storage, key, "value"+string(rune('0'+i)))
		require.NoError(t, err, "Failed to insert %s", key)
	}

	// Get all leaves in order
	var leaves []*Node
	current, err := tree.findLeftmostLeaf(ctx, storage)
	require.NoError(t, err, "Failed to find leftmost leaf")

	for current != nil {
		leaves = append(leaves, current)
		if current.NextID == "" {
			break
		}
		current, err = storage.LoadNode(ctx, current.NextID)
		require.NoError(t, err, "Failed to load next leaf")
	}

	require.GreaterOrEqual(t, len(leaves), 2, "Should have at least 2 leaves")

	// Test findPreviousLeaf for each leaf (except the first)
	for i := 1; i < len(leaves); i++ {
		targetLeaf := leaves[i]
		expectedPrevious := leaves[i-1]

		foundPrevious, err := tree.findPreviousLeaf(ctx, storage, targetLeaf.ID)
		require.NoError(t, err, "Failed to find previous leaf")
		require.NotNil(t, foundPrevious, "Should find previous leaf")
		require.Equal(t, expectedPrevious.ID, foundPrevious.ID, "Should find correct previous leaf")
	}

	// Test that the first leaf has no previous
	firstLeaf := leaves[0]
	previous, err := tree.findPreviousLeaf(ctx, storage, firstLeaf.ID)
	require.NoError(t, err, "Should not error when finding previous of first leaf")
	require.Nil(t, previous, "First leaf should have no previous")
}
