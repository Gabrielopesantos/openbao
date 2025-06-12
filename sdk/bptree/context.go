package bptree

import "context"

// Context keys for tree identification
type contextKey string

const TreeIDContextKey contextKey = "bptree-tree-id"

// WithTreeID adds a tree ID to the context
func WithTreeID(ctx context.Context, treeID string) context.Context {
	return context.WithValue(ctx, TreeIDContextKey, treeID)
}

// GetTreeID extracts the tree ID from context, returns default if not found
func GetTreeID(ctx context.Context) (string, bool) {
	treeID, ok := ctx.Value(TreeIDContextKey).(string)
	return treeID, ok
}

// GetTreeIDOrDefault extracts tree ID from context or returns a default
func GetTreeIDOrDefault(ctx context.Context, defaultTreeID string) string {
	if treeID, ok := GetTreeID(ctx); ok && treeID != "" {
		return treeID
	}
	return defaultTreeID
}
