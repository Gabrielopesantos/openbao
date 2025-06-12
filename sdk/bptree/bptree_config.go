package bptree

import "fmt"

const (
	// DefaultOrder is the default maximum number of children per B+ tree node
	DefaultOrder = 32
	// DefaultTreeID is the default identifier for a B+ tree when no specific ID is provided
	DefaultTreeID string = "default"
)

// BPlusTreeConfig holds configuration options for the B+ tree.
// This struct serves both as runtime configuration and persistent metadata.
type BPlusTreeConfig struct {
	TreeID  string `json:"tree_id"` // Tree name/identifier for multi-tree storage
	Order   int    `json:"order"`   // Maximum number of children per node
	Version int    `json:"version"` // Metadata version for future schema evolution
}

func NewDefaultBPlusTreeConfig() *BPlusTreeConfig {
	return &BPlusTreeConfig{
		TreeID:  DefaultTreeID,
		Order:   DefaultOrder,
		Version: 1,
	}
}

func NewBPlusTreeConfig(treeID string, order int) (*BPlusTreeConfig, error) {
	if order < 3 {
		return nil, fmt.Errorf("order must be at least 3, got %d", order)
	}

	return &BPlusTreeConfig{
		TreeID:  treeID,
		Order:   order,
		Version: 1, // Current metadata version
	}, nil
}

// validateConfig checks if the BPlusTreeConfig is valid
func validateConfig(config *BPlusTreeConfig) error {
	if config == nil {
		return fmt.Errorf("BPlusTreeConfig cannot be nil")
	}
	if config.Order < 3 {
		return fmt.Errorf("order must be at least 3, got %d", config.Order)
	}
	return nil
}
