package bptree

import "github.com/hashicorp/go-uuid"

// genUUID generates a UUID
func genUUID() string {
	aUuid, err := uuid.GenerateUUID()
	if err != nil {
		panic(err)
	}
	return aUuid
}

// calculatePrefixLimit calculates the smallest string that's lexicographically larger
// than any string that could start with the given prefix
func calculatePrefixLimit(prefix string) string {
	if prefix == "" {
		return ""
	}

	// Convert to rune slice to handle Unicode properly
	runes := []rune(prefix)

	// Try to increment the last character
	for i := len(runes) - 1; i >= 0; i-- {
		if runes[i] < '\U0010FFFF' { // Not the maximum Unicode character
			runes[i]++
			// Truncate everything after this position
			return string(runes[:i+1])
		}
		// If we can't increment this character, try the previous one
	}

	// If we can't increment any character (very rare case),
	// return a string that's definitely larger
	return prefix + "\U0010FFFF"
}
