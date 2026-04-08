package rzset

import "github.com/tsmask/redka/internal/core"

// SetItem represents an element with its score in a sorted set.
type SetItem struct {
	Elem  core.Value
	Score float64
}

// ScanResult represents the result of a scan operation.
type ScanResult struct {
	Cursor int
	Items  []SetItem
}
