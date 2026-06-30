package dataframe

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// Sample returns a new DataFrame containing n rows selected at random without
// replacement. The selected rows appear in random order. Index labels of the
// selected rows are preserved.
//
// An optional seed makes the selection deterministic; without it, a time-based
// seed is used.
//
// This is analogous to df.sample(n=...) in pandas.
//
// Example:
//
//	s, err := df.Sample(100)        // 100 random rows
//	s, err := df.Sample(100, 42)    // deterministic with seed 42
func (df *DataFrame) Sample(n int, seed ...int64) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("Sample: DataFrame is nil")
	}

	df.RLock()

	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	df.RUnlock()

	if n < 0 || n > rowCount {
		return nil, fmt.Errorf("Sample: n (%d) must be in range [0, %d]", n, rowCount)
	}

	var src rand.Source
	if len(seed) > 0 {
		src = rand.NewSource(seed[0])
	} else {
		src = rand.NewSource(time.Now().UnixNano())
	}
	rng := rand.New(src)

	perm := rng.Perm(rowCount)
	indices := perm[:n]

	return df.Slice(indices)
}
