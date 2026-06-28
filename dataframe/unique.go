package dataframe

import (
	"errors"
	"fmt"
	"strings"
)

// Unique returns the distinct values of a column in order of first appearance.
// If the column contains nulls, a single nil entry is included at the position
// of its first occurrence.
//
// This is analogous to df["col"].unique() in pandas.
//
// Example:
//
//	values, err := df.Unique("Department")
func (df *DataFrame) Unique(column string) ([]any, error) {
	if df == nil {
		return nil, errors.New("Unique: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("Unique: column '%s' not found", column)
	}

	n := series.Len()
	seen := make(map[any]bool)
	seenNull := false
	out := make([]any, 0)

	for i := 0; i < n; i++ {
		if series.IsNull(i) {
			if !seenNull {
				seenNull = true
				out = append(out, nil)
			}
			continue
		}
		val, err := series.At(i)
		if err != nil {
			return nil, fmt.Errorf("Unique: error reading row %d: %w", i, err)
		}
		if !seen[val] {
			seen[val] = true
			out = append(out, val)
		}
	}

	return out, nil
}

// NUnique returns the number of distinct non-null values in a column.
//
// This is analogous to df["col"].nunique() in pandas (nulls excluded).
//
// Example:
//
//	count, err := df.NUnique("Department")
func (df *DataFrame) NUnique(column string) (int, error) {
	if df == nil {
		return 0, errors.New("NUnique: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return 0, fmt.Errorf("NUnique: column '%s' not found", column)
	}

	n := series.Len()
	seen := make(map[any]bool)
	for i := 0; i < n; i++ {
		if series.IsNull(i) {
			continue
		}
		val, err := series.At(i)
		if err != nil {
			return 0, fmt.Errorf("NUnique: error reading row %d: %w", i, err)
		}
		seen[val] = true
	}
	return len(seen), nil
}

// Duplicated returns a boolean slice marking duplicate rows, aligned to row
// order. Rows are compared on the columns in subset (or all columns if subset is
// empty).
//
// The keep parameter controls which occurrence is treated as the unique one:
//   - "first" (default): mark all duplicates true except the first occurrence.
//   - "last": mark all duplicates true except the last occurrence.
//   - "none": mark all occurrences of duplicated rows true.
//
// This is analogous to df.duplicated(subset=..., keep=...) in pandas.
//
// Example:
//
//	mask, err := df.Duplicated([]string{"Email"}, "first")
func (df *DataFrame) Duplicated(subset []string, keep string) ([]bool, error) {
	if df == nil {
		return nil, errors.New("Duplicated: DataFrame is nil")
	}
	if keep == "" {
		keep = "first"
	}
	if keep != "first" && keep != "last" && keep != "none" {
		return nil, fmt.Errorf("Duplicated: keep must be 'first', 'last', or 'none', got '%s'", keep)
	}

	df.RLock()
	defer df.RUnlock()

	cols, err := df.resolveSubset(subset, "Duplicated")
	if err != nil {
		return nil, err
	}

	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	keys := make([]string, rowCount)
	groups := make(map[string][]int)
	for i := 0; i < rowCount; i++ {
		key, err := df.rowKey(i, cols)
		if err != nil {
			return nil, fmt.Errorf("Duplicated: %w", err)
		}
		keys[i] = key
		groups[key] = append(groups[key], i)
	}

	mask := make([]bool, rowCount)
	switch keep {
	case "first":
		for _, idxs := range groups {
			for _, idx := range idxs[1:] {
				mask[idx] = true
			}
		}
	case "last":
		for _, idxs := range groups {
			for _, idx := range idxs[:len(idxs)-1] {
				mask[idx] = true
			}
		}
	case "none":
		for _, idxs := range groups {
			if len(idxs) > 1 {
				for _, idx := range idxs {
					mask[idx] = true
				}
			}
		}
	}

	return mask, nil
}

// DropDuplicates returns a new DataFrame with duplicate rows removed, comparing
// on the columns in subset (or all columns if subset is empty).
//
// The keep parameter controls which occurrence is retained:
//   - "first" (default): keep the first occurrence.
//   - "last": keep the last occurrence.
//   - "none": drop all rows that have duplicates.
//
// Index labels of the surviving rows are preserved.
//
// This is analogous to df.drop_duplicates(subset=..., keep=...) in pandas.
//
// Example:
//
//	deduped, err := df.DropDuplicates([]string{"Email"}, "first")
func (df *DataFrame) DropDuplicates(subset []string, keep string) (*DataFrame, error) {
	mask, err := df.Duplicated(subset, keep)
	if err != nil {
		return nil, fmt.Errorf("DropDuplicates: %s", strings.TrimPrefix(err.Error(), "Duplicated: "))
	}

	keep_indices := make([]int, 0, len(mask))
	for i, dup := range mask {
		if !dup {
			keep_indices = append(keep_indices, i)
		}
	}

	return df.Slice(keep_indices)
}

// resolveSubset validates and returns the columns to consider. An empty subset
// means all columns. Must be called with the read lock held.
func (df *DataFrame) resolveSubset(subset []string, op string) ([]string, error) {
	if len(subset) == 0 {
		return df.ColumnOrder, nil
	}
	for _, c := range subset {
		if _, ok := df.Columns[c]; !ok {
			return nil, fmt.Errorf("%s: column '%s' not found", op, c)
		}
	}
	return subset, nil
}

// rowKey builds a string key for a row from the given columns, with explicit
// null handling so nulls never collide with real values. Must be called with the
// read lock held.
func (df *DataFrame) rowKey(row int, cols []string) (string, error) {
	var b strings.Builder
	for j, c := range cols {
		if j > 0 {
			b.WriteByte('\x01')
		}
		series := df.Columns[c]
		if series.IsNull(row) {
			b.WriteString("\x00NULL\x00")
			continue
		}
		val, err := series.At(row)
		if err != nil {
			return "", fmt.Errorf("error reading column '%s' row %d: %w", c, row, err)
		}
		fmt.Fprintf(&b, "%v", val)
	}
	return b.String(), nil
}
