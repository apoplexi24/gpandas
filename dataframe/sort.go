package dataframe

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// NaPosition specifies where null/NA values should be placed during sorting.
type NaPosition string

const (
	// NaLast places null values at the end of the sorted result (default).
	NaLast NaPosition = "last"
	// NaFirst places null values at the beginning of the sorted result.
	NaFirst NaPosition = "first"
)

// SortOptions configures the SortValues operation.
type SortOptions struct {
	// By specifies the column names to sort by.
	// Required: at least one column must be specified.
	By []string

	// Ascending specifies the sort order for each column in By.
	// If a single value is provided, it applies to all columns.
	// If multiple values are provided, they must match the length of By.
	// Default: all ascending (true).
	Ascending []bool

	// NaPosition specifies where null values should appear.
	// "last" (default): nulls at the end.
	// "first": nulls at the beginning.
	NaPosition NaPosition

	// Inplace specifies whether to modify the DataFrame in place.
	// If true, modifies in place and returns nil.
	// If false (default), returns a new sorted DataFrame.
	Inplace bool

	// IgnoreIndex specifies whether to reset the index after sorting.
	// If true, the resulting index will be 0, 1, 2, ...
	// If false (default), the original index labels are preserved (reordered).
	IgnoreIndex bool
}

// SortValues sorts the DataFrame by the values in one or more columns.
//
// This method is analogous to pandas' df.sort_values(). It supports multi-column
// sorting with independent ascending/descending order per column, null handling
// (nulls first or last), in-place modification, and index reset.
//
// Parameters:
//   - opts: SortOptions struct configuring the sort operation
//
// Returns:
//   - *DataFrame: a new sorted DataFrame (nil if Inplace=true)
//   - error: nil if successful, otherwise an error describing what went wrong
//
// Supported column types for comparison:
//   - float64, int64, string, bool (true > false)
//
// Null values are handled according to NaPosition:
//   - NaLast (default): nulls are placed after all non-null values
//   - NaFirst: nulls are placed before all non-null values
//
// Example:
//
//	// Sort by single column ascending
//	sorted, err := df.SortValues(dataframe.SortOptions{By: []string{"Age"}})
//
//	// Sort by multiple columns with mixed order
//	sorted, err := df.SortValues(dataframe.SortOptions{
//	    By:        []string{"Department", "Salary"},
//	    Ascending: []bool{true, false},
//	})
//
//	// Sort with nulls first
//	sorted, err := df.SortValues(dataframe.SortOptions{
//	    By:         []string{"Score"},
//	    NaPosition: dataframe.NaFirst,
//	})
//
//	// Sort in place with index reset
//	_, err := df.SortValues(dataframe.SortOptions{
//	    By:          []string{"Name"},
//	    Inplace:     true,
//	    IgnoreIndex: true,
//	})
func (df *DataFrame) SortValues(opts SortOptions) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("SortValues: DataFrame is nil")
	}

	// Validate By
	if len(opts.By) == 0 {
		return nil, errors.New("SortValues: 'By' must contain at least one column name")
	}

	df.RLock()

	// Validate columns exist
	for _, col := range opts.By {
		if _, ok := df.Columns[col]; !ok {
			df.RUnlock()
			return nil, fmt.Errorf("SortValues: column '%s' not found in DataFrame", col)
		}
	}

	// Resolve ascending flags
	ascending := opts.Ascending
	if len(ascending) == 0 {
		// Default: all ascending
		ascending = make([]bool, len(opts.By))
		for i := range ascending {
			ascending[i] = true
		}
	} else if len(ascending) == 1 && len(opts.By) > 1 {
		// Single value applies to all columns
		val := ascending[0]
		ascending = make([]bool, len(opts.By))
		for i := range ascending {
			ascending[i] = val
		}
	} else if len(ascending) != len(opts.By) {
		df.RUnlock()
		return nil, fmt.Errorf("SortValues: length of 'Ascending' (%d) must match length of 'By' (%d) or be 1", len(opts.Ascending), len(opts.By))
	}

	// Default NaPosition
	naPosition := opts.NaPosition
	if naPosition == "" {
		naPosition = NaLast
	}
	if naPosition != NaLast && naPosition != NaFirst {
		df.RUnlock()
		return nil, fmt.Errorf("SortValues: NaPosition must be 'last' or 'first', got '%s'", naPosition)
	}

	// Build row indices
	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	indices := make([]int, rowCount)
	for i := range indices {
		indices[i] = i
	}

	// Pre-extract values and masks for the sort columns to avoid repeated interface calls
	type sortColumn struct {
		values []any
		mask   []bool
		asc    bool
	}
	sortCols := make([]sortColumn, len(opts.By))
	for i, colName := range opts.By {
		series := df.Columns[colName]
		sortCols[i] = sortColumn{
			values: series.ValuesCopy(),
			mask:   series.MaskCopy(),
			asc:    ascending[i],
		}
	}

	df.RUnlock()

	// Sort indices
	var sortErr error
	sort.SliceStable(indices, func(a, b int) bool {
		if sortErr != nil {
			return false
		}
		for _, sc := range sortCols {
			aNull := sc.mask[indices[a]]
			bNull := sc.mask[indices[b]]

			// Handle null values
			if aNull && bNull {
				continue // Both null, check next column
			}
			if aNull {
				// a is null
				return naPosition == NaFirst // null first → a before b
			}
			if bNull {
				// b is null
				return naPosition != NaFirst // null first → b before a, so a NOT before b is false → return naPosition != NaFirst
			}

			// Compare non-null values
			cmp, err := compareValues(sc.values[indices[a]], sc.values[indices[b]])
			if err != nil {
				sortErr = err
				return false
			}

			if cmp == 0 {
				continue // Equal, check next column
			}

			if sc.asc {
				return cmp < 0
			}
			return cmp > 0
		}
		// All keys equal: preserve original order (stable sort)
		return false
	})

	if sortErr != nil {
		return nil, fmt.Errorf("SortValues: comparison error: %w", sortErr)
	}

	// Reorder using the sorted indices
	return df.reorderByIndices(indices, opts.Inplace, opts.IgnoreIndex)
}

// SortIndex sorts the DataFrame by its index labels.
//
// This method is analogous to pandas' df.sort_index(). It sorts rows by their
// index labels in lexicographic order.
//
// Parameters:
//   - ascending: if true, sort in ascending order; if false, descending
//
// Returns:
//   - *DataFrame: a new sorted DataFrame
//   - error: nil if successful, otherwise an error
//
// Example:
//
//	sorted, err := df.SortIndex(true)  // ascending
//	sorted, err := df.SortIndex(false) // descending
func (df *DataFrame) SortIndex(ascending bool) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("SortIndex: DataFrame is nil")
	}

	df.RLock()

	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	if len(df.Index) != rowCount {
		df.RUnlock()
		return nil, errors.New("SortIndex: index length does not match row count")
	}

	// Copy index labels for sorting
	indexCopy := make([]string, len(df.Index))
	copy(indexCopy, df.Index)

	df.RUnlock()

	indices := make([]int, rowCount)
	for i := range indices {
		indices[i] = i
	}

	sort.SliceStable(indices, func(a, b int) bool {
		if ascending {
			return indexCopy[indices[a]] < indexCopy[indices[b]]
		}
		return indexCopy[indices[a]] > indexCopy[indices[b]]
	})

	// Reorder, never ignore index for SortIndex, never inplace
	return df.reorderByIndices(indices, false, false)
}

// reorderByIndices creates a new DataFrame (or modifies in place) with rows
// reordered according to the given index permutation.
func (df *DataFrame) reorderByIndices(indices []int, inplace bool, ignoreIndex bool) (*DataFrame, error) {
	df.RLock()

	newCols := make(map[string]collection.Series, len(df.Columns))
	for name, series := range df.Columns {
		newSeries := collection.NewSeriesOfTypeWithSize(series.DType(), len(indices))
		for newIdx, oldIdx := range indices {
			if series.IsNull(oldIdx) {
				newSeries.SetNull(newIdx)
			} else {
				val, _ := series.At(oldIdx)
				newSeries.Set(newIdx, val)
			}
		}
		newCols[name] = newSeries
	}

	// Reorder index
	newIndex := make([]string, len(indices))
	if ignoreIndex {
		for i := range newIndex {
			newIndex[i] = fmt.Sprintf("%d", i)
		}
	} else {
		for i, idx := range indices {
			if idx >= 0 && idx < len(df.Index) {
				newIndex[i] = df.Index[idx]
			} else {
				newIndex[i] = fmt.Sprintf("%d", i)
			}
		}
	}

	columnOrder := append([]string(nil), df.ColumnOrder...)

	df.RUnlock()

	if inplace {
		df.Lock()
		df.Columns = newCols
		df.Index = newIndex
		df.Unlock()
		return nil, nil
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: columnOrder,
		Index:       newIndex,
	}, nil
}

// compareValues compares two non-nil values and returns:
//
//	-1 if a < b,  0 if a == b,  +1 if a > b
func compareValues(a, b any) (int, error) {
	// Fast path: same concrete type
	switch av := a.(type) {
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return 0, fmt.Errorf("type mismatch: cannot compare %T and %T", a, b)
		}
		return compareOrdered(av, bv), nil

	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0, fmt.Errorf("type mismatch: cannot compare %T and %T", a, b)
		}
		return compareOrdered(av, bv), nil

	case int:
		bv, ok := b.(int)
		if !ok {
			return 0, fmt.Errorf("type mismatch: cannot compare %T and %T", a, b)
		}
		return compareOrdered(av, bv), nil

	case string:
		bv, ok := b.(string)
		if !ok {
			return 0, fmt.Errorf("type mismatch: cannot compare %T and %T", a, b)
		}
		return strings.Compare(av, bv), nil

	case bool:
		bv, ok := b.(bool)
		if !ok {
			return 0, fmt.Errorf("type mismatch: cannot compare %T and %T", a, b)
		}
		return compareBool(av, bv), nil
	}

	// Fallback: compare by string representation
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if reflect.TypeOf(a) == reflect.TypeOf(b) {
		return strings.Compare(aStr, bStr), nil
	}
	return 0, fmt.Errorf("unsupported types for comparison: %T and %T", a, b)
}

// compareOrdered compares two ordered (numeric) values.
func compareOrdered[T ~int | ~int64 | ~float64](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// compareBool compares two booleans. false < true.
func compareBool(a, b bool) int {
	if a == b {
		return 0
	}
	if !a { // a is false, b is true
		return -1
	}
	return 1
}
