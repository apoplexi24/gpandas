package dataframe

import (
	"errors"
	"fmt"
	"strings"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// MergeOn combines two DataFrames on one or more key columns, generalizing Merge
// to composite keys.
//
// The keys in `on` must exist in both DataFrames. The merge type `how` is one of
// InnerMerge, LeftMerge, RightMerge, or FullMerge. Rows with a null in any key
// column never match. The result contains the left columns followed by the right
// columns excluding the join keys.
//
// This is analogous to pd.merge(left, right, on=[...], how=...) in pandas.
//
// Example:
//
//	result, err := df1.MergeOn(df2, []string{"year", "region"}, dataframe.InnerMerge)
func (df *DataFrame) MergeOn(other *DataFrame, on []string, how MergeHow) (*DataFrame, error) {
	if df == nil || other == nil {
		return nil, errors.New("MergeOn: both DataFrames must be non-nil")
	}
	if len(on) == 0 {
		return nil, errors.New("MergeOn: at least one key column is required")
	}
	for _, key := range on {
		if _, ok := df.Columns[key]; !ok {
			return nil, fmt.Errorf("MergeOn: key column '%s' not found in left DataFrame", key)
		}
		if _, ok := other.Columns[key]; !ok {
			return nil, fmt.Errorf("MergeOn: key column '%s' not found in right DataFrame", key)
		}
	}

	df.RLock()
	defer df.RUnlock()
	other.RLock()
	defer other.RUnlock()

	leftRows := dfRowCount(df)
	rightRows := dfRowCount(other)

	onSet := make(map[string]bool, len(on))
	for _, k := range on {
		onSet[k] = true
	}

	// Build composite-key lookup for the right DataFrame (skipping null keys).
	rightMap := make(map[string][]int)
	for j := 0; j < rightRows; j++ {
		key, ok := compositeKey(other, on, j)
		if !ok {
			continue
		}
		rightMap[key] = append(rightMap[key], j)
	}

	// Result column layout: all left columns, then right columns except keys.
	resultColumns := make([]string, 0, len(df.ColumnOrder)+len(other.ColumnOrder))
	resultColumns = append(resultColumns, df.ColumnOrder...)
	rightExtra := make([]string, 0, len(other.ColumnOrder))
	for _, col := range other.ColumnOrder {
		if !onSet[col] {
			rightExtra = append(rightExtra, col)
			resultColumns = append(resultColumns, col)
		}
	}

	var rows []mergeRow
	switch how {
	case InnerMerge:
		rows = multiInner(df, other, on, rightExtra, rightMap, leftRows)
	case LeftMerge:
		rows = multiLeft(df, other, on, rightExtra, rightMap, leftRows)
	case RightMerge:
		rows = multiRight(df, other, on, onSet, rightExtra, leftRows, rightRows)
	case FullMerge:
		rows = multiFull(df, other, on, onSet, rightExtra, rightMap, leftRows, rightRows)
	default:
		return nil, fmt.Errorf("MergeOn: invalid merge type '%s'", how)
	}

	// Convert rows to columnar Series.
	cols := make(map[string]collection.Series, len(resultColumns))
	for colIdx, colName := range resultColumns {
		values := make([]any, len(rows))
		nulls := make([]bool, len(rows))
		for r, row := range rows {
			if colIdx < len(row.values) {
				values[r] = row.values[colIdx]
				nulls[r] = row.nulls[colIdx]
			} else {
				nulls[r] = true
			}
		}
		s, err := createTypedSeriesFromMerge(values, nulls, df, other, colName, "")
		if err != nil {
			return nil, err
		}
		cols[colName] = s
	}

	index := make([]string, len(rows))
	for i := range index {
		index[i] = fmt.Sprintf("%d", i)
	}

	return &DataFrame{Columns: cols, ColumnOrder: resultColumns, Index: index}, nil
}

// dfRowCount returns the (minimum) row count of a DataFrame.
func dfRowCount(df *DataFrame) int {
	if len(df.ColumnOrder) == 0 {
		return 0
	}
	n := df.Columns[df.ColumnOrder[0]].Len()
	for _, c := range df.ColumnOrder[1:] {
		if s := df.Columns[c]; s != nil && s.Len() < n {
			n = s.Len()
		}
	}
	return n
}

// compositeKey builds a string key from the given columns at row i. Returns
// (key, false) if any key column is null.
func compositeKey(df *DataFrame, on []string, i int) (string, bool) {
	var b strings.Builder
	for k, col := range on {
		s := df.Columns[col]
		if s.IsNull(i) {
			return "", false
		}
		if k > 0 {
			b.WriteByte('\x01')
		}
		v, _ := s.At(i)
		fmt.Fprintf(&b, "%v", v)
	}
	return b.String(), true
}

// leftValues appends all left-row values for row i to a mergeRow.
func appendLeft(row *mergeRow, df *DataFrame, i int) {
	for _, col := range df.ColumnOrder {
		s := df.Columns[col]
		row.values = append(row.values, valueAt(s, i))
		row.nulls = append(row.nulls, s.IsNull(i))
	}
}

// appendRight appends the right-row extra (non-key) values for row j.
func appendRight(row *mergeRow, other *DataFrame, rightExtra []string, j int) {
	for _, col := range rightExtra {
		s := other.Columns[col]
		row.values = append(row.values, valueAt(s, j))
		row.nulls = append(row.nulls, s.IsNull(j))
	}
}

// appendRightNulls appends nulls for all right extra columns.
func appendRightNulls(row *mergeRow, rightExtra []string) {
	for range rightExtra {
		row.values = append(row.values, nil)
		row.nulls = append(row.nulls, true)
	}
}

func valueAt(s collection.Series, i int) any {
	if s.IsNull(i) {
		return nil
	}
	v, _ := s.At(i)
	return v
}

func multiInner(df, other *DataFrame, on, rightExtra []string, rightMap map[string][]int, leftRows int) []mergeRow {
	var result []mergeRow
	for i := 0; i < leftRows; i++ {
		key, ok := compositeKey(df, on, i)
		if !ok {
			continue
		}
		for _, j := range rightMap[key] {
			row := mergeRow{}
			appendLeft(&row, df, i)
			appendRight(&row, other, rightExtra, j)
			result = append(result, row)
		}
	}
	return result
}

func multiLeft(df, other *DataFrame, on, rightExtra []string, rightMap map[string][]int, leftRows int) []mergeRow {
	var result []mergeRow
	for i := 0; i < leftRows; i++ {
		key, ok := compositeKey(df, on, i)
		var matches []int
		if ok {
			matches = rightMap[key]
		}
		if len(matches) > 0 {
			for _, j := range matches {
				row := mergeRow{}
				appendLeft(&row, df, i)
				appendRight(&row, other, rightExtra, j)
				result = append(result, row)
			}
		} else {
			row := mergeRow{}
			appendLeft(&row, df, i)
			appendRightNulls(&row, rightExtra)
			result = append(result, row)
		}
	}
	return result
}

func multiRight(df, other *DataFrame, on []string, onSet map[string]bool, rightExtra []string, leftRows, rightRows int) []mergeRow {
	// Build left lookup.
	leftMap := make(map[string][]int)
	for i := 0; i < leftRows; i++ {
		if key, ok := compositeKey(df, on, i); ok {
			leftMap[key] = append(leftMap[key], i)
		}
	}

	var result []mergeRow
	for j := 0; j < rightRows; j++ {
		key, ok := compositeKey(other, on, j)
		var matches []int
		if ok {
			matches = leftMap[key]
		}
		if len(matches) > 0 {
			for _, i := range matches {
				row := mergeRow{}
				appendLeft(&row, df, i)
				appendRight(&row, other, rightExtra, j)
				result = append(result, row)
			}
		} else {
			// No left match: left columns null except keys, which take right values.
			row := mergeRow{}
			for _, col := range df.ColumnOrder {
				if onSet[col] {
					rs := other.Columns[col]
					row.values = append(row.values, valueAt(rs, j))
					row.nulls = append(row.nulls, rs.IsNull(j))
				} else {
					row.values = append(row.values, nil)
					row.nulls = append(row.nulls, true)
				}
			}
			appendRight(&row, other, rightExtra, j)
			result = append(result, row)
		}
	}
	return result
}

func multiFull(df, other *DataFrame, on []string, onSet map[string]bool, rightExtra []string, rightMap map[string][]int, leftRows, rightRows int) []mergeRow {
	result := multiLeft(df, other, on, rightExtra, rightMap, leftRows)

	// Track processed right keys (those that matched at least one left row).
	processed := make(map[string]bool)
	for i := 0; i < leftRows; i++ {
		if key, ok := compositeKey(df, on, i); ok {
			processed[key] = true
		}
	}

	for j := 0; j < rightRows; j++ {
		key, ok := compositeKey(other, on, j)
		if ok && processed[key] {
			continue
		}
		row := mergeRow{}
		for _, col := range df.ColumnOrder {
			if onSet[col] {
				rs := other.Columns[col]
				row.values = append(row.values, valueAt(rs, j))
				row.nulls = append(row.nulls, rs.IsNull(j))
			} else {
				row.values = append(row.values, nil)
				row.nulls = append(row.nulls, true)
			}
		}
		appendRight(&row, other, rightExtra, j)
		result = append(result, row)
	}
	return result
}
