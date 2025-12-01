package dataframe

import (
	"errors"
	"fmt"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// MergeHow represents the type of merge operation
type MergeHow string

const (
	LeftMerge  MergeHow = "left"
	RightMerge MergeHow = "right"
	InnerMerge MergeHow = "inner"
	FullMerge  MergeHow = "full"
)

// mergeRow represents a single row in the merge result with null tracking
type mergeRow struct {
	values []any
	nulls  []bool
}

// Merge combines two DataFrames based on a specified column and merge type.
//
// Parameters:
//
// other: The DataFrame to merge with the current DataFrame.
//
// on: The name of the column to merge on. This column must exist in both DataFrames.
//
// how: The type of merge to perform. It can be one of the following:
//   - LeftMerge: Keep all rows from the left DataFrame and match rows from the right DataFrame.
//   - RightMerge: Keep all rows from the right DataFrame and match rows from the left DataFrame.
//   - InnerMerge: Keep only rows that have matching values in both DataFrames.
//   - FullMerge: Keep all rows from both DataFrames, filling in missing values with null.
//
// Returns:
//   - A new DataFrame containing the merged data with proper null handling.
//   - An error if the merge operation fails, such as if the specified column does not exist in one or both DataFrames.
//
// Note: Null values in the merge key column are handled specially - they never match with other null values.
//
// Examples:
//
//	// Create two sample DataFrames
//	df1 := &DataFrame{
//		Columns: []string{"ID", "Name"},
//		Data: [][]any{
//			{1, "Alice"},
//			{2, "Bob"},
//			{3, "Charlie"},
//		},
//	}
//
//	df2 := &DataFrame{
//		Columns: []string{"ID", "Age"},
//		Data: [][]any{
//			{1, 25},
//			{2, 30},
//			{4, 35},
//		},
//	}
//
//	// Inner merge example (only matching IDs)
//	result, err := df1.Merge(df2, "ID", InnerMerge)
//	// Result:
//	// ID | Name    | Age
//	// 1  | Alice   | 25
//	// 2  | Bob     | 30
//
//	// Left merge example (all rows from df1)
//	result, err := df1.Merge(df2, "ID", LeftMerge)
//	// Result:
//	// ID | Name    | Age
//	// 1  | Alice   | 25
//	// 2  | Bob     | 30
//	// 3  | Charlie | null
//
//	// Full merge example (all rows from both)
//	result, err := df1.Merge(df2, "ID", FullMerge)
//	// Result:
//	// ID | Name    | Age
//	// 1  | Alice   | 25
//	// 2  | Bob     | 30
//	// 3  | Charlie | null
//	// 4  | null    | 35
func (df *DataFrame) Merge(other *DataFrame, on string, how MergeHow) (*DataFrame, error) {
	if df == nil || other == nil {
		return nil, errors.New("both DataFrames must be non-nil")
	}

	// Validate 'on' column exists in both DataFrames
	if _, ok := df.Columns[on]; !ok {
		return nil, fmt.Errorf("column '%s' not found in left DataFrame", on)
	}
	if _, ok := other.Columns[on]; !ok {
		return nil, fmt.Errorf("column '%s' not found in right DataFrame", on)
	}

	// Determine row counts (use series lengths)
	leftRows := 0
	if len(df.ColumnOrder) > 0 && df.Columns[df.ColumnOrder[0]] != nil {
		leftRows = df.Columns[df.ColumnOrder[0]].Len()
		for _, c := range df.ColumnOrder[1:] {
			if s := df.Columns[c]; s != nil && s.Len() < leftRows {
				leftRows = s.Len()
			}
		}
	}
	rightRows := 0
	if len(other.ColumnOrder) > 0 && other.Columns[other.ColumnOrder[0]] != nil {
		rightRows = other.Columns[other.ColumnOrder[0]].Len()
		for _, c := range other.ColumnOrder[1:] {
			if s := other.Columns[c]; s != nil && s.Len() < rightRows {
				rightRows = s.Len()
			}
		}
	}

	// Build lookup for right DataFrame on key column (excluding nulls)
	df2Map := make(map[any][]int)
	rightKeySeries := other.Columns[on]
	for i := 0; i < rightRows; i++ {
		if rightKeySeries.IsNull(i) {
			continue // Null keys don't participate in matching
		}
		v, _ := rightKeySeries.At(i)
		df2Map[v] = append(df2Map[v], i)
	}

	// Prepare result columns
	resultColumns := make([]string, 0, len(df.ColumnOrder)+len(other.ColumnOrder))
	resultColumns = append(resultColumns, df.ColumnOrder...)
	for _, col := range other.ColumnOrder {
		if col != on {
			resultColumns = append(resultColumns, col)
		}
	}

	// Prepare result rows based on merge type
	var resultRows []mergeRow
	switch how {
	case InnerMerge:
		resultRows = performInnerMerge(df, other, on, df2Map, leftRows, rightRows)
	case LeftMerge:
		resultRows = performLeftMerge(df, other, on, df2Map, leftRows, rightRows)
	case RightMerge:
		resultRows = performRightMerge(df, other, on, df2Map, leftRows, rightRows)
	case FullMerge:
		resultRows = performFullMerge(df, other, on, df2Map, leftRows, rightRows)
	default:
		return nil, fmt.Errorf("invalid merge type: %s", how)
	}

	// Convert row-wise to columnar Series with proper null handling
	cols := make(map[string]collection.Series, len(resultColumns))

	for colIdx, colName := range resultColumns {
		// Collect values and nulls for this column
		values := make([]any, len(resultRows))
		nulls := make([]bool, len(resultRows))

		for rowIdx, row := range resultRows {
			if colIdx < len(row.values) {
				values[rowIdx] = row.values[colIdx]
				nulls[rowIdx] = row.nulls[colIdx]
			} else {
				nulls[rowIdx] = true
			}
		}

		// Create appropriate typed series
		s, err := createTypedSeriesFromMerge(values, nulls, df, other, colName, on)
		if err != nil {
			return nil, err
		}
		cols[colName] = s
	}

	// Create default index for result
	index := make([]string, len(resultRows))
	for i := 0; i < len(resultRows); i++ {
		index[i] = fmt.Sprintf("%d", i)
	}

	return &DataFrame{Columns: cols, ColumnOrder: resultColumns, Index: index}, nil
}

// createTypedSeriesFromMerge creates a typed series for merge results
func createTypedSeriesFromMerge(values []any, nulls []bool, df1, df2 *DataFrame, colName, on string) (collection.Series, error) {
	// Determine source series for type inference
	var sourceSeries collection.Series
	if s, ok := df1.Columns[colName]; ok {
		sourceSeries = s
	} else if s, ok := df2.Columns[colName]; ok {
		sourceSeries = s
	}

	if sourceSeries == nil {
		return collection.NewAnySeriesFromData(values, nulls)
	}

	dtype := sourceSeries.DType()
	if dtype == nil {
		return collection.NewAnySeriesFromData(values, nulls)
	}

	// Create typed series based on source dtype
	return collection.NewSeriesWithData(dtype, values)
}

// performInnerMerge combines two DataFrames, returning only matching rows
func performInnerMerge(df1, df2 *DataFrame, on string, df2Map map[any][]int, leftRows, rightRows int) []mergeRow {
	if df1 == nil || df2 == nil {
		return nil
	}

	var result []mergeRow
	leftKeySeries := df1.Columns[on]
	totalCols := len(df1.ColumnOrder) + len(df2.ColumnOrder) - 1

	for i := 0; i < leftRows; i++ {
		// Skip null keys - they never match
		if leftKeySeries.IsNull(i) {
			continue
		}

		key, _ := leftKeySeries.At(i)
		if matches, ok := df2Map[key]; ok {
			for _, matchIdx := range matches {
				row := mergeRow{
					values: make([]any, 0, totalCols),
					nulls:  make([]bool, 0, totalCols),
				}

				// Add left row values
				for _, col := range df1.ColumnOrder {
					series := df1.Columns[col]
					isNull := series.IsNull(i)
					val, _ := series.At(i)
					row.values = append(row.values, val)
					row.nulls = append(row.nulls, isNull)
				}

				// Add right row values (excluding key)
				for _, col := range df2.ColumnOrder {
					if col == on {
						continue
					}
					series := df2.Columns[col]
					isNull := series.IsNull(matchIdx)
					val, _ := series.At(matchIdx)
					row.values = append(row.values, val)
					row.nulls = append(row.nulls, isNull)
				}

				result = append(result, row)
			}
		}
	}
	return result
}

// performLeftMerge keeps all rows from left, matching with right where possible
func performLeftMerge(df1, df2 *DataFrame, on string, df2Map map[any][]int, leftRows, rightRows int) []mergeRow {
	if df1 == nil || df2 == nil {
		return nil
	}

	var result []mergeRow
	leftKeySeries := df1.Columns[on]
	totalCols := len(df1.ColumnOrder) + len(df2.ColumnOrder) - 1
	rightColCount := len(df2.ColumnOrder) - 1 // Excluding the key column

	for i := 0; i < leftRows; i++ {
		isNullKey := leftKeySeries.IsNull(i)
		var key any
		if !isNullKey {
			key, _ = leftKeySeries.At(i)
		}

		// Check for matches (null keys never match)
		var matches []int
		if !isNullKey {
			matches = df2Map[key]
		}

		if len(matches) > 0 {
			for _, matchIdx := range matches {
				row := mergeRow{
					values: make([]any, 0, totalCols),
					nulls:  make([]bool, 0, totalCols),
				}

				// Add left row values
				for _, col := range df1.ColumnOrder {
					series := df1.Columns[col]
					isNull := series.IsNull(i)
					val, _ := series.At(i)
					row.values = append(row.values, val)
					row.nulls = append(row.nulls, isNull)
				}

				// Add right row values (excluding key)
				for _, col := range df2.ColumnOrder {
					if col == on {
						continue
					}
					series := df2.Columns[col]
					isNull := series.IsNull(matchIdx)
					val, _ := series.At(matchIdx)
					row.values = append(row.values, val)
					row.nulls = append(row.nulls, isNull)
				}

				result = append(result, row)
			}
		} else {
			// No match - add left values with nulls for right columns
			row := mergeRow{
				values: make([]any, 0, totalCols),
				nulls:  make([]bool, 0, totalCols),
			}

			// Add left row values
			for _, col := range df1.ColumnOrder {
				series := df1.Columns[col]
				isNull := series.IsNull(i)
				val, _ := series.At(i)
				row.values = append(row.values, val)
				row.nulls = append(row.nulls, isNull)
			}

			// Add nulls for right columns
			for j := 0; j < rightColCount; j++ {
				row.values = append(row.values, nil)
				row.nulls = append(row.nulls, true)
			}

			result = append(result, row)
		}
	}
	return result
}

// performRightMerge keeps all rows from right, matching with left where possible
func performRightMerge(df1, df2 *DataFrame, on string, _ map[any][]int, leftRows, rightRows int) []mergeRow {
	if df1 == nil || df2 == nil {
		return nil
	}

	// Create reverse mapping for df1 (excluding null keys)
	df1Map := make(map[any][]int)
	leftKeySeries := df1.Columns[on]
	for i := 0; i < leftRows; i++ {
		if leftKeySeries.IsNull(i) {
			continue
		}
		key, _ := leftKeySeries.At(i)
		df1Map[key] = append(df1Map[key], i)
	}

	var result []mergeRow
	rightKeySeries := df2.Columns[on]
	totalCols := len(df1.ColumnOrder) + len(df2.ColumnOrder) - 1

	// Find the position of the key column in left DataFrame
	leftKeyIdx := -1
	for idx, name := range df1.ColumnOrder {
		if name == on {
			leftKeyIdx = idx
			break
		}
	}

	for j := 0; j < rightRows; j++ {
		isNullKey := rightKeySeries.IsNull(j)
		var key any
		if !isNullKey {
			key, _ = rightKeySeries.At(j)
		}

		// Check for matches (null keys never match)
		var matches []int
		if !isNullKey {
			matches = df1Map[key]
		}

		if len(matches) > 0 {
			for _, matchIdx := range matches {
				row := mergeRow{
					values: make([]any, 0, totalCols),
					nulls:  make([]bool, 0, totalCols),
				}

				// Add left row values
				for _, col := range df1.ColumnOrder {
					series := df1.Columns[col]
					isNull := series.IsNull(matchIdx)
					val, _ := series.At(matchIdx)
					row.values = append(row.values, val)
					row.nulls = append(row.nulls, isNull)
				}

				// Add right row values (excluding key)
				for _, col := range df2.ColumnOrder {
					if col == on {
						continue
					}
					series := df2.Columns[col]
					isNull := series.IsNull(j)
					val, _ := series.At(j)
					row.values = append(row.values, val)
					row.nulls = append(row.nulls, isNull)
				}

				result = append(result, row)
			}
		} else {
			// No match - add nulls for left columns (except key column which gets right's key)
			row := mergeRow{
				values: make([]any, 0, totalCols),
				nulls:  make([]bool, 0, totalCols),
			}

			// Add nulls for left columns (but use right key value for the key column)
			for idx := range df1.ColumnOrder {
				if idx == leftKeyIdx {
					row.values = append(row.values, key)
					row.nulls = append(row.nulls, isNullKey)
				} else {
					row.values = append(row.values, nil)
					row.nulls = append(row.nulls, true)
				}
			}

			// Add right row values (excluding key)
			for _, col := range df2.ColumnOrder {
				if col == on {
					continue
				}
				series := df2.Columns[col]
				isNull := series.IsNull(j)
				val, _ := series.At(j)
				row.values = append(row.values, val)
				row.nulls = append(row.nulls, isNull)
			}

			result = append(result, row)
		}
	}
	return result
}

// performFullMerge keeps all rows from both DataFrames
func performFullMerge(df1, df2 *DataFrame, on string, df2Map map[any][]int, leftRows, rightRows int) []mergeRow {
	if df1 == nil || df2 == nil {
		return nil
	}

	// Get all rows from left merge
	result := performLeftMerge(df1, df2, on, df2Map, leftRows, rightRows)

	// Create set of keys already processed (excluding nulls)
	processedKeys := make(map[any]bool)
	leftKeySeries := df1.Columns[on]
	for i := 0; i < leftRows; i++ {
		if !leftKeySeries.IsNull(i) {
			k, _ := leftKeySeries.At(i)
			processedKeys[k] = true
		}
	}

	// Find the position of the key column in left DataFrame
	leftKeyIdx := -1
	for idx, name := range df1.ColumnOrder {
		if name == on {
			leftKeyIdx = idx
			break
		}
	}

	totalCols := len(df1.ColumnOrder) + len(df2.ColumnOrder) - 1

	// Add remaining rows from right DataFrame that weren't matched
	rightKeySeries := df2.Columns[on]
	for j := 0; j < rightRows; j++ {
		isNullKey := rightKeySeries.IsNull(j)

		// Skip if this key was already processed (null keys are never processed)
		if !isNullKey {
			key, _ := rightKeySeries.At(j)
			if processedKeys[key] {
				continue
			}
		}

		// This is an unmatched right row
		row := mergeRow{
			values: make([]any, 0, totalCols),
			nulls:  make([]bool, 0, totalCols),
		}

		// Add nulls for left columns (but use right key value for the key column)
		key, _ := rightKeySeries.At(j)
		for idx := range df1.ColumnOrder {
			if idx == leftKeyIdx {
				row.values = append(row.values, key)
				row.nulls = append(row.nulls, isNullKey)
			} else {
				row.values = append(row.values, nil)
				row.nulls = append(row.nulls, true)
			}
		}

		// Add right row values (excluding key)
		for _, col := range df2.ColumnOrder {
			if col == on {
				continue
			}
			series := df2.Columns[col]
			isNull := series.IsNull(j)
			val, _ := series.At(j)
			row.values = append(row.values, val)
			row.nulls = append(row.nulls, isNull)
		}

		result = append(result, row)
	}

	return result
}

