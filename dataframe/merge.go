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
//   - FullMerge: Keep all rows from both DataFrames, filling in missing values with nil.
//
// Returns:
//   - A new DataFrame containing the merged data.
//   - An error if the merge operation fails, such as if the specified column does not exist in one or both DataFrames.
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
//	// 3  | Charlie | nil
//
//	// Full merge example (all rows from both)
//	result, err := df1.Merge(df2, "ID", FullMerge)
//	// Result:
//	// ID | Name    | Age
//	// 1  | Alice   | 25
//	// 2  | Bob     | 30
//	// 3  | Charlie | nil
//	// 4  | nil     | 35
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

	// Build lookup for right DataFrame on key column
	df2Map := make(map[any][]int)
	for i := 0; i < rightRows; i++ {
		v, _ := other.Columns[on].At(i)
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

	// Prepare result rows based on merge type (intermediate row-wise build, then columnize)
	var resultRows [][]any
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

	// Convert row-wise to columnar Series
	cols := make(map[string]*collection.Series, len(resultColumns))
	buffers := make([][]any, len(resultColumns))
	for i := range buffers {
		buffers[i] = make([]any, 0, len(resultRows))
	}
	for _, row := range resultRows {
		for i := range resultColumns {
			var v any
			if i < len(row) {
				v = row[i]
			}
			buffers[i] = append(buffers[i], v)
		}
	}
	for i, name := range resultColumns {
		s, err := collection.NewSeriesWithData(nil, buffers[i])
		if err != nil {
			return nil, err
		}
		cols[name] = s
	}

	return &DataFrame{Columns: cols, ColumnOrder: resultColumns}, nil
}

// performInnerMerge combines two DataFrames based on a specified column index,
// returning only the rows that have matching values in both DataFrames.
//
// Parameters:
//   - df1: The first DataFrame to merge.
//   - df2: The second DataFrame to merge.
//   - df1ColIdx: The index of the column in the first DataFrame to merge on.
//   - df2ColIdx: The index of the column in the second DataFrame to merge on.
//   - df2Map: A map created from the second DataFrame for faster lookups, where the key is the value
//     in the merge column and the value is a slice of indices of rows in the second DataFrame that
//     have that key.
//
// Returns: A slice of slices containing the merged data, where each inner slice represents a row.
// The resulting rows will include all columns from the first DataFrame and the columns from the second DataFrame, excluding the merge column from the second DataFrame.
//
// Example:
//
//	result := performInnerMerge(df1, df2, 0, 0, df2Map)
//	// This will merge df1 and df2 on the first column of each DataFrame,
//	// returning only the rows with matching values in that column.
func performInnerMerge(df1, df2 *DataFrame, on string, df2Map map[any][]int, leftRows, rightRows int) [][]any {
	if df1 == nil || df2 == nil {
		return nil
	}
	var result [][]any
	for i := 0; i < leftRows; i++ {
		key, _ := df1.Columns[on].At(i)
		if matches, ok := df2Map[key]; ok {
			for _, matchIdx := range matches {
				newRow := make([]any, 0, len(df1.ColumnOrder)+len(df2.ColumnOrder)-1)
				// left row values
				for _, col := range df1.ColumnOrder {
					v, _ := df1.Columns[col].At(i)
					newRow = append(newRow, v)
				}
				// right row values excluding key
				for _, col := range df2.ColumnOrder {
					if col == on {
						continue
					}
					v, _ := df2.Columns[col].At(matchIdx)
					newRow = append(newRow, v)
				}
				result = append(result, newRow)
			}
		}
	}
	return result
}

// performLeftMerge combines two DataFrames based on a specified column index,
// keeping all rows from the first DataFrame and matching rows from the second DataFrame.
//
// Parameters:
//   - df1: The first DataFrame to merge.
//   - df2: The second DataFrame to merge.
//   - df1ColIdx: The index of the column in the first DataFrame to merge on.
//   - df2ColIdx: The index of the column in the second DataFrame to merge on.
//   - df2Map: A map created from the second DataFrame for faster lookups, where the key is the value
//     in the merge column and the value is a slice of indices of rows in the second DataFrame that
//     have that key.
//
// Returns: A slice of slices containing the merged data, where each inner slice represents a row.
// The resulting rows will include all rows from the first DataFrame, with matching data from the second DataFrame where available, and nil values where no match exists.
//
// Example:
//
//	result := performLeftMerge(df1, df2, 0, 0, df2Map)
//	// This will keep all rows from df1 and add matching columns from df2,
//	// filling with nil values when there's no match in df2.
func performLeftMerge(df1, df2 *DataFrame, on string, df2Map map[any][]int, leftRows, rightRows int) [][]any {
	if df1 == nil || df2 == nil {
		return nil
	}
	var result [][]any
	nullRow := make([]any, len(df2.ColumnOrder)-1)

	for i := 0; i < leftRows; i++ {
		key, _ := df1.Columns[on].At(i)
		if matches, ok := df2Map[key]; ok {
			for _, matchIdx := range matches {
				newRow := make([]any, 0, len(df1.ColumnOrder)+len(df2.ColumnOrder)-1)
				for _, col := range df1.ColumnOrder {
					v, _ := df1.Columns[col].At(i)
					newRow = append(newRow, v)
				}
				for _, col := range df2.ColumnOrder {
					if col == on {
						continue
					}
					v, _ := df2.Columns[col].At(matchIdx)
					newRow = append(newRow, v)
				}
				result = append(result, newRow)
			}
		} else {
			newRow := make([]any, 0, len(df1.ColumnOrder)+len(df2.ColumnOrder)-1)
			for _, col := range df1.ColumnOrder {
				v, _ := df1.Columns[col].At(i)
				newRow = append(newRow, v)
			}
			newRow = append(newRow, nullRow...)
			result = append(result, newRow)
		}
	}
	return result
}

// performRightMerge combines two DataFrames based on a specified column index,
// keeping all rows from the second DataFrame and matching rows from the first DataFrame.
//
// Parameters:
//   - df1: The first DataFrame to merge.
//   - df2: The second DataFrame to merge.
//   - df1ColIdx: The index of the column in the first DataFrame to merge on.
//   - df2ColIdx: The index of the column in the second DataFrame to merge on.
//   - df2Map: A map created from the second DataFrame for faster lookups (unused in right merge).
//
// Returns: A slice of slices containing the merged data, where each inner slice represents a row.
// The resulting rows will include all rows from the second DataFrame, with matching data from the first DataFrame where available, and nil values where no match exists.
//
// Example:
//
//	result := performRightMerge(df1, df2, 0, 0, df2Map)
//	// This will keep all rows from df2 and add matching columns from df1,
//	// filling with nil values when there's no match in df1.
func performRightMerge(df1, df2 *DataFrame, on string, _ map[any][]int, leftRows, rightRows int) [][]any {
	if df1 == nil || df2 == nil {
		return nil
	}
	// Create reverse mapping for df1
	df1Map := make(map[any][]int)
	for i := 0; i < leftRows; i++ {
		key, _ := df1.Columns[on].At(i)
		df1Map[key] = append(df1Map[key], i)
	}

	var result [][]any
	nullRow := make([]any, len(df1.ColumnOrder))

	for j := 0; j < rightRows; j++ {
		key, _ := df2.Columns[on].At(j)
		if matches, ok := df1Map[key]; ok {
			for _, matchIdx := range matches {
				newRow := make([]any, 0, len(df1.ColumnOrder)+len(df2.ColumnOrder)-1)
				for _, col := range df1.ColumnOrder {
					v, _ := df1.Columns[col].At(matchIdx)
					newRow = append(newRow, v)
				}
				for _, col := range df2.ColumnOrder {
					if col == on {
						continue
					}
					v, _ := df2.Columns[col].At(j)
					newRow = append(newRow, v)
				}
				result = append(result, newRow)
			}
		} else {
			newRow := make([]any, 0, len(df1.ColumnOrder)+len(df2.ColumnOrder)-1)
			// Set the key column value instead of using null
			// Find key position in left
			leftKeyIdx := 0
			for idx, name := range df1.ColumnOrder {
				if name == on {
					leftKeyIdx = idx
					break
				}
			}
			nullRow[leftKeyIdx] = key
			newRow = append(newRow, nullRow...)
			for _, col := range df2.ColumnOrder {
				if col == on {
					continue
				}
				v, _ := df2.Columns[col].At(j)
				newRow = append(newRow, v)
			}
			result = append(result, newRow)
			// Reset the key column back to nil for next iteration
			nullRow[leftKeyIdx] = nil
		}
	}
	return result
}

// performFullMerge combines two DataFrames based on a specified column index,
// keeping all rows from both DataFrames and matching where possible.
//
// Parameters:
//   - df1: The first DataFrame to merge.
//   - df2: The second DataFrame to merge.
//   - df1ColIdx: The index of the column in the first DataFrame to merge on.
//   - df2ColIdx: The index of the column in the second DataFrame to merge on.
//   - df2Map: A map created from the second DataFrame for faster lookups, where the key is the value
//     in the merge column and the value is a slice of indices of rows in the second DataFrame that
//     have that key.
//
// Returns: A slice of slices containing the merged data, where each inner slice represents a row.
// The resulting rows will include all rows from both DataFrames, with matching data where available and nil values where no match exists.
//
// Example:
//
//	result := performFullMerge(df1, df2, 0, 0, df2Map)
//	// This will keep all rows from both df1 and df2, matching where possible,
//	// filling with nil values when there's no match in either DataFrame.
func performFullMerge(df1, df2 *DataFrame, on string, df2Map map[any][]int, leftRows, rightRows int) [][]any {
	if df1 == nil || df2 == nil {
		return nil
	}
	// Get all rows from left merge
	result := performLeftMerge(df1, df2, on, df2Map, leftRows, rightRows)

	// Create set of keys already processed
	processedKeys := make(map[any]bool)
	for i := 0; i < leftRows; i++ {
		k, _ := df1.Columns[on].At(i)
		processedKeys[k] = true
	}

	// Add remaining rows from right DataFrame
	nullRow := make([]any, len(df1.ColumnOrder))
	for j := 0; j < rightRows; j++ {
		key, _ := df2.Columns[on].At(j)
		if !processedKeys[key] {
			newRow := make([]any, 0, len(df1.ColumnOrder)+len(df2.ColumnOrder)-1)
			// Set the key column value instead of using null
			leftKeyIdx := 0
			for idx, name := range df1.ColumnOrder {
				if name == on {
					leftKeyIdx = idx
					break
				}
			}
			nullRow[leftKeyIdx] = key
			newRow = append(newRow, nullRow...)
			for _, col := range df2.ColumnOrder {
				if col == on {
					continue
				}
				v, _ := df2.Columns[col].At(j)
				newRow = append(newRow, v)
			}
			result = append(result, newRow)
			// Reset the key column back to nil for next iteration
			nullRow[leftKeyIdx] = nil
		}
	}
	return result
}
