package dataframe

import (
	"errors"
	"fmt"
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
	df1ColIdx := -1
	df2ColIdx := -1
	for i, col := range df.Columns {
		if col == on {
			df1ColIdx = i
			break
		}
	}
	for i, col := range other.Columns {
		if col == on {
			df2ColIdx = i
			break
		}
	}
	if df1ColIdx == -1 || df2ColIdx == -1 {
		return nil, fmt.Errorf("column '%s' not found in both DataFrames", on)
	}

	// Create maps for faster lookups
	df2Map := make(map[any][]int)
	for i, row := range other.Data {
		key := row[df2ColIdx]
		df2Map[key] = append(df2Map[key], i)
	}

	// Prepare result columns
	resultColumns := make([]string, 0)
	resultColumns = append(resultColumns, df.Columns...)
	for _, col := range other.Columns {
		if col != on {
			resultColumns = append(resultColumns, col)
		}
	}

	// Prepare result data based on merge type
	var resultData [][]any
	switch how {
	case InnerMerge:
		resultData = performInnerMerge(df, other, df1ColIdx, df2ColIdx, df2Map)
	case LeftMerge:
		resultData = performLeftMerge(df, other, df1ColIdx, df2ColIdx, df2Map)
	case RightMerge:
		resultData = performRightMerge(df, other, df1ColIdx, df2ColIdx, df2Map)
	case FullMerge:
		resultData = performFullMerge(df, other, df1ColIdx, df2ColIdx, df2Map)
	default:
		return nil, fmt.Errorf("invalid merge type: %s", how)
	}

	return &DataFrame{
		Columns: resultColumns,
		Data:    resultData,
	}, nil
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
func performInnerMerge(df1, df2 *DataFrame, df1ColIdx, df2ColIdx int, df2Map map[any][]int) [][]any {
	if df1 == nil || df2 == nil {
		return nil
	}
	var result [][]any
	for _, row1 := range df1.Data {
		key := row1[df1ColIdx]
		if matches, ok := df2Map[key]; ok {
			for _, matchIdx := range matches {
				row2 := df2.Data[matchIdx]
				newRow := make([]any, 0)
				newRow = append(newRow, row1...)
				for j, val := range row2 {
					if j != df2ColIdx {
						newRow = append(newRow, val)
					}
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
func performLeftMerge(df1, df2 *DataFrame, df1ColIdx, df2ColIdx int, df2Map map[any][]int) [][]any {
	if df1 == nil || df2 == nil {
		return nil
	}
	var result [][]any
	nullRow := make([]any, len(df2.Columns)-1)

	for _, row1 := range df1.Data {
		key := row1[df1ColIdx]
		if matches, ok := df2Map[key]; ok {
			for _, matchIdx := range matches {
				row2 := df2.Data[matchIdx]
				newRow := make([]any, 0)
				newRow = append(newRow, row1...)
				for j, val := range row2 {
					if j != df2ColIdx {
						newRow = append(newRow, val)
					}
				}
				result = append(result, newRow)
			}
		} else {
			newRow := make([]any, 0)
			newRow = append(newRow, row1...)
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
func performRightMerge(df1, df2 *DataFrame, df1ColIdx, df2ColIdx int, _ map[any][]int) [][]any {
	if df1 == nil || df2 == nil {
		return nil
	}
	// Create reverse mapping for df1
	df1Map := make(map[any][]int)
	for i, row := range df1.Data {
		key := row[df1ColIdx]
		df1Map[key] = append(df1Map[key], i)
	}

	var result [][]any
	nullRow := make([]any, len(df1.Columns))

	for _, row2 := range df2.Data {
		key := row2[df2ColIdx]
		if matches, ok := df1Map[key]; ok {
			for _, matchIdx := range matches {
				row1 := df1.Data[matchIdx]
				newRow := make([]any, 0)
				newRow = append(newRow, row1...)
				for j, val := range row2 {
					if j != df2ColIdx {
						newRow = append(newRow, val)
					}
				}
				result = append(result, newRow)
			}
		} else {
			newRow := make([]any, 0)
			// Set the key column value instead of using null
			nullRow[df1ColIdx] = key
			newRow = append(newRow, nullRow...)
			for j, val := range row2 {
				if j != df2ColIdx {
					newRow = append(newRow, val)
				}
			}
			result = append(result, newRow)
			// Reset the key column back to nil for next iteration
			nullRow[df1ColIdx] = nil
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
func performFullMerge(df1, df2 *DataFrame, df1ColIdx, df2ColIdx int, df2Map map[any][]int) [][]any {
	if df1 == nil || df2 == nil {
		return nil
	}
	// Get all rows from left merge
	result := performLeftMerge(df1, df2, df1ColIdx, df2ColIdx, df2Map)

	// Create set of keys already processed
	processedKeys := make(map[any]bool)
	for _, row := range df1.Data {
		processedKeys[row[df1ColIdx]] = true
	}

	// Add remaining rows from right DataFrame
	nullRow := make([]any, len(df1.Columns))
	for _, row2 := range df2.Data {
		key := row2[df2ColIdx]
		if !processedKeys[key] {
			newRow := make([]any, 0)
			// Set the key column value instead of using null
			nullRow[df1ColIdx] = key
			newRow = append(newRow, nullRow...)
			for j, val := range row2 {
				if j != df2ColIdx {
					newRow = append(newRow, val)
				}
			}
			result = append(result, newRow)
			// Reset the key column back to nil for next iteration
			nullRow[df1ColIdx] = nil
		}
	}
	return result
}
