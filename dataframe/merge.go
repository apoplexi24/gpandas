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
func (df *DataFrame) Merge(other *DataFrame, on string, how MergeHow) (*DataFrame, error) {
	if df == nil || other == nil {
		return nil, errors.New("both DataFrames must be non-nil")
	}

	// Validate 'on' column exists in both DataFrames
	_, okLeft := df.Series[on]
	_, okRight := other.Series[on]

	if !okLeft || !okRight {
		return nil, fmt.Errorf("column '%s' not found in both DataFrames", on)
	}

	// Create map for faster lookups
	rightKeysMap := make(map[any][]int)
	rightSeries := other.Series[on]
	for i := 0; i < rightSeries.Len(); i++ {
		if !rightSeries.IsNull(i) {
			key := rightSeries.GetValue(i)
			rightKeysMap[key] = append(rightKeysMap[key], i)
		}
	}

	// Prepare result columns (excluding the join column from the right DataFrame)
	resultColumns := make([]string, 0, len(df.Columns)+len(other.Columns)-1)
	resultColumns = append(resultColumns, df.Columns...)

	for _, col := range other.Columns {
		if col != on {
			resultColumns = append(resultColumns, col)
		}
	}

	// Perform appropriate merge operation
	switch how {
	case InnerMerge:
		return performInnerMerge(df, other, on, rightKeysMap)
	case LeftMerge:
		return performLeftMerge(df, other, on, rightKeysMap)
	case RightMerge:
		return performRightMerge(df, other, on, rightKeysMap)
	case FullMerge:
		return performFullMerge(df, other, on, rightKeysMap)
	default:
		return nil, fmt.Errorf("invalid merge type: %s", how)
	}
}

// performInnerMerge combines two DataFrames with an inner join
func performInnerMerge(left, right *DataFrame, on string, rightKeysMap map[any][]int) (*DataFrame, error) {
	// Determine columns for result (all from left + non-join columns from right)
	resultColumns := make([]string, 0, len(left.Columns)+len(right.Columns)-1)
	resultColumns = append(resultColumns, left.Columns...)

	rightColumns := make([]string, 0, len(right.Columns)-1)
	for _, col := range right.Columns {
		if col != on {
			resultColumns = append(resultColumns, col)
			rightColumns = append(rightColumns, col)
		}
	}

	result := NewDataFrame(resultColumns)

	// Count total rows for pre-allocation
	totalRows := 0
	leftSeries := left.Series[on]

	for i := 0; i < leftSeries.Len(); i++ {
		if leftSeries.IsNull(i) {
			continue
		}

		key := leftSeries.GetValue(i)
		if matches, ok := rightKeysMap[key]; ok {
			totalRows += len(matches)
		}
	}

	// Create series for result with pre-allocated space
	for _, col := range left.Columns {
		leftCol := left.Series[col]
		var seriesType SeriesType

		switch leftCol.(type) {
		case *IntSeries:
			seriesType = IntType
		case *FloatSeries:
			seriesType = FloatType
		case *StringSeries:
			seriesType = StringType
		case *BoolSeries:
			seriesType = BoolType
		default:
			seriesType = StringType
		}

		newSeries := CreateSeries(seriesType, col, totalRows)
		result.Series[col] = newSeries
	}

	for _, col := range rightColumns {
		rightCol := right.Series[col]
		var seriesType SeriesType

		switch rightCol.(type) {
		case *IntSeries:
			seriesType = IntType
		case *FloatSeries:
			seriesType = FloatType
		case *StringSeries:
			seriesType = StringType
		case *BoolSeries:
			seriesType = BoolType
		default:
			seriesType = StringType
		}

		newSeries := CreateSeries(seriesType, col, totalRows)
		result.Series[col] = newSeries
	}

	// Perform inner join
	resultIdx := 0
	for i := 0; i < leftSeries.Len(); i++ {
		if leftSeries.IsNull(i) {
			continue
		}

		key := leftSeries.GetValue(i)
		if matches, ok := rightKeysMap[key]; ok {
			for _, j := range matches {
				// Copy values from left DataFrame
				for _, col := range left.Columns {
					val := left.Series[col].GetValue(i)
					result.Series[col].SetValue(resultIdx, val)
				}

				// Copy values from right DataFrame (excluding the join column)
				for _, col := range rightColumns {
					val := right.Series[col].GetValue(j)
					result.Series[col].SetValue(resultIdx, val)
				}

				resultIdx++
			}
		}
	}

	return result, nil
}

// performLeftMerge combines two DataFrames with a left join
func performLeftMerge(left, right *DataFrame, on string, rightKeysMap map[any][]int) (*DataFrame, error) {
	// Determine columns for result (all from left + non-join columns from right)
	resultColumns := make([]string, 0, len(left.Columns)+len(right.Columns)-1)
	resultColumns = append(resultColumns, left.Columns...)

	rightColumns := make([]string, 0, len(right.Columns)-1)
	for _, col := range right.Columns {
		if col != on {
			resultColumns = append(resultColumns, col)
			rightColumns = append(rightColumns, col)
		}
	}

	result := NewDataFrame(resultColumns)

	// Count total rows for pre-allocation
	leftRows := left.Rows()

	// Create series for result with pre-allocated space
	for _, col := range left.Columns {
		leftCol := left.Series[col]
		var seriesType SeriesType

		switch leftCol.(type) {
		case *IntSeries:
			seriesType = IntType
		case *FloatSeries:
			seriesType = FloatType
		case *StringSeries:
			seriesType = StringType
		case *BoolSeries:
			seriesType = BoolType
		default:
			seriesType = StringType
		}

		newSeries := CreateSeries(seriesType, col, leftRows)
		result.Series[col] = newSeries
	}

	for _, col := range rightColumns {
		rightCol := right.Series[col]
		var seriesType SeriesType

		switch rightCol.(type) {
		case *IntSeries:
			seriesType = IntType
		case *FloatSeries:
			seriesType = FloatType
		case *StringSeries:
			seriesType = StringType
		case *BoolSeries:
			seriesType = BoolType
		default:
			seriesType = StringType
		}

		newSeries := CreateSeries(seriesType, col, leftRows)
		result.Series[col] = newSeries
	}

	// Perform left join
	leftSeries := left.Series[on]

	for i := 0; i < leftSeries.Len(); i++ {
		// Copy values from left DataFrame
		for _, col := range left.Columns {
			val := left.Series[col].GetValue(i)
			result.Series[col].SetValue(i, val)
		}

		if leftSeries.IsNull(i) {
			// For null join values, set nulls for right columns
			for _, col := range rightColumns {
				result.Series[col].SetValue(i, nil)
			}
			continue
		}

		key := leftSeries.GetValue(i)
		if matches, ok := rightKeysMap[key]; ok && len(matches) > 0 {
			// Match found - use first match
			j := matches[0]

			// Copy values from right DataFrame (excluding the join column)
			for _, col := range rightColumns {
				val := right.Series[col].GetValue(j)
				result.Series[col].SetValue(i, val)
			}
		} else {
			// No match - set nulls for right columns
			for _, col := range rightColumns {
				result.Series[col].SetValue(i, nil)
			}
		}
	}

	return result, nil
}

// performRightMerge combines two DataFrames with a right join
func performRightMerge(left, right *DataFrame, on string, rightKeysMap map[any][]int) (*DataFrame, error) {
	// Create a left map similar to the right map
	leftKeysMap := make(map[any][]int)
	leftSeries := left.Series[on]
	for i := 0; i < leftSeries.Len(); i++ {
		if !leftSeries.IsNull(i) {
			key := leftSeries.GetValue(i)
			leftKeysMap[key] = append(leftKeysMap[key], i)
		}
	}

	// Determine columns for result (all from left + non-join columns from right)
	resultColumns := make([]string, 0, len(left.Columns)+len(right.Columns)-1)
	resultColumns = append(resultColumns, left.Columns...)

	rightColumns := make([]string, 0, len(right.Columns)-1)
	for _, col := range right.Columns {
		if col != on {
			resultColumns = append(resultColumns, col)
			rightColumns = append(rightColumns, col)
		}
	}

	result := NewDataFrame(resultColumns)

	// Count total rows for pre-allocation
	rightRows := right.Rows()

	// Create series for result with pre-allocated space
	for _, col := range left.Columns {
		leftCol := left.Series[col]
		var seriesType SeriesType

		switch leftCol.(type) {
		case *IntSeries:
			seriesType = IntType
		case *FloatSeries:
			seriesType = FloatType
		case *StringSeries:
			seriesType = StringType
		case *BoolSeries:
			seriesType = BoolType
		default:
			seriesType = StringType
		}

		newSeries := CreateSeries(seriesType, col, rightRows)
		result.Series[col] = newSeries
	}

	for _, col := range rightColumns {
		rightCol := right.Series[col]
		var seriesType SeriesType

		switch rightCol.(type) {
		case *IntSeries:
			seriesType = IntType
		case *FloatSeries:
			seriesType = FloatType
		case *StringSeries:
			seriesType = StringType
		case *BoolSeries:
			seriesType = BoolType
		default:
			seriesType = StringType
		}

		newSeries := CreateSeries(seriesType, col, rightRows)
		result.Series[col] = newSeries
	}

	// Perform right join
	rightSeries := right.Series[on]

	for i := 0; i < rightSeries.Len(); i++ {
		// Copy values from right DataFrame (excluding join column)
		for _, col := range rightColumns {
			val := right.Series[col].GetValue(i)
			result.Series[col].SetValue(i, val)
		}

		// Copy join column value
		result.Series[on].SetValue(i, rightSeries.GetValue(i))

		if rightSeries.IsNull(i) {
			// For null join values, set nulls for left columns (except join column)
			for _, col := range left.Columns {
				if col != on {
					result.Series[col].SetValue(i, nil)
				}
			}
			continue
		}

		key := rightSeries.GetValue(i)
		if matches, ok := leftKeysMap[key]; ok && len(matches) > 0 {
			// Match found - use first match
			j := matches[0]

			// Copy values from left DataFrame (excluding the join column)
			for _, col := range left.Columns {
				if col != on {
					val := left.Series[col].GetValue(j)
					result.Series[col].SetValue(i, val)
				}
			}
		} else {
			// No match - set nulls for left columns (except join column)
			for _, col := range left.Columns {
				if col != on {
					result.Series[col].SetValue(i, nil)
				}
			}
		}
	}

	return result, nil
}

// performFullMerge combines two DataFrames with a full outer join
func performFullMerge(left, right *DataFrame, on string, rightKeysMap map[any][]int) (*DataFrame, error) {
	// Create a left map similar to the right map
	leftKeysMap := make(map[any][]int)
	leftSeries := left.Series[on]
	for i := 0; i < leftSeries.Len(); i++ {
		if !leftSeries.IsNull(i) {
			key := leftSeries.GetValue(i)
			leftKeysMap[key] = append(leftKeysMap[key], i)
		}
	}

	// Determine columns for result (all from left + non-join columns from right)
	resultColumns := make([]string, 0, len(left.Columns)+len(right.Columns)-1)
	resultColumns = append(resultColumns, left.Columns...)

	rightColumns := make([]string, 0, len(right.Columns)-1)
	for _, col := range right.Columns {
		if col != on {
			resultColumns = append(resultColumns, col)
			rightColumns = append(rightColumns, col)
		}
	}

	result := NewDataFrame(resultColumns)

	// For full join, we'll build the result in phases

	// Phase 1: Add all rows from left with matching rows from right
	leftRows := left.Rows()
	leftMatchedKeys := make(map[any]bool)

	// Create temporary series to collect data
	tempSeries := make(map[string][]any)
	for _, col := range resultColumns {
		tempSeries[col] = make([]any, 0, leftRows)
	}

	// Process left DataFrame
	for i := 0; i < leftSeries.Len(); i++ {
		// Copy values from left DataFrame
		for _, col := range left.Columns {
			tempSeries[col] = append(tempSeries[col], left.Series[col].GetValue(i))
		}

		// Initialize right columns to null
		for _, col := range rightColumns {
			tempSeries[col] = append(tempSeries[col], nil)
		}

		if leftSeries.IsNull(i) {
			// For null join values, keep nulls for right columns
			continue
		}

		key := leftSeries.GetValue(i)
		leftMatchedKeys[key] = true

		if matches, ok := rightKeysMap[key]; ok && len(matches) > 0 {
			// Match found - use first match
			j := matches[0]

			// Update values from right DataFrame (excluding the join column)
			for _, col := range rightColumns {
				lastIdx := len(tempSeries[col]) - 1
				tempSeries[col][lastIdx] = right.Series[col].GetValue(j)
			}
		}
	}

	// Phase 2: Add rows from right that had no match in left
	rightSeries := right.Series[on]

	for i := 0; i < rightSeries.Len(); i++ {
		if rightSeries.IsNull(i) {
			// For null join values, add a new row
			// Set nulls for left columns (except join column which is null)
			for _, col := range left.Columns {
				if col == on {
					tempSeries[col] = append(tempSeries[col], nil)
				} else {
					tempSeries[col] = append(tempSeries[col], nil)
				}
			}

			// Set values for right columns
			for _, col := range rightColumns {
				tempSeries[col] = append(tempSeries[col], right.Series[col].GetValue(i))
			}
			continue
		}

		key := rightSeries.GetValue(i)

		// Skip keys that were already matched with left
		if _, ok := leftMatchedKeys[key]; ok {
			continue
		}

		// Add new row for unmatched right row
		for _, col := range left.Columns {
			if col == on {
				tempSeries[col] = append(tempSeries[col], key)
			} else {
				tempSeries[col] = append(tempSeries[col], nil)
			}
		}

		// Set values for right columns
		for _, col := range rightColumns {
			tempSeries[col] = append(tempSeries[col], right.Series[col].GetValue(i))
		}
	}

	// Create Series from temporary data
	totalRows := len(tempSeries[resultColumns[0]])

	for _, col := range resultColumns {
		// Determine series type from data
		var seriesType SeriesType = StringType // Default

		if col == on {
			// For join column, try to use the same type as in original DataFrames
			if _, ok := left.Series[col].(*IntSeries); ok {
				seriesType = IntType
			} else if _, ok := left.Series[col].(*FloatSeries); ok {
				seriesType = FloatType
			} else if _, ok := left.Series[col].(*BoolSeries); ok {
				seriesType = BoolType
			}
		} else if _, ok := left.Series[col]; ok {
			// For left columns, use the same type as in left DataFrame
			if _, ok := left.Series[col].(*IntSeries); ok {
				seriesType = IntType
			} else if _, ok := left.Series[col].(*FloatSeries); ok {
				seriesType = FloatType
			} else if _, ok := left.Series[col].(*BoolSeries); ok {
				seriesType = BoolType
			}
		} else {
			// For right columns, use the same type as in right DataFrame
			if _, ok := right.Series[col].(*IntSeries); ok {
				seriesType = IntType
			} else if _, ok := right.Series[col].(*FloatSeries); ok {
				seriesType = FloatType
			} else if _, ok := right.Series[col].(*BoolSeries); ok {
				seriesType = BoolType
			}
		}

		newSeries := CreateSeries(seriesType, col, totalRows)
		for i, val := range tempSeries[col] {
			newSeries.SetValue(i, val)
		}

		result.Series[col] = newSeries
	}

	return result, nil
}
