package dataframe

import (
	"errors"
	"fmt"
	"sort"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// ConcatAxis specifies the axis along which to concatenate.
type ConcatAxis int

const (
	// AxisIndex (0) concatenates along rows (stacking DataFrames vertically).
	AxisIndex ConcatAxis = 0
	// AxisColumns (1) concatenates along columns (joining DataFrames horizontally).
	AxisColumns ConcatAxis = 1
)

// ConcatJoin specifies how to handle indexes on the non-concatenation axis.
type ConcatJoin string

const (
	// JoinOuter takes the union of indexes (all columns/rows, with nulls for missing).
	JoinOuter ConcatJoin = "outer"
	// JoinInner takes the intersection of indexes (only common columns/rows).
	JoinInner ConcatJoin = "inner"
)

// ConcatOptions configures the behavior of the Concat function.
type ConcatOptions struct {
	// Axis is the axis to concatenate along. Default: AxisIndex (0).
	Axis ConcatAxis

	// Join determines how to handle indexes on other axis. Default: JoinOuter.
	Join ConcatJoin

	// IgnoreIndex if true, do not use the index values along the concatenation axis.
	// The resulting axis will be labeled 0, 1, ..., n-1. Default: false.
	IgnoreIndex bool

	// VerifyIntegrity if true, check whether the new concatenated axis contains duplicates.
	// This can be expensive. Default: false.
	VerifyIntegrity bool

	// Sort if true, sort non-concatenation axis if it is not already aligned. Default: false.
	Sort bool
}

// DefaultConcatOptions returns the default options for Concat.
func DefaultConcatOptions() ConcatOptions {
	return ConcatOptions{
		Axis:            AxisIndex,
		Join:            JoinOuter,
		IgnoreIndex:     false,
		VerifyIntegrity: false,
		Sort:            false,
	}
}

// Concat concatenates DataFrames along a particular axis.
// This is an internal version used by other dataframe methods.
// For the public API, use gpandas.Concat.
func Concat(objs []*DataFrame, opts ...ConcatOptions) (*DataFrame, error) {
	// Apply default options
	options := DefaultConcatOptions()
	if len(opts) > 0 {
		options = opts[0]
	}

	// Filter out nil DataFrames
	validDFs := make([]*DataFrame, 0, len(objs))
	for _, df := range objs {
		if df != nil {
			validDFs = append(validDFs, df)
		}
	}

	if len(validDFs) == 0 {
		return nil, errors.New("no valid DataFrames to concatenate (all nil or empty input)")
	}

	if len(validDFs) == 1 {
		// Return a copy of the single DataFrame
		return copyDataFrame(validDFs[0]), nil
	}

	switch options.Axis {
	case AxisIndex:
		return concatAlongRows(validDFs, options)
	case AxisColumns:
		return concatAlongColumns(validDFs, options)
	default:
		return nil, fmt.Errorf("invalid axis: %d (must be 0 or 1)", options.Axis)
	}
}

// concatAlongRows concatenates DataFrames vertically (stacking rows).
func concatAlongRows(dfs []*DataFrame, opts ConcatOptions) (*DataFrame, error) {
	// Determine the final column set based on join type
	allColumns := make(map[string]bool)
	columnSets := make([]map[string]bool, len(dfs))

	for i, df := range dfs {
		df.RLock()
		columnSets[i] = make(map[string]bool)
		for _, col := range df.ColumnOrder {
			allColumns[col] = true
			columnSets[i][col] = true
		}
		df.RUnlock()
	}

	var resultColumns []string
	if opts.Join == JoinInner {
		// Intersection: columns present in ALL DataFrames
		for col := range allColumns {
			presentInAll := true
			for _, colSet := range columnSets {
				if !colSet[col] {
					presentInAll = false
					break
				}
			}
			if presentInAll {
				resultColumns = append(resultColumns, col)
			}
		}
	} else {
		// Outer join: union of all columns
		for col := range allColumns {
			resultColumns = append(resultColumns, col)
		}
	}

	if len(resultColumns) == 0 {
		return nil, errors.New("no columns to concatenate (inner join resulted in empty column set)")
	}

	// Sort columns if requested
	if opts.Sort {
		sort.Strings(resultColumns)
	} else {
		// Preserve order from the first DataFrame, then append new columns
		orderedCols := make([]string, 0, len(resultColumns))
		seen := make(map[string]bool)
		for _, df := range dfs {
			df.RLock()
			for _, col := range df.ColumnOrder {
				if allColumns[col] && !seen[col] {
					// For inner join, only include if in resultColumns
					inResult := false
					for _, rc := range resultColumns {
						if rc == col {
							inResult = true
							break
						}
					}
					if inResult || opts.Join == JoinOuter {
						orderedCols = append(orderedCols, col)
						seen[col] = true
					}
				}
			}
			df.RUnlock()
		}
		// For outer join, only use columns that are in resultColumns
		if opts.Join == JoinOuter {
			resultColumns = orderedCols
		} else {
			// For inner join, filter orderedCols to only include resultColumns
			filtered := make([]string, 0, len(resultColumns))
			resultSet := make(map[string]bool)
			for _, col := range resultColumns {
				resultSet[col] = true
			}
			for _, col := range orderedCols {
				if resultSet[col] {
					filtered = append(filtered, col)
				}
			}
			resultColumns = filtered
		}
	}

	// Calculate total rows
	totalRows := 0
	for _, df := range dfs {
		df.RLock()
		totalRows += df.Len()
		df.RUnlock()
	}

	// Create result series for each column using AnySeries for simplicity
	resultSeries := make(map[string]collection.Series)
	for _, col := range resultColumns {
		resultSeries[col] = collection.NewAnySeries(totalRows)
	}

	// Append data from each DataFrame
	resultIndex := make([]string, 0, totalRows)
	rowOffset := 0

	for _, df := range dfs {
		df.RLock()
		numRows := df.Len()

		for r := 0; r < numRows; r++ {
			for _, col := range resultColumns {
				series := df.Columns[col]
				if series != nil && r < series.Len() {
					if series.IsNull(r) {
						resultSeries[col].AppendNull()
					} else {
						val, _ := series.At(r)
						resultSeries[col].Append(val)
					}
				} else {
					// Column doesn't exist in this DataFrame, append null
					resultSeries[col].AppendNull()
				}
			}

			// Handle index
			if opts.IgnoreIndex {
				resultIndex = append(resultIndex, fmt.Sprintf("%d", rowOffset+r))
			} else if r < len(df.Index) {
				resultIndex = append(resultIndex, df.Index[r])
			} else {
				resultIndex = append(resultIndex, fmt.Sprintf("%d", rowOffset+r))
			}
		}

		rowOffset += numRows
		df.RUnlock()
	}

	// Verify integrity if requested
	if opts.VerifyIntegrity {
		seen := make(map[string]bool)
		for _, idx := range resultIndex {
			if seen[idx] {
				return nil, fmt.Errorf("duplicate index value: %s", idx)
			}
			seen[idx] = true
		}
	}

	return &DataFrame{
		Columns:     resultSeries,
		ColumnOrder: resultColumns,
		Index:       resultIndex,
	}, nil
}

// concatAlongColumns concatenates DataFrames horizontally (joining columns side-by-side).
func concatAlongColumns(dfs []*DataFrame, opts ConcatOptions) (*DataFrame, error) {
	// For axis=1, we need to align rows based on index
	// Collect all unique indices
	allIndices := make(map[string]bool)
	indexSets := make([]map[string]int, len(dfs)) // Map index label to row position

	for i, df := range dfs {
		df.RLock()
		indexSets[i] = make(map[string]int)
		for r := 0; r < df.Len(); r++ {
			var idx string
			if r < len(df.Index) {
				idx = df.Index[r]
			} else {
				idx = fmt.Sprintf("%d", r)
			}
			allIndices[idx] = true
			indexSets[i][idx] = r
		}
		df.RUnlock()
	}

	var resultIndex []string
	if opts.Join == JoinInner {
		// Intersection: indices present in ALL DataFrames
		for idx := range allIndices {
			presentInAll := true
			for _, idxSet := range indexSets {
				if _, ok := idxSet[idx]; !ok {
					presentInAll = false
					break
				}
			}
			if presentInAll {
				resultIndex = append(resultIndex, idx)
			}
		}
	} else {
		// Outer join: union of all indices
		for idx := range allIndices {
			resultIndex = append(resultIndex, idx)
		}
	}

	if len(resultIndex) == 0 {
		return nil, errors.New("no rows to concatenate (inner join resulted in empty index set)")
	}

	// Sort index if requested
	if opts.Sort {
		sort.Strings(resultIndex)
	}

	// Collect all columns (must be unique across DataFrames)
	resultColumns := make([]string, 0)
	resultSeries := make(map[string]collection.Series)
	columnsSeen := make(map[string]bool)

	for dfIdx, df := range dfs {
		df.RLock()

		for _, col := range df.ColumnOrder {
			// Check for duplicate column names
			if columnsSeen[col] {
				df.RUnlock()
				return nil, fmt.Errorf("duplicate column name: %s", col)
			}
			columnsSeen[col] = true
			resultColumns = append(resultColumns, col)

			// Create new series for this column
			series := df.Columns[col]
			newSeries := collection.NewAnySeries(len(resultIndex))

			for _, idx := range resultIndex {
				if rowPos, ok := indexSets[dfIdx][idx]; ok && rowPos < series.Len() {
					if series.IsNull(rowPos) {
						newSeries.AppendNull()
					} else {
						val, _ := series.At(rowPos)
						newSeries.Append(val)
					}
				} else {
					// Row doesn't exist in this DataFrame
					newSeries.AppendNull()
				}
			}

			resultSeries[col] = newSeries
		}

		df.RUnlock()
	}

	// Handle IgnoreIndex for axis=1 (reset column names - not typically used, but supported)
	finalIndex := resultIndex
	if opts.IgnoreIndex {
		finalIndex = make([]string, len(resultIndex))
		for i := range finalIndex {
			finalIndex[i] = fmt.Sprintf("%d", i)
		}
	}

	return &DataFrame{
		Columns:     resultSeries,
		ColumnOrder: resultColumns,
		Index:       finalIndex,
	}, nil
}

// copyDataFrame creates a shallow copy of a DataFrame.
func copyDataFrame(df *DataFrame) *DataFrame {
	if df == nil {
		return nil
	}

	df.RLock()
	defer df.RUnlock()

	newCols := make(map[string]collection.Series, len(df.Columns))
	for name, series := range df.Columns {
		newCols[name] = series // Shallow copy - series are shared
	}

	newOrder := make([]string, len(df.ColumnOrder))
	copy(newOrder, df.ColumnOrder)

	newIndex := make([]string, len(df.Index))
	copy(newIndex, df.Index)

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: newOrder,
		Index:       newIndex,
	}
}
