package dataframe

import (
	"errors"
	"fmt"
	"reflect"
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
//
// Column types are preserved: when every input agrees on a column's dtype the
// result keeps it, a mix of integer and floating-point columns widens to float64
// (as pandas does), and only a genuine type conflict falls back to an untyped
// (any) column.
//
// gpandas.Concat is the public entry point and delegates here.
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

	// Create a result series per column, preserving the input dtype when every
	// DataFrame that has the column agrees on it. Only a genuine type conflict
	// falls back to an untyped (any) Series.
	colTypes := make(map[string][]reflect.Type, len(resultColumns))
	for _, df := range dfs {
		df.RLock()
		for _, col := range resultColumns {
			if s := df.Columns[col]; s != nil {
				colTypes[col] = append(colTypes[col], s.DType())
			}
		}
		df.RUnlock()
	}

	resultSeries := make(map[string]collection.Series, len(resultColumns))
	resultDTypes := make(map[string]reflect.Type, len(resultColumns))
	for _, col := range resultColumns {
		dtype := concatDType(colTypes[col])
		resultDTypes[col] = dtype
		if dtype == nil {
			resultSeries[col] = collection.NewAnySeries(totalRows)
		} else {
			resultSeries[col] = collection.NewSeriesOfType(dtype, totalRows)
		}
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
						if err := appendConcatValue(resultSeries[col], resultDTypes[col], val); err != nil {
							df.RUnlock()
							return nil, fmt.Errorf("concat: column '%s' row %d: %w", col, r, err)
						}
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

			// Create new series for this column. Each column comes from exactly
			// one DataFrame here, so its dtype carries over directly.
			series := df.Columns[col]
			dtype := concatDType([]reflect.Type{series.DType()})
			var newSeries collection.Series
			if dtype == nil {
				newSeries = collection.NewAnySeries(len(resultIndex))
			} else {
				newSeries = collection.NewSeriesOfType(dtype, len(resultIndex))
			}

			for _, idx := range resultIndex {
				if rowPos, ok := indexSets[dfIdx][idx]; ok && rowPos < series.Len() {
					if series.IsNull(rowPos) {
						newSeries.AppendNull()
					} else {
						val, _ := series.At(rowPos)
						if err := appendConcatValue(newSeries, dtype, val); err != nil {
							df.RUnlock()
							return nil, fmt.Errorf("concat: column '%s': %w", col, err)
						}
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

// concatDType resolves the dtype of a concatenated column from the dtypes of the
// inputs that contribute to it.
//
// When every input agrees, that dtype is kept, so stacking two Int64 columns
// yields an Int64 column rather than an untyped one. A mix of integer and
// floating-point columns widens to float64, mirroring pandas. Any other
// disagreement (say string and int64) is a genuine conflict and returns nil,
// meaning the caller should fall back to an untyped (any) Series.
//
// A nil entry in types, or an interface-kinded one (an AnySeries), also yields
// nil: an untyped input cannot constrain the result.
func concatDType(types []reflect.Type) reflect.Type {
	if len(types) == 0 {
		return nil
	}

	var (
		first    reflect.Type
		allSame  = true
		allNum   = true
		anyFloat bool
	)

	for i, t := range types {
		// An untyped input cannot constrain the result.
		if t == nil || t.Kind() == reflect.Interface {
			return nil
		}
		if i == 0 {
			first = t
		} else if t != first {
			allSame = false
		}
		switch t.Kind() {
		case reflect.Float64:
			anyFloat = true
		case reflect.Int64, reflect.Int:
			// integer, no flag needed
		default:
			allNum = false
		}
	}

	switch {
	case allSame:
		return first
	case allNum && anyFloat:
		// Mixed integer and floating-point columns widen, mirroring pandas.
		return reflect.TypeOf(float64(0))
	case allNum:
		// Different integer widths (int vs int64) all back an Int64Series.
		return reflect.TypeOf(int64(0))
	default:
		// Genuine conflict, e.g. string and int64.
		return nil
	}
}

// appendConcatValue appends v to s, widening integers to float64 when the target
// column resolved to a float dtype (see concatDType).
func appendConcatValue(s collection.Series, dtype reflect.Type, v any) error {
	if dtype != nil {
		switch dtype.Kind() {
		case reflect.Float64:
			if f, ok := toFloat64(v); ok {
				return s.Append(f)
			}
		case reflect.Int64, reflect.Int:
			switch v.(type) {
			case int, int8, int16, int32, int64:
				return s.Append(toInt64(v))
			}
		}
	}
	return s.Append(v)
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
