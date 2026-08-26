package gpandas

import (
	"github.com/apoplexi24/gpandas/dataframe"
)

// The concat configuration types are aliases of their dataframe counterparts, so
// gpandas.ConcatOptions and dataframe.ConcatOptions are the same type and the
// concatenation logic lives in exactly one place.

// ConcatAxis specifies the axis along which to concatenate.
type ConcatAxis = dataframe.ConcatAxis

const (
	// AxisIndex (0) concatenates along rows (stacking DataFrames vertically).
	AxisIndex = dataframe.AxisIndex
	// AxisColumns (1) concatenates along columns (joining DataFrames horizontally).
	AxisColumns = dataframe.AxisColumns
)

// ConcatJoin specifies how to handle indexes on the non-concatenation axis.
type ConcatJoin = dataframe.ConcatJoin

const (
	// JoinOuter takes the union of indexes (all columns/rows, with nulls for missing).
	JoinOuter = dataframe.JoinOuter
	// JoinInner takes the intersection of indexes (only common columns/rows).
	JoinInner = dataframe.JoinInner
)

// ConcatOptions configures the behavior of the Concat function.
type ConcatOptions = dataframe.ConcatOptions

// DefaultConcatOptions returns the default options for Concat.
func DefaultConcatOptions() ConcatOptions {
	return dataframe.DefaultConcatOptions()
}

// Concat concatenates pandas objects along a particular axis.
//
// This function mirrors the behavior of pandas.concat for ease of switching
// from Python to Go for data scientists.
//
// Parameters:
//   - objs: A slice of DataFrame pointers to concatenate. Nil DataFrames are skipped.
//   - opts: Optional ConcatOptions. If not provided, defaults are used.
//
// Returns:
//   - A new DataFrame containing the concatenated data, or an error if the operation fails.
//
// Column types are preserved: when every input agrees on a column's dtype the
// result keeps it, a mix of integer and floating-point columns widens to float64,
// and only a genuine type conflict falls back to an untyped column.
//
// Example:
//
//	df1 := &dataframe.DataFrame{Columns: map[string]Series{"A": ...}, ColumnOrder: []string{"A"}}
//	df2 := &dataframe.DataFrame{Columns: map[string]Series{"A": ...}, ColumnOrder: []string{"A"}}
//	result, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2})
//	// result contains all rows from df1 followed by rows from df2
//
//	// With options:
//	result, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2}, gpandas.ConcatOptions{Axis: gpandas.AxisColumns, Join: gpandas.JoinInner})
func Concat(objs []*dataframe.DataFrame, opts ...ConcatOptions) (*dataframe.DataFrame, error) {
	return dataframe.Concat(objs, opts...)
}
