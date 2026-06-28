package dataframe

import (
	"errors"
	"fmt"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// Assign adds a new column to the DataFrame, or replaces an existing column with
// the same name. The operation modifies the DataFrame in place.
//
// The series length must match the number of rows in the DataFrame. When the
// DataFrame is empty (has no columns), the series defines the row count and a
// default integer index is created.
//
// This is analogous to df["new_col"] = series in pandas.
//
// Example:
//
//	col, _ := collection.NewFloat64SeriesFromData([]float64{1, 2, 3}, nil)
//	err := df.Assign("Score", col)
func (df *DataFrame) Assign(name string, series collection.Series) error {
	if df == nil {
		return errors.New("Assign: DataFrame is nil")
	}
	if series == nil {
		return errors.New("Assign: series must not be nil")
	}

	df.Lock()
	defer df.Unlock()

	if err := df.validateNewColumnLen(series.Len()); err != nil {
		return fmt.Errorf("Assign: %w", err)
	}

	if _, exists := df.Columns[name]; !exists {
		df.ColumnOrder = append(df.ColumnOrder, name)
	}
	df.Columns[name] = series

	df.ensureIndex(series.Len())
	return nil
}

// AssignFunc adds (or replaces) a column whose values are computed from each row.
// The function receives a map of column name to value (nulls as nil) and returns
// the value for the new column. The resulting column type is inferred from the
// returned values. The operation modifies the DataFrame in place.
//
// This is analogous to df.assign(new_col=lambda row: ...) in pandas.
//
// Example:
//
//	err := df.AssignFunc("Tax", func(row map[string]any) any {
//	    return row["Salary"].(float64) * 0.3
//	})
func (df *DataFrame) AssignFunc(name string, fn func(row map[string]any) any) error {
	if df == nil {
		return errors.New("AssignFunc: DataFrame is nil")
	}
	if fn == nil {
		return errors.New("AssignFunc: fn must not be nil")
	}

	df.Lock()
	defer df.Unlock()

	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	results := make([]any, rowCount)
	row := make(map[string]any, len(df.ColumnOrder))
	for i := 0; i < rowCount; i++ {
		for _, colName := range df.ColumnOrder {
			series := df.Columns[colName]
			if series.IsNull(i) {
				row[colName] = nil
				continue
			}
			val, err := series.At(i)
			if err != nil {
				return fmt.Errorf("AssignFunc: error reading column '%s' row %d: %w", colName, i, err)
			}
			row[colName] = val
		}
		results[i] = fn(row)
	}

	series, err := seriesFromAnyValues(results)
	if err != nil {
		return fmt.Errorf("AssignFunc: failed building column '%s': %w", name, err)
	}

	if _, exists := df.Columns[name]; !exists {
		df.ColumnOrder = append(df.ColumnOrder, name)
	}
	df.Columns[name] = series
	return nil
}

// Insert adds a new column at the given position in the column order. The
// operation modifies the DataFrame in place.
//
// loc must be in the range [0, number of columns]. An error is returned if a
// column with the same name already exists. The series length must match the
// number of rows.
//
// This is analogous to df.insert(loc, name, series) in pandas.
//
// Example:
//
//	col, _ := collection.NewInt64SeriesFromData([]int64{1, 2, 3}, nil)
//	err := df.Insert(0, "ID", col)
func (df *DataFrame) Insert(loc int, name string, series collection.Series) error {
	if df == nil {
		return errors.New("Insert: DataFrame is nil")
	}
	if series == nil {
		return errors.New("Insert: series must not be nil")
	}

	df.Lock()
	defer df.Unlock()

	if _, exists := df.Columns[name]; exists {
		return fmt.Errorf("Insert: column '%s' already exists", name)
	}
	if loc < 0 || loc > len(df.ColumnOrder) {
		return fmt.Errorf("Insert: loc %d out of range [0, %d]", loc, len(df.ColumnOrder))
	}
	if err := df.validateNewColumnLen(series.Len()); err != nil {
		return fmt.Errorf("Insert: %w", err)
	}

	// Insert into ColumnOrder at loc.
	newOrder := make([]string, 0, len(df.ColumnOrder)+1)
	newOrder = append(newOrder, df.ColumnOrder[:loc]...)
	newOrder = append(newOrder, name)
	newOrder = append(newOrder, df.ColumnOrder[loc:]...)
	df.ColumnOrder = newOrder

	df.Columns[name] = series
	df.ensureIndex(series.Len())
	return nil
}

// validateNewColumnLen checks that a new column's length matches the existing
// row count. When the DataFrame has no columns yet, any length is accepted.
func (df *DataFrame) validateNewColumnLen(length int) error {
	if len(df.ColumnOrder) == 0 {
		return nil
	}
	rowCount := df.Columns[df.ColumnOrder[0]].Len()
	if length != rowCount {
		return fmt.Errorf("length mismatch: column has %d rows, DataFrame has %d", length, rowCount)
	}
	return nil
}

// ensureIndex creates a default integer index when the current index does not
// match the row count (e.g. after adding the first column to an empty frame).
func (df *DataFrame) ensureIndex(rowCount int) {
	if len(df.Index) == rowCount {
		return
	}
	df.Index = make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		df.Index[i] = fmt.Sprintf("%d", i)
	}
}
