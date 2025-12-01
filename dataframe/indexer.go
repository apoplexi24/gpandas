package dataframe

import (
	"errors"
	"fmt"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// LocIndexer provides label-based indexing for DataFrames
type LocIndexer struct {
	df *DataFrame
}

// iLocIndexer provides integer position-based indexing for DataFrames
type iLocIndexer struct {
	df *DataFrame
}

// At returns a single value using row label and column name
func (l *LocIndexer) At(rowLabel string, columnName string) (any, error) {
	if l.df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	l.df.RLock()
	defer l.df.RUnlock()

	// Find row index from label
	rowIdx := -1
	for i, label := range l.df.Index {
		if label == rowLabel {
			rowIdx = i
			break
		}
	}
	if rowIdx == -1 {
		return nil, fmt.Errorf("row label '%s' not found in index", rowLabel)
	}

	// Get column
	series, ok := l.df.Columns[columnName]
	if !ok {
		return nil, fmt.Errorf("column '%s' not found", columnName)
	}

	// Get value at row index
	return series.At(rowIdx)
}

// IsNullAt returns whether the value at the given row label and column name is null
func (l *LocIndexer) IsNullAt(rowLabel string, columnName string) (bool, error) {
	if l.df == nil {
		return false, errors.New("DataFrame is nil")
	}

	l.df.RLock()
	defer l.df.RUnlock()

	// Find row index from label
	rowIdx := -1
	for i, label := range l.df.Index {
		if label == rowLabel {
			rowIdx = i
			break
		}
	}
	if rowIdx == -1 {
		return false, fmt.Errorf("row label '%s' not found in index", rowLabel)
	}

	// Get column
	series, ok := l.df.Columns[columnName]
	if !ok {
		return false, fmt.Errorf("column '%s' not found", columnName)
	}

	return series.IsNull(rowIdx), nil
}

// Row returns a single row as a new DataFrame using row label
func (l *LocIndexer) Row(rowLabel string) (*DataFrame, error) {
	if l.df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	l.df.RLock()
	defer l.df.RUnlock()

	// Find row index
	rowIdx := -1
	for i, label := range l.df.Index {
		if label == rowLabel {
			rowIdx = i
			break
		}
	}
	if rowIdx == -1 {
		return nil, fmt.Errorf("row label '%s' not found in index", rowLabel)
	}

	// Create new DataFrame with single row, preserving null masks
	newCols := make(map[string]collection.Series, len(l.df.ColumnOrder))
	for _, colName := range l.df.ColumnOrder {
		series := l.df.Columns[colName]
		isNull := series.IsNull(rowIdx)
		val, err := series.At(rowIdx)
		if err != nil {
			return nil, fmt.Errorf("error accessing column %s: %w", colName, err)
		}

		// Create new series with single value, preserving null status
		var newSeries collection.Series
		if isNull {
			newSeries = collection.NewSeriesOfType(series.DType(), 1)
			newSeries.AppendNull()
		} else {
			newSeries = collection.NewSeriesOfType(series.DType(), 1)
			if err := newSeries.Append(val); err != nil {
				return nil, fmt.Errorf("error creating series for column %s: %w", colName, err)
			}
		}
		newCols[colName] = newSeries
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), l.df.ColumnOrder...),
		Index:       []string{rowLabel},
	}, nil
}

// Rows returns multiple rows as a new DataFrame using row labels
func (l *LocIndexer) Rows(rowLabels []string) (*DataFrame, error) {
	if l.df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	l.df.RLock()
	defer l.df.RUnlock()

	// Find row indices
	rowIndices := make([]int, 0, len(rowLabels))
	for _, label := range rowLabels {
		found := false
		for i, idxLabel := range l.df.Index {
			if idxLabel == label {
				rowIndices = append(rowIndices, i)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("row label '%s' not found in index", label)
		}
	}

	// Create new DataFrame with selected rows, preserving null masks
	newCols := make(map[string]collection.Series, len(l.df.ColumnOrder))
	for _, colName := range l.df.ColumnOrder {
		series := l.df.Columns[colName]
		newSeries := collection.NewSeriesOfType(series.DType(), len(rowIndices))

		for _, rowIdx := range rowIndices {
			if series.IsNull(rowIdx) {
				newSeries.AppendNull()
			} else {
				val, err := series.At(rowIdx)
				if err != nil {
					return nil, fmt.Errorf("error accessing column %s at row %d: %w", colName, rowIdx, err)
				}
				if err := newSeries.Append(val); err != nil {
					return nil, fmt.Errorf("error appending to series for column %s: %w", colName, err)
				}
			}
		}
		newCols[colName] = newSeries
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), l.df.ColumnOrder...),
		Index:       append([]string(nil), rowLabels...),
	}, nil
}

// Col returns a single column as a Series reference
func (l *LocIndexer) Col(columnName string) (collection.Series, error) {
	if l.df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	l.df.RLock()
	defer l.df.RUnlock()

	series, ok := l.df.Columns[columnName]
	if !ok {
		return nil, fmt.Errorf("column '%s' not found", columnName)
	}

	return series, nil
}

// Cols returns multiple columns as a new DataFrame
func (l *LocIndexer) Cols(columnNames []string) (*DataFrame, error) {
	if l.df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	l.df.RLock()
	defer l.df.RUnlock()

	// Validate all columns exist
	for _, colName := range columnNames {
		if _, ok := l.df.Columns[colName]; !ok {
			return nil, fmt.Errorf("column '%s' not found", colName)
		}
	}

	// Create new DataFrame with selected columns (zero-copy - just reference same Series)
	newCols := make(map[string]collection.Series, len(columnNames))
	for _, colName := range columnNames {
		newCols[colName] = l.df.Columns[colName]
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), columnNames...),
		Index:       append([]string(nil), l.df.Index...),
	}, nil
}

// At returns a single value using row and column positions
func (il *iLocIndexer) At(rowPos int, colPos int) (any, error) {
	if il.df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	il.df.RLock()
	defer il.df.RUnlock()

	// Validate column position
	if colPos < 0 || colPos >= len(il.df.ColumnOrder) {
		return nil, fmt.Errorf("column position %d out of range [0, %d)", colPos, len(il.df.ColumnOrder))
	}

	colName := il.df.ColumnOrder[colPos]
	series := il.df.Columns[colName]

	// Validate row position
	if rowPos < 0 || rowPos >= series.Len() {
		return nil, fmt.Errorf("row position %d out of range [0, %d)", rowPos, series.Len())
	}

	return series.At(rowPos)
}

// IsNullAt returns whether the value at the given row and column positions is null
func (il *iLocIndexer) IsNullAt(rowPos int, colPos int) (bool, error) {
	if il.df == nil {
		return false, errors.New("DataFrame is nil")
	}

	il.df.RLock()
	defer il.df.RUnlock()

	// Validate column position
	if colPos < 0 || colPos >= len(il.df.ColumnOrder) {
		return false, fmt.Errorf("column position %d out of range [0, %d)", colPos, len(il.df.ColumnOrder))
	}

	colName := il.df.ColumnOrder[colPos]
	series := il.df.Columns[colName]

	// Validate row position
	if rowPos < 0 || rowPos >= series.Len() {
		return false, fmt.Errorf("row position %d out of range [0, %d)", rowPos, series.Len())
	}

	return series.IsNull(rowPos), nil
}

// Row returns a single row at the given position as a new DataFrame
func (il *iLocIndexer) Row(rowPos int) (*DataFrame, error) {
	if il.df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	il.df.RLock()
	defer il.df.RUnlock()

	// Validate row position using first column
	if len(il.df.ColumnOrder) == 0 {
		return nil, errors.New("DataFrame has no columns")
	}

	rowCount := il.df.Columns[il.df.ColumnOrder[0]].Len()
	if rowPos < 0 || rowPos >= rowCount {
		return nil, fmt.Errorf("row position %d out of range [0, %d)", rowPos, rowCount)
	}

	// Create new DataFrame with single row, preserving null masks
	newCols := make(map[string]collection.Series, len(il.df.ColumnOrder))
	for _, colName := range il.df.ColumnOrder {
		series := il.df.Columns[colName]
		isNull := series.IsNull(rowPos)
		val, err := series.At(rowPos)
		if err != nil {
			return nil, fmt.Errorf("error accessing column %s: %w", colName, err)
		}

		// Create new series with single value, preserving null status
		var newSeries collection.Series
		if isNull {
			newSeries = collection.NewSeriesOfType(series.DType(), 1)
			newSeries.AppendNull()
		} else {
			newSeries = collection.NewSeriesOfType(series.DType(), 1)
			if err := newSeries.Append(val); err != nil {
				return nil, fmt.Errorf("error creating series for column %s: %w", colName, err)
			}
		}
		newCols[colName] = newSeries
	}

	rowLabel := il.df.Index[rowPos]
	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), il.df.ColumnOrder...),
		Index:       []string{rowLabel},
	}, nil
}

// Rows returns multiple rows at the given positions as a new DataFrame
func (il *iLocIndexer) Rows(rowPositions []int) (*DataFrame, error) {
	if il.df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	il.df.RLock()
	defer il.df.RUnlock()

	if len(il.df.ColumnOrder) == 0 {
		return nil, errors.New("DataFrame has no columns")
	}

	rowCount := il.df.Columns[il.df.ColumnOrder[0]].Len()

	// Validate all row positions
	for _, pos := range rowPositions {
		if pos < 0 || pos >= rowCount {
			return nil, fmt.Errorf("row position %d out of range [0, %d)", pos, rowCount)
		}
	}

	// Create new DataFrame with selected rows, preserving null masks
	newCols := make(map[string]collection.Series, len(il.df.ColumnOrder))
	for _, colName := range il.df.ColumnOrder {
		series := il.df.Columns[colName]
		newSeries := collection.NewSeriesOfType(series.DType(), len(rowPositions))

		for _, rowPos := range rowPositions {
			if series.IsNull(rowPos) {
				newSeries.AppendNull()
			} else {
				val, err := series.At(rowPos)
				if err != nil {
					return nil, fmt.Errorf("error accessing column %s at row %d: %w", colName, rowPos, err)
				}
				if err := newSeries.Append(val); err != nil {
					return nil, fmt.Errorf("error appending to series for column %s: %w", colName, err)
				}
			}
		}
		newCols[colName] = newSeries
	}

	// Build index for selected rows
	newIndex := make([]string, len(rowPositions))
	for i, pos := range rowPositions {
		newIndex[i] = il.df.Index[pos]
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), il.df.ColumnOrder...),
		Index:       newIndex,
	}, nil
}

// Range returns rows in the range [start, end) as a new DataFrame
func (il *iLocIndexer) Range(start int, end int) (*DataFrame, error) {
	if il.df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	il.df.RLock()
	defer il.df.RUnlock()

	if len(il.df.ColumnOrder) == 0 {
		return nil, errors.New("DataFrame has no columns")
	}

	rowCount := il.df.Columns[il.df.ColumnOrder[0]].Len()

	// Validate range
	if start < 0 || start > rowCount {
		return nil, fmt.Errorf("start position %d out of range [0, %d]", start, rowCount)
	}
	if end < start || end > rowCount {
		return nil, fmt.Errorf("end position %d out of range [%d, %d]", end, start, rowCount)
	}

	// Create positions slice
	rowPositions := make([]int, end-start)
	for i := range rowPositions {
		rowPositions[i] = start + i
	}

	// Create new DataFrame with selected rows, preserving null masks
	newCols := make(map[string]collection.Series, len(il.df.ColumnOrder))
	for _, colName := range il.df.ColumnOrder {
		series := il.df.Columns[colName]
		newSeries := collection.NewSeriesOfType(series.DType(), len(rowPositions))

		for _, rowPos := range rowPositions {
			if series.IsNull(rowPos) {
				newSeries.AppendNull()
			} else {
				val, err := series.At(rowPos)
				if err != nil {
					return nil, fmt.Errorf("error accessing column %s at row %d: %w", colName, rowPos, err)
				}
				if err := newSeries.Append(val); err != nil {
					return nil, fmt.Errorf("error appending to series for column %s: %w", colName, err)
				}
			}
		}
		newCols[colName] = newSeries
	}

	// Build index for selected rows
	newIndex := make([]string, len(rowPositions))
	for i, pos := range rowPositions {
		newIndex[i] = il.df.Index[pos]
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), il.df.ColumnOrder...),
		Index:       newIndex,
	}, nil
}

// Col returns a single column at the given position as a Series reference
func (il *iLocIndexer) Col(colPos int) (collection.Series, error) {
	if il.df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	il.df.RLock()
	defer il.df.RUnlock()

	if colPos < 0 || colPos >= len(il.df.ColumnOrder) {
		return nil, fmt.Errorf("column position %d out of range [0, %d)", colPos, len(il.df.ColumnOrder))
	}

	colName := il.df.ColumnOrder[colPos]
	return il.df.Columns[colName], nil
}

// Cols returns multiple columns at the given positions as a new DataFrame
func (il *iLocIndexer) Cols(colPositions []int) (*DataFrame, error) {
	if il.df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	il.df.RLock()
	defer il.df.RUnlock()

	// Validate all column positions
	for _, pos := range colPositions {
		if pos < 0 || pos >= len(il.df.ColumnOrder) {
			return nil, fmt.Errorf("column position %d out of range [0, %d)", pos, len(il.df.ColumnOrder))
		}
	}

	// Build column names from positions
	columnNames := make([]string, len(colPositions))
	for i, pos := range colPositions {
		columnNames[i] = il.df.ColumnOrder[pos]
	}

	// Create new DataFrame with selected columns (zero-copy)
	newCols := make(map[string]collection.Series, len(columnNames))
	for _, colName := range columnNames {
		newCols[colName] = il.df.Columns[colName]
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: columnNames,
		Index:       append([]string(nil), il.df.Index...),
	}, nil
}
