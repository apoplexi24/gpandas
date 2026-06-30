package dataframe

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// SetMultiIndex returns a new DataFrame whose index is a composite key built by
// joining the values of the given columns with the separator (default "_").
//
// Unlike pandas' true hierarchical MultiIndex, GPandas represents the composite
// index as a single joined string label per row. The source columns are kept in
// the DataFrame so the operation is non-destructive and reversible.
//
// This is analogous to df.set_index([...]) with a flattened label.
//
// Example:
//
//	indexed, err := df.SetMultiIndex([]string{"Country", "City"})
//	// index labels like "USA_NYC", "USA_LA", ...
func (df *DataFrame) SetMultiIndex(columns []string, sep ...string) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("SetMultiIndex: DataFrame is nil")
	}
	if len(columns) == 0 {
		return nil, errors.New("SetMultiIndex: at least one column is required")
	}

	separator := "_"
	if len(sep) > 0 {
		separator = sep[0]
	}

	df.RLock()
	defer df.RUnlock()

	for _, c := range columns {
		if _, ok := df.Columns[c]; !ok {
			return nil, fmt.Errorf("SetMultiIndex: column '%s' not found", c)
		}
	}

	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	newIndex := make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		parts := make([]string, len(columns))
		for j, c := range columns {
			series := df.Columns[c]
			if series.IsNull(i) {
				parts[j] = "null"
				continue
			}
			v, _ := series.At(i)
			parts[j] = fmt.Sprintf("%v", v)
		}
		newIndex[i] = strings.Join(parts, separator)
	}

	// Share column Series (zero-copy); only the index changes.
	newCols := make(map[string]collection.Series, len(df.Columns))
	for name, s := range df.Columns {
		newCols[name] = s
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), df.ColumnOrder...),
		Index:       newIndex,
	}, nil
}

// Stack reshapes the DataFrame from wide to long format, producing a DataFrame
// with three columns: "index" (the original row label), "variable" (the former
// column name), and "value" (the cell value). Each non-null cell becomes one row.
//
// This is analogous to df.stack() (with the default dropna behaviour).
//
// Example:
//
//	long, err := df.Stack()
func (df *DataFrame) Stack() (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("Stack: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	indexVals := make([]any, 0, rowCount*len(df.ColumnOrder))
	variableVals := make([]any, 0, rowCount*len(df.ColumnOrder))
	valueVals := make([]any, 0, rowCount*len(df.ColumnOrder))

	for i := 0; i < rowCount; i++ {
		label := fmt.Sprintf("%d", i)
		if i < len(df.Index) {
			label = df.Index[i]
		}
		for _, colName := range df.ColumnOrder {
			series := df.Columns[colName]
			if series.IsNull(i) {
				continue // drop nulls, matching pandas default
			}
			v, err := series.At(i)
			if err != nil {
				return nil, fmt.Errorf("Stack: column '%s' row %d: %w", colName, i, err)
			}
			indexVals = append(indexVals, label)
			variableVals = append(variableVals, colName)
			valueVals = append(valueVals, v)
		}
	}

	indexSeries, _ := seriesFromAnyValues(indexVals)
	variableSeries, _ := seriesFromAnyValues(variableVals)
	valueSeries, err := seriesFromAnyValues(valueVals)
	if err != nil {
		return nil, fmt.Errorf("Stack: building value column: %w", err)
	}

	n := len(valueVals)
	idx := make([]string, n)
	for i := 0; i < n; i++ {
		idx[i] = fmt.Sprintf("%d", i)
	}

	return &DataFrame{
		Columns: map[string]collection.Series{
			"index":    indexSeries,
			"variable": variableSeries,
			"value":    valueSeries,
		},
		ColumnOrder: []string{"index", "variable", "value"},
		Index:       idx,
	}, nil
}

// Unstack reshapes a long-format DataFrame (as produced by Stack) back to wide
// format. It expects columns named "index", "variable", and "value": "index"
// values become row labels, distinct "variable" values become columns (sorted),
// and "value" fills the cells. Missing combinations are null.
//
// This is analogous to df.unstack() as the inverse of Stack.
//
// Example:
//
//	wide, err := long.Unstack()
func (df *DataFrame) Unstack() (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("Unstack: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	for _, required := range []string{"index", "variable", "value"} {
		if _, ok := df.Columns[required]; !ok {
			return nil, fmt.Errorf("Unstack: required column '%s' not found (expects the output of Stack)", required)
		}
	}

	indexSeries := df.Columns["index"]
	variableSeries := df.Columns["variable"]
	valueSeries := df.Columns["value"]
	rowCount := indexSeries.Len()

	// Collect distinct row labels (first-appearance order) and column names (sorted).
	rowOrder := make([]string, 0)
	rowSeen := make(map[string]bool)
	colSeen := make(map[string]bool)
	// cell maps "rowLabel\x00colName" -> value
	cell := make(map[string]any)

	for i := 0; i < rowCount; i++ {
		rv, _ := indexSeries.At(i)
		cv, _ := variableSeries.At(i)
		rowLabel := fmt.Sprintf("%v", rv)
		colName := fmt.Sprintf("%v", cv)

		if !rowSeen[rowLabel] {
			rowSeen[rowLabel] = true
			rowOrder = append(rowOrder, rowLabel)
		}
		colSeen[colName] = true

		var val any
		if !valueSeries.IsNull(i) {
			val, _ = valueSeries.At(i)
		}
		cell[rowLabel+"\x00"+colName] = val
	}

	colOrder := make([]string, 0, len(colSeen))
	for c := range colSeen {
		colOrder = append(colOrder, c)
	}
	sort.Strings(colOrder)

	// Build each output column.
	newCols := make(map[string]collection.Series, len(colOrder))
	for _, colName := range colOrder {
		values := make([]any, len(rowOrder))
		for r, rowLabel := range rowOrder {
			if v, ok := cell[rowLabel+"\x00"+colName]; ok {
				values[r] = v
			} else {
				values[r] = nil
			}
		}
		s, err := seriesFromAnyValues(values)
		if err != nil {
			return nil, fmt.Errorf("Unstack: building column '%s': %w", colName, err)
		}
		newCols[colName] = s
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: colOrder,
		Index:       rowOrder,
	}, nil
}
