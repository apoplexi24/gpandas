package dataframe

import (
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// Apply returns a new DataFrame in which the values of the given column have
// been transformed element-wise by fn. The function receives each cell value
// (nil for nulls) and returns the new value (return nil to produce a null).
//
// The resulting column's type is inferred from the returned values: if all
// non-nil results share a single supported kind (float64, int/int64, string,
// bool) a typed Series is produced; otherwise an untyped (any) Series is used.
//
// Other columns are shared by reference (zero-copy) with the original DataFrame.
//
// This is analogous to df["col"].apply(fn) in pandas.
//
// Parameters:
//   - column: the column to transform
//   - fn: element-wise transformation function
//
// Returns:
//   - *DataFrame: a new DataFrame with the transformed column
//   - error: nil if successful, otherwise an error
//
// Example:
//
//	// Give everyone a 10% raise
//	result, err := df.Apply("Salary", func(v any) any {
//	    return v.(float64) * 1.1
//	})
func (df *DataFrame) Apply(column string, fn func(any) any) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("Apply: DataFrame is nil")
	}
	if fn == nil {
		return nil, errors.New("Apply: fn must not be nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("Apply: column '%s' not found", column)
	}

	rowCount := series.Len()
	results := make([]any, rowCount)
	for i := 0; i < rowCount; i++ {
		var in any
		if !series.IsNull(i) {
			v, err := series.At(i)
			if err != nil {
				return nil, fmt.Errorf("Apply: error reading row %d: %w", i, err)
			}
			in = v
		}
		results[i] = fn(in)
	}

	newSeries, err := seriesFromAnyValues(results)
	if err != nil {
		return nil, fmt.Errorf("Apply: failed building result column: %w", err)
	}

	newCols := make(map[string]collection.Series, len(df.Columns))
	for name, s := range df.Columns {
		newCols[name] = s
	}
	newCols[column] = newSeries

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), df.ColumnOrder...),
		Index:       append([]string(nil), df.Index...),
	}, nil
}

// Map returns a new DataFrame in which values of the given column are replaced
// according to the provided mapping. Values present as keys in the mapping are
// substituted with the mapped value; values not present are kept unchanged.
// Null values remain null.
//
// This is analogous to df["col"].map(mapping) / replace in pandas.
//
// Parameters:
//   - column: the column whose values to remap
//   - mapping: a map of original value -> replacement value
//
// Returns:
//   - *DataFrame: a new DataFrame with the remapped column
//   - error: nil if successful, otherwise an error
//
// Example:
//
//	// Convert "Y"/"N" flags to booleans
//	result, err := df.Map("Active", map[any]any{"Y": true, "N": false})
func (df *DataFrame) Map(column string, mapping map[any]any) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("Map: DataFrame is nil")
	}
	if mapping == nil {
		return nil, errors.New("Map: mapping must not be nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("Map: column '%s' not found", column)
	}

	rowCount := series.Len()
	results := make([]any, rowCount)
	for i := 0; i < rowCount; i++ {
		if series.IsNull(i) {
			results[i] = nil
			continue
		}
		val, err := series.At(i)
		if err != nil {
			return nil, fmt.Errorf("Map: error reading row %d: %w", i, err)
		}
		if replacement, found := mapping[val]; found {
			results[i] = replacement
		} else {
			results[i] = val
		}
	}

	newSeries, err := seriesFromAnyValues(results)
	if err != nil {
		return nil, fmt.Errorf("Map: failed building result column: %w", err)
	}

	newCols := make(map[string]collection.Series, len(df.Columns))
	for name, s := range df.Columns {
		newCols[name] = s
	}
	newCols[column] = newSeries

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), df.ColumnOrder...),
		Index:       append([]string(nil), df.Index...),
	}, nil
}

// ApplyRow returns a new DataFrame produced by applying fn to every row. The
// function receives a map of column name to value (nil for nulls) and returns a
// map describing the transformed row. Keys present in the original ColumnOrder
// keep their position; any new keys introduced by fn are appended in sorted
// order. Missing keys for a given row produce a null in that cell.
//
// This is analogous to df.apply(fn, axis=1) in pandas.
//
// Parameters:
//   - fn: row-wise transformation function
//
// Returns:
//   - *DataFrame: a new DataFrame built from the transformed rows
//   - error: nil if successful, otherwise an error
//
// Example:
//
//	// Add a derived "Tax" column computed from Salary
//	result, err := df.ApplyRow(func(row map[string]any) map[string]any {
//	    row["Tax"] = row["Salary"].(float64) * 0.3
//	    return row
//	})
func (df *DataFrame) ApplyRow(fn func(map[string]any) map[string]any) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("ApplyRow: DataFrame is nil")
	}
	if fn == nil {
		return nil, errors.New("ApplyRow: fn must not be nil")
	}

	df.RLock()

	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	// Track output column order: existing columns first, then new keys (sorted).
	outOrder := append([]string(nil), df.ColumnOrder...)
	known := make(map[string]bool, len(outOrder))
	for _, c := range outOrder {
		known[c] = true
	}

	// Collect per-column result values.
	colValues := make(map[string][]any)
	for _, c := range outOrder {
		colValues[c] = make([]any, rowCount)
	}

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
				df.RUnlock()
				return nil, fmt.Errorf("ApplyRow: error reading column '%s' row %d: %w", colName, i, err)
			}
			row[colName] = val
		}

		out := fn(row)

		// Register any new keys produced by fn.
		for k := range out {
			if !known[k] {
				known[k] = true
				outOrder = append(outOrder, k)
				colValues[k] = make([]any, rowCount)
			}
		}

		// Record values for all known output columns for this row.
		for _, c := range outOrder {
			colValues[c][i] = out[c] // missing key yields nil
		}
	}

	df.RUnlock()

	// Sort newly-added keys (those after the original ColumnOrder) for determinism.
	if len(outOrder) > len(df.ColumnOrder) {
		added := outOrder[len(df.ColumnOrder):]
		sort.Strings(added)
	}

	newCols := make(map[string]collection.Series, len(outOrder))
	for _, c := range outOrder {
		s, err := seriesFromAnyValues(colValues[c])
		if err != nil {
			return nil, fmt.Errorf("ApplyRow: failed building column '%s': %w", c, err)
		}
		newCols[c] = s
	}

	// Build index (default if lengths drift).
	index := make([]string, rowCount)
	if len(df.Index) == rowCount {
		copy(index, df.Index)
	} else {
		for i := 0; i < rowCount; i++ {
			index[i] = fmt.Sprintf("%d", i)
		}
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: outOrder,
		Index:       index,
	}, nil
}

// seriesFromAnyValues builds a Series from a slice of values, inferring a typed
// Series from the value kinds present. Mixed integer and floating-point values
// are promoted to a float64 Series (mirroring pandas). When the non-null values
// span otherwise-incompatible kinds (e.g. strings and numbers), or contain an
// unsupported type, an untyped (any) Series is used. nil values become nulls.
func seriesFromAnyValues(values []any) (collection.Series, error) {
	var hasFloat, hasInt, hasString, hasBool, hasOther, hasAny bool

	for _, v := range values {
		if v == nil {
			continue
		}
		hasAny = true
		switch normalizedKind(v) {
		case reflect.Float64:
			hasFloat = true
		case reflect.Int64:
			hasInt = true
		case reflect.String:
			hasString = true
		case reflect.Bool:
			hasBool = true
		default:
			hasOther = true
		}
	}

	// Count how many distinct (incompatible) categories are present. Integers
	// and floats are treated as a single "numeric" category that promotes to
	// float64 when mixed.
	numeric := hasFloat || hasInt
	categories := 0
	if numeric {
		categories++
	}
	if hasString {
		categories++
	}
	if hasBool {
		categories++
	}

	// Fall back to an untyped series when there are no values, an unsupported
	// type is present, or multiple incompatible categories are mixed.
	if !hasAny || hasOther || categories > 1 {
		return collection.NewAnySeriesFromData(values, nil)
	}

	n := len(values)
	mask := make([]bool, n)

	switch {
	case numeric && hasFloat:
		// Promote any integers to float64.
		data := make([]float64, n)
		for i, v := range values {
			if v == nil {
				mask[i] = true
				continue
			}
			f, _ := toFloat64(v)
			data[i] = f
		}
		return collection.NewFloat64SeriesFromData(data, mask)

	case numeric:
		// All-integer.
		data := make([]int64, n)
		for i, v := range values {
			if v == nil {
				mask[i] = true
				continue
			}
			data[i] = toInt64(v)
		}
		return collection.NewInt64SeriesFromData(data, mask)

	case hasString:
		data := make([]string, n)
		for i, v := range values {
			if v == nil {
				mask[i] = true
				continue
			}
			data[i] = v.(string)
		}
		return collection.NewStringSeriesFromData(data, mask)

	case hasBool:
		data := make([]bool, n)
		for i, v := range values {
			if v == nil {
				mask[i] = true
				continue
			}
			data[i] = v.(bool)
		}
		return collection.NewBoolSeriesFromData(data, mask)

	default:
		return collection.NewAnySeriesFromData(values, nil)
	}
}

// toInt64 converts any supported integer value to int64.
func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case int16:
		return int64(x)
	case int8:
		return int64(x)
	default:
		return 0
	}
}

// normalizedKind maps a value to one of the canonical Series kinds
// (Float64, Int64, String, Bool) or reflect.Invalid if unsupported.
func normalizedKind(v any) reflect.Kind {
	switch v.(type) {
	case float64, float32:
		return reflect.Float64
	case int, int64, int32, int16, int8:
		return reflect.Int64
	case string:
		return reflect.String
	case bool:
		return reflect.Bool
	default:
		return reflect.Invalid
	}
}
