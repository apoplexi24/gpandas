package dataframe

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// FillNA returns a new DataFrame with null values replaced by the given constant
// across all compatible columns.
//
// For each column, the fill value is coerced to the column's dtype (numeric
// values are converted between int and float as needed). Columns whose dtype is
// incompatible with the fill value are left unchanged. The original DataFrame is
// not modified.
//
// This is analogous to df.fillna(value) in pandas.
//
// Example:
//
//	filled, err := df.FillNA(0.0)
func (df *DataFrame) FillNA(value any) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("FillNA: DataFrame is nil")
	}
	if value == nil {
		return nil, errors.New("FillNA: fill value must not be nil")
	}

	df.RLock()
	defer df.RUnlock()

	newCols := make(map[string]collection.Series, len(df.Columns))
	for name, series := range df.Columns {
		coerced, ok := coerceForSeries(series, value)
		if !ok {
			// Incompatible type: keep the column as-is (zero-copy reference).
			newCols[name] = series
			continue
		}
		filled, err := fillSeries(series, coerced)
		if err != nil {
			return nil, fmt.Errorf("FillNA: column '%s': %w", name, err)
		}
		newCols[name] = filled
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), df.ColumnOrder...),
		Index:       append([]string(nil), df.Index...),
	}, nil
}

// FillNAColumn returns a new DataFrame with null values in a single column
// replaced by the given constant. Other columns are referenced unchanged.
//
// The value is coerced to the column's dtype; an error is returned if the value
// is incompatible with the column.
//
// Example:
//
//	filled, err := df.FillNAColumn("Score", 0.0)
func (df *DataFrame) FillNAColumn(column string, value any) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("FillNAColumn: DataFrame is nil")
	}
	if value == nil {
		return nil, errors.New("FillNAColumn: fill value must not be nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("FillNAColumn: column '%s' not found", column)
	}

	coerced, ok := coerceForSeries(series, value)
	if !ok {
		return nil, fmt.Errorf("FillNAColumn: value of type %T is incompatible with column '%s'", value, column)
	}
	filled, err := fillSeries(series, coerced)
	if err != nil {
		return nil, fmt.Errorf("FillNAColumn: column '%s': %w", column, err)
	}

	newCols := make(map[string]collection.Series, len(df.Columns))
	for name, s := range df.Columns {
		newCols[name] = s
	}
	newCols[column] = filled

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), df.ColumnOrder...),
		Index:       append([]string(nil), df.Index...),
	}, nil
}

// FillNAMethod returns a new DataFrame with null values filled by propagation.
//
// The method is "ffill" (forward fill: propagate the last valid value forward)
// or "bfill" (backward fill: propagate the next valid value backward). Leading
// nulls for ffill (and trailing nulls for bfill) that have no valid neighbour
// remain null.
//
// This is analogous to df.fillna(method="ffill"|"bfill") in pandas.
//
// Example:
//
//	filled, err := df.FillNAMethod("ffill")
func (df *DataFrame) FillNAMethod(method string) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("FillNAMethod: DataFrame is nil")
	}
	if method != "ffill" && method != "bfill" {
		return nil, fmt.Errorf("FillNAMethod: method must be 'ffill' or 'bfill', got '%s'", method)
	}

	df.RLock()
	defer df.RUnlock()

	newCols := make(map[string]collection.Series, len(df.Columns))
	for name, series := range df.Columns {
		clone, err := series.Slice(0, series.Len())
		if err != nil {
			return nil, fmt.Errorf("FillNAMethod: column '%s': %w", name, err)
		}
		n := clone.Len()

		if method == "ffill" {
			var last any
			haveLast := false
			for i := 0; i < n; i++ {
				if clone.IsNull(i) {
					if haveLast {
						if err := clone.Set(i, last); err != nil {
							return nil, fmt.Errorf("FillNAMethod: column '%s': %w", name, err)
						}
					}
				} else {
					v, _ := clone.At(i)
					last = v
					haveLast = true
				}
			}
		} else { // bfill
			var next any
			haveNext := false
			for i := n - 1; i >= 0; i-- {
				if clone.IsNull(i) {
					if haveNext {
						if err := clone.Set(i, next); err != nil {
							return nil, fmt.Errorf("FillNAMethod: column '%s': %w", name, err)
						}
					}
				} else {
					v, _ := clone.At(i)
					next = v
					haveNext = true
				}
			}
		}
		newCols[name] = clone
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), df.ColumnOrder...),
		Index:       append([]string(nil), df.Index...),
	}, nil
}

// DropNA returns a new DataFrame with rows containing null values removed.
//
// Parameters:
//   - how: "any" (default) drops a row if any considered column is null;
//     "all" drops a row only if every considered column is null.
//   - subset: the columns to consider. If empty, all columns are considered.
//
// Index labels of the surviving rows are preserved.
//
// This is analogous to df.dropna(how=..., subset=...) in pandas.
//
// Example:
//
//	cleaned, err := df.DropNA("any", nil)
//	cleaned, err := df.DropNA("all", []string{"A", "B"})
func (df *DataFrame) DropNA(how string, subset []string) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("DropNA: DataFrame is nil")
	}
	if how == "" {
		how = "any"
	}
	if how != "any" && how != "all" {
		return nil, fmt.Errorf("DropNA: how must be 'any' or 'all', got '%s'", how)
	}

	df.RLock()

	// Resolve considered columns.
	cols := subset
	if len(cols) == 0 {
		cols = df.ColumnOrder
	} else {
		for _, c := range cols {
			if _, ok := df.Columns[c]; !ok {
				df.RUnlock()
				return nil, fmt.Errorf("DropNA: column '%s' not found", c)
			}
		}
	}

	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	keep := make([]int, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		nullCount := 0
		for _, c := range cols {
			if df.Columns[c].IsNull(i) {
				nullCount++
			}
		}
		drop := false
		switch how {
		case "any":
			drop = nullCount > 0
		case "all":
			drop = nullCount == len(cols)
		}
		if !drop {
			keep = append(keep, i)
		}
	}

	df.RUnlock()

	return df.Slice(keep)
}

// IsNA returns a new DataFrame of booleans where each cell is true if the
// corresponding cell in the original DataFrame is null.
//
// This is analogous to df.isna() in pandas.
func (df *DataFrame) IsNA() *DataFrame {
	return df.nullMaskFrame(false)
}

// NotNA returns a new DataFrame of booleans where each cell is true if the
// corresponding cell in the original DataFrame is NOT null.
//
// This is analogous to df.notna() in pandas.
func (df *DataFrame) NotNA() *DataFrame {
	return df.nullMaskFrame(true)
}

// nullMaskFrame builds a boolean DataFrame from the null mask. When negate is
// true the values are inverted (used by NotNA).
func (df *DataFrame) nullMaskFrame(negate bool) *DataFrame {
	if df == nil {
		return nil
	}

	df.RLock()
	defer df.RUnlock()

	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	newCols := make(map[string]collection.Series, len(df.Columns))
	for _, name := range df.ColumnOrder {
		series := df.Columns[name]
		data := make([]bool, rowCount)
		for i := 0; i < rowCount; i++ {
			isNull := series.IsNull(i)
			if negate {
				data[i] = !isNull
			} else {
				data[i] = isNull
			}
		}
		s, _ := collection.NewBoolSeriesFromData(data, nil)
		newCols[name] = s
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), df.ColumnOrder...),
		Index:       append([]string(nil), df.Index...),
	}
}

// fillSeries clones a series and replaces its null values with the given
// (already coerced) value.
func fillSeries(series collection.Series, value any) (collection.Series, error) {
	clone, err := series.Slice(0, series.Len())
	if err != nil {
		return nil, err
	}
	n := clone.Len()
	for i := 0; i < n; i++ {
		if clone.IsNull(i) {
			if err := clone.Set(i, value); err != nil {
				return nil, err
			}
		}
	}
	return clone, nil
}

// coerceForSeries converts a fill value to the series' dtype where possible.
// It returns the coerced value and true on success, or (nil, false) if the value
// is incompatible with the column type. Untyped (any) series accept any value.
func coerceForSeries(series collection.Series, value any) (any, bool) {
	dt := series.DType()
	if dt == nil {
		return value, true
	}
	switch dt.Kind() {
	case reflect.Float64:
		if f, ok := toFloat64(value); ok {
			return f, true
		}
		return nil, false
	case reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
		switch value.(type) {
		case int, int64, int32, int16, int8:
			return toInt64(value), true
		case float64, float32:
			f, _ := toFloat64(value)
			return int64(f), true
		}
		return nil, false
	case reflect.String:
		if s, ok := value.(string); ok {
			return s, true
		}
		return nil, false
	case reflect.Bool:
		if b, ok := value.(bool); ok {
			return b, true
		}
		return nil, false
	default:
		// Interface / any series accept any value.
		return value, true
	}
}
