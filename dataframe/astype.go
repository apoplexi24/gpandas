package dataframe

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// AsType returns a new DataFrame with the given column converted to the target
// type. Other columns are referenced unchanged.
//
// The targetType is specified using one of the column type markers: FloatCol{},
// IntCol{}, StringCol{}, or BoolCol{}. String aliases ("float64", "int64",
// "string", "bool") are also accepted.
//
// Null values are preserved. Conversion rules:
//   - to float64: numbers convert directly; strings are parsed; bools become 1/0.
//   - to int64: ints convert directly; floats are truncated; strings are parsed;
//     bools become 1/0.
//   - to string: values are formatted with their default representation.
//   - to bool: bools pass through; "true"/"false"/"1"/"0" strings are parsed;
//     numbers are true when non-zero.
//
// An error is returned if a value cannot be converted to the target type.
//
// This is analogous to df["col"].astype(dtype) in pandas.
//
// Example:
//
//	typed, err := df.AsType("Age", dataframe.IntCol{})
func (df *DataFrame) AsType(column string, targetType any) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("AsType: DataFrame is nil")
	}

	kind, err := resolveTargetKind(targetType)
	if err != nil {
		return nil, fmt.Errorf("AsType: %w", err)
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("AsType: column '%s' not found", column)
	}

	converted, err := convertSeries(series, kind, column)
	if err != nil {
		return nil, err
	}

	newCols := make(map[string]collection.Series, len(df.Columns))
	for name, s := range df.Columns {
		newCols[name] = s
	}
	newCols[column] = converted

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), df.ColumnOrder...),
		Index:       append([]string(nil), df.Index...),
	}, nil
}

// DTypes returns a map of column name to its data type name (e.g. "float64",
// "int64", "string", "bool", or "any").
//
// This is analogous to df.dtypes in pandas.
func (df *DataFrame) DTypes() map[string]string {
	out := make(map[string]string)
	if df == nil {
		return out
	}
	df.RLock()
	defer df.RUnlock()
	for _, name := range df.ColumnOrder {
		out[name] = dtypeName(df.Columns[name].DType())
	}
	return out
}

// Info returns a human-readable summary of the DataFrame, including the row
// count, and each column's index, name, non-null count, and dtype.
//
// This is analogous to df.info() in pandas.
func (df *DataFrame) Info() string {
	if df == nil {
		return "DataFrame is nil"
	}

	df.RLock()
	defer df.RUnlock()

	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	var b strings.Builder
	fmt.Fprintf(&b, "DataFrame: %d rows x %d columns\n", rowCount, len(df.ColumnOrder))

	if len(df.ColumnOrder) == 0 {
		return b.String()
	}

	// Compute column widths for alignment.
	nameHeader := "Column"
	dtypeHeader := "Dtype"
	nameWidth := len(nameHeader)
	dtypeWidth := len(dtypeHeader)
	for _, name := range df.ColumnOrder {
		if len(name) > nameWidth {
			nameWidth = len(name)
		}
		if dn := dtypeName(df.Columns[name].DType()); len(dn) > dtypeWidth {
			dtypeWidth = len(dn)
		}
	}

	fmt.Fprintf(&b, " %-3s  %-*s  %-15s  %-*s\n", "#", nameWidth, nameHeader, "Non-Null Count", dtypeWidth, dtypeHeader)
	for i, name := range df.ColumnOrder {
		series := df.Columns[name]
		nonNull := series.Len() - series.NullCount()
		nonNullStr := fmt.Sprintf("%d non-null", nonNull)
		fmt.Fprintf(&b, " %-3d  %-*s  %-15s  %-*s\n", i, nameWidth, name, nonNullStr, dtypeWidth, dtypeName(series.DType()))
	}

	return b.String()
}

// resolveTargetKind maps a target type marker or string alias to a reflect.Kind.
func resolveTargetKind(targetType any) (reflect.Kind, error) {
	switch t := targetType.(type) {
	case FloatCol:
		return reflect.Float64, nil
	case IntCol:
		return reflect.Int64, nil
	case StringCol:
		return reflect.String, nil
	case BoolCol:
		return reflect.Bool, nil
	case string:
		switch strings.ToLower(t) {
		case "float64", "float", "float32":
			return reflect.Float64, nil
		case "int64", "int", "int32":
			return reflect.Int64, nil
		case "string", "str":
			return reflect.String, nil
		case "bool", "boolean":
			return reflect.Bool, nil
		default:
			return reflect.Invalid, fmt.Errorf("unsupported target type '%s'", t)
		}
	default:
		return reflect.Invalid, fmt.Errorf("unsupported target type %T (use FloatCol{}, IntCol{}, StringCol{}, BoolCol{}, or a string alias)", targetType)
	}
}

// convertSeries builds a new typed series by converting each value of the source
// series to the target kind. Null values are preserved.
func convertSeries(series collection.Series, kind reflect.Kind, column string) (collection.Series, error) {
	n := series.Len()
	mask := make([]bool, n)

	switch kind {
	case reflect.Float64:
		data := make([]float64, n)
		for i := 0; i < n; i++ {
			if series.IsNull(i) {
				mask[i] = true
				continue
			}
			val, _ := series.At(i)
			f, err := convertToFloat(val)
			if err != nil {
				return nil, fmt.Errorf("AsType: column '%s' row %d: %w", column, i, err)
			}
			data[i] = f
		}
		return collection.NewFloat64SeriesFromData(data, mask)

	case reflect.Int64:
		data := make([]int64, n)
		for i := 0; i < n; i++ {
			if series.IsNull(i) {
				mask[i] = true
				continue
			}
			val, _ := series.At(i)
			x, err := convertToInt(val)
			if err != nil {
				return nil, fmt.Errorf("AsType: column '%s' row %d: %w", column, i, err)
			}
			data[i] = x
		}
		return collection.NewInt64SeriesFromData(data, mask)

	case reflect.String:
		data := make([]string, n)
		for i := 0; i < n; i++ {
			if series.IsNull(i) {
				mask[i] = true
				continue
			}
			val, _ := series.At(i)
			data[i] = fmt.Sprintf("%v", val)
		}
		return collection.NewStringSeriesFromData(data, mask)

	case reflect.Bool:
		data := make([]bool, n)
		for i := 0; i < n; i++ {
			if series.IsNull(i) {
				mask[i] = true
				continue
			}
			val, _ := series.At(i)
			b, err := convertToBool(val)
			if err != nil {
				return nil, fmt.Errorf("AsType: column '%s' row %d: %w", column, i, err)
			}
			data[i] = b
		}
		return collection.NewBoolSeriesFromData(data, mask)

	default:
		return nil, fmt.Errorf("AsType: unsupported target kind %v", kind)
	}
}

// convertToFloat converts a value to float64.
func convertToFloat(val any) (float64, error) {
	switch v := val.(type) {
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %q to float64", v)
		}
		return f, nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		if f, ok := toFloat64(val); ok {
			return f, nil
		}
		return 0, fmt.Errorf("cannot convert %T to float64", val)
	}
}

// convertToInt converts a value to int64 (floats are truncated).
func convertToInt(val any) (int64, error) {
	switch v := val.(type) {
	case string:
		s := strings.TrimSpace(v)
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n, nil
		}
		// Allow integral floats like "3.0".
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int64(f), nil
		}
		return 0, fmt.Errorf("cannot convert %q to int64", v)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case float64, float32:
		f, _ := toFloat64(val)
		return int64(f), nil
	default:
		switch val.(type) {
		case int, int64, int32, int16, int8:
			return toInt64(val), nil
		}
		return 0, fmt.Errorf("cannot convert %T to int64", val)
	}
}

// convertToBool converts a value to bool.
func convertToBool(val any) (bool, error) {
	switch v := val.(type) {
	case bool:
		return v, nil
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "1", "t", "yes", "y":
			return true, nil
		case "false", "0", "f", "no", "n":
			return false, nil
		default:
			return false, fmt.Errorf("cannot convert %q to bool", v)
		}
	default:
		if f, ok := toFloat64(val); ok {
			return f != 0, nil
		}
		return false, fmt.Errorf("cannot convert %T to bool", val)
	}
}

// dtypeName returns a friendly name for a series dtype.
func dtypeName(t reflect.Type) string {
	if t == nil {
		return "any"
	}
	switch t.Kind() {
	case reflect.Float64:
		return "float64"
	case reflect.Float32:
		return "float32"
	case reflect.Int64, reflect.Int:
		return "int64"
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "bool"
	case reflect.Interface:
		return "any"
	default:
		return t.String()
	}
}
