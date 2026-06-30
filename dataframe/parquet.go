package dataframe

import (
	"errors"
	"fmt"
	"os"
	"reflect"

	"github.com/apoplexi24/gpandas/utils/collection"
	"github.com/parquet-go/parquet-go"
)

// parquet column kinds used when mapping gpandas dtypes to a parquet schema.
type pqKind int

const (
	pqDouble pqKind = iota
	pqInt
	pqBool
	pqString
)

// ToParquet writes the DataFrame to a Parquet file.
//
// Columns are mapped to Parquet types as follows: float64 -> DOUBLE,
// int64/int -> INT64, bool -> BOOLEAN, and everything else (string, datetime,
// categorical, any) -> UTF8 string.
//
// Limitations: Parquet columns are written as required (non-nullable). Null
// values are written as the zero value for the column type (0, 0.0, "", false).
// Column names are stored in the Parquet schema, which orders fields
// alphabetically on read.
//
// This is analogous to df.to_parquet(path) in pandas.
//
// Example:
//
//	err := df.ToParquet("data.parquet")
func (df *DataFrame) ToParquet(filepath string) error {
	if df == nil {
		return errors.New("ToParquet: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	if len(df.ColumnOrder) == 0 {
		return errors.New("ToParquet: DataFrame has no columns")
	}

	// Determine each column's parquet kind and build the schema group.
	kinds := make(map[string]pqKind, len(df.ColumnOrder))
	group := parquet.Group{}
	for _, name := range df.ColumnOrder {
		k := pqKindFor(df.Columns[name])
		kinds[name] = k
		switch k {
		case pqDouble:
			group[name] = parquet.Leaf(parquet.DoubleType)
		case pqInt:
			group[name] = parquet.Leaf(parquet.Int64Type)
		case pqBool:
			group[name] = parquet.Leaf(parquet.BooleanType)
		default:
			group[name] = parquet.String()
		}
	}
	schema := parquet.NewSchema("gpandas", group)

	rowCount := df.Columns[df.ColumnOrder[0]].Len()
	rows := make([]map[string]any, rowCount)
	for r := 0; r < rowCount; r++ {
		row := make(map[string]any, len(df.ColumnOrder))
		for _, name := range df.ColumnOrder {
			series := df.Columns[name]
			k := kinds[name]
			if series.IsNull(r) {
				row[name] = zeroForKind(k)
				continue
			}
			v, _ := series.At(r)
			row[name] = convertForKind(k, v)
		}
		rows[r] = row
	}

	f, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("ToParquet: failed to create file: %w", err)
	}
	defer f.Close()

	w := parquet.NewGenericWriter[map[string]any](f, schema)
	if _, err := w.Write(rows); err != nil {
		return fmt.Errorf("ToParquet: failed to write rows: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("ToParquet: failed to finalize file: %w", err)
	}
	return nil
}

// pqKindFor maps a Series dtype to a parquet column kind.
func pqKindFor(series collection.Series) pqKind {
	dt := series.DType()
	if dt == nil {
		return pqString
	}
	switch dt.Kind() {
	case reflect.Float64, reflect.Float32:
		return pqDouble
	case reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
		return pqInt
	case reflect.Bool:
		return pqBool
	case reflect.String:
		return pqString
	default:
		return pqString
	}
}

func zeroForKind(k pqKind) any {
	switch k {
	case pqDouble:
		return 0.0
	case pqInt:
		return int64(0)
	case pqBool:
		return false
	default:
		return ""
	}
}

func convertForKind(k pqKind, v any) any {
	switch k {
	case pqDouble:
		if f, ok := toFloat64(v); ok {
			return f
		}
		return 0.0
	case pqInt:
		return toInt64(v)
	case pqBool:
		if b, ok := v.(bool); ok {
			return b
		}
		return false
	default:
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	}
}
