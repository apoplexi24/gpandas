package dataframe

import (
	"fmt"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// NewDataFrameFromColumns builds a DataFrame from a column order and a map of
// column name to its values (as a Column / []any). The type of each column is
// inferred from its values: homogeneous numeric, string, or bool columns become
// typed Series, mixed int/float values are promoted to float64, and otherwise an
// untyped (any) Series is used. nil values become nulls.
//
// A default integer index ("0", "1", ...) is created. All columns must have the
// same length.
//
// This is primarily used by data loaders (e.g. Read_json) but is also useful for
// constructing DataFrames from heterogeneous in-memory data.
func NewDataFrameFromColumns(order []string, cols map[string]Column) (*DataFrame, error) {
	if len(order) == 0 {
		return nil, fmt.Errorf("at least one column is required")
	}

	rowCount := -1
	newCols := make(map[string]collection.Series, len(order))
	for _, name := range order {
		values, ok := cols[name]
		if !ok {
			return nil, fmt.Errorf("column '%s' missing from values map", name)
		}
		if rowCount == -1 {
			rowCount = len(values)
		} else if len(values) != rowCount {
			return nil, fmt.Errorf("inconsistent row count: column '%s' has %d, expected %d", name, len(values), rowCount)
		}
		s, err := seriesFromAnyValues([]any(values))
		if err != nil {
			return nil, fmt.Errorf("building column '%s': %w", name, err)
		}
		newCols[name] = s
	}

	index := make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		index[i] = fmt.Sprintf("%d", i)
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), order...),
		Index:       index,
	}, nil
}
