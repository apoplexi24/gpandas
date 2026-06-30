package dataframe

import (
	"errors"
	"fmt"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// AsCategorical returns a new DataFrame with the given column converted to a
// memory-efficient categorical column. Values are stored as integer codes into a
// shared category list, which reduces memory for columns with many repeated
// string values. Null values are preserved.
//
// This is analogous to df["col"].astype("category") in pandas.
//
// Example:
//
//	df, err := df.AsCategorical("Department")
func (df *DataFrame) AsCategorical(column string) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("AsCategorical: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("AsCategorical: column '%s' not found", column)
	}

	n := series.Len()
	values := make([]string, n)
	mask := make([]bool, n)
	for i := 0; i < n; i++ {
		if series.IsNull(i) {
			mask[i] = true
			continue
		}
		v, _ := series.At(i)
		values[i] = fmt.Sprintf("%v", v)
	}

	cat, err := collection.NewCategoricalSeriesFromStrings(values, mask)
	if err != nil {
		return nil, fmt.Errorf("AsCategorical: %w", err)
	}

	newCols := make(map[string]collection.Series, len(df.Columns))
	for name, s := range df.Columns {
		newCols[name] = s
	}
	newCols[column] = cat

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), df.ColumnOrder...),
		Index:       append([]string(nil), df.Index...),
	}, nil
}

// Categories returns the distinct categories of a categorical column in code
// order. An error is returned if the column is not categorical.
func (df *DataFrame) Categories(column string) ([]string, error) {
	if df == nil {
		return nil, errors.New("Categories: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("Categories: column '%s' not found", column)
	}
	cat, ok := series.(*collection.CategoricalSeries)
	if !ok {
		return nil, fmt.Errorf("Categories: column '%s' is not categorical (use AsCategorical first)", column)
	}
	return cat.Categories(), nil
}
