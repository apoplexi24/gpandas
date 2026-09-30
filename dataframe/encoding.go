package dataframe

import (
	"errors"
	"fmt"
	"sort"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// GetDummies returns a new DataFrame with a column one-hot encoded: the column
// is replaced, in place, by one boolean indicator column per distinct value.
// Each indicator is named "<column>_<value>" and is true on the rows holding
// that value.
//
// For a categorical column (from AsCategorical, Cut, or Qcut) the indicators
// follow the category order and include categories no row uses, so binned data
// keeps one column per bin. For any other column the distinct non-null values
// are sorted: numerically for numeric columns, lexically otherwise.
//
// A null row is false in every indicator, as in pandas, so the indicators never
// contain nulls and can be fed straight to a model. It remains recognisable as
// the only kind of row with no true indicator. Other columns and index labels
// are preserved.
//
// This is analogous to pd.get_dummies(df, columns=[column]) in pandas, except
// that the indicators take the original column's position rather than moving to
// the end.
//
// Parameters:
//   - column: the column to encode
//
// Returns:
//   - *DataFrame: a new DataFrame with the column replaced by indicators
//   - error: nil if successful, or an error if the column is missing or an
//     indicator name collides with an existing column
//
// Example:
//
//	// City (NYC, LA) becomes City_LA and City_NYC
//	encoded, err := df.GetDummies("City")
func (df *DataFrame) GetDummies(column string) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("GetDummies: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("GetDummies: column '%s' not found", column)
	}

	categories, keys := dummyCategories(series)

	catIndex := make(map[string]int, len(categories))
	for i, c := range categories {
		catIndex[c] = i
	}

	n := series.Len()
	indicators := make([][]bool, len(categories))
	for i := range indicators {
		indicators[i] = make([]bool, n)
	}
	for row, key := range keys {
		if idx, ok := catIndex[key]; ok && !series.IsNull(row) {
			indicators[idx][row] = true
		}
	}

	newCols := make(map[string]collection.Series, len(df.Columns)+len(categories))
	for name, s := range df.Columns {
		if name != column {
			newCols[name] = s
		}
	}

	dummyNames := make([]string, len(categories))
	for i, c := range categories {
		name := column + "_" + c
		if _, clash := newCols[name]; clash {
			return nil, fmt.Errorf("GetDummies: indicator column '%s' already exists", name)
		}
		s, err := collection.NewBoolSeriesFromData(indicators[i], nil)
		if err != nil {
			return nil, fmt.Errorf("GetDummies: %w", err)
		}
		newCols[name] = s
		dummyNames[i] = name
	}

	order := make([]string, 0, len(df.ColumnOrder)-1+len(dummyNames))
	for _, name := range df.ColumnOrder {
		if name == column {
			order = append(order, dummyNames...)
		} else {
			order = append(order, name)
		}
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: order,
		Index:       append([]string(nil), df.Index...),
	}, nil
}

// dummyCategories returns the categories to encode, in indicator order, and the
// string key of every row (empty for nulls). Values are keyed by their "%v"
// form, the same rule AsCategorical uses.
func dummyCategories(series collection.Series) (categories, keys []string) {
	n := series.Len()
	keys = make([]string, n)
	for i := 0; i < n; i++ {
		if series.IsNull(i) {
			continue
		}
		if v, err := series.At(i); err == nil {
			keys[i] = fmt.Sprintf("%v", v)
		}
	}

	if cat, ok := series.(*collection.CategoricalSeries); ok {
		return cat.Categories(), keys
	}

	// Remember one raw value per key so numeric columns can sort by value, which
	// keeps 2 ahead of 10.
	firstRow := make(map[string]int)
	for i, k := range keys {
		if series.IsNull(i) {
			continue
		}
		if _, seen := firstRow[k]; !seen {
			firstRow[k] = i
			categories = append(categories, k)
		}
	}

	if isNumericSeries(series) {
		num := make(map[string]float64, len(categories))
		for _, k := range categories {
			v, _ := series.At(firstRow[k])
			num[k], _ = toFloat64(v)
		}
		sort.Slice(categories, func(a, b int) bool { return num[categories[a]] < num[categories[b]] })
	} else {
		sort.Strings(categories)
	}
	return categories, keys
}
