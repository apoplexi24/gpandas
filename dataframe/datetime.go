package dataframe

import (
	"errors"
	"fmt"
	"time"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// ToDatetime returns a new DataFrame with the given column parsed into a
// datetime column. Each non-null value is parsed with the provided layout (a Go
// reference-time layout). If layout is empty, common formats are tried in order:
// RFC3339, "2006-01-02 15:04:05", and "2006-01-02".
//
// Values that cannot be parsed produce an error. Null values are preserved.
//
// This is analogous to pd.to_datetime(df["col"]) in pandas.
//
// Example:
//
//	df, err := df.ToDatetime("created_at", "")            // auto-detect
//	df, err := df.ToDatetime("created_at", "2006-01-02")  // explicit layout
func (df *DataFrame) ToDatetime(column string, layout string) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("ToDatetime: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("ToDatetime: column '%s' not found", column)
	}

	n := series.Len()
	data := make([]time.Time, n)
	mask := make([]bool, n)
	for i := 0; i < n; i++ {
		if series.IsNull(i) {
			mask[i] = true
			continue
		}
		v, _ := series.At(i)
		// Already a time.Time? Keep as-is.
		if t, isTime := v.(time.Time); isTime {
			data[i] = t
			continue
		}
		str := fmt.Sprintf("%v", v)
		t, err := parseDateTime(str, layout)
		if err != nil {
			return nil, fmt.Errorf("ToDatetime: column '%s' row %d: %w", column, i, err)
		}
		data[i] = t
	}

	newSeries, err := collection.NewDateTimeSeriesFromData(data, mask)
	if err != nil {
		return nil, fmt.Errorf("ToDatetime: %w", err)
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

// Dt returns a datetime accessor for a datetime column, enabling extraction of
// components like Year, Month, Day, and Weekday. The returned Series can be added
// back to a DataFrame with Assign.
//
// An error is returned if the column does not exist or is not a datetime column.
//
// Example:
//
//	acc, _ := df.Dt("created_at")
//	df.Assign("year", acc.Year())
func (df *DataFrame) Dt(column string) (*collection.DtAccessor, error) {
	if df == nil {
		return nil, errors.New("Dt: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("Dt: column '%s' not found", column)
	}
	dtSeries, ok := series.(*collection.DateTimeSeries)
	if !ok {
		return nil, fmt.Errorf("Dt: column '%s' is not a datetime column (use ToDatetime first)", column)
	}
	return dtSeries.Dt(), nil
}

// parseDateTime parses a string into a time.Time. If layout is non-empty it is
// used directly; otherwise a set of common layouts is tried.
func parseDateTime(s, layout string) (time.Time, error) {
	if layout != "" {
		return time.Parse(layout, s)
	}
	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
		"01/02/2006",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse %q as datetime", s)
}
