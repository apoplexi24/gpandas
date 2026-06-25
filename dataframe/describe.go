package dataframe

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// describeStats is the ordered list of statistics produced by Describe.
var describeStats = []string{"count", "mean", "std", "min", "25%", "50%", "75%", "max"}

// Describe returns a new DataFrame of summary statistics for the numeric columns
// of the DataFrame. The result has a leading "statistic" column whose rows are
// count, mean, std, min, 25%, 50%, 75%, max, followed by one column per numeric
// column of the original DataFrame.
//
// Standard deviation uses the sample formula (ddof=1), matching pandas' default.
// Quantiles use linear interpolation. Null values are ignored. If a statistic
// is undefined (e.g. std with fewer than two values) it is reported as NaN.
//
// This is analogous to df.describe() in pandas.
//
// Returns:
//   - *DataFrame: the summary-statistics DataFrame
//   - error: nil if successful, or an error if there are no numeric columns
//
// Example:
//
//	summary, err := df.Describe()
//	fmt.Println(summary)
func (df *DataFrame) Describe() (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("Describe: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	numericCols := make([]string, 0, len(df.ColumnOrder))
	for _, name := range df.ColumnOrder {
		if isNumericSeries(df.Columns[name]) {
			numericCols = append(numericCols, name)
		}
	}

	if len(numericCols) == 0 {
		return nil, errors.New("Describe: no numeric columns to describe")
	}

	numStats := len(describeStats)

	statLabels := make([]string, numStats)
	copy(statLabels, describeStats)
	statSeries, _ := collection.NewStringSeriesFromData(statLabels, nil)

	resultCols := make(map[string]collection.Series, len(numericCols)+1)
	resultCols["statistic"] = statSeries
	resultOrder := make([]string, 0, len(numericCols)+1)
	resultOrder = append(resultOrder, "statistic")

	for _, name := range numericCols {
		vals := numericValues(df.Columns[name])
		stats := computeDescribe(vals)
		col, err := collection.NewFloat64SeriesFromData(stats, nil)
		if err != nil {
			return nil, fmt.Errorf("Describe: failed building column '%s': %w", name, err)
		}
		resultCols[name] = col
		resultOrder = append(resultOrder, name)
	}

	index := make([]string, numStats)
	for i := 0; i < numStats; i++ {
		index[i] = fmt.Sprintf("%d", i)
	}

	return &DataFrame{
		Columns:     resultCols,
		ColumnOrder: resultOrder,
		Index:       index,
	}, nil
}

// Mean returns the arithmetic mean of each numeric column, keyed by column name.
// Columns with no non-null values report NaN.
func (df *DataFrame) Mean() map[string]float64 {
	return df.reduceNumeric(func(vals []float64) float64 {
		if len(vals) == 0 {
			return math.NaN()
		}
		return sumFloats(vals) / float64(len(vals))
	})
}

// Sum returns the sum of each numeric column, keyed by column name.
// Columns with no non-null values report 0.
func (df *DataFrame) Sum() map[string]float64 {
	return df.reduceNumeric(func(vals []float64) float64 {
		return sumFloats(vals)
	})
}

// Std returns the sample standard deviation (ddof=1) of each numeric column,
// keyed by column name. Columns with fewer than two values report NaN.
func (df *DataFrame) Std() map[string]float64 {
	return df.reduceNumeric(stdSample)
}

// Median returns the median (50th percentile) of each numeric column, keyed by
// column name. Columns with no non-null values report NaN.
func (df *DataFrame) Median() map[string]float64 {
	return df.reduceNumeric(func(vals []float64) float64 {
		if len(vals) == 0 {
			return math.NaN()
		}
		sorted := append([]float64(nil), vals...)
		sort.Float64s(sorted)
		return quantileSorted(sorted, 0.5)
	})
}

// Min returns the minimum of each numeric column, keyed by column name.
// Columns with no non-null values report NaN.
func (df *DataFrame) Min() map[string]float64 {
	return df.reduceNumeric(func(vals []float64) float64 {
		if len(vals) == 0 {
			return math.NaN()
		}
		m := vals[0]
		for _, v := range vals[1:] {
			if v < m {
				m = v
			}
		}
		return m
	})
}

// Max returns the maximum of each numeric column, keyed by column name.
// Columns with no non-null values report NaN.
func (df *DataFrame) Max() map[string]float64 {
	return df.reduceNumeric(func(vals []float64) float64 {
		if len(vals) == 0 {
			return math.NaN()
		}
		m := vals[0]
		for _, v := range vals[1:] {
			if v > m {
				m = v
			}
		}
		return m
	})
}

// NullCount returns the number of null values in each column, keyed by column name.
func (df *DataFrame) NullCount() map[string]int {
	out := make(map[string]int)
	if df == nil {
		return out
	}
	df.RLock()
	defer df.RUnlock()
	for _, name := range df.ColumnOrder {
		out[name] = df.Columns[name].NullCount()
	}
	return out
}

// ValueCounts returns a new DataFrame containing the frequency of each unique
// (non-null) value in the given column. The result has two columns: the original
// column name (holding the unique values) and "count" (int64 frequencies). Rows
// are ordered by descending count, with ties broken by ascending value.
//
// This is analogous to df["col"].value_counts() in pandas.
//
// Parameters:
//   - column: the column to tabulate
//
// Returns:
//   - *DataFrame: the value-count table
//   - error: nil if successful, otherwise an error
func (df *DataFrame) ValueCounts(column string) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("ValueCounts: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("ValueCounts: column '%s' not found", column)
	}

	rowCount := series.Len()
	counts := make(map[any]int64)
	order := make([]any, 0) // first-seen order of distinct values
	for i := 0; i < rowCount; i++ {
		if series.IsNull(i) {
			continue
		}
		val, err := series.At(i)
		if err != nil {
			return nil, fmt.Errorf("ValueCounts: error reading row %d: %w", i, err)
		}
		if _, seen := counts[val]; !seen {
			order = append(order, val)
		}
		counts[val]++
	}

	// Sort by count descending, then by string representation ascending.
	sort.SliceStable(order, func(a, b int) bool {
		ca, cb := counts[order[a]], counts[order[b]]
		if ca != cb {
			return ca > cb
		}
		return fmt.Sprintf("%v", order[a]) < fmt.Sprintf("%v", order[b])
	})

	values := make([]any, len(order))
	countData := make([]int64, len(order))
	for i, v := range order {
		values[i] = v
		countData[i] = counts[v]
	}

	valueSeries, err := seriesFromAnyValues(values)
	if err != nil {
		return nil, fmt.Errorf("ValueCounts: failed building value column: %w", err)
	}
	countSeries, err := collection.NewInt64SeriesFromData(countData, nil)
	if err != nil {
		return nil, fmt.Errorf("ValueCounts: failed building count column: %w", err)
	}

	index := make([]string, len(order))
	for i := range index {
		index[i] = fmt.Sprintf("%d", i)
	}

	return &DataFrame{
		Columns: map[string]collection.Series{
			column:  valueSeries,
			"count": countSeries,
		},
		ColumnOrder: []string{column, "count"},
		Index:       index,
	}, nil
}

// reduceNumeric applies reducer to the non-null numeric values of every numeric
// column and returns the results keyed by column name.
func (df *DataFrame) reduceNumeric(reducer func([]float64) float64) map[string]float64 {
	out := make(map[string]float64)
	if df == nil {
		return out
	}
	df.RLock()
	defer df.RUnlock()
	for _, name := range df.ColumnOrder {
		series := df.Columns[name]
		if !isNumericSeries(series) {
			continue
		}
		out[name] = reducer(numericValues(series))
	}
	return out
}

// computeDescribe returns the describe statistics for a slice of values in the
// order defined by describeStats.
func computeDescribe(vals []float64) []float64 {
	out := make([]float64, len(describeStats))
	count := float64(len(vals))
	out[0] = count // count

	if len(vals) == 0 {
		for i := 1; i < len(out); i++ {
			out[i] = math.NaN()
		}
		return out
	}

	out[1] = sumFloats(vals) / count // mean
	out[2] = stdSample(vals)         // std

	sorted := append([]float64(nil), vals...)
	sort.Float64s(sorted)
	out[3] = sorted[0]                    // min
	out[4] = quantileSorted(sorted, 0.25) // 25%
	out[5] = quantileSorted(sorted, 0.50) // 50%
	out[6] = quantileSorted(sorted, 0.75) // 75%
	out[7] = sorted[len(sorted)-1]        // max

	return out
}

// stdSample computes the sample standard deviation (ddof=1).
func stdSample(vals []float64) float64 {
	n := len(vals)
	if n < 2 {
		return math.NaN()
	}
	mean := sumFloats(vals) / float64(n)
	var ss float64
	for _, v := range vals {
		d := v - mean
		ss += d * d
	}
	return math.Sqrt(ss / float64(n-1))
}

// quantileSorted computes the q-quantile (0<=q<=1) of an ascending-sorted slice
// using linear interpolation between data points.
func quantileSorted(sorted []float64, q float64) float64 {
	n := len(sorted)
	if n == 0 {
		return math.NaN()
	}
	if n == 1 {
		return sorted[0]
	}
	pos := q * float64(n-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	frac := pos - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

// sumFloats returns the sum of a slice of float64 values.
func sumFloats(vals []float64) float64 {
	var s float64
	for _, v := range vals {
		s += v
	}
	return s
}

// numericValues returns the non-null values of a series converted to float64.
// Non-numeric values are skipped.
func numericValues(series collection.Series) []float64 {
	n := series.Len()
	out := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		if series.IsNull(i) {
			continue
		}
		val, err := series.At(i)
		if err != nil {
			continue
		}
		if f, ok := toFloat64(val); ok {
			out = append(out, f)
		}
	}
	return out
}

// isNumericSeries reports whether a series should be treated as numeric. A
// series qualifies if it has a numeric dtype (float64, int64, ...), or if it is
// an untyped (any) series whose non-null values are all numeric. An all-null or
// empty untyped series is not considered numeric.
func isNumericSeries(series collection.Series) bool {
	if series == nil {
		return false
	}
	if dt := series.DType(); dt != nil {
		switch dt.Kind() {
		case reflect.Float64, reflect.Float32, reflect.Int64, reflect.Int, reflect.Int32, reflect.Int16, reflect.Int8:
			return true
		}
	}

	// Fallback: inspect values. This covers AnySeries holding numbers, which is
	// common when DataFrames are built directly from heterogeneous data.
	n := series.Len()
	hasValue := false
	for i := 0; i < n; i++ {
		if series.IsNull(i) {
			continue
		}
		v, err := series.At(i)
		if err != nil {
			return false
		}
		if _, ok := toFloat64(v); !ok {
			return false
		}
		hasValue = true
	}
	return hasValue
}
