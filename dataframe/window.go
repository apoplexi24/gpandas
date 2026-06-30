package dataframe

import (
	"errors"
	"fmt"
	"math"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// RollingWindow represents a fixed-size rolling view over a DataFrame, created
// by DataFrame.Rolling. Aggregations are computed over a trailing window of rows.
type RollingWindow struct {
	df     *DataFrame
	window int
}

// Rolling creates a rolling window of the given size for computing moving
// aggregations. The window must be at least 1.
//
// This is analogous to df.rolling(window) in pandas.
//
// Example:
//
//	result, err := df.Rolling(3).Mean()
func (df *DataFrame) Rolling(window int) *RollingWindow {
	return &RollingWindow{df: df, window: window}
}

// Mean computes the rolling mean over each numeric column.
func (rw *RollingWindow) Mean() (*DataFrame, error) {
	return rw.apply("mean")
}

// Sum computes the rolling sum over each numeric column.
func (rw *RollingWindow) Sum() (*DataFrame, error) {
	return rw.apply("sum")
}

// Min computes the rolling minimum over each numeric column.
func (rw *RollingWindow) Min() (*DataFrame, error) {
	return rw.apply("min")
}

// Max computes the rolling maximum over each numeric column.
func (rw *RollingWindow) Max() (*DataFrame, error) {
	return rw.apply("max")
}

// Std computes the rolling sample standard deviation (ddof=1) over each numeric
// column.
func (rw *RollingWindow) Std() (*DataFrame, error) {
	return rw.apply("std")
}

// apply computes the given rolling statistic. Numeric columns produce float64
// results; non-numeric columns are passed through unchanged. A result is null
// for any position that does not have a full window of non-null values.
func (rw *RollingWindow) apply(stat string) (*DataFrame, error) {
	if rw.df == nil {
		return nil, errors.New("Rolling: DataFrame is nil")
	}
	if rw.window < 1 {
		return nil, fmt.Errorf("Rolling: window must be >= 1, got %d", rw.window)
	}

	rw.df.RLock()
	defer rw.df.RUnlock()

	rowCount := 0
	if len(rw.df.ColumnOrder) > 0 {
		rowCount = rw.df.Columns[rw.df.ColumnOrder[0]].Len()
	}

	newCols := make(map[string]collection.Series, len(rw.df.Columns))
	for _, name := range rw.df.ColumnOrder {
		series := rw.df.Columns[name]
		if !isNumericSeries(series) {
			// Pass through non-numeric columns unchanged (zero-copy).
			newCols[name] = series
			continue
		}

		data := make([]float64, rowCount)
		mask := make([]bool, rowCount)
		for i := 0; i < rowCount; i++ {
			if i+1 < rw.window {
				mask[i] = true // not enough history yet
				continue
			}
			// Collect the trailing window [i-window+1, i].
			vals := make([]float64, 0, rw.window)
			full := true
			for j := i - rw.window + 1; j <= i; j++ {
				if series.IsNull(j) {
					full = false
					break
				}
				v, _ := series.At(j)
				f, ok := toFloat64(v)
				if !ok {
					full = false
					break
				}
				vals = append(vals, f)
			}
			if !full {
				mask[i] = true
				continue
			}
			data[i] = computeRollingStat(stat, vals)
		}
		s, err := collection.NewFloat64SeriesFromData(data, mask)
		if err != nil {
			return nil, fmt.Errorf("Rolling: column '%s': %w", name, err)
		}
		newCols[name] = s
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), rw.df.ColumnOrder...),
		Index:       append([]string(nil), rw.df.Index...),
	}, nil
}

// computeRollingStat computes a single statistic over a full window of values.
func computeRollingStat(stat string, vals []float64) float64 {
	switch stat {
	case "sum":
		return sumFloats(vals)
	case "mean":
		return sumFloats(vals) / float64(len(vals))
	case "std":
		return stdSample(vals)
	case "min":
		m := vals[0]
		for _, v := range vals[1:] {
			m = math.Min(m, v)
		}
		return m
	case "max":
		m := vals[0]
		for _, v := range vals[1:] {
			m = math.Max(m, v)
		}
		return m
	default:
		return math.NaN()
	}
}

// Shift returns a new DataFrame with all values shifted by the given number of
// periods. Positive periods shift values downward (toward higher indices);
// negative periods shift upward. Vacated positions become null. Index labels are
// preserved.
//
// This is analogous to df.shift(periods) in pandas.
//
// Example:
//
//	shifted, err := df.Shift(1)
func (df *DataFrame) Shift(periods int) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("Shift: DataFrame is nil")
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
		shifted := collection.NewSeriesOfTypeWithSize(series.DType(), rowCount)
		for i := 0; i < rowCount; i++ {
			src := i - periods
			if src < 0 || src >= rowCount || series.IsNull(src) {
				shifted.SetNull(i)
				continue
			}
			v, _ := series.At(src)
			if err := shifted.Set(i, v); err != nil {
				return nil, fmt.Errorf("Shift: column '%s': %w", name, err)
			}
		}
		newCols[name] = shifted
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), df.ColumnOrder...),
		Index:       append([]string(nil), df.Index...),
	}, nil
}

// CumSum returns a new DataFrame with the cumulative sum of each numeric column.
// CumMax, CumMin, and CumProd compute the running maximum, minimum, and product.
// Null cells remain null and are skipped in the accumulation; non-numeric
// columns are passed through unchanged.
//
// These are analogous to df.cumsum(), df.cummax(), df.cummin(), df.cumprod().
func (df *DataFrame) CumSum() (*DataFrame, error) { return df.cumulative("sum") }

// CumMax returns a new DataFrame with the running maximum of each numeric column.
func (df *DataFrame) CumMax() (*DataFrame, error) { return df.cumulative("max") }

// CumMin returns a new DataFrame with the running minimum of each numeric column.
func (df *DataFrame) CumMin() (*DataFrame, error) { return df.cumulative("min") }

// CumProd returns a new DataFrame with the running product of each numeric column.
func (df *DataFrame) CumProd() (*DataFrame, error) { return df.cumulative("prod") }

// cumulative computes a cumulative statistic over each numeric column.
func (df *DataFrame) cumulative(op string) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("cumulative: DataFrame is nil")
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
		if !isNumericSeries(series) {
			newCols[name] = series
			continue
		}

		data := make([]float64, rowCount)
		mask := make([]bool, rowCount)
		var acc float64
		started := false
		for i := 0; i < rowCount; i++ {
			if series.IsNull(i) {
				mask[i] = true
				continue
			}
			v, _ := series.At(i)
			f, ok := toFloat64(v)
			if !ok {
				mask[i] = true
				continue
			}
			if !started {
				acc = f
				started = true
			} else {
				switch op {
				case "sum":
					acc += f
				case "prod":
					acc *= f
				case "max":
					acc = math.Max(acc, f)
				case "min":
					acc = math.Min(acc, f)
				}
			}
			data[i] = acc
		}
		s, err := collection.NewFloat64SeriesFromData(data, mask)
		if err != nil {
			return nil, fmt.Errorf("cumulative: column '%s': %w", name, err)
		}
		newCols[name] = s
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), df.ColumnOrder...),
		Index:       append([]string(nil), df.Index...),
	}, nil
}
