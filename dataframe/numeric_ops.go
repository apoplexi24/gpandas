package dataframe

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// RankMethod selects how DataFrame.Rank resolves ties, mirroring the pandas
// DataFrame.rank "method" argument.
//
// The zero value ("") behaves like RankAverage.
type RankMethod string

const (
	// RankAverage assigns every tied row the mean of the ranks they span. This is
	// the default.
	RankAverage RankMethod = "average"
	// RankMin assigns every tied row the lowest rank of the group (pandas'
	// "competition" ranking).
	RankMin RankMethod = "min"
	// RankMax assigns every tied row the highest rank of the group.
	RankMax RankMethod = "max"
	// RankDense assigns consecutive ranks to distinct values, so no rank is
	// skipped after a tie.
	RankDense RankMethod = "dense"
	// RankFirst breaks ties by row order, so every row gets a distinct rank.
	RankFirst RankMethod = "first"
)

// Round returns a new DataFrame with every numeric column rounded to the given
// number of decimal places. A negative decimals rounds to the left of the
// decimal point, so -1 rounds to the nearest ten.
//
// Halfway values round to the nearest even number, matching the behaviour of
// pandas and NumPy: rounding 0.5 gives 0 and rounding 1.5 gives 2. Integer
// columns stay integer-typed because a rounded integer is still an integer.
//
// Null values stay null, non-numeric columns pass through unchanged, and both
// column order and index labels are preserved.
//
// This is analogous to df.round(decimals) in pandas.
//
// Parameters:
//   - decimals: the number of decimal places to keep; may be negative
//
// Returns:
//   - *DataFrame: a new DataFrame with rounded numeric columns
//   - error: nil if successful, otherwise an error
//
// Example:
//
//	rounded, err := df.Round(2)
func (df *DataFrame) Round(decimals int) (*DataFrame, error) {
	factor := math.Pow(10, float64(decimals))
	return df.numericElementwise("Round", true, func(v float64) float64 {
		return roundTo(v, factor)
	})
}

// Clip returns a new DataFrame with every numeric value bounded to the range
// [lower, upper]. Values below lower become lower and values above upper become
// upper.
//
// To leave one side unbounded, pass an infinity: Clip(0, math.Inf(1)) applies a
// floor only, and Clip(math.Inf(-1), 100) applies a ceiling only. Integer
// columns stay integer-typed when both bounds are whole numbers.
//
// Null values stay null, non-numeric columns pass through unchanged, and both
// column order and index labels are preserved.
//
// This is analogous to df.clip(lower, upper) in pandas.
//
// Parameters:
//   - lower: the minimum value to keep
//   - upper: the maximum value to keep
//
// Returns:
//   - *DataFrame: a new DataFrame with clipped numeric columns
//   - error: nil if successful, or an error if a bound is NaN or lower > upper
//
// Example:
//
//	// Bound scores to 0..100
//	bounded, err := df.Clip(0, 100)
func (df *DataFrame) Clip(lower, upper float64) (*DataFrame, error) {
	if math.IsNaN(lower) || math.IsNaN(upper) {
		return nil, errors.New("Clip: bounds must not be NaN")
	}
	if lower > upper {
		return nil, fmt.Errorf("Clip: lower bound %v must not exceed upper bound %v", lower, upper)
	}

	// An integer column can only stay integer if neither bound would introduce a
	// fractional value.
	keepInt := isWholeBound(lower) && isWholeBound(upper)

	return df.numericElementwise("Clip", keepInt, func(v float64) float64 {
		if v < lower {
			return lower
		}
		if v > upper {
			return upper
		}
		return v
	})
}

// Abs returns a new DataFrame with the absolute value of every numeric column.
// Integer columns stay integer-typed.
//
// Null values stay null, non-numeric columns pass through unchanged, and both
// column order and index labels are preserved.
//
// This is analogous to df.abs() in pandas.
//
// Returns:
//   - *DataFrame: a new DataFrame with absolute values
//   - error: nil if successful, otherwise an error
//
// Example:
//
//	magnitudes, err := df.Abs()
func (df *DataFrame) Abs() (*DataFrame, error) {
	return df.numericElementwise("Abs", true, math.Abs)
}

// Diff returns a new DataFrame holding the difference between each value and the
// value periods rows away. Positive periods look backward, so Diff(1) subtracts
// the previous row; negative periods look forward, so Diff(-1) subtracts the next
// row.
//
// Cells with no counterpart are null, the same rule Shift follows, and a null on
// either side of the subtraction also yields null. Integer columns stay
// integer-typed, since the null mask removes pandas' need to promote to float.
//
// Non-numeric columns pass through unchanged, and both column order and index
// labels are preserved.
//
// This is analogous to df.diff(periods) in pandas.
//
// Parameters:
//   - periods: the row offset to subtract; may be negative or zero
//
// Returns:
//   - *DataFrame: a new DataFrame of differences
//   - error: nil if successful, otherwise an error
//
// Example:
//
//	// Day-over-day change
//	delta, err := df.Diff(1)
func (df *DataFrame) Diff(periods int) (*DataFrame, error) {
	return df.offsetChange("Diff", periods, false)
}

// PctChange returns a new DataFrame holding the fractional change between each
// value and the value periods rows away, computed as (current - previous) /
// previous. Multiply by 100 for a percentage.
//
// The result is always a float64 column. A previous value of zero yields +Inf,
// -Inf, or NaN rather than an error, matching Div. Cells with no counterpart are
// null, as are cells where either side of the calculation is null.
//
// Non-numeric columns pass through unchanged, and both column order and index
// labels are preserved.
//
// This is analogous to df.pct_change(periods) in pandas.
//
// Parameters:
//   - periods: the row offset to compare against; may be negative or zero
//
// Returns:
//   - *DataFrame: a new DataFrame of fractional changes
//   - error: nil if successful, otherwise an error
//
// Example:
//
//	// Growth relative to the previous row
//	growth, err := df.PctChange(1)
func (df *DataFrame) PctChange(periods int) (*DataFrame, error) {
	return df.offsetChange("PctChange", periods, true)
}

// Rank returns a new DataFrame with each numeric column replaced by the ascending
// rank of its values, starting at 1. How tied values share ranks is controlled by
// method; the zero value ("") means RankAverage.
//
// Ranks are always float64 because RankAverage can produce halves. Null values
// stay null and take no rank at all, so they never shift the ranks of the values
// around them (pandas' default na_option="keep").
//
// Non-numeric columns pass through unchanged, and both column order and index
// labels are preserved.
//
// This is analogous to df.rank(method=...) in pandas.
//
// For the values 10, 20, 20, 30 the methods produce:
//
//	RankAverage: 1, 2.5, 2.5, 4
//	RankMin:     1, 2,   2,   4
//	RankMax:     1, 3,   3,   4
//	RankDense:   1, 2,   2,   3
//	RankFirst:   1, 2,   3,   4
//
// Parameters:
//   - method: how to resolve ties
//
// Returns:
//   - *DataFrame: a new DataFrame of ranks
//   - error: nil if successful, or an error if method is not recognised
//
// Example:
//
//	ranks, err := df.Rank(dataframe.RankDense)
func (df *DataFrame) Rank(method RankMethod) (*DataFrame, error) {
	switch method {
	case RankAverage, RankMin, RankMax, RankDense, RankFirst, "":
		// supported
	default:
		return nil, fmt.Errorf("Rank: unsupported method '%s'", method)
	}

	return df.mapNumericColumns("Rank", func(_ string, data []float64, mask []bool, _ bool) (collection.Series, error) {
		out, outMask := rankValues(data, mask, method)
		return collection.NewFloat64SeriesFromData(out, outMask)
	})
}

// numericElementwise applies fn to every non-null value of every numeric column.
// keepInt reports whether an integer column may stay integer-typed, which is only
// true when fn maps whole numbers to whole numbers.
func (df *DataFrame) numericElementwise(name string, keepInt bool, fn func(float64) float64) (*DataFrame, error) {
	return df.mapNumericColumns(name, func(_ string, data []float64, mask []bool, isInt bool) (collection.Series, error) {
		n := len(data)

		if isInt && keepInt {
			out := make([]int64, n)
			for i := 0; i < n; i++ {
				if mask[i] {
					continue
				}
				out[i] = int64(fn(data[i]))
			}
			return collection.NewInt64SeriesFromData(out, mask)
		}

		out := make([]float64, n)
		for i := 0; i < n; i++ {
			if mask[i] {
				continue
			}
			out[i] = fn(data[i])
		}
		return collection.NewFloat64SeriesFromData(out, mask)
	})
}

// offsetChange implements Diff and PctChange, which share their null handling and
// differ only in the final arithmetic. When fractional is set the difference is
// divided by the earlier value and the result is always float64.
func (df *DataFrame) offsetChange(name string, periods int, fractional bool) (*DataFrame, error) {
	return df.mapNumericColumns(name, func(_ string, data []float64, mask []bool, isInt bool) (collection.Series, error) {
		n := len(data)
		outMask := make([]bool, n)

		// Integer differences stay exact, but a fractional change never can.
		if isInt && !fractional {
			out := make([]int64, n)
			for i := 0; i < n; i++ {
				src := i - periods
				if src < 0 || src >= n || mask[i] || mask[src] {
					outMask[i] = true
					continue
				}
				out[i] = int64(data[i]) - int64(data[src])
			}
			return collection.NewInt64SeriesFromData(out, outMask)
		}

		out := make([]float64, n)
		for i := 0; i < n; i++ {
			src := i - periods
			if src < 0 || src >= n || mask[i] || mask[src] {
				outMask[i] = true
				continue
			}
			delta := data[i] - data[src]
			if fractional {
				delta /= data[src]
			}
			out[i] = delta
		}
		return collection.NewFloat64SeriesFromData(out, outMask)
	})
}

// mapNumericColumns builds a new DataFrame by replacing every numeric column with
// the Series returned by build. Non-numeric columns are passed through unchanged,
// and column order and index labels are preserved.
func (df *DataFrame) mapNumericColumns(
	name string,
	build func(colName string, data []float64, mask []bool, isInt bool) (collection.Series, error),
) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New(name + ": DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	newCols := make(map[string]collection.Series, len(df.Columns))
	for _, colName := range df.ColumnOrder {
		series := df.Columns[colName]
		if !isNumericSeries(series) {
			newCols[colName] = series // pass through, zero-copy
			continue
		}

		data, mask, isInt, err := extractNumeric(series)
		if err != nil {
			return nil, fmt.Errorf("%s: column '%s': %w", name, colName, err)
		}

		built, err := build(colName, data, mask, isInt)
		if err != nil {
			return nil, fmt.Errorf("%s: column '%s': %w", name, colName, err)
		}
		newCols[colName] = built
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), df.ColumnOrder...),
		Index:       append([]string(nil), df.Index...),
	}, nil
}

// roundTo rounds v to the precision implied by factor (10^decimals), using
// half-to-even at the midpoint so results match pandas and NumPy.
func roundTo(v, factor float64) float64 {
	// A factor that has overflowed to infinity means far more precision was
	// requested than a float64 holds, so v is already at that precision.
	if math.IsInf(factor, 0) {
		return v
	}
	// A factor that has underflowed to zero means every value collapses.
	if factor == 0 {
		return 0
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return v
	}
	return math.RoundToEven(v*factor) / factor
}

// isWholeBound reports whether a Clip bound leaves integer columns integral. An
// infinite bound qualifies because it can never be substituted into the data.
func isWholeBound(bound float64) bool {
	if math.IsInf(bound, 0) {
		return true
	}
	return bound == math.Trunc(bound)
}

// rankEntry pairs a value with the row it came from, so ties can be broken by
// row order.
type rankEntry struct {
	val float64
	row int
}

// rankValues computes ascending ranks over the non-null values of a column. Null
// positions stay null and are not ranked.
func rankValues(data []float64, mask []bool, method RankMethod) ([]float64, []bool) {
	n := len(data)
	out := make([]float64, n)
	outMask := make([]bool, n)

	entries := make([]rankEntry, 0, n)
	for i := 0; i < n; i++ {
		if mask[i] {
			outMask[i] = true
			continue
		}
		entries = append(entries, rankEntry{val: data[i], row: i})
	}

	// Sort by value, then by row so that tied rows appear in their original order
	// and RankFirst can simply walk the group.
	sort.Slice(entries, func(a, b int) bool {
		if entries[a].val != entries[b].val {
			return entries[a].val < entries[b].val
		}
		return entries[a].row < entries[b].row
	})

	dense := 0.0
	for start := 0; start < len(entries); {
		// Extend the group over every entry tied with entries[start].
		end := start + 1
		for end < len(entries) && entries[end].val == entries[start].val {
			end++
		}

		lowRank := float64(start + 1) // ranks are 1-based
		highRank := float64(end)
		dense++

		for k := start; k < end; k++ {
			switch method {
			case RankMin:
				out[entries[k].row] = lowRank
			case RankMax:
				out[entries[k].row] = highRank
			case RankDense:
				out[entries[k].row] = dense
			case RankFirst:
				out[entries[k].row] = float64(k + 1)
			default: // RankAverage and the zero value
				out[entries[k].row] = (lowRank + highRank) / 2
			}
		}

		start = end
	}

	return out, outMask
}
