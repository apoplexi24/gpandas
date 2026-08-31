package dataframe

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// Var returns the sample variance (ddof=1) of each numeric column, keyed by
// column name. It is the square of Std, so the two always agree.
//
// Null values are excluded. Columns with fewer than two non-null values report
// NaN because the estimator is undefined.
//
// This is analogous to df.var() in pandas.
//
// Example:
//
//	variances := df.Var()
//	fmt.Println(variances["Salary"])
func (df *DataFrame) Var() map[string]float64 {
	return df.reduceNumeric(varSample)
}

// Quantile returns the q-quantile of each numeric column, keyed by column name.
// q must lie in [0, 1]. Quantiles use the same linear interpolation between
// neighbouring data points that Describe uses, so Quantile(0.25) matches the
// "25%" row of Describe and Quantile(0.5) matches Median.
//
// Null values are excluded. Columns with no non-null values report NaN.
//
// This is analogous to df.quantile(q) in pandas.
//
// Parameters:
//   - q: the quantile to compute, in the range [0, 1]
//
// Returns:
//   - map[string]float64: the quantile per numeric column
//   - error: nil if successful, or an error if q is outside [0, 1] or NaN
//
// Example:
//
//	p90, err := df.Quantile(0.9)
func (df *DataFrame) Quantile(q float64) (map[string]float64, error) {
	if math.IsNaN(q) {
		return nil, errors.New("Quantile: q must not be NaN")
	}
	if q < 0 || q > 1 {
		return nil, fmt.Errorf("Quantile: q must be in [0, 1], got %v", q)
	}
	return df.reduceNumeric(func(vals []float64) float64 {
		if len(vals) == 0 {
			return math.NaN()
		}
		sorted := append([]float64(nil), vals...)
		sort.Float64s(sorted)
		return quantileSorted(sorted, q)
	}), nil
}

// Skew returns the sample skewness of each numeric column, keyed by column name.
// It uses the adjusted Fisher-Pearson standardized moment coefficient (G1),
// matching pandas' default and scipy's skew(bias=False).
//
// Null values are excluded. Columns with fewer than three non-null values, or
// with zero variance, report NaN.
//
// This is analogous to df.skew() in pandas.
//
// Example:
//
//	skewness := df.Skew()
func (df *DataFrame) Skew() map[string]float64 {
	return df.reduceNumeric(skewSample)
}

// Kurt returns the sample excess kurtosis of each numeric column, keyed by
// column name. It uses the unbiased Fisher definition (G2, normal distribution
// = 0), matching pandas' default and scipy's kurtosis(bias=False).
//
// Null values are excluded. Columns with fewer than four non-null values, or
// with zero variance, report NaN.
//
// This is analogous to df.kurt() in pandas.
//
// Example:
//
//	kurtosis := df.Kurt()
func (df *DataFrame) Kurt() map[string]float64 {
	return df.reduceNumeric(kurtSample)
}

// Mode returns the most frequent value(s) of every column, keyed by column name.
// Unlike the numeric reductions, Mode covers all columns because the most common
// value is meaningful for strings and booleans too.
//
// When several values tie for the highest frequency they are all returned, sorted
// ascending (numerically for numbers, lexicographically for strings, false before
// true for booleans). A column with no non-null values yields an empty slice.
//
// Null values are excluded, and values are compared by exact equality, so an
// untyped column holding both int64(1) and float64(1) counts them separately —
// the same rule ValueCounts follows. Values of a non-comparable type (a slice,
// for example) cannot be counted and are skipped.
//
// This is analogous to df.mode() in pandas.
//
// Example:
//
//	modes := df.Mode()
//	fmt.Println(modes["City"]) // e.g. [NYC]
func (df *DataFrame) Mode() map[string][]any {
	out := make(map[string][]any)
	if df == nil {
		return out
	}

	df.RLock()
	defer df.RUnlock()

	for _, name := range df.ColumnOrder {
		out[name] = modeOf(df.Columns[name])
	}
	return out
}

// IdxMax returns the index label of the first occurrence of the maximum value in
// each numeric column, keyed by column name.
//
// Ties resolve to the earliest row, matching pandas' behaviour. Null and NaN
// values are ignored; a column with no usable value is omitted from the result
// because it has no label to report. Labels come from the DataFrame's Index, with
// the row number used as a fallback when the index is shorter than the column.
//
// This is analogous to df.idxmax() in pandas.
//
// Example:
//
//	labels := df.IdxMax()
//	fmt.Println(labels["Salary"]) // index label of the highest salary
func (df *DataFrame) IdxMax() map[string]string {
	return df.idxExtreme(true)
}

// IdxMin returns the index label of the first occurrence of the minimum value in
// each numeric column, keyed by column name. It mirrors IdxMax in tie, null and
// label handling.
//
// This is analogous to df.idxmin() in pandas.
//
// Example:
//
//	labels := df.IdxMin()
func (df *DataFrame) IdxMin() map[string]string {
	return df.idxExtreme(false)
}

// Any reports, per column, whether at least one value is truthy. Boolean columns
// are truthy when true; numeric columns are truthy when non-zero. Columns of any
// other type (strings, datetimes) are omitted because they have no meaningful
// truth value.
//
// Null and NaN values are skipped, so a column with no usable value reports
// false, consistent with pandas' any() over an empty selection.
//
// This is analogous to df.any() in pandas.
//
// Example:
//
//	if df.Any()["IsActive"] {
//	    // at least one active row
//	}
func (df *DataFrame) Any() map[string]bool {
	return df.reduceTruth(false)
}

// All reports, per column, whether every value is truthy, using the same column
// eligibility and null handling as Any. A column with no usable value reports
// true, consistent with pandas' all() over an empty selection.
//
// This is analogous to df.all() in pandas.
//
// Example:
//
//	if df.All()["IsActive"] {
//	    // every row is active
//	}
func (df *DataFrame) All() map[string]bool {
	return df.reduceTruth(true)
}

// idxExtreme finds the label of the extreme value of every numeric column.
// largest selects the maximum, otherwise the minimum is selected.
func (df *DataFrame) idxExtreme(largest bool) map[string]string {
	out := make(map[string]string)
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

		bestRow := -1
		var best float64
		for i, n := 0, series.Len(); i < n; i++ {
			f, ok := numericAt(series, i)
			if !ok {
				continue // null, NaN or non-numeric values cannot be ranked
			}
			// A strict comparison keeps the earliest row on ties.
			if bestRow == -1 || (largest && f > best) || (!largest && f < best) {
				best, bestRow = f, i
			}
		}

		if bestRow == -1 {
			continue // nothing to label
		}
		out[name] = df.rowLabelAt(bestRow)
	}
	return out
}

// reduceTruth evaluates a boolean reduction over every eligible column. When
// requireAll is set the reduction is All, otherwise it is Any.
func (df *DataFrame) reduceTruth(requireAll bool) map[string]bool {
	out := make(map[string]bool)
	if df == nil {
		return out
	}

	df.RLock()
	defer df.RUnlock()

	for _, name := range df.ColumnOrder {
		series := df.Columns[name]
		if !isTruthySeries(series) {
			continue
		}

		// Any starts false and latches on the first truthy value; All starts true
		// and latches on the first falsy one. Both short-circuit.
		result := requireAll
		for i, n := 0, series.Len(); i < n; i++ {
			truthy, ok := truthAt(series, i)
			if !ok {
				continue // nulls and NaNs are skipped
			}
			if truthy != requireAll {
				result = truthy
				break
			}
		}
		out[name] = result
	}
	return out
}

// rowLabelAt returns the index label of a row, falling back to the row number
// when the DataFrame's index does not cover it. Callers must hold the read lock.
func (df *DataFrame) rowLabelAt(row int) string {
	if row >= 0 && row < len(df.Index) {
		return df.Index[row]
	}
	return strconv.Itoa(row)
}

// modeOf returns the most frequent non-null values of a series, sorted ascending.
func modeOf(series collection.Series) []any {
	out := make([]any, 0)
	if series == nil {
		return out
	}

	counts := make(map[any]int)
	order := make([]any, 0) // first-seen order, so equal counts stay deterministic
	best := 0

	for i, n := 0, series.Len(); i < n; i++ {
		if series.IsNull(i) {
			continue
		}
		val, err := series.At(i)
		if err != nil || val == nil {
			continue
		}
		if !reflect.TypeOf(val).Comparable() {
			continue // cannot be used as a map key, so it cannot be counted
		}
		if _, seen := counts[val]; !seen {
			order = append(order, val)
		}
		counts[val]++
		if counts[val] > best {
			best = counts[val]
		}
	}

	for _, v := range order {
		if counts[v] == best {
			out = append(out, v)
		}
	}
	sort.SliceStable(out, func(a, b int) bool { return lessAnyValues(out[a], out[b]) })
	return out
}

// lessAnyValues orders two values for Mode's output. Same-kind values compare
// naturally; mixed kinds fall back to a stable textual ordering.
func lessAnyValues(a, b any) bool {
	if af, aOK := toFloat64(a); aOK {
		if bf, bOK := toFloat64(b); bOK {
			return af < bf
		}
	}
	if as, aOK := a.(string); aOK {
		if bs, bOK := b.(string); bOK {
			return as < bs
		}
	}
	if ab, aOK := a.(bool); aOK {
		if bb, bOK := b.(bool); bOK {
			return !ab && bb // false sorts before true
		}
	}
	return fmt.Sprintf("%v", a) < fmt.Sprintf("%v", b)
}

// numericAt returns the value at row i as a float64, reporting false when the
// value is null, NaN or not numeric.
func numericAt(series collection.Series, i int) (float64, bool) {
	if series.IsNull(i) {
		return 0, false
	}
	val, err := series.At(i)
	if err != nil {
		return 0, false
	}
	f, ok := toFloat64(val)
	if !ok || math.IsNaN(f) {
		return 0, false
	}
	return f, true
}

// truthAt returns the truth value at row i, reporting false when the value is
// null, NaN or of a type without a truth value.
func truthAt(series collection.Series, i int) (bool, bool) {
	if series.IsNull(i) {
		return false, false
	}
	val, err := series.At(i)
	if err != nil {
		return false, false
	}
	if b, ok := val.(bool); ok {
		return b, true
	}
	if f, ok := toFloat64(val); ok {
		if math.IsNaN(f) {
			return false, false
		}
		return f != 0, true
	}
	return false, false
}

// isTruthySeries reports whether a series can take part in Any/All. Boolean and
// numeric dtypes always qualify; an untyped (any) series qualifies when all of
// its non-null values are booleans or numbers. An all-null or empty untyped
// series does not qualify, mirroring isNumericSeries.
func isTruthySeries(series collection.Series) bool {
	if series == nil {
		return false
	}
	if dt := series.DType(); dt != nil && dt.Kind() == reflect.Bool {
		return true
	}
	if isNumericSeries(series) {
		return true
	}

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
		if _, ok := v.(bool); ok {
			hasValue = true
			continue
		}
		if _, ok := toFloat64(v); !ok {
			return false
		}
		hasValue = true
	}
	return hasValue
}

// skewSample computes the adjusted Fisher-Pearson standardized moment
// coefficient (G1), the sample skewness pandas reports by default.
func skewSample(vals []float64) float64 {
	n := len(vals)
	if n < 3 {
		return math.NaN()
	}
	nf := float64(n)
	mean := sumFloats(vals) / nf

	var m2, m3 float64
	for _, v := range vals {
		d := v - mean
		d2 := d * d
		m2 += d2
		m3 += d2 * d
	}
	if m2 == 0 {
		return math.NaN() // zero variance leaves skewness undefined
	}

	// Population skewness, then the small-sample correction.
	g1 := (m3 / nf) / math.Pow(m2/nf, 1.5)
	return math.Sqrt(nf*(nf-1)) / (nf - 2) * g1
}

// kurtSample computes the unbiased sample excess kurtosis (G2) using Fisher's
// definition, so a normal distribution scores 0. This matches pandas' kurt().
func kurtSample(vals []float64) float64 {
	n := len(vals)
	if n < 4 {
		return math.NaN()
	}
	nf := float64(n)
	mean := sumFloats(vals) / nf

	var m2, m4 float64
	for _, v := range vals {
		d := v - mean
		d2 := d * d
		m2 += d2
		m4 += d2 * d2
	}
	if m2 == 0 {
		return math.NaN() // zero variance leaves kurtosis undefined
	}

	numerator := nf * (nf + 1) * (nf - 1) * m4
	denominator := (nf - 2) * (nf - 3) * m2 * m2
	adjustment := 3 * (nf - 1) * (nf - 1) / ((nf - 2) * (nf - 3))
	return numerator/denominator - adjustment
}
