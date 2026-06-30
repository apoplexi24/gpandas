package dataframe

import (
	"errors"
	"math"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// Corr computes the pairwise Pearson correlation matrix over the numeric columns
// of the DataFrame. The result is a square DataFrame whose columns and index are
// the numeric column names, with each cell holding the correlation coefficient.
//
// Correlations are computed over rows where both columns are non-null (pairwise
// complete observations). A pair with fewer than two overlapping observations,
// or with zero variance, yields NaN.
//
// This is analogous to df.corr() in pandas.
//
// Example:
//
//	c, err := df.Corr()
func (df *DataFrame) Corr() (*DataFrame, error) {
	return df.pairwiseMatrix(true)
}

// Cov computes the pairwise sample covariance matrix (ddof=1) over the numeric
// columns of the DataFrame, structured like Corr.
//
// This is analogous to df.cov() in pandas.
//
// Example:
//
//	c, err := df.Cov()
func (df *DataFrame) Cov() (*DataFrame, error) {
	return df.pairwiseMatrix(false)
}

// pairwiseMatrix builds a correlation (corr=true) or covariance (corr=false)
// matrix over numeric columns.
func (df *DataFrame) pairwiseMatrix(corr bool) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("pairwise: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	// Identify numeric columns in order.
	numericCols := make([]string, 0, len(df.ColumnOrder))
	for _, name := range df.ColumnOrder {
		if isNumericSeries(df.Columns[name]) {
			numericCols = append(numericCols, name)
		}
	}
	if len(numericCols) == 0 {
		return nil, errors.New("pairwise: no numeric columns")
	}

	// Pre-extract raw values and null masks per column.
	type colData struct {
		vals []float64
		null []bool
	}
	data := make(map[string]colData, len(numericCols))
	rowCount := df.Columns[numericCols[0]].Len()
	for _, name := range numericCols {
		s := df.Columns[name]
		cv := colData{vals: make([]float64, rowCount), null: make([]bool, rowCount)}
		for i := 0; i < rowCount; i++ {
			if s.IsNull(i) {
				cv.null[i] = true
				continue
			}
			v, _ := s.At(i)
			f, ok := toFloat64(v)
			if !ok {
				cv.null[i] = true
				continue
			}
			cv.vals[i] = f
		}
		data[name] = cv
	}

	// Build result columns (one float64 column per numeric column).
	resultCols := make(map[string]collection.Series, len(numericCols))
	for _, colName := range numericCols {
		cells := make([]float64, len(numericCols))
		mask := make([]bool, len(numericCols))
		a := data[colName]
		for r, rowName := range numericCols {
			b := data[rowName]
			val, ok := pairwiseStat(a.vals, a.null, b.vals, b.null, rowCount, corr)
			if !ok {
				mask[r] = true
			} else {
				cells[r] = val
			}
		}
		s, _ := collection.NewFloat64SeriesFromData(cells, mask)
		resultCols[colName] = s
	}

	return &DataFrame{
		Columns:     resultCols,
		ColumnOrder: append([]string(nil), numericCols...),
		Index:       append([]string(nil), numericCols...),
	}, nil
}

// pairwiseStat computes the covariance or Pearson correlation between two
// columns over their pairwise-complete (both non-null) observations. Returns
// (value, true) on success, or (_, false) when the result is undefined.
func pairwiseStat(ax []float64, an []bool, bx []float64, bn []bool, n int, corr bool) (float64, bool) {
	// Collect paired observations.
	var sumA, sumB float64
	count := 0
	for i := 0; i < n; i++ {
		if an[i] || bn[i] {
			continue
		}
		sumA += ax[i]
		sumB += bx[i]
		count++
	}
	if count < 2 {
		return 0, false
	}
	meanA := sumA / float64(count)
	meanB := sumB / float64(count)

	var cov, varA, varB float64
	for i := 0; i < n; i++ {
		if an[i] || bn[i] {
			continue
		}
		da := ax[i] - meanA
		db := bx[i] - meanB
		cov += da * db
		varA += da * da
		varB += db * db
	}
	denom := float64(count - 1)
	cov /= denom

	if !corr {
		return cov, true
	}

	// Correlation = cov / (stdA * stdB).
	stdA := math.Sqrt(varA / denom)
	stdB := math.Sqrt(varB / denom)
	if stdA == 0 || stdB == 0 {
		return 0, false
	}
	return cov / (stdA * stdB), true
}
