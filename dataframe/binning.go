package dataframe

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// Cut returns a new DataFrame with a numeric column replaced by a categorical
// column naming the bin each value falls into.
//
// bins holds the bin edges and must be strictly increasing, so n+1 edges make n
// bins. Bins are closed on the right, (edges[i], edges[i+1]], exactly as in
// pandas: a value equal to the lowest edge falls outside every bin. Use
// math.Inf(-1) and math.Inf(1) as the outer edges to catch everything.
//
// labels names the bins and must have one entry per bin. Pass nil to use
// interval labels such as "(0, 10]".
//
// Nulls, NaN, and values outside the edges become null. The categories of the
// result list every bin in edge order, including bins no value landed in, so
// GetDummies on the result produces one column per bin. Other columns, column
// order, and index labels are preserved.
//
// This is analogous to pd.cut(df[column], bins, labels=labels) in pandas.
//
// Parameters:
//   - column: the numeric column to bin
//   - bins: strictly increasing bin edges; at least two
//   - labels: one name per bin, or nil for interval labels
//
// Returns:
//   - *DataFrame: a new DataFrame with the column binned
//   - error: nil if successful, otherwise an error
//
// Example:
//
//	// Ages into three labelled groups
//	df, err := df.Cut("Age", []float64{0, 18, 65, math.Inf(1)}, []string{"child", "adult", "senior"})
func (df *DataFrame) Cut(column string, bins []float64, labels []string) (*DataFrame, error) {
	return df.binColumn("Cut", column, labels, false, func([]float64, []bool) ([]float64, error) {
		return bins, nil
	})
}

// Qcut returns a new DataFrame with a numeric column replaced by a categorical
// column that splits its values into q bins holding roughly equal numbers of
// rows.
//
// The bin edges are the column's quantiles at 0, 1/q, ..., 1, computed with the
// same linear interpolation as Quantile and Describe. The first bin is closed on
// both sides so the minimum is included; the rest are closed on the right. Every
// non-null value therefore lands in exactly one bin.
//
// labels names the bins and must have q entries. Pass nil to use interval
// labels such as "[1, 2.5]" and "(2.5, 4]".
//
// A column with too few distinct values produces repeated edges, and an
// all-null column produces none; both are errors rather than silently yielding
// fewer bins than asked for. Nulls and NaN stay null. Other columns, column
// order, and index labels are preserved.
//
// This is analogous to pd.qcut(df[column], q, labels=labels) in pandas.
//
// Parameters:
//   - column: the numeric column to bin
//   - q: the number of bins; at least one
//   - labels: q bin names, or nil for interval labels
//
// Returns:
//   - *DataFrame: a new DataFrame with the column binned
//   - error: nil if successful, otherwise an error
//
// Example:
//
//	// Income quartiles
//	df, err := df.Qcut("Income", 4, []string{"Q1", "Q2", "Q3", "Q4"})
func (df *DataFrame) Qcut(column string, q int, labels []string) (*DataFrame, error) {
	if q < 1 {
		return nil, fmt.Errorf("Qcut: q must be at least 1, got %d", q)
	}
	return df.binColumn("Qcut", column, labels, true, func(data []float64, mask []bool) ([]float64, error) {
		sorted := make([]float64, 0, len(data))
		for i, v := range data {
			if !mask[i] && !math.IsNaN(v) {
				sorted = append(sorted, v)
			}
		}
		if len(sorted) == 0 {
			return nil, errors.New("column has no non-null values to take quantiles of")
		}
		sort.Float64s(sorted)

		edges := make([]float64, q+1)
		for i := range edges {
			edges[i] = quantileSorted(sorted, float64(i)/float64(q))
		}
		for i := 1; i < len(edges); i++ {
			if edges[i] == edges[i-1] {
				return nil, fmt.Errorf("bin edges %v are not unique; the column has too few distinct values for %d bins", edges, q)
			}
		}
		return edges, nil
	})
}

// binColumn implements Cut and Qcut. edgesFor receives the column's values and
// null mask and returns the bin edges. includeLowest closes the first bin on the
// left so that a value equal to the lowest edge is kept.
func (df *DataFrame) binColumn(
	name, column string,
	labels []string,
	includeLowest bool,
	edgesFor func(data []float64, mask []bool) ([]float64, error),
) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New(name + ": DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("%s: column '%s' not found", name, column)
	}
	data, mask, _, err := extractNumeric(series)
	if err != nil {
		return nil, fmt.Errorf("%s: column '%s' is not numeric: %w", name, column, err)
	}

	edges, err := edgesFor(data, mask)
	if err != nil {
		return nil, fmt.Errorf("%s: column '%s': %w", name, column, err)
	}
	if len(edges) < 2 {
		return nil, fmt.Errorf("%s: need at least two bin edges, got %d", name, len(edges))
	}
	for i, e := range edges {
		if math.IsNaN(e) {
			return nil, fmt.Errorf("%s: bin edge %d is NaN", name, i)
		}
		if i > 0 && e <= edges[i-1] {
			return nil, fmt.Errorf("%s: bin edges must be strictly increasing, got %v", name, edges)
		}
	}

	nBins := len(edges) - 1
	if labels == nil {
		labels = intervalLabels(edges, includeLowest)
	} else if len(labels) != nBins {
		return nil, fmt.Errorf("%s: got %d labels for %d bins", name, len(labels), nBins)
	}

	codes := make([]int32, len(data))
	for i, v := range data {
		codes[i] = binCode(v, mask[i], edges, includeLowest)
	}

	binned, err := collection.NewCategoricalSeriesFromCodes(codes, labels)
	if err != nil {
		return nil, fmt.Errorf("%s: labels: %w", name, err)
	}

	newCols := make(map[string]collection.Series, len(df.Columns))
	for colName, s := range df.Columns {
		newCols[colName] = s
	}
	newCols[column] = binned

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), df.ColumnOrder...),
		Index:       append([]string(nil), df.Index...),
	}, nil
}

// binCode returns the index of the right-closed bin (edges[i], edges[i+1]] that
// holds v, or -1 (null) when v is null, NaN, or outside every bin.
func binCode(v float64, null bool, edges []float64, includeLowest bool) int32 {
	if null || math.IsNaN(v) {
		return -1
	}
	// idx is the first edge >= v, so v lies in the bin ending at edges[idx].
	idx := sort.SearchFloat64s(edges, v)
	switch {
	case idx == 0 && includeLowest && v == edges[0]:
		return 0
	case idx == 0 || idx == len(edges):
		return -1
	default:
		return int32(idx - 1)
	}
}

// intervalLabels names each bin in interval notation, for example "(0, 10]".
// The first bin opens with "[" when it includes its lower edge.
func intervalLabels(edges []float64, includeLowest bool) []string {
	labels := make([]string, len(edges)-1)
	for i := range labels {
		open := "("
		if i == 0 && includeLowest {
			open = "["
		}
		labels[i] = open + formatEdge(edges[i]) + ", " + formatEdge(edges[i+1]) + "]"
	}
	return labels
}

// formatEdge prints an edge in its shortest exact form without an exponent, so
// 1000000 prints as "1000000" rather than "1e+06".
func formatEdge(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
