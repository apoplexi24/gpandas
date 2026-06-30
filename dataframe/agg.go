package dataframe

import (
	"fmt"
	"math"
	"sort"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// Additional aggregation functions usable with GroupBy.Agg (in addition to
// AggSum, AggMean, AggCount, AggMin, AggMax defined in pivot.go).
const (
	// AggStd computes the sample standard deviation (ddof=1).
	AggStd AggFunc = "std"
	// AggMedian computes the median.
	AggMedian AggFunc = "median"
	// AggFirst returns the first non-null value in the group.
	AggFirst AggFunc = "first"
	// AggLast returns the last non-null value in the group.
	AggLast AggFunc = "last"
)

// Agg applies one or more aggregation functions to one or more columns of each
// group, producing a new DataFrame.
//
// The spec maps a column name to the list of aggregation functions to apply to
// it. The result contains the grouping columns followed by one column per
// (column, function) pair, named "<column>_<func>" (e.g. "revenue_sum"). Rows
// correspond to the groups, ordered by group key.
//
// Supported functions: AggSum, AggMean, AggCount, AggMin, AggMax, AggStd,
// AggMedian, AggFirst, AggLast. Numeric functions ignore null and non-numeric
// values; AggCount counts non-null values; AggFirst/AggLast return the
// first/last non-null value of any type.
//
// This is analogous to df.groupby(...).agg({...}) in pandas.
//
// Example:
//
//	gb, _ := df.GroupBy([]string{"Department"}, 0)
//	result, _ := gb.Agg(map[string][]dataframe.AggFunc{
//	    "Salary": {dataframe.AggMean, dataframe.AggMax},
//	    "Name":   {dataframe.AggCount},
//	})
func (gb *GroupBy) Agg(spec map[string][]AggFunc) (*DataFrame, error) {
	if gb == nil || gb.df == nil {
		return nil, fmt.Errorf("Agg: GroupBy is nil")
	}
	if len(spec) == 0 {
		return nil, fmt.Errorf("Agg: spec must contain at least one column")
	}

	// Validate spec columns exist.
	for col := range spec {
		if _, ok := gb.df.Columns[col]; !ok {
			return nil, fmt.Errorf("Agg: column '%s' not found", col)
		}
	}

	sortedKeys := gb.getSortedKeys()
	numGroups := len(sortedKeys)

	resultCols := make(map[string]collection.Series, len(gb.colNames)+len(spec))
	resultOrder := make([]string, 0, len(gb.colNames)+len(spec))

	// Grouping columns first (preserve original value types).
	for _, colName := range gb.colNames {
		values := make([]any, numGroups)
		for i, key := range sortedKeys {
			firstIdx := gb.groups[key][0]
			v, _ := gb.df.Columns[colName].At(firstIdx)
			values[i] = v
		}
		s, err := seriesFromAnyValues(values)
		if err != nil {
			return nil, fmt.Errorf("Agg: building grouping column '%s': %w", colName, err)
		}
		resultCols[colName] = s
		resultOrder = append(resultOrder, colName)
	}

	// Aggregated columns, iterating value columns in the DataFrame's column
	// order for deterministic output, then the requested functions in order.
	for _, colName := range gb.df.ColumnOrder {
		funcs, ok := spec[colName]
		if !ok {
			continue
		}
		series := gb.df.Columns[colName]
		for _, fn := range funcs {
			outName := fmt.Sprintf("%s_%s", colName, fn)
			values := make([]any, numGroups)
			for i, key := range sortedKeys {
				v, err := aggregateGroup(series, gb.groups[key], fn)
				if err != nil {
					return nil, fmt.Errorf("Agg: column '%s' func '%s': %w", colName, fn, err)
				}
				values[i] = v
			}
			s, err := seriesFromAnyValues(values)
			if err != nil {
				return nil, fmt.Errorf("Agg: building column '%s': %w", outName, err)
			}
			resultCols[outName] = s
			resultOrder = append(resultOrder, outName)
		}
	}

	index := make([]string, numGroups)
	for i := 0; i < numGroups; i++ {
		index[i] = fmt.Sprintf("%d", i)
	}

	return &DataFrame{
		Columns:     resultCols,
		ColumnOrder: resultOrder,
		Index:       index,
	}, nil
}

// aggregateGroup applies a single aggregation function to the given row indices
// of a series and returns the scalar result.
func aggregateGroup(series collection.Series, indices []int, fn AggFunc) (any, error) {
	switch fn {
	case AggCount:
		count := int64(0)
		for _, idx := range indices {
			if !series.IsNull(idx) {
				count++
			}
		}
		return count, nil

	case AggFirst:
		for _, idx := range indices {
			if !series.IsNull(idx) {
				v, _ := series.At(idx)
				return v, nil
			}
		}
		return nil, nil

	case AggLast:
		for i := len(indices) - 1; i >= 0; i-- {
			if !series.IsNull(indices[i]) {
				v, _ := series.At(indices[i])
				return v, nil
			}
		}
		return nil, nil
	}

	// Numeric aggregations: collect non-null numeric values.
	vals := make([]float64, 0, len(indices))
	for _, idx := range indices {
		if series.IsNull(idx) {
			continue
		}
		v, _ := series.At(idx)
		if f, ok := toFloat64(v); ok {
			vals = append(vals, f)
		}
	}

	switch fn {
	case AggSum:
		return sumFloats(vals), nil // 0 for empty group, matching pandas
	case AggMean:
		if len(vals) == 0 {
			return nil, nil
		}
		return sumFloats(vals) / float64(len(vals)), nil
	case AggStd:
		return stdSample(vals), nil // NaN when < 2 values
	case AggMedian:
		if len(vals) == 0 {
			return nil, nil
		}
		sorted := append([]float64(nil), vals...)
		sort.Float64s(sorted)
		return quantileSorted(sorted, 0.5), nil
	case AggMin:
		if len(vals) == 0 {
			return nil, nil
		}
		m := vals[0]
		for _, v := range vals[1:] {
			m = math.Min(m, v)
		}
		return m, nil
	case AggMax:
		if len(vals) == 0 {
			return nil, nil
		}
		m := vals[0]
		for _, v := range vals[1:] {
			m = math.Max(m, v)
		}
		return m, nil
	default:
		return nil, fmt.Errorf("unsupported aggregation function '%s'", fn)
	}
}
