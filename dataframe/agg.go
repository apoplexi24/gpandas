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
	// AggVar computes the sample variance (ddof=1).
	AggVar AggFunc = "var"
	// AggSize counts the rows in the group, nulls included. Unlike AggCount it
	// does not depend on the column's values.
	AggSize AggFunc = "size"
)

// aggTarget is one output column of an aggregation: fn applied to the source
// column col, written to out. col is empty for AggSize, which reads no column.
type aggTarget struct {
	out string
	col string
	fn  AggFunc
}

// Agg applies one or more aggregation functions to one or more columns of each
// group, producing a new DataFrame.
//
// The spec maps a column name to the list of aggregation functions to apply to
// it. The result contains the grouping columns followed by one column per
// (column, function) pair, named "<column>_<func>" (e.g. "revenue_sum"). Rows
// correspond to the groups, ordered by group key.
//
// Supported functions: AggSum, AggMean, AggCount, AggMin, AggMax, AggStd,
// AggVar, AggMedian, AggFirst, AggLast, AggSize. Numeric functions ignore null
// and non-numeric values; AggCount counts non-null values; AggSize counts rows;
// AggFirst/AggLast return the first/last non-null value of any type.
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

	// Iterate value columns in the DataFrame's column order for deterministic
	// output, then the requested functions in order.
	var targets []aggTarget
	for _, colName := range gb.df.ColumnOrder {
		for _, fn := range spec[colName] {
			targets = append(targets, aggTarget{out: fmt.Sprintf("%s_%s", colName, fn), col: colName, fn: fn})
		}
	}
	return gb.aggregateTargets("Agg", targets)
}

// aggregateTargets builds the one-row-per-group result shared by Agg and the
// convenience methods: the grouping columns, with their original value types,
// followed by one column per target. Rows are ordered by group key.
func (gb *GroupBy) aggregateTargets(name string, targets []aggTarget) (*DataFrame, error) {
	gb.df.RLock()
	defer gb.df.RUnlock()

	sortedKeys := gb.getSortedKeys()
	numGroups := len(sortedKeys)

	resultCols := make(map[string]collection.Series, len(gb.colNames)+len(targets))
	resultOrder := make([]string, 0, len(gb.colNames)+len(targets))

	for _, colName := range gb.colNames {
		values := make([]any, numGroups)
		for i, key := range sortedKeys {
			values[i], _ = gb.df.Columns[colName].At(gb.groups[key][0])
		}
		s, err := seriesFromAnyValues(values)
		if err != nil {
			return nil, fmt.Errorf("%s: building grouping column '%s': %w", name, colName, err)
		}
		resultCols[colName] = s
		resultOrder = append(resultOrder, colName)
	}

	for _, t := range targets {
		series := gb.df.Columns[t.col] // nil for AggSize, which reads no column
		values := make([]any, numGroups)
		for i, key := range sortedKeys {
			v, err := aggregateGroup(series, gb.groups[key], t.fn)
			if err != nil {
				return nil, fmt.Errorf("%s: column '%s' func '%s': %w", name, t.col, t.fn, err)
			}
			values[i] = v
		}
		s, err := seriesFromAnyValues(values)
		if err != nil {
			return nil, fmt.Errorf("%s: building column '%s': %w", name, t.out, err)
		}
		resultCols[t.out] = s
		resultOrder = append(resultOrder, t.out)
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

// targetsFor returns the output columns fn produces when applied to a whole
// GroupBy, as the convenience methods and Transform do. Numeric functions cover
// the numeric non-grouping columns, matching DataFrame.Std and friends; AggCount,
// AggFirst, and AggLast cover every non-grouping column; AggSize produces a
// single "size" column. Each output keeps its source column's name.
func (gb *GroupBy) targetsFor(fn AggFunc) ([]aggTarget, error) {
	var numericOnly bool
	switch fn {
	case AggSize:
		return []aggTarget{{out: "size", fn: AggSize}}, nil
	case AggSum, AggMean, AggMin, AggMax, AggStd, AggVar, AggMedian:
		numericOnly = true
	case AggCount, AggFirst, AggLast:
		numericOnly = false
	default:
		return nil, fmt.Errorf("unsupported aggregation function '%s'", fn)
	}

	isKey := make(map[string]bool, len(gb.colNames))
	for _, c := range gb.colNames {
		isKey[c] = true
	}

	gb.df.RLock()
	defer gb.df.RUnlock()

	var targets []aggTarget
	for _, col := range gb.df.ColumnOrder {
		if isKey[col] || (numericOnly && !isNumericSeries(gb.df.Columns[col])) {
			continue
		}
		targets = append(targets, aggTarget{out: col, col: col, fn: fn})
	}
	return targets, nil
}

// aggregateGroup applies a single aggregation function to the given row indices
// of a series and returns the scalar result. series may be nil for AggSize.
func aggregateGroup(series collection.Series, indices []int, fn AggFunc) (any, error) {
	switch fn {
	case AggSize:
		return int64(len(indices)), nil

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
	case AggVar:
		return varSample(vals), nil // NaN when < 2 values
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
