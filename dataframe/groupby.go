package dataframe

import (
	"errors"
	"fmt"
	"sort"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// GroupBy represents a grouped DataFrame.
type GroupBy struct {
	df       *DataFrame
	groups   map[string][]int // Map of group key to row indices
	axis     int
	colNames []string // Columns used for grouping
}

// GroupBy groups the DataFrame using a mapper or by a Series of columns.
// A groupby operation involves some combination of splitting the object, applying a function, and combining the results.
// This can be used to group large amounts of data and compute operations on these groups.
//
// Parameters:
//   - by: A slice of strings representing the column names to group by.
//   - axis: The axis to group along. 0 for rows, 1 for columns. Currently only axis 0 is supported for grouping by columns.
//
// Returns:
//   - A pointer to a GroupBy object.
//   - An error if the operation fails (e.g., invalid column names).
func (df *DataFrame) GroupBy(by []string, axis int) (*GroupBy, error) {
	if axis != 0 {
		return nil, fmt.Errorf("axis %d is not supported yet, only axis 0 (rows) is supported", axis)
	}

	// Validate columns
	for _, col := range by {
		if _, ok := df.Columns[col]; !ok {
			return nil, fmt.Errorf("column %s not found", col)
		}
	}

	if len(by) == 0 {
		return nil, fmt.Errorf("at least one grouping column is required")
	}

	groups := make(map[string][]int)
	numRows := df.Len()

	// Iterate over rows to build groups. The key is built with a non-printable
	// separator so that multi-column keys cannot collide (see keys.go).
	for i := 0; i < numRows; i++ {
		key := compositeKeyAt(df, by, i)
		groups[key] = append(groups[key], i)
	}

	return &GroupBy{
		df:       df,
		groups:   groups,
		axis:     axis,
		colNames: by,
	}, nil
}

// getSortedKeys returns the group keys sorted to ensure deterministic output order.
func (gb *GroupBy) getSortedKeys() []string {
	keys := make([]string, 0, len(gb.groups))
	for k := range gb.groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// The methods below return one row per group: the grouping columns, keeping
// their original value types, followed by one column per aggregated column
// under its original name. Rows are ordered by group key. Each method gives the
// same values as the equivalent Agg spec, because both run the same kernels.
//
// Numeric methods (Mean, Sum, Min, Max, Std, Var, Median) cover the numeric
// columns and skip the rest, as DataFrame.Mean does. Count, First, and Last
// cover every non-grouping column.

// Mean computes the mean of each numeric column per group. Nulls are skipped; a
// group with no values yields null.
//
// This is analogous to df.groupby(...).mean(numeric_only=True) in pandas.
func (gb *GroupBy) Mean() (*DataFrame, error) { return gb.aggregateEach("Mean", AggMean) }

// Sum computes the sum of each numeric column per group. Nulls are skipped; a
// group with no values sums to 0, as in pandas.
func (gb *GroupBy) Sum() (*DataFrame, error) { return gb.aggregateEach("Sum", AggSum) }

// Min computes the minimum of each numeric column per group. A group with no
// values yields null.
func (gb *GroupBy) Min() (*DataFrame, error) { return gb.aggregateEach("Min", AggMin) }

// Max computes the maximum of each numeric column per group. A group with no
// values yields null.
func (gb *GroupBy) Max() (*DataFrame, error) { return gb.aggregateEach("Max", AggMax) }

// Std computes the sample standard deviation (ddof=1) of each numeric column per
// group. A group with fewer than two values yields NaN, as in pandas.
func (gb *GroupBy) Std() (*DataFrame, error) { return gb.aggregateEach("Std", AggStd) }

// Var computes the sample variance (ddof=1) of each numeric column per group. A
// group with fewer than two values yields NaN, as in pandas.
func (gb *GroupBy) Var() (*DataFrame, error) { return gb.aggregateEach("Var", AggVar) }

// Median computes the median of each numeric column per group. A group with no
// values yields null.
func (gb *GroupBy) Median() (*DataFrame, error) { return gb.aggregateEach("Median", AggMedian) }

// Count counts the non-null values of every non-grouping column per group, as
// int64. Use Size to count rows regardless of nulls.
func (gb *GroupBy) Count() (*DataFrame, error) { return gb.aggregateEach("Count", AggCount) }

// First returns the first non-null value of every non-grouping column per group.
// A group whose column is entirely null yields null.
func (gb *GroupBy) First() (*DataFrame, error) { return gb.aggregateEach("First", AggFirst) }

// Last returns the last non-null value of every non-grouping column per group. A
// group whose column is entirely null yields null.
func (gb *GroupBy) Last() (*DataFrame, error) { return gb.aggregateEach("Last", AggLast) }

// Size counts the rows in each group, nulls included, in a single int64 column
// named "size" after the grouping columns.
//
// This is analogous to df.groupby(...).size() in pandas, returned as a
// DataFrame rather than a Series.
func (gb *GroupBy) Size() (*DataFrame, error) { return gb.aggregateEach("Size", AggSize) }

// Cumcount numbers the rows of each group 0, 1, 2, ... in row order. The result
// is aligned with the source DataFrame, so it can be added back with Assign.
//
// This is analogous to df.groupby(...).cumcount() in pandas.
//
// Example:
//
//	gb, _ := df.GroupBy([]string{"Dept"}, 0)
//	nth, _ := gb.Cumcount()
//	_ = df.Assign("NthInDept", nth)
func (gb *GroupBy) Cumcount() (collection.Series, error) {
	if gb == nil || gb.df == nil {
		return nil, errors.New("Cumcount: GroupBy is nil")
	}
	out := make([]int64, gb.df.Len())
	for _, rows := range gb.groups {
		// Rows were appended in ascending order when the groups were built.
		for pos, row := range rows {
			out[row] = int64(pos)
		}
	}
	return collection.NewInt64SeriesFromData(out, nil)
}

// Transform applies an aggregation per group and broadcasts each group's result
// back to that group's rows, so the result has the same rows and index labels as
// the source DataFrame. The grouping columns are left out, as in pandas.
//
// fn covers the same columns as the matching convenience method: numeric
// functions cover numeric columns, AggCount, AggFirst, and AggLast cover every
// non-grouping column, and AggSize yields a single "size" column.
//
// This is analogous to df.groupby(...).transform(fn) in pandas with a named
// function.
//
// Example:
//
//	// Each salary alongside its department's mean salary
//	gb, _ := df.GroupBy([]string{"Dept"}, 0)
//	means, _ := gb.Transform(dataframe.AggMean)
//	_ = df.Assign("DeptMean", means.Columns["Salary"])
func (gb *GroupBy) Transform(fn AggFunc) (*DataFrame, error) {
	if gb == nil || gb.df == nil {
		return nil, errors.New("Transform: GroupBy is nil")
	}
	targets, err := gb.targetsFor(fn)
	if err != nil {
		return nil, fmt.Errorf("Transform: %w", err)
	}

	gb.df.RLock()
	defer gb.df.RUnlock()

	n := gb.df.Len()
	cols := make(map[string]collection.Series, len(targets))
	order := make([]string, 0, len(targets))
	for _, t := range targets {
		series := gb.df.Columns[t.col] // nil for AggSize
		values := make([]any, n)
		for _, rows := range gb.groups {
			v, err := aggregateGroup(series, rows, t.fn)
			if err != nil {
				return nil, fmt.Errorf("Transform: column '%s': %w", t.col, err)
			}
			for _, row := range rows {
				values[row] = v
			}
		}
		s, err := seriesFromAnyValues(values)
		if err != nil {
			return nil, fmt.Errorf("Transform: building column '%s': %w", t.out, err)
		}
		cols[t.out] = s
		order = append(order, t.out)
	}

	return &DataFrame{
		Columns:     cols,
		ColumnOrder: order,
		Index:       append([]string(nil), gb.df.Index...),
	}, nil
}

// aggregateEach applies fn to every column it covers and names each result
// column after its source column.
func (gb *GroupBy) aggregateEach(name string, fn AggFunc) (*DataFrame, error) {
	if gb == nil || gb.df == nil {
		return nil, errors.New(name + ": GroupBy is nil")
	}
	targets, err := gb.targetsFor(fn)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return gb.aggregateTargets(name, targets)
}

// Apply applies a function to each group and combines the results.
func (gb *GroupBy) Apply(f func(*DataFrame) (*DataFrame, error)) (*DataFrame, error) {
	sortedKeys := gb.getSortedKeys()
	var resultParts []*DataFrame

	for _, key := range sortedKeys {
		indices := gb.groups[key]

		// Create sub-DataFrame for the group
		subDF, err := gb.df.Slice(indices)
		if err != nil {
			return nil, err
		}

		// Apply function
		resDF, err := f(subDF)
		if err != nil {
			return nil, err
		}

		if resDF != nil {
			resultParts = append(resultParts, resDF)
		}
	}

	if len(resultParts) == 0 {
		return nil, nil // Or empty DataFrame
	}

	// Combine results using Concat
	return Concat(resultParts, ConcatOptions{IgnoreIndex: true})
}
