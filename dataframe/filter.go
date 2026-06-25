package dataframe

import (
	"errors"
	"fmt"
)

// FilterOp represents a comparison operator used by DataFrame.Filter.
type FilterOp string

const (
	// Equals keeps rows where the column value equals the target value.
	Equals FilterOp = "=="
	// NotEquals keeps rows where the column value differs from the target value.
	NotEquals FilterOp = "!="
	// GreaterThan keeps rows where the column value is greater than the target value.
	GreaterThan FilterOp = ">"
	// GreaterThanOrEqual keeps rows where the column value is >= the target value.
	GreaterThanOrEqual FilterOp = ">="
	// LessThan keeps rows where the column value is less than the target value.
	LessThan FilterOp = "<"
	// LessThanOrEqual keeps rows where the column value is <= the target value.
	LessThanOrEqual FilterOp = "<="
)

// FilterChain enables fluent, error-deferred row filtering, mirroring pandas
// boolean indexing like df[df.a > 1][df.b == "x"].
//
// A chain is started with DataFrame.Filter or DataFrame.Where, continued with
// additional Filter/Where calls, and terminated with Result (or MustResult).
// If any step fails, the error is carried through the chain and surfaced by the
// terminal call; subsequent steps become no-ops.
//
// Example:
//
//	result, err := df.
//	    Filter("Age", dataframe.GreaterThan, int64(25)).
//	    Filter("City", dataframe.Equals, "NYC").
//	    Result()
type FilterChain struct {
	df  *DataFrame
	err error
}

// Filter starts a fluent filter chain, keeping rows where the value in the given
// column satisfies the comparison (column <op> value).
//
// This is analogous to boolean indexing in pandas, e.g. df[df["age"] > 25].
//
// Null values never satisfy a comparison and are therefore excluded from the
// result (mirroring pandas' behaviour where comparisons against NaN are False).
//
// Numeric values are compared numerically across int, int64 and float64, so a
// float64 column can be filtered with an int literal and vice versa. Strings and
// booleans are compared with their natural ordering (false < true).
//
// The returned *FilterChain can be chained with further Filter/Where calls and
// must be terminated with Result (or MustResult) to obtain the DataFrame.
//
// Example:
//
//	// Single condition
//	adults, err := df.Filter("Age", dataframe.GreaterThan, int64(25)).Result()
//
//	// Chained conditions
//	result, err := df.
//	    Filter("Age", dataframe.GreaterThan, int64(25)).
//	    Filter("City", dataframe.Equals, "NYC").
//	    Result()
func (df *DataFrame) Filter(column string, op FilterOp, value any) *FilterChain {
	return (&FilterChain{df: df}).Filter(column, op, value)
}

// Where starts a fluent filter chain, keeping rows for which the predicate
// returns true. The predicate receives a map of column name to value for the row
// (null values are passed as nil), enabling arbitrary multi-column conditions.
//
// The returned *FilterChain can be chained with further Filter/Where calls and
// must be terminated with Result (or MustResult).
//
// Example:
//
//	result, err := df.Where(func(row map[string]any) bool {
//	    age, _ := row["Age"].(int64)
//	    return age > 25 && row["City"] == "NYC"
//	}).Result()
func (df *DataFrame) Where(predicate func(row map[string]any) bool) *FilterChain {
	return (&FilterChain{df: df}).Where(predicate)
}

// Filter applies an additional comparison filter to the chain. If the chain
// already holds an error, it is returned unchanged.
func (c *FilterChain) Filter(column string, op FilterOp, value any) *FilterChain {
	if c.err != nil {
		return c
	}
	newDF, err := c.df.filterOnce(column, op, value)
	if err != nil {
		return &FilterChain{df: c.df, err: err}
	}
	return &FilterChain{df: newDF}
}

// Where applies an additional predicate filter to the chain. If the chain
// already holds an error, it is returned unchanged.
func (c *FilterChain) Where(predicate func(row map[string]any) bool) *FilterChain {
	if c.err != nil {
		return c
	}
	newDF, err := c.df.whereOnce(predicate)
	if err != nil {
		return &FilterChain{df: c.df, err: err}
	}
	return &FilterChain{df: newDF}
}

// Result terminates the chain, returning the resulting DataFrame and the first
// error encountered (if any).
func (c *FilterChain) Result() (*DataFrame, error) {
	return c.df, c.err
}

// Err returns the first error encountered in the chain, or nil.
func (c *FilterChain) Err() error {
	return c.err
}

// MustResult terminates the chain and returns the resulting DataFrame, panicking
// if the chain encountered an error. Useful in tests or when inputs are known
// to be valid.
func (c *FilterChain) MustResult() *DataFrame {
	if c.err != nil {
		panic(c.err)
	}
	return c.df
}

// filterOnce performs a single comparison filter and returns a new DataFrame.
func (df *DataFrame) filterOnce(column string, op FilterOp, value any) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("Filter: DataFrame is nil")
	}

	switch op {
	case Equals, NotEquals, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual:
		// valid
	default:
		return nil, fmt.Errorf("Filter: unsupported operator '%s'", op)
	}

	df.RLock()

	series, ok := df.Columns[column]
	if !ok {
		df.RUnlock()
		return nil, fmt.Errorf("Filter: column '%s' not found", column)
	}

	rowCount := series.Len()
	keep := make([]int, 0, rowCount)

	for i := 0; i < rowCount; i++ {
		if series.IsNull(i) {
			continue // nulls never match a comparison
		}
		val, err := series.At(i)
		if err != nil {
			df.RUnlock()
			return nil, fmt.Errorf("Filter: error reading row %d: %w", i, err)
		}

		cmp, err := compareForFilter(val, value)
		if err != nil {
			df.RUnlock()
			return nil, fmt.Errorf("Filter: %w", err)
		}

		if matchesOp(op, cmp) {
			keep = append(keep, i)
		}
	}

	df.RUnlock()

	return df.Slice(keep)
}

// whereOnce performs a single predicate filter and returns a new DataFrame.
func (df *DataFrame) whereOnce(predicate func(row map[string]any) bool) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("Where: DataFrame is nil")
	}
	if predicate == nil {
		return nil, errors.New("Where: predicate must not be nil")
	}

	df.RLock()

	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	keep := make([]int, 0, rowCount)
	row := make(map[string]any, len(df.ColumnOrder))

	for i := 0; i < rowCount; i++ {
		for _, colName := range df.ColumnOrder {
			series := df.Columns[colName]
			if series.IsNull(i) {
				row[colName] = nil
				continue
			}
			val, err := series.At(i)
			if err != nil {
				df.RUnlock()
				return nil, fmt.Errorf("Where: error reading column '%s' row %d: %w", colName, i, err)
			}
			row[colName] = val
		}
		if predicate(row) {
			keep = append(keep, i)
		}
	}

	df.RUnlock()

	return df.Slice(keep)
}

// compareForFilter compares two non-nil values, treating all numeric kinds
// (int, int64, float64) as cross-comparable. Falls back to the strongly-typed
// compareValues for strings and booleans.
func compareForFilter(a, b any) (int, error) {
	af, aok := toFloat64(a)
	bf, bok := toFloat64(b)
	if aok && bok {
		switch {
		case af < bf:
			return -1, nil
		case af > bf:
			return 1, nil
		default:
			return 0, nil
		}
	}
	return compareValues(a, b)
}

// matchesOp reports whether the comparison result satisfies the operator.
func matchesOp(op FilterOp, cmp int) bool {
	switch op {
	case Equals:
		return cmp == 0
	case NotEquals:
		return cmp != 0
	case GreaterThan:
		return cmp > 0
	case GreaterThanOrEqual:
		return cmp >= 0
	case LessThan:
		return cmp < 0
	case LessThanOrEqual:
		return cmp <= 0
	default:
		return false
	}
}
