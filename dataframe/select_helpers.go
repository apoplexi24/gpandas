package dataframe

import (
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// Inclusive controls which bounds DataFrame.Between treats as part of the range,
// mirroring the pandas Series.between "inclusive" argument.
//
// The zero value ("") behaves like InclusiveBoth.
type Inclusive string

const (
	// InclusiveBoth keeps values in [low, high]. This is the default.
	InclusiveBoth Inclusive = "both"
	// InclusiveNeither keeps values in (low, high).
	InclusiveNeither Inclusive = "neither"
	// InclusiveLeft keeps values in [low, high).
	InclusiveLeft Inclusive = "left"
	// InclusiveRight keeps values in (low, high].
	InclusiveRight Inclusive = "right"
)

// Isin starts a fluent filter chain, keeping rows whose value in the given column
// is a member of values. It is the analogue of pandas df[df["col"].isin([...])].
//
// Membership is tested with a hash set built once from values, so the filter runs
// in O(rows + len(values)) time. Numeric values are matched across int, int64 and
// float64 (an int column can be filtered with float64 members and vice versa);
// strings, booleans and other comparable types are matched by equality.
//
// Null values never match, and an empty values slice selects no rows (consistent
// with pandas, where isin([]) is False everywhere).
//
// The returned *FilterChain can be chained with further Filter/Where/Isin/Between
// calls and must be terminated with Result (or MustResult).
//
// Example:
//
//	result, err := df.Isin("City", []any{"NYC", "LA"}).Result()
func (df *DataFrame) Isin(column string, values []any) *FilterChain {
	return (&FilterChain{df: df}).Isin(column, values)
}

// Between starts a fluent filter chain, keeping rows whose value in the given
// column falls between low and high. It is the analogue of pandas
// df[df["col"].between(low, high)].
//
// Which bounds are part of the range is controlled by inclusive; the zero value
// ("") means InclusiveBoth. Numeric bounds are compared across int, int64 and
// float64, and null values never fall inside the range.
//
// The returned *FilterChain can be chained with further Filter/Where/Isin/Between
// calls and must be terminated with Result (or MustResult).
//
// Example:
//
//	// 25 <= Age <= 35
//	result, err := df.Between("Age", 25, 35, dataframe.InclusiveBoth).Result()
func (df *DataFrame) Between(column string, low, high any, inclusive Inclusive) *FilterChain {
	return (&FilterChain{df: df}).Between(column, low, high, inclusive)
}

// Nlargest starts a fluent filter chain, keeping the n rows with the largest
// values in the given column, ordered from largest to smallest. It is the
// analogue of pandas df.nlargest(n, "col").
//
// Selection uses a bounded heap of size n rather than a full sort, so it runs in
// O(rows * log n) time with O(n) extra space. Ties keep the row that appears
// first (pandas' keep="first" default), null values are never ranked, and fewer
// than n rows are returned when the column holds fewer non-null values.
//
// The returned *FilterChain can be chained with further Filter/Where/Isin/Between
// calls and must be terminated with Result (or MustResult).
//
// Example:
//
//	top3, err := df.Nlargest(3, "Salary").Result()
func (df *DataFrame) Nlargest(n int, column string) *FilterChain {
	return (&FilterChain{df: df}).Nlargest(n, column)
}

// Nsmallest starts a fluent filter chain, keeping the n rows with the smallest
// values in the given column, ordered from smallest to largest. It is the
// analogue of pandas df.nsmallest(n, "col").
//
// It shares Nlargest's cost model (O(rows * log n) time, O(n) extra space), tie
// handling and null handling.
//
// Example:
//
//	cheapest, err := df.Nsmallest(3, "Price").Result()
func (df *DataFrame) Nsmallest(n int, column string) *FilterChain {
	return (&FilterChain{df: df}).Nsmallest(n, column)
}

// Isin applies an additional set-membership filter to the chain. If the chain
// already holds an error, it is returned unchanged.
func (c *FilterChain) Isin(column string, values []any) *FilterChain {
	if c.err != nil {
		return c
	}
	newDF, err := c.df.isinOnce(column, values)
	if err != nil {
		return &FilterChain{df: c.df, err: err}
	}
	return &FilterChain{df: newDF}
}

// Between applies an additional range filter to the chain. If the chain already
// holds an error, it is returned unchanged.
func (c *FilterChain) Between(column string, low, high any, inclusive Inclusive) *FilterChain {
	if c.err != nil {
		return c
	}
	newDF, err := c.df.betweenOnce(column, low, high, inclusive)
	if err != nil {
		return &FilterChain{df: c.df, err: err}
	}
	return &FilterChain{df: newDF}
}

// Nlargest applies an additional top-n selection to the chain. If the chain
// already holds an error, it is returned unchanged.
func (c *FilterChain) Nlargest(n int, column string) *FilterChain {
	if c.err != nil {
		return c
	}
	newDF, err := c.df.topKOnce(column, n, true, "Nlargest")
	if err != nil {
		return &FilterChain{df: c.df, err: err}
	}
	return &FilterChain{df: newDF}
}

// Nsmallest applies an additional bottom-n selection to the chain. If the chain
// already holds an error, it is returned unchanged.
func (c *FilterChain) Nsmallest(n int, column string) *FilterChain {
	if c.err != nil {
		return c
	}
	newDF, err := c.df.topKOnce(column, n, false, "Nsmallest")
	if err != nil {
		return &FilterChain{df: c.df, err: err}
	}
	return &FilterChain{df: newDF}
}

// isinOnce performs a single set-membership filter and returns a new DataFrame.
func (df *DataFrame) isinOnce(column string, values []any) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("Isin: DataFrame is nil")
	}

	set, err := newMembershipSet(values, "Isin")
	if err != nil {
		return nil, err
	}

	df.RLock()

	series, ok := df.Columns[column]
	if !ok {
		df.RUnlock()
		return nil, fmt.Errorf("Isin: column '%s' not found", column)
	}

	rowCount := series.Len()
	keep := make([]int, 0, rowCount)

	// An empty set matches nothing, so the scan can be skipped entirely.
	if !set.empty() {
		for i := 0; i < rowCount; i++ {
			if series.IsNull(i) {
				continue // nulls never match a membership test
			}
			val, err := series.At(i)
			if err != nil {
				df.RUnlock()
				return nil, fmt.Errorf("Isin: error reading row %d: %w", i, err)
			}
			if set.contains(val) {
				keep = append(keep, i)
			}
		}
	}

	df.RUnlock()

	return df.Slice(keep)
}

// betweenOnce performs a single range filter and returns a new DataFrame.
func (df *DataFrame) betweenOnce(column string, low, high any, inclusive Inclusive) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("Between: DataFrame is nil")
	}
	if low == nil || high == nil {
		return nil, errors.New("Between: low and high must not be nil")
	}

	lowOp, highOp, err := betweenOps(inclusive)
	if err != nil {
		return nil, err
	}

	df.RLock()

	series, ok := df.Columns[column]
	if !ok {
		df.RUnlock()
		return nil, fmt.Errorf("Between: column '%s' not found", column)
	}

	rowCount := series.Len()
	keep := make([]int, 0, rowCount)

	for i := 0; i < rowCount; i++ {
		if series.IsNull(i) {
			continue // nulls never fall inside a range
		}
		val, err := series.At(i)
		if err != nil {
			df.RUnlock()
			return nil, fmt.Errorf("Between: error reading row %d: %w", i, err)
		}

		cmpLow, err := compareForFilter(val, low)
		if err != nil {
			df.RUnlock()
			return nil, fmt.Errorf("Between: %w", err)
		}
		if !matchesOp(lowOp, cmpLow) {
			continue // below the lower bound; skip the upper bound comparison
		}

		cmpHigh, err := compareForFilter(val, high)
		if err != nil {
			df.RUnlock()
			return nil, fmt.Errorf("Between: %w", err)
		}
		if matchesOp(highOp, cmpHigh) {
			keep = append(keep, i)
		}
	}

	df.RUnlock()

	return df.Slice(keep)
}

// betweenOps translates an Inclusive option into the pair of operators applied to
// the lower and upper bound.
func betweenOps(inclusive Inclusive) (FilterOp, FilterOp, error) {
	switch inclusive {
	case InclusiveBoth, "":
		return GreaterThanOrEqual, LessThanOrEqual, nil
	case InclusiveNeither:
		return GreaterThan, LessThan, nil
	case InclusiveLeft:
		return GreaterThanOrEqual, LessThan, nil
	case InclusiveRight:
		return GreaterThan, LessThanOrEqual, nil
	default:
		return "", "", fmt.Errorf("Between: unsupported inclusive option '%s'", inclusive)
	}
}

// membershipSet is a hash set of the values accepted by Isin. Numeric members are
// normalised to float64 so that int, int64 and float64 match interchangeably, the
// same cross-type rule Filter uses.
type membershipSet struct {
	numeric map[float64]struct{}
	other   map[any]struct{}
}

// newMembershipSet builds the lookup set once per Isin call. nil members are
// dropped because nulls are excluded from the result anyway.
func newMembershipSet(values []any, name string) (*membershipSet, error) {
	set := &membershipSet{}
	for _, v := range values {
		if v == nil {
			continue
		}
		if f, ok := toFloat64(v); ok {
			if set.numeric == nil {
				set.numeric = make(map[float64]struct{}, len(values))
			}
			set.numeric[f] = struct{}{}
			continue
		}
		if !reflect.TypeOf(v).Comparable() {
			return nil, fmt.Errorf("%s: value of type %T cannot be compared for membership", name, v)
		}
		if set.other == nil {
			set.other = make(map[any]struct{}, len(values))
		}
		set.other[v] = struct{}{}
	}
	return set, nil
}

// empty reports whether the set can never match.
func (s *membershipSet) empty() bool {
	return len(s.numeric) == 0 && len(s.other) == 0
}

// contains reports whether a non-null column value is a member of the set.
func (s *membershipSet) contains(v any) bool {
	if f, ok := toFloat64(v); ok {
		_, found := s.numeric[f]
		return found
	}
	if len(s.other) == 0 {
		return false
	}
	if !reflect.TypeOf(v).Comparable() {
		return false
	}
	_, found := s.other[v]
	return found
}

// topKOnce selects the n best rows of a column and returns a new DataFrame whose
// rows are ordered best first. largest selects the maximum values, otherwise the
// minimum values are selected.
func (df *DataFrame) topKOnce(column string, n int, largest bool, name string) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New(name + ": DataFrame is nil")
	}
	if n < 0 {
		return nil, fmt.Errorf("%s: n must not be negative, got %d", name, n)
	}

	df.RLock()

	series, ok := df.Columns[column]
	if !ok {
		df.RUnlock()
		return nil, fmt.Errorf("%s: column '%s' not found", name, column)
	}

	rowCount := series.Len()
	if n > rowCount {
		n = rowCount // fewer rows than requested is not an error
	}
	if n == 0 {
		df.RUnlock()
		return df.Slice(nil)
	}

	// The ordering key is chosen from the first non-null value: numeric columns
	// rank on float64 keys so int/float mixes stay consistent with Filter, text
	// columns rank lexicographically.
	first, firstFound, err := firstNonNull(series, rowCount, name)
	if err != nil {
		df.RUnlock()
		return nil, err
	}
	if !firstFound {
		df.RUnlock()
		return df.Slice(nil) // every value is null, so nothing can be ranked
	}

	var keep []int
	if _, isNumeric := toFloat64(first); isNumeric {
		keep, err = collectTopK(series, column, rowCount, n, largest, name, toFloat64)
	} else if _, isString := first.(string); isString {
		keep, err = collectTopK(series, column, rowCount, n, largest, name, asString)
	} else {
		df.RUnlock()
		return nil, fmt.Errorf("%s: column '%s' of type %T cannot be ranked", name, column, first)
	}

	df.RUnlock()

	if err != nil {
		return nil, err
	}

	return df.Slice(keep)
}

// firstNonNull returns the first non-null value of a series, reporting whether one
// exists.
func firstNonNull(series collection.Series, rowCount int, name string) (any, bool, error) {
	for i := 0; i < rowCount; i++ {
		if series.IsNull(i) {
			continue
		}
		val, err := series.At(i)
		if err != nil {
			return nil, false, fmt.Errorf("%s: error reading row %d: %w", name, i, err)
		}
		return val, true, nil
	}
	return nil, false, nil
}

// asString extracts a string ordering key.
func asString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

// collectTopK streams the column through a bounded heap and returns the selected
// row indices, ordered best first. Only n entries are ever held in memory.
func collectTopK[T orderedKey](
	series collection.Series,
	column string,
	rowCount, n int,
	largest bool,
	name string,
	keyOf func(any) (T, bool),
) ([]int, error) {
	h := &topKHeap[T]{largest: largest, entries: make([]topKEntry[T], 0, n)}

	for i := 0; i < rowCount; i++ {
		if series.IsNull(i) {
			continue // nulls are never ranked
		}
		val, err := series.At(i)
		if err != nil {
			return nil, fmt.Errorf("%s: error reading row %d: %w", name, i, err)
		}
		key, ok := keyOf(val)
		if !ok {
			return nil, fmt.Errorf("%s: column '%s' contains a value of type %T that cannot be ranked", name, column, val)
		}
		h.offer(topKEntry[T]{key: key, row: i}, n)
	}

	return h.rows(), nil
}

// orderedKey constrains the key types used for top-n selection.
type orderedKey interface {
	~float64 | ~string
}

// topKEntry pairs an ordering key with the row it came from.
type topKEntry[T orderedKey] struct {
	key T
	row int
}

// topKHeap keeps the best n entries seen so far. It is a binary heap ordered so
// that the root is the entry that would be discarded next, which makes the
// "is this candidate better than the worst entry kept" test O(1) and the whole
// selection O(rows * log n) time with O(n) space.
type topKHeap[T orderedKey] struct {
	entries []topKEntry[T]
	largest bool
}

// worse reports whether a should be discarded before b.
func (h *topKHeap[T]) worse(a, b topKEntry[T]) bool {
	if a.key != b.key {
		if h.largest {
			return a.key < b.key
		}
		return a.key > b.key
	}
	// Ties keep the earliest row, so the later row is discarded first.
	return a.row > b.row
}

// offer adds an entry while fewer than n are held, otherwise it replaces the
// worst entry when the candidate beats it. Rows arrive in ascending order, so an
// entry tied with the root is never swapped in and ties keep the earliest row.
func (h *topKHeap[T]) offer(e topKEntry[T], n int) {
	if len(h.entries) < n {
		h.entries = append(h.entries, e)
		h.up(len(h.entries) - 1)
		return
	}
	if h.worse(h.entries[0], e) {
		h.entries[0] = e
		h.down(0)
	}
}

// up restores the heap invariant by sifting the entry at i towards the root.
func (h *topKHeap[T]) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.worse(h.entries[i], h.entries[parent]) {
			break
		}
		h.entries[i], h.entries[parent] = h.entries[parent], h.entries[i]
		i = parent
	}
}

// down restores the heap invariant by sifting the entry at i towards the leaves.
func (h *topKHeap[T]) down(i int) {
	n := len(h.entries)
	for {
		left := 2*i + 1
		if left >= n {
			return
		}
		worst := left
		if right := left + 1; right < n && h.worse(h.entries[right], h.entries[left]) {
			worst = right
		}
		if !h.worse(h.entries[worst], h.entries[i]) {
			return
		}
		h.entries[i], h.entries[worst] = h.entries[worst], h.entries[i]
		i = worst
	}
}

// rows returns the selected row indices ordered best first.
func (h *topKHeap[T]) rows() []int {
	sort.Slice(h.entries, func(i, j int) bool {
		return h.worse(h.entries[j], h.entries[i])
	})

	rows := make([]int, len(h.entries))
	for i, e := range h.entries {
		rows[i] = e.row
	}
	return rows
}
