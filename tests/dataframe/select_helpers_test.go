package dataframe_test

import (
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func selectTestDF() *dataframe.DataFrame {
	return &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name":   mustSeries("Alice", "Bob", "Charlie", "Diana", "Eve"),
			"Age":    mustSeries(30, 25, 35, 28, 32),
			"City":   mustSeries("NYC", "LA", "NYC", "SF", "LA"),
			"Salary": mustSeries(95000.0, 55000.0, 105000.0, 62000.0, 72000.0),
		},
		ColumnOrder: []string{"Name", "Age", "City", "Salary"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}
}

// columnValues collects a column's values, using nil for nulls.
func columnValues(t *testing.T, df *dataframe.DataFrame, column string) []any {
	t.Helper()
	series, ok := df.Columns[column]
	if !ok {
		t.Fatalf("column %q not found", column)
	}
	vals := make([]any, series.Len())
	for i := range vals {
		if series.IsNull(i) {
			continue
		}
		v, err := series.At(i)
		if err != nil {
			t.Fatalf("unexpected error reading %s[%d]: %v", column, i, err)
		}
		vals[i] = v
	}
	return vals
}

func TestIsin(t *testing.T) {
	t.Run("string membership", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Isin("City", []any{"NYC", "SF"}).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 3 {
			t.Fatalf("expected 3 rows, got %d", result.Len())
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Alice", "Charlie", "Diana"}) {
			t.Errorf("expected [Alice Charlie Diana], got %v", names)
		}
	})

	t.Run("numeric membership across types", func(t *testing.T) {
		df := selectTestDF()
		// Age holds int values; members are float64 and int64.
		result, err := df.Isin("Age", []any{30.0, int64(35)}).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Alice", "Charlie"}) {
			t.Errorf("expected [Alice Charlie], got %v", names)
		}
	})

	t.Run("preserves row order and index labels", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Isin("City", []any{"LA"}).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strSliceEqual(result.Index, []string{"1", "4"}) {
			t.Errorf("expected index [1 4], got %v", result.Index)
		}
	})

	t.Run("empty values match nothing", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Isin("City", []any{}).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 0 {
			t.Errorf("expected 0 rows, got %d", result.Len())
		}
	})

	t.Run("nil values match nothing", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Isin("City", nil).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 0 {
			t.Errorf("expected 0 rows, got %d", result.Len())
		}
	})

	t.Run("nulls never match", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Score": mustSeries(10.0, nil, 30.0, nil),
			},
			ColumnOrder: []string{"Score"},
			Index:       []string{"0", "1", "2", "3"},
		}
		// nil is listed as a member but nulls are still excluded.
		result, err := df.Isin("Score", []any{10.0, nil}).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 1 {
			t.Fatalf("expected 1 row, got %d", result.Len())
		}
		if !strSliceEqual(result.Index, []string{"0"}) {
			t.Errorf("expected index [0], got %v", result.Index)
		}
	})

	t.Run("boolean membership", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Active": mustSeries(true, false, true),
			},
			ColumnOrder: []string{"Active"},
			Index:       []string{"0", "1", "2"},
		}
		result, err := df.Isin("Active", []any{true}).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 2 {
			t.Errorf("expected 2 rows, got %d", result.Len())
		}
	})

	t.Run("mismatched member type does not match", func(t *testing.T) {
		df := selectTestDF()
		// "30" is a string; the Age column holds numbers.
		result, err := df.Isin("Age", []any{"30"}).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 0 {
			t.Errorf("expected 0 rows, got %d", result.Len())
		}
	})

	t.Run("errors", func(t *testing.T) {
		var nilDF *dataframe.DataFrame
		if _, err := nilDF.Isin("City", []any{"NYC"}).Result(); err == nil {
			t.Error("expected error for nil DataFrame")
		}
		df := selectTestDF()
		if _, err := df.Isin("Missing", []any{"NYC"}).Result(); err == nil {
			t.Error("expected error for missing column")
		}
		if _, err := df.Isin("City", []any{[]string{"NYC"}}).Result(); err == nil {
			t.Error("expected error for non-comparable member type")
		}
	})
}

func TestBetween(t *testing.T) {
	t.Run("inclusive both is the default", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Between("Age", 28, 32, "").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Alice", "Diana", "Eve"}) {
			t.Errorf("expected [Alice Diana Eve], got %v", names)
		}
	})

	t.Run("inclusive both", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Between("Age", 28, 32, dataframe.InclusiveBoth).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 3 { // 30, 28, 32
			t.Errorf("expected 3 rows, got %d", result.Len())
		}
	})

	t.Run("inclusive neither", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Between("Age", 28, 32, dataframe.InclusiveNeither).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Alice"}) { // only 30 is strictly inside
			t.Errorf("expected [Alice], got %v", names)
		}
	})

	t.Run("inclusive left", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Between("Age", 28, 32, dataframe.InclusiveLeft).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Alice", "Diana"}) { // 30 and 28, excludes 32
			t.Errorf("expected [Alice Diana], got %v", names)
		}
	})

	t.Run("inclusive right", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Between("Age", 28, 32, dataframe.InclusiveRight).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Alice", "Eve"}) { // 30 and 32, excludes 28
			t.Errorf("expected [Alice Eve], got %v", names)
		}
	})

	t.Run("numeric bounds across types", func(t *testing.T) {
		df := selectTestDF()
		// Age holds int values; bounds are float64.
		result, err := df.Between("Age", 24.5, 28.5, dataframe.InclusiveBoth).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Bob", "Diana"}) {
			t.Errorf("expected [Bob Diana], got %v", names)
		}
	})

	t.Run("string bounds", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Between("Name", "B", "D", dataframe.InclusiveBoth).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Bob", "Charlie"}) {
			t.Errorf("expected [Bob Charlie], got %v", names)
		}
	})

	t.Run("nulls never fall in range", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Score": mustSeries(10.0, nil, 30.0, nil, 5.0),
			},
			ColumnOrder: []string{"Score"},
			Index:       []string{"0", "1", "2", "3", "4"},
		}
		result, err := df.Between("Score", 0.0, 100.0, dataframe.InclusiveBoth).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 3 {
			t.Errorf("expected 3 rows, got %d", result.Len())
		}
	})

	t.Run("low above high yields no rows", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Between("Age", 40, 20, dataframe.InclusiveBoth).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 0 {
			t.Errorf("expected 0 rows, got %d", result.Len())
		}
	})

	t.Run("preserves index labels", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Between("Age", 30, 35, dataframe.InclusiveBoth).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strSliceEqual(result.Index, []string{"0", "2", "4"}) {
			t.Errorf("expected index [0 2 4], got %v", result.Index)
		}
	})

	t.Run("errors", func(t *testing.T) {
		var nilDF *dataframe.DataFrame
		if _, err := nilDF.Between("Age", 1, 2, dataframe.InclusiveBoth).Result(); err == nil {
			t.Error("expected error for nil DataFrame")
		}
		df := selectTestDF()
		if _, err := df.Between("Missing", 1, 2, dataframe.InclusiveBoth).Result(); err == nil {
			t.Error("expected error for missing column")
		}
		if _, err := df.Between("Age", nil, 2, dataframe.InclusiveBoth).Result(); err == nil {
			t.Error("expected error for nil low bound")
		}
		if _, err := df.Between("Age", 1, nil, dataframe.InclusiveBoth).Result(); err == nil {
			t.Error("expected error for nil high bound")
		}
		if _, err := df.Between("Age", 1, 2, dataframe.Inclusive("sideways")).Result(); err == nil {
			t.Error("expected error for invalid inclusive option")
		}
		if _, err := df.Between("Name", 1, 2, dataframe.InclusiveBoth).Result(); err == nil {
			t.Error("expected error comparing string column against numeric bounds")
		}
	})
}

func TestNlargest(t *testing.T) {
	t.Run("returns rows ordered largest first", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Nlargest(3, "Salary").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 3 {
			t.Fatalf("expected 3 rows, got %d", result.Len())
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Charlie", "Alice", "Eve"}) {
			t.Errorf("expected [Charlie Alice Eve], got %v", names)
		}
		if !strSliceEqual(result.Index, []string{"2", "0", "4"}) {
			t.Errorf("expected index [2 0 4], got %v", result.Index)
		}
	})

	t.Run("integer column", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Nlargest(2, "Age").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Charlie", "Eve"}) { // 35, 32
			t.Errorf("expected [Charlie Eve], got %v", names)
		}
	})

	t.Run("n larger than row count returns all non-null rows", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Nlargest(50, "Age").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 5 {
			t.Errorf("expected 5 rows, got %d", result.Len())
		}
	})

	t.Run("ties keep the first occurrence", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Tag":   mustSeries("a", "b", "c", "d"),
				"Score": mustSeries(10, 10, 10, 5),
			},
			ColumnOrder: []string{"Tag", "Score"},
			Index:       []string{"0", "1", "2", "3"},
		}
		result, err := df.Nlargest(2, "Score").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tags := columnValues(t, result, "Tag")
		if !sliceEqual(tags, []any{"a", "b"}) {
			t.Errorf("expected [a b], got %v", tags)
		}
	})

	t.Run("nulls are never ranked", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Tag":   mustSeries("a", "b", "c", "d"),
				"Score": mustSeries(nil, 7.0, nil, 9.0),
			},
			ColumnOrder: []string{"Tag", "Score"},
			Index:       []string{"0", "1", "2", "3"},
		}
		result, err := df.Nlargest(3, "Score").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 2 {
			t.Fatalf("expected 2 rows, got %d", result.Len())
		}
		tags := columnValues(t, result, "Tag")
		if !sliceEqual(tags, []any{"d", "b"}) {
			t.Errorf("expected [d b], got %v", tags)
		}
	})

	t.Run("all null column yields no rows", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Score": mustSeries(nil, nil, nil),
			},
			ColumnOrder: []string{"Score"},
			Index:       []string{"0", "1", "2"},
		}
		result, err := df.Nlargest(2, "Score").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 0 {
			t.Errorf("expected 0 rows, got %d", result.Len())
		}
	})

	t.Run("string column ranks lexicographically", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Nlargest(2, "Name").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Eve", "Diana"}) {
			t.Errorf("expected [Eve Diana], got %v", names)
		}
	})

	t.Run("zero n yields no rows", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Nlargest(0, "Age").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 0 {
			t.Errorf("expected 0 rows, got %d", result.Len())
		}
	})

	t.Run("errors", func(t *testing.T) {
		var nilDF *dataframe.DataFrame
		if _, err := nilDF.Nlargest(1, "Age").Result(); err == nil {
			t.Error("expected error for nil DataFrame")
		}
		df := selectTestDF()
		if _, err := df.Nlargest(1, "Missing").Result(); err == nil {
			t.Error("expected error for missing column")
		}
		if _, err := df.Nlargest(-1, "Age").Result(); err == nil {
			t.Error("expected error for negative n")
		}
		boolDF := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Active": mustSeries(true, false),
			},
			ColumnOrder: []string{"Active"},
			Index:       []string{"0", "1"},
		}
		if _, err := boolDF.Nlargest(1, "Active").Result(); err == nil {
			t.Error("expected error ranking a boolean column")
		}
		mixedDF := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Mixed": mustSeries(1, "two", 3),
			},
			ColumnOrder: []string{"Mixed"},
			Index:       []string{"0", "1", "2"},
		}
		if _, err := mixedDF.Nlargest(2, "Mixed").Result(); err == nil {
			t.Error("expected error ranking a column with mixed types")
		}
	})
}

func TestNsmallest(t *testing.T) {
	t.Run("returns rows ordered smallest first", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.Nsmallest(3, "Salary").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Bob", "Diana", "Eve"}) {
			t.Errorf("expected [Bob Diana Eve], got %v", names)
		}
		if !strSliceEqual(result.Index, []string{"1", "3", "4"}) {
			t.Errorf("expected index [1 3 4], got %v", result.Index)
		}
	})

	t.Run("ties keep the first occurrence", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Tag":   mustSeries("a", "b", "c", "d"),
				"Score": mustSeries(5, 1, 1, 1),
			},
			ColumnOrder: []string{"Tag", "Score"},
			Index:       []string{"0", "1", "2", "3"},
		}
		result, err := df.Nsmallest(2, "Score").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tags := columnValues(t, result, "Tag")
		if !sliceEqual(tags, []any{"b", "c"}) {
			t.Errorf("expected [b c], got %v", tags)
		}
	})

	t.Run("nulls are never ranked", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Score": mustSeries(nil, 7.0, nil, 9.0, 3.0),
			},
			ColumnOrder: []string{"Score"},
			Index:       []string{"0", "1", "2", "3", "4"},
		}
		result, err := df.Nsmallest(4, "Score").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strSliceEqual(result.Index, []string{"4", "1", "3"}) {
			t.Errorf("expected index [4 1 3], got %v", result.Index)
		}
	})

	t.Run("single row column", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Score": mustSeries(42.0),
			},
			ColumnOrder: []string{"Score"},
			Index:       []string{"0"},
		}
		result, err := df.Nsmallest(5, "Score").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 1 {
			t.Errorf("expected 1 row, got %d", result.Len())
		}
	})

	t.Run("errors", func(t *testing.T) {
		var nilDF *dataframe.DataFrame
		if _, err := nilDF.Nsmallest(1, "Age").Result(); err == nil {
			t.Error("expected error for nil DataFrame")
		}
		df := selectTestDF()
		if _, err := df.Nsmallest(1, "Missing").Result(); err == nil {
			t.Error("expected error for missing column")
		}
		if _, err := df.Nsmallest(-3, "Age").Result(); err == nil {
			t.Error("expected error for negative n")
		}
	})
}

func TestSelectHelpersChaining(t *testing.T) {
	t.Run("Filter then Isin", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.
			Filter("Age", dataframe.GreaterThan, 26).
			Isin("City", []any{"NYC", "SF"}).
			Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Alice", "Charlie", "Diana"}) {
			t.Errorf("expected [Alice Charlie Diana], got %v", names)
		}
	})

	t.Run("Isin then Between then Nlargest", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.
			Isin("City", []any{"NYC", "LA"}).
			Between("Age", 25, 32, dataframe.InclusiveBoth).
			Nlargest(1, "Salary").
			Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 1 {
			t.Fatalf("expected 1 row, got %d", result.Len())
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Alice"}) {
			t.Errorf("expected [Alice], got %v", names)
		}
	})

	t.Run("Between then Where", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.
			Between("Salary", 60000.0, 100000.0, dataframe.InclusiveBoth).
			Where(func(row map[string]any) bool {
				return row["City"] == "NYC"
			}).
			Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Alice"}) {
			t.Errorf("expected [Alice], got %v", names)
		}
	})

	t.Run("Nsmallest composes with Filter", func(t *testing.T) {
		df := selectTestDF()
		result, err := df.
			Filter("City", dataframe.NotEquals, "SF").
			Nsmallest(2, "Salary").
			Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		names := columnValues(t, result, "Name")
		if !sliceEqual(names, []any{"Bob", "Eve"}) {
			t.Errorf("expected [Bob Eve], got %v", names)
		}
	})

	t.Run("error short-circuits the rest of the chain", func(t *testing.T) {
		df := selectTestDF()
		_, err := df.
			Isin("City", []any{"NYC"}).
			Between("Missing", 1, 2, dataframe.InclusiveBoth). // errors here
			Nlargest(1, "Salary").                             // skipped
			Result()
		if err == nil {
			t.Fatal("expected the missing column error to propagate")
		}
	})

	t.Run("Err and MustResult", func(t *testing.T) {
		df := selectTestDF()
		if df.Isin("Missing", []any{1}).Err() == nil {
			t.Error("expected Err() to report the failure")
		}
		if got := df.Isin("City", []any{"SF"}).MustResult().Len(); got != 1 {
			t.Errorf("expected 1 row from MustResult, got %d", got)
		}
	})
}

// referenceTopK ranks rows the obvious way (stable full sort, nulls dropped) so
// the heap-based Nlargest/Nsmallest can be checked against it.
func referenceTopK(scores []any, n int, largest bool) []string {
	rows := make([]int, 0, len(scores))
	for i, v := range scores {
		if v != nil {
			rows = append(rows, i)
		}
	}
	sort.SliceStable(rows, func(a, b int) bool {
		va := scores[rows[a]].(float64)
		vb := scores[rows[b]].(float64)
		if va == vb {
			return rows[a] < rows[b] // ties keep the earliest row
		}
		if largest {
			return va > vb
		}
		return va < vb
	})
	if n > len(rows) {
		n = len(rows)
	}
	labels := make([]string, n)
	for i := 0; i < n; i++ {
		labels[i] = strconv.Itoa(rows[i])
	}
	return labels
}

// TestTopKMatchesFullSort exercises the bounded-heap selection against a
// reference full sort over a larger frame containing heavy ties and nulls.
func TestTopKMatchesFullSort(t *testing.T) {
	const rowCount = 200

	scores := make([]any, rowCount)
	index := make([]string, rowCount)
	// Deterministic pseudo-random values with many duplicates, plus periodic nulls.
	state := uint32(12345)
	for i := 0; i < rowCount; i++ {
		state = state*1664525 + 1013904223
		index[i] = strconv.Itoa(i)
		if i%17 == 0 {
			scores[i] = nil
			continue
		}
		scores[i] = float64(state % 25) // small range forces ties
	}

	newDF := func() *dataframe.DataFrame {
		return &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Score": mustSeries(scores...),
			},
			ColumnOrder: []string{"Score"},
			Index:       append([]string(nil), index...),
		}
	}

	for _, n := range []int{1, 2, 7, 25, 100, 190, 500} {
		for _, largest := range []bool{true, false} {
			name := "nsmallest"
			if largest {
				name = "nlargest"
			}
			t.Run(fmt.Sprintf("%s n=%d", name, n), func(t *testing.T) {
				df := newDF()
				var (
					result *dataframe.DataFrame
					err    error
				)
				if largest {
					result, err = df.Nlargest(n, "Score").Result()
				} else {
					result, err = df.Nsmallest(n, "Score").Result()
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				want := referenceTopK(scores, n, largest)
				if !strSliceEqual(result.Index, want) {
					t.Fatalf("selection mismatch\n got: %v\nwant: %v", result.Index, want)
				}

				// Values must stay aligned with their original rows.
				for i, label := range result.Index {
					row, convErr := strconv.Atoi(label)
					if convErr != nil {
						t.Fatalf("unexpected index label %q", label)
					}
					got, atErr := result.Columns["Score"].At(i)
					if atErr != nil {
						t.Fatalf("unexpected error: %v", atErr)
					}
					if got != scores[row] {
						t.Errorf("row %s: expected %v, got %v", label, scores[row], got)
					}
				}
			})
		}
	}
}
