package gpandas_test

import (
	"testing"

	"github.com/apoplexi24/gpandas"
	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// Helper function to create a Series from values
func mustSeries(vals ...any) collection.Series {
	s, err := collection.NewSeriesWithData(nil, vals)
	if err != nil {
		panic(err)
	}
	return s
}

// Helper function to compare string slices
func strSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestConcatBasicAxis0 tests basic row-wise concatenation
func TestConcatBasicAxis0(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1, 2),
			"B": mustSeries("x", "y"),
		},
		ColumnOrder: []string{"A", "B"},
		Index:       []string{"0", "1"},
	}

	df2 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(3, 4),
			"B": mustSeries("z", "w"),
		},
		ColumnOrder: []string{"A", "B"},
		Index:       []string{"2", "3"},
	}

	result, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check row count
	if result.Len() != 4 {
		t.Errorf("expected 4 rows, got %d", result.Len())
	}

	// Check columns
	if !strSliceEqual(result.ColumnOrder, []string{"A", "B"}) {
		t.Errorf("expected columns [A, B], got %v", result.ColumnOrder)
	}

	// Verify data
	colA := result.Columns["A"]
	for i, expected := range []any{1, 2, 3, 4} {
		val, _ := colA.At(i)
		// Type may be float64 due to any series
		if val != expected && val != float64(expected.(int)) {
			t.Errorf("column A index %d: expected %v, got %v", i, expected, val)
		}
	}
}

// TestConcatBasicAxis1 tests basic column-wise concatenation
func TestConcatBasicAxis1(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1, 2, 3),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"0", "1", "2"},
	}

	df2 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"B": mustSeries(4, 5, 6),
		},
		ColumnOrder: []string{"B"},
		Index:       []string{"0", "1", "2"},
	}

	result, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2}, gpandas.ConcatOptions{
		Axis: gpandas.AxisColumns,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check columns
	if len(result.ColumnOrder) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.ColumnOrder))
	}

	// Check row count
	if result.Len() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Len())
	}

	// Verify both columns exist
	if _, ok := result.Columns["A"]; !ok {
		t.Error("expected column A to exist")
	}
	if _, ok := result.Columns["B"]; !ok {
		t.Error("expected column B to exist")
	}
}

// TestConcatOuterJoinAxis0 tests outer join with different columns
func TestConcatOuterJoinAxis0(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1, 2),
			"B": mustSeries("x", "y"),
		},
		ColumnOrder: []string{"A", "B"},
		Index:       []string{"0", "1"},
	}

	df2 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(3),
			"C": mustSeries(100),
		},
		ColumnOrder: []string{"A", "C"},
		Index:       []string{"2"},
	}

	result, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2}, gpandas.ConcatOptions{
		Join: gpandas.JoinOuter,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 3 rows
	if result.Len() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Len())
	}

	// Should have 3 columns (A, B, C)
	if len(result.ColumnOrder) != 3 {
		t.Errorf("expected 3 columns, got %d", len(result.ColumnOrder))
	}

	// Column B should have null for the third row
	colB := result.Columns["B"]
	if !colB.IsNull(2) {
		t.Error("expected column B row 2 to be null")
	}

	// Column C should have null for first two rows
	colC := result.Columns["C"]
	if !colC.IsNull(0) || !colC.IsNull(1) {
		t.Error("expected column C rows 0,1 to be null")
	}
}

// TestConcatInnerJoinAxis0 tests inner join keeping only common columns
func TestConcatInnerJoinAxis0(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1, 2),
			"B": mustSeries("x", "y"),
		},
		ColumnOrder: []string{"A", "B"},
		Index:       []string{"0", "1"},
	}

	df2 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(3),
			"C": mustSeries(100),
		},
		ColumnOrder: []string{"A", "C"},
		Index:       []string{"2"},
	}

	result, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2}, gpandas.ConcatOptions{
		Join: gpandas.JoinInner,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 3 rows
	if result.Len() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Len())
	}

	// Should have only 1 column (A - the common one)
	if len(result.ColumnOrder) != 1 {
		t.Errorf("expected 1 column, got %d: %v", len(result.ColumnOrder), result.ColumnOrder)
	}

	if result.ColumnOrder[0] != "A" {
		t.Errorf("expected column A, got %s", result.ColumnOrder[0])
	}
}

// TestConcatIgnoreIndex tests index reset
func TestConcatIgnoreIndex(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1, 2),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"a", "b"},
	}

	df2 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(3, 4),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"c", "d"},
	}

	result, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2}, gpandas.ConcatOptions{
		IgnoreIndex: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Index should be reset to 0, 1, 2, 3
	expectedIndex := []string{"0", "1", "2", "3"}
	if !strSliceEqual(result.Index, expectedIndex) {
		t.Errorf("expected index %v, got %v", expectedIndex, result.Index)
	}
}

// TestConcatVerifyIntegrity tests duplicate index detection
func TestConcatVerifyIntegrity(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"0"},
	}

	df2 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(2),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"0"}, // Duplicate index
	}

	_, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2}, gpandas.ConcatOptions{
		VerifyIntegrity: true,
	})

	if err == nil {
		t.Error("expected error for duplicate index, got nil")
	}
}

// TestConcatNilDataFrames tests that nil DataFrames are skipped
func TestConcatNilDataFrames(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1, 2),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"0", "1"},
	}

	result, err := gpandas.Concat([]*dataframe.DataFrame{nil, df1, nil})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Len() != 2 {
		t.Errorf("expected 2 rows, got %d", result.Len())
	}
}

// TestConcatAllNil tests error when all DataFrames are nil
func TestConcatAllNil(t *testing.T) {
	_, err := gpandas.Concat([]*dataframe.DataFrame{nil, nil})
	if err == nil {
		t.Error("expected error for all nil DataFrames, got nil")
	}
}

// TestConcatEmptySlice tests error for empty input
func TestConcatEmptySlice(t *testing.T) {
	_, err := gpandas.Concat([]*dataframe.DataFrame{})
	if err == nil {
		t.Error("expected error for empty input, got nil")
	}
}

// TestConcatSingleDataFrame tests that single DataFrame returns copy
func TestConcatSingleDataFrame(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1, 2, 3),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"0", "1", "2"},
	}

	result, err := gpandas.Concat([]*dataframe.DataFrame{df})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Len() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Len())
	}
}

// TestConcatMultipleDataFrames tests concatenating more than 2 DataFrames
func TestConcatMultipleDataFrames(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"0"},
	}

	df2 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(2),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"1"},
	}

	df3 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(3),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"2"},
	}

	result, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2, df3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Len() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Len())
	}
}

// TestConcatAxis1DuplicateColumns tests error for duplicate columns in axis=1
func TestConcatAxis1DuplicateColumns(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"0"},
	}

	df2 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(2), // Same column name
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"0"},
	}

	_, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2}, gpandas.ConcatOptions{
		Axis: gpandas.AxisColumns,
	})

	if err == nil {
		t.Error("expected error for duplicate column names, got nil")
	}
}

// TestConcatAxis1InnerJoin tests inner join for axis=1
func TestConcatAxis1InnerJoin(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1, 2, 3),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"0", "1", "2"},
	}

	df2 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"B": mustSeries(4, 5),
		},
		ColumnOrder: []string{"B"},
		Index:       []string{"0", "1"}, // Missing index "2"
	}

	result, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2}, gpandas.ConcatOptions{
		Axis: gpandas.AxisColumns,
		Join: gpandas.JoinInner,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Inner join should only keep rows 0 and 1
	if result.Len() != 2 {
		t.Errorf("expected 2 rows, got %d", result.Len())
	}
}

// TestConcatAxis1OuterJoin tests outer join for axis=1 with nulls
func TestConcatAxis1OuterJoin(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1, 2, 3),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"0", "1", "2"},
	}

	df2 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"B": mustSeries(4, 5),
		},
		ColumnOrder: []string{"B"},
		Index:       []string{"0", "1"}, // Missing index "2"
	}

	result, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2}, gpandas.ConcatOptions{
		Axis: gpandas.AxisColumns,
		Join: gpandas.JoinOuter,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Outer join should keep all 3 rows
	if result.Len() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Len())
	}

	// Column B should have a null for the row with index "2"
	colB := result.Columns["B"]
	hasNullForTwo := false
	for i := 0; i < colB.Len(); i++ {
		if result.Index[i] == "2" && colB.IsNull(i) {
			hasNullForTwo = true
			break
		}
	}
	if !hasNullForTwo {
		t.Error("expected column B to have null for index '2'")
	}
}

// TestConcatSort tests sorting of columns
func TestConcatSort(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"B": mustSeries(1),
			"A": mustSeries(2),
		},
		ColumnOrder: []string{"B", "A"},
		Index:       []string{"0"},
	}

	df2 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"C": mustSeries(3),
			"B": mustSeries(4),
			"A": mustSeries(5),
		},
		ColumnOrder: []string{"C", "B", "A"},
		Index:       []string{"1"},
	}

	result, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2}, gpandas.ConcatOptions{
		Sort: true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Columns should be sorted alphabetically
	expectedOrder := []string{"A", "B", "C"}
	if !strSliceEqual(result.ColumnOrder, expectedOrder) {
		t.Errorf("expected columns %v, got %v", expectedOrder, result.ColumnOrder)
	}
}

// TestConcatWithNullValues tests that null values are preserved
func TestConcatWithNullValues(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1, nil, 3),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"0", "1", "2"},
	}

	df2 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(nil, 5),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"3", "4"},
	}

	result, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	colA := result.Columns["A"]

	// Check null positions
	if !colA.IsNull(1) {
		t.Error("expected index 1 to be null")
	}
	if !colA.IsNull(3) {
		t.Error("expected index 3 to be null")
	}
	if colA.IsNull(0) || colA.IsNull(2) || colA.IsNull(4) {
		t.Error("unexpected null at non-null positions")
	}
}

// TestDefaultConcatOptions verifies default options
func TestDefaultConcatOptions(t *testing.T) {
	opts := gpandas.DefaultConcatOptions()

	if opts.Axis != gpandas.AxisIndex {
		t.Errorf("expected default axis AxisIndex, got %v", opts.Axis)
	}
	if opts.Join != gpandas.JoinOuter {
		t.Errorf("expected default join JoinOuter, got %v", opts.Join)
	}
	if opts.IgnoreIndex {
		t.Error("expected default IgnoreIndex false")
	}
	if opts.VerifyIntegrity {
		t.Error("expected default VerifyIntegrity false")
	}
	if opts.Sort {
		t.Error("expected default Sort false")
	}
}
