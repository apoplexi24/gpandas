package dataframe_test

import (
	"reflect"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func strCol(t *testing.T, vals ...string) collection.Series {
	t.Helper()
	s, err := collection.NewStringSeriesFromData(vals, nil)
	if err != nil {
		t.Fatalf("building string series: %v", err)
	}
	return s
}

func intCol(t *testing.T, vals ...int64) collection.Series {
	t.Helper()
	s, err := collection.NewInt64SeriesFromData(vals, nil)
	if err != nil {
		t.Fatalf("building int64 series: %v", err)
	}
	return s
}

func floatCol(t *testing.T, vals ...float64) collection.Series {
	t.Helper()
	s, err := collection.NewFloat64SeriesFromData(vals, nil)
	if err != nil {
		t.Fatalf("building float64 series: %v", err)
	}
	return s
}

// -----------------------------------------------------------------------------
// GroupBy composite key collisions
// -----------------------------------------------------------------------------

// TestGroupByMultiKeyNoCollision guards against composite group keys being built
// with a printable separator. Joining ("x", "y_z") and ("x_y", "z") with "_"
// produces the same key "x_y_z" for both rows, silently merging two distinct
// groups into one.
func TestGroupByMultiKeyNoCollision(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": strCol(t, "x", "x_y"),
			"B": strCol(t, "y_z", "z"),
			"V": floatCol(t, 1, 2),
		},
		ColumnOrder: []string{"A", "B", "V"},
		Index:       []string{"0", "1"},
	}

	gb, err := df.GroupBy([]string{"A", "B"}, 0)
	if err != nil {
		t.Fatalf("GroupBy failed: %v", err)
	}

	sum, err := gb.Sum()
	if err != nil {
		t.Fatalf("Sum failed: %v", err)
	}

	if sum.Len() != 2 {
		t.Fatalf("expected 2 distinct groups for (x, y_z) and (x_y, z), got %d", sum.Len())
	}

	// Each group holds exactly one row, so each sum is that row's value.
	vCol := sum.Columns["V"]
	got := make(map[float64]bool)
	for i := 0; i < vCol.Len(); i++ {
		v, _ := vCol.At(i)
		f, ok := v.(float64)
		if !ok {
			t.Fatalf("expected float64 sum, got %T", v)
		}
		got[f] = true
	}
	if !got[1] || !got[2] {
		t.Errorf("expected group sums {1, 2}, got %v", got)
	}
}

// TestGroupBySingleKeyWithSeparatorChar checks that a single-column grouping key
// whose values contain the old separator still groups correctly.
func TestGroupBySingleKeyWithSeparatorChar(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": strCol(t, "a_b", "a_b", "a"),
			"V": floatCol(t, 1, 2, 3),
		},
		ColumnOrder: []string{"A", "V"},
		Index:       []string{"0", "1", "2"},
	}

	gb, err := df.GroupBy([]string{"A"}, 0)
	if err != nil {
		t.Fatalf("GroupBy failed: %v", err)
	}
	sum, err := gb.Sum()
	if err != nil {
		t.Fatalf("Sum failed: %v", err)
	}
	if sum.Len() != 2 {
		t.Fatalf("expected 2 groups (a, a_b), got %d", sum.Len())
	}
}

// TestGroupByNullKeyDistinctFromLiteral checks that a null key does not collide
// with a value that formats identically to nil.
func TestGroupByNullKeyDistinctFromLiteral(t *testing.T) {
	withNull, err := collection.NewStringSeriesFromData(
		[]string{"<nil>", ""}, []bool{false, true})
	if err != nil {
		t.Fatalf("building series: %v", err)
	}

	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": withNull,
			"V": floatCol(t, 1, 2),
		},
		ColumnOrder: []string{"A", "V"},
		Index:       []string{"0", "1"},
	}

	gb, err := df.GroupBy([]string{"A"}, 0)
	if err != nil {
		t.Fatalf("GroupBy failed: %v", err)
	}
	sum, err := gb.Sum()
	if err != nil {
		t.Fatalf("Sum failed: %v", err)
	}
	if sum.Len() != 2 {
		t.Errorf("expected the literal \"<nil>\" and a null to form 2 groups, got %d", sum.Len())
	}
}

// TestPivotTableIndexNoCollision is the PivotTable counterpart: two distinct
// index combinations must not collapse into one row.
func TestPivotTableIndexNoCollision(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"R1": strCol(t, "x", "x_y"),
			"R2": strCol(t, "y_z", "z"),
			"C":  strCol(t, "c", "c"),
			"V":  floatCol(t, 1, 2),
		},
		ColumnOrder: []string{"R1", "R2", "C", "V"},
		Index:       []string{"0", "1"},
	}

	pivot, err := df.PivotTable(dataframe.PivotTableOptions{
		Index:   []string{"R1", "R2"},
		Columns: "C",
		Values:  []string{"V"},
		AggFunc: dataframe.AggSum,
	})
	if err != nil {
		t.Fatalf("PivotTable failed: %v", err)
	}
	if pivot.Len() != 2 {
		t.Errorf("expected 2 pivot rows for distinct index combinations, got %d", pivot.Len())
	}
}

// -----------------------------------------------------------------------------
// Merge overlapping column names
// -----------------------------------------------------------------------------

// TestMergeSuffixesOverlappingColumns guards against overlapping non-key columns
// silently colliding. Previously the result carried a duplicate "Value" entry in
// ColumnOrder while the Columns map held only one, losing the left side's data.
func TestMergeSuffixesOverlappingColumns(t *testing.T) {
	left := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"ID":    intCol(t, 1, 2),
			"Value": strCol(t, "left1", "left2"),
		},
		ColumnOrder: []string{"ID", "Value"},
		Index:       []string{"0", "1"},
	}
	right := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"ID":    intCol(t, 1, 2),
			"Value": strCol(t, "right1", "right2"),
		},
		ColumnOrder: []string{"ID", "Value"},
		Index:       []string{"0", "1"},
	}

	result, err := left.Merge(right, "ID", dataframe.InnerMerge)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	want := []string{"ID", "Value_x", "Value_y"}
	if !strSliceEqual(result.ColumnOrder, want) {
		t.Fatalf("expected column order %v, got %v", want, result.ColumnOrder)
	}
	if len(result.Columns) != len(want) {
		t.Fatalf("expected %d columns in the map, got %d", len(want), len(result.Columns))
	}

	// Both sides' data must survive.
	for _, tc := range []struct {
		col  string
		want []string
	}{
		{"Value_x", []string{"left1", "left2"}},
		{"Value_y", []string{"right1", "right2"}},
	} {
		s, ok := result.Columns[tc.col]
		if !ok {
			t.Fatalf("column %q missing from result", tc.col)
		}
		for i, exp := range tc.want {
			v, _ := s.At(i)
			if v != exp {
				t.Errorf("%s[%d]: expected %q, got %v", tc.col, i, exp, v)
			}
		}
	}
}

// TestMergeNoSuffixWhenNoOverlap checks the common case is untouched: with no
// overlapping non-key columns, names are left alone.
func TestMergeNoSuffixWhenNoOverlap(t *testing.T) {
	left := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"ID":   intCol(t, 1, 2),
			"Name": strCol(t, "Alice", "Bob"),
		},
		ColumnOrder: []string{"ID", "Name"},
		Index:       []string{"0", "1"},
	}
	right := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"ID":  intCol(t, 1, 2),
			"Age": intCol(t, 30, 40),
		},
		ColumnOrder: []string{"ID", "Age"},
		Index:       []string{"0", "1"},
	}

	result, err := left.Merge(right, "ID", dataframe.InnerMerge)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	want := []string{"ID", "Name", "Age"}
	if !strSliceEqual(result.ColumnOrder, want) {
		t.Errorf("expected column order %v, got %v", want, result.ColumnOrder)
	}
}

// TestMergeSuffixCollisionErrors checks that a suffixed name which would still
// collide is reported instead of silently dropping a column.
func TestMergeSuffixCollisionErrors(t *testing.T) {
	left := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"ID":      intCol(t, 1),
			"Value":   strCol(t, "a"),
			"Value_x": strCol(t, "b"),
		},
		ColumnOrder: []string{"ID", "Value", "Value_x"},
		Index:       []string{"0"},
	}
	right := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"ID":    intCol(t, 1),
			"Value": strCol(t, "c"),
		},
		ColumnOrder: []string{"ID", "Value"},
		Index:       []string{"0"},
	}

	if _, err := left.Merge(right, "ID", dataframe.InnerMerge); err == nil {
		t.Error("expected an error when the suffixed name collides with an existing column")
	}
}

// TestMergeOnSuffixesOverlappingColumns is the multi-key MergeOn counterpart.
func TestMergeOnSuffixesOverlappingColumns(t *testing.T) {
	left := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"year":   intCol(t, 2020, 2021),
			"region": strCol(t, "N", "N"),
			"amount": floatCol(t, 1, 2),
		},
		ColumnOrder: []string{"year", "region", "amount"},
		Index:       []string{"0", "1"},
	}
	right := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"year":   intCol(t, 2020, 2021),
			"region": strCol(t, "N", "N"),
			"amount": floatCol(t, 10, 20),
		},
		ColumnOrder: []string{"year", "region", "amount"},
		Index:       []string{"0", "1"},
	}

	result, err := left.MergeOn(right, []string{"year", "region"}, dataframe.InnerMerge)
	if err != nil {
		t.Fatalf("MergeOn failed: %v", err)
	}

	want := []string{"year", "region", "amount_x", "amount_y"}
	if !strSliceEqual(result.ColumnOrder, want) {
		t.Fatalf("expected column order %v, got %v", want, result.ColumnOrder)
	}

	xs := result.Columns["amount_x"]
	ys := result.Columns["amount_y"]
	for i, exp := range []float64{1, 2} {
		v, _ := xs.At(i)
		if v != exp {
			t.Errorf("amount_x[%d]: expected %v, got %v", i, exp, v)
		}
	}
	for i, exp := range []float64{10, 20} {
		v, _ := ys.At(i)
		if v != exp {
			t.Errorf("amount_y[%d]: expected %v, got %v", i, exp, v)
		}
	}
}

// -----------------------------------------------------------------------------
// Concat dtype preservation
// -----------------------------------------------------------------------------

// TestConcatPreservesDTypes checks that stacking frames whose columns agree on a
// dtype keeps that dtype, instead of degrading every column to an untyped Series.
func TestConcatPreservesDTypes(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"i": intCol(t, 1, 2),
			"f": floatCol(t, 1.5, 2.5),
			"s": strCol(t, "a", "b"),
		},
		ColumnOrder: []string{"i", "f", "s"},
		Index:       []string{"0", "1"},
	}
	df2 := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"i": intCol(t, 3, 4),
			"f": floatCol(t, 3.5, 4.5),
			"s": strCol(t, "c", "d"),
		},
		ColumnOrder: []string{"i", "f", "s"},
		Index:       []string{"2", "3"},
	}

	result, err := dataframe.Concat([]*dataframe.DataFrame{df1, df2})
	if err != nil {
		t.Fatalf("Concat failed: %v", err)
	}
	if result.Len() != 4 {
		t.Fatalf("expected 4 rows, got %d", result.Len())
	}

	wantTypes := map[string]reflect.Type{
		"i": reflect.TypeOf(int64(0)),
		"f": reflect.TypeOf(float64(0)),
		"s": reflect.TypeOf(""),
	}
	for col, want := range wantTypes {
		got := result.Columns[col].DType()
		if got != want {
			t.Errorf("column %q: expected dtype %v, got %v", col, want, got)
		}
	}

	// Values must survive the typed round-trip.
	iCol := result.Columns["i"]
	for i, exp := range []int64{1, 2, 3, 4} {
		v, _ := iCol.At(i)
		if v != exp {
			t.Errorf("i[%d]: expected %v, got %v (%T)", i, exp, v, v)
		}
	}
}

// TestConcatPromotesIntAndFloat checks that mixing an integer column with a
// float column widens to float64, as pandas does, rather than falling back to an
// untyped Series.
func TestConcatPromotesIntAndFloat(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"v": intCol(t, 1, 2)},
		ColumnOrder: []string{"v"},
		Index:       []string{"0", "1"},
	}
	df2 := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"v": floatCol(t, 3.5)},
		ColumnOrder: []string{"v"},
		Index:       []string{"2"},
	}

	result, err := dataframe.Concat([]*dataframe.DataFrame{df1, df2})
	if err != nil {
		t.Fatalf("Concat failed: %v", err)
	}

	if got, want := result.Columns["v"].DType(), reflect.TypeOf(float64(0)); got != want {
		t.Fatalf("expected dtype %v after int/float mix, got %v", want, got)
	}
	for i, exp := range []float64{1, 2, 3.5} {
		v, _ := result.Columns["v"].At(i)
		if v != exp {
			t.Errorf("v[%d]: expected %v, got %v (%T)", i, exp, v, v)
		}
	}
}

// TestConcatConflictingTypesFallBackToAny checks that a genuine type conflict
// still produces a usable untyped column rather than an error.
func TestConcatConflictingTypesFallBackToAny(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"v": intCol(t, 1)},
		ColumnOrder: []string{"v"},
		Index:       []string{"0"},
	}
	df2 := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"v": strCol(t, "a")},
		ColumnOrder: []string{"v"},
		Index:       []string{"1"},
	}

	result, err := dataframe.Concat([]*dataframe.DataFrame{df1, df2})
	if err != nil {
		t.Fatalf("Concat failed: %v", err)
	}

	if got := result.Columns["v"].DType(); got.Kind() != reflect.Interface {
		t.Errorf("expected an untyped column on type conflict, got dtype %v", got)
	}
	v0, _ := result.Columns["v"].At(0)
	v1, _ := result.Columns["v"].At(1)
	if v0 != int64(1) || v1 != "a" {
		t.Errorf("expected values [1 a], got [%v %v]", v0, v1)
	}
}

// TestConcatPreservesNullsWithTypedColumns checks null handling survives the
// switch from untyped to typed result columns.
func TestConcatPreservesNullsWithTypedColumns(t *testing.T) {
	withNull, err := collection.NewInt64SeriesFromData([]int64{1, 0}, []bool{false, true})
	if err != nil {
		t.Fatalf("building series: %v", err)
	}
	df1 := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"v": withNull},
		ColumnOrder: []string{"v"},
		Index:       []string{"0", "1"},
	}
	df2 := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"v": intCol(t, 3)},
		ColumnOrder: []string{"v"},
		Index:       []string{"2"},
	}

	result, err := dataframe.Concat([]*dataframe.DataFrame{df1, df2})
	if err != nil {
		t.Fatalf("Concat failed: %v", err)
	}

	v := result.Columns["v"]
	if v.Len() != 3 {
		t.Fatalf("expected 3 rows, got %d", v.Len())
	}
	if v.IsNull(0) || !v.IsNull(1) || v.IsNull(2) {
		t.Errorf("expected null mask [false true false], got [%v %v %v]",
			v.IsNull(0), v.IsNull(1), v.IsNull(2))
	}
}

// TestConcatAxisColumnsPreservesDTypes checks the horizontal path also keeps the
// source dtype.
func TestConcatAxisColumnsPreservesDTypes(t *testing.T) {
	df1 := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"a": intCol(t, 1, 2)},
		ColumnOrder: []string{"a"},
		Index:       []string{"0", "1"},
	}
	df2 := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"b": strCol(t, "x", "y")},
		ColumnOrder: []string{"b"},
		Index:       []string{"0", "1"},
	}

	result, err := dataframe.Concat([]*dataframe.DataFrame{df1, df2}, dataframe.ConcatOptions{
		Axis: dataframe.AxisColumns,
		Join: dataframe.JoinOuter,
	})
	if err != nil {
		t.Fatalf("Concat failed: %v", err)
	}

	if got, want := result.Columns["a"].DType(), reflect.TypeOf(int64(0)); got != want {
		t.Errorf("column a: expected dtype %v, got %v", want, got)
	}
	if got, want := result.Columns["b"].DType(), reflect.TypeOf(""); got != want {
		t.Errorf("column b: expected dtype %v, got %v", want, got)
	}
}
