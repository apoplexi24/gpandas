package gpandas_test

import (
	"reflect"
	"testing"

	"github.com/apoplexi24/gpandas"
	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// TestPublicConcatPreservesDTypes checks that the public gpandas.Concat entry
// point preserves column dtypes. It previously had its own copy of the
// concatenation logic that built every result column as an untyped Series, so
// fixing the dataframe package alone left the public API wrong.
func TestPublicConcatPreservesDTypes(t *testing.T) {
	ints1, err := collection.NewInt64SeriesFromData([]int64{1, 2}, nil)
	if err != nil {
		t.Fatalf("building series: %v", err)
	}
	ints2, err := collection.NewInt64SeriesFromData([]int64{3}, nil)
	if err != nil {
		t.Fatalf("building series: %v", err)
	}
	strs1, err := collection.NewStringSeriesFromData([]string{"a", "b"}, nil)
	if err != nil {
		t.Fatalf("building series: %v", err)
	}
	strs2, err := collection.NewStringSeriesFromData([]string{"c"}, nil)
	if err != nil {
		t.Fatalf("building series: %v", err)
	}

	df1 := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"n": ints1, "s": strs1},
		ColumnOrder: []string{"n", "s"},
		Index:       []string{"0", "1"},
	}
	df2 := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"n": ints2, "s": strs2},
		ColumnOrder: []string{"n", "s"},
		Index:       []string{"2"},
	}

	result, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2})
	if err != nil {
		t.Fatalf("Concat failed: %v", err)
	}
	if result.Len() != 3 {
		t.Fatalf("expected 3 rows, got %d", result.Len())
	}

	if got, want := result.Columns["n"].DType(), reflect.TypeOf(int64(0)); got != want {
		t.Errorf("column n: expected dtype %v, got %v", want, got)
	}
	if got, want := result.Columns["s"].DType(), reflect.TypeOf(""); got != want {
		t.Errorf("column s: expected dtype %v, got %v", want, got)
	}

	for i, exp := range []int64{1, 2, 3} {
		v, _ := result.Columns["n"].At(i)
		if v != exp {
			t.Errorf("n[%d]: expected %v, got %v (%T)", i, exp, v, v)
		}
	}
}

// TestPublicConcatOptionsAreDataframeOptions guards the type aliasing that lets
// the public wrapper delegate to the single implementation.
func TestPublicConcatOptionsAreDataframeOptions(t *testing.T) {
	var opts gpandas.ConcatOptions = dataframe.ConcatOptions{
		Axis: dataframe.AxisColumns,
		Join: dataframe.JoinInner,
	}
	if opts.Axis != gpandas.AxisColumns {
		t.Errorf("expected axis %v, got %v", gpandas.AxisColumns, opts.Axis)
	}
	if opts.Join != gpandas.JoinInner {
		t.Errorf("expected join %v, got %v", gpandas.JoinInner, opts.Join)
	}
}
