package dataframe_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// binnedValues returns a column's values, with nil for nulls, and its categories.
func binnedValues(t *testing.T, df *dataframe.DataFrame, column string) ([]any, []string) {
	t.Helper()
	cat, ok := df.Columns[column].(*collection.CategoricalSeries)
	if !ok {
		t.Fatalf("column %q: expected *CategoricalSeries, got %T", column, df.Columns[column])
	}
	return cat.ValuesCopy(), cat.Categories()
}

func binningDF(t *testing.T, values []float64, mask []bool) *dataframe.DataFrame {
	t.Helper()
	ids := make([]any, len(values))
	idx := make([]string, len(values))
	for i := range values {
		ids[i] = int64(i)
		idx[i] = string(rune('a' + i))
	}
	return &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Id":    mustSeries(ids...),
			"Value": mustFloat64Series(t, values, mask),
		},
		ColumnOrder: []string{"Value", "Id"},
		Index:       idx,
	}
}

// -----------------------------------------------------------------------------
// Cut
// -----------------------------------------------------------------------------

func TestCutRightClosedIntervals(t *testing.T) {
	df := binningDF(t,
		[]float64{0, 5, 10, 15, 20, 25, 0, math.NaN()},
		[]bool{false, false, false, false, false, false, true, false},
	)

	got, err := df.Cut("Value", []float64{0, 10, 20}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	vals, cats := binnedValues(t, got, "Value")
	// 0 sits on the lowest edge, which a right-closed bin excludes; 25 is past the
	// top edge; then a null and a NaN.
	want := []any{nil, "(0, 10]", "(0, 10]", "(10, 20]", "(10, 20]", nil, nil, nil}
	if !reflect.DeepEqual(vals, want) {
		t.Errorf("values: expected %v, got %v", want, vals)
	}
	if !strSliceEqual(cats, []string{"(0, 10]", "(10, 20]"}) {
		t.Errorf("categories: got %v", cats)
	}
}

func TestCutLabelsKeepEmptyBins(t *testing.T) {
	df := binningDF(t, []float64{3, 70, 40, 12}, nil)

	got, err := df.Cut("Value", []float64{math.Inf(-1), 18, 30, 65, math.Inf(1)}, []string{"minor", "young", "adult", "senior"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	vals, cats := binnedValues(t, got, "Value")
	if want := []any{"minor", "senior", "adult", "minor"}; !reflect.DeepEqual(vals, want) {
		t.Errorf("values: expected %v, got %v", want, vals)
	}
	// "young" holds no value but stays a category, in edge order.
	if !strSliceEqual(cats, []string{"minor", "young", "adult", "senior"}) {
		t.Errorf("categories: got %v", cats)
	}
}

func TestCutIntegerColumn(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"N": mustInt64Series(t, []int64{1, 5, 9}, []bool{false, true, false})},
		ColumnOrder: []string{"N"},
		Index:       []string{"0", "1", "2"},
	}
	got, err := df.Cut("N", []float64{0, 4, 10}, []string{"low", "high"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	vals, _ := binnedValues(t, got, "N")
	if want := []any{"low", nil, "high"}; !reflect.DeepEqual(vals, want) {
		t.Errorf("expected %v, got %v", want, vals)
	}
}

func TestCutPreservesFrame(t *testing.T) {
	df := binningDF(t, []float64{1, 2, 3}, nil)
	got, err := df.Cut("Value", []float64{0, 2, 4}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strSliceEqual(got.ColumnOrder, []string{"Value", "Id"}) {
		t.Errorf("column order: got %v", got.ColumnOrder)
	}
	if !strSliceEqual(got.Index, []string{"a", "b", "c"}) {
		t.Errorf("index: got %v", got.Index)
	}
	if got.Columns["Id"] != df.Columns["Id"] {
		t.Error("untouched column should pass through as the same Series")
	}
	if _, ok := df.Columns["Value"].(*collection.Float64Series); !ok {
		t.Error("source DataFrame was mutated")
	}
}

func TestCutErrors(t *testing.T) {
	df := binningDF(t, []float64{1, 2, 3}, nil)
	df.Columns["Name"] = mustSeries("x", "y", "z")
	df.ColumnOrder = append(df.ColumnOrder, "Name")

	cases := []struct {
		name   string
		column string
		bins   []float64
		labels []string
	}{
		{"missing column", "Nope", []float64{0, 1}, nil},
		{"string column", "Name", []float64{0, 1}, nil},
		{"one edge", "Value", []float64{0}, nil},
		{"decreasing edges", "Value", []float64{0, 5, 3}, nil},
		{"repeated edge", "Value", []float64{0, 5, 5}, nil},
		{"NaN edge", "Value", []float64{0, math.NaN()}, nil},
		{"too few labels", "Value", []float64{0, 1, 2}, []string{"a"}},
		{"duplicate labels", "Value", []float64{0, 1, 2}, []string{"a", "a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := df.Cut(tc.column, tc.bins, tc.labels); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Qcut
// -----------------------------------------------------------------------------

func TestQcutQuartiles(t *testing.T) {
	df := binningDF(t,
		[]float64{8, 1, 5, 0, 3, 2, 7, 4, 6},
		[]bool{false, false, false, true, false, false, false, false, false},
	)

	got, err := df.Qcut("Value", 4, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Over 1..8 the linear-interpolation quartiles are 1, 2.75, 4.5, 6.25, 8. The
	// first bin is closed on the left so the minimum, 1, is kept.
	vals, cats := binnedValues(t, got, "Value")
	q1, q2, q3, q4 := "[1, 2.75]", "(2.75, 4.5]", "(4.5, 6.25]", "(6.25, 8]"
	want := []any{q4, q1, q3, nil, q2, q1, q4, q2, q3}
	if !reflect.DeepEqual(vals, want) {
		t.Errorf("values: expected %v, got %v", want, vals)
	}
	if !strSliceEqual(cats, []string{q1, q2, q3, q4}) {
		t.Errorf("categories: got %v", cats)
	}
}

func TestQcutEveryValueInOneBin(t *testing.T) {
	// Skewed data with ties and a NaN.
	values := []float64{0.1, 0.1, 0.2, 3, 3, 7.5, 40, 41, 1000, -2, math.NaN(), 12}
	df := binningDF(t, values, nil)

	for q := 1; q <= 5; q++ {
		got, err := df.Qcut("Value", q, nil)
		if err != nil {
			t.Fatalf("q=%d: unexpected error: %v", q, err)
		}
		vals, cats := binnedValues(t, got, "Value")
		if len(cats) != q {
			t.Errorf("q=%d: expected %d categories, got %d", q, q, len(cats))
		}
		for i, v := range values {
			if isNull := vals[i] == nil; isNull != math.IsNaN(v) {
				t.Errorf("q=%d row %d (%v): null=%v", q, i, v, isNull)
			}
		}
	}
}

func TestQcutLabels(t *testing.T) {
	df := binningDF(t, []float64{10, 20, 30, 40}, nil)
	got, err := df.Qcut("Value", 2, []string{"low", "high"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	vals, _ := binnedValues(t, got, "Value")
	if want := []any{"low", "low", "high", "high"}; !reflect.DeepEqual(vals, want) {
		t.Errorf("expected %v, got %v", want, vals)
	}
}

func TestQcutErrors(t *testing.T) {
	cases := []struct {
		name   string
		df     *dataframe.DataFrame
		q      int
		labels []string
	}{
		{"q zero", binningDF(t, []float64{1, 2}, nil), 0, nil},
		{"constant column", binningDF(t, []float64{5, 5, 5, 5}, nil), 2, nil},
		{"mostly one value", binningDF(t, []float64{0, 0, 0, 0, 0, 1}, nil), 4, nil},
		{"all null", binningDF(t, []float64{0, 0}, []bool{true, true}), 2, nil},
		{"label count", binningDF(t, []float64{1, 2, 3}, nil), 3, []string{"a", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.df.Qcut("Value", tc.q, tc.labels); err == nil {
				t.Error("expected an error")
			}
		})
	}
}
