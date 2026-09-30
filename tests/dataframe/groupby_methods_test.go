package dataframe_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// groupMethodsDF has three groups (Eng rows 0,2,4; Ops row 5; Sales rows 1,3),
// interleaved so that row alignment is actually exercised, plus a null in a
// numeric column and a null in a string column.
func groupMethodsDF(t *testing.T) *dataframe.DataFrame {
	t.Helper()
	name, err := collection.NewStringSeriesFromData(
		[]string{"a", "b", "", "d", "e", "f"},
		[]bool{false, false, true, false, false, false})
	if err != nil {
		t.Fatal(err)
	}
	return &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Dept":   mustSeries("Eng", "Sales", "Eng", "Sales", "Eng", "Ops"),
			"Salary": mustFloat64Series(t, []float64{100, 50, 200, 70, 0, 90}, []bool{false, false, false, false, true, false}),
			"Years":  mustInt64Series(t, []int64{1, 2, 3, 4, 5, 6}, nil),
			"Name":   name,
		},
		ColumnOrder: []string{"Dept", "Salary", "Years", "Name"},
		Index:       []string{"r0", "r1", "r2", "r3", "r4", "r5"},
	}
}

func groupByDept(t *testing.T) *dataframe.GroupBy {
	t.Helper()
	gb, err := groupMethodsDF(t).GroupBy([]string{"Dept"}, 0)
	if err != nil {
		t.Fatalf("GroupBy failed: %v", err)
	}
	return gb
}

// sameValues compares two columns cell by cell, treating NaN as equal to NaN.
func sameValues(a, b collection.Series) bool {
	if a.Len() != b.Len() {
		return false
	}
	for i := 0; i < a.Len(); i++ {
		if a.IsNull(i) != b.IsNull(i) {
			return false
		}
		va, _ := a.At(i)
		vb, _ := b.At(i)
		fa, okA := va.(float64)
		fb, okB := vb.(float64)
		if okA && okB && math.IsNaN(fa) && math.IsNaN(fb) {
			continue
		}
		if !reflect.DeepEqual(va, vb) {
			return false
		}
	}
	return true
}

func TestGroupByMethodsMatchAgg(t *testing.T) {
	gb := groupByDept(t)

	cases := []struct {
		fn      dataframe.AggFunc
		method  func() (*dataframe.DataFrame, error)
		columns []string
	}{
		{dataframe.AggMean, gb.Mean, []string{"Salary", "Years"}},
		{dataframe.AggSum, gb.Sum, []string{"Salary", "Years"}},
		{dataframe.AggMin, gb.Min, []string{"Salary", "Years"}},
		{dataframe.AggMax, gb.Max, []string{"Salary", "Years"}},
		{dataframe.AggStd, gb.Std, []string{"Salary", "Years"}},
		{dataframe.AggVar, gb.Var, []string{"Salary", "Years"}},
		{dataframe.AggMedian, gb.Median, []string{"Salary", "Years"}},
		{dataframe.AggCount, gb.Count, []string{"Salary", "Years", "Name"}},
		{dataframe.AggFirst, gb.First, []string{"Salary", "Years", "Name"}},
		{dataframe.AggLast, gb.Last, []string{"Salary", "Years", "Name"}},
	}

	for _, tc := range cases {
		t.Run(string(tc.fn), func(t *testing.T) {
			got, err := tc.method()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			wantOrder := append([]string{"Dept"}, tc.columns...)
			if !strSliceEqual(got.ColumnOrder, wantOrder) {
				t.Fatalf("columns: expected %v, got %v", wantOrder, got.ColumnOrder)
			}

			spec := map[string][]dataframe.AggFunc{}
			for _, c := range tc.columns {
				spec[c] = []dataframe.AggFunc{tc.fn}
			}
			ref, err := gb.Agg(spec)
			if err != nil {
				t.Fatalf("Agg failed: %v", err)
			}
			if !sameValues(got.Columns["Dept"], ref.Columns["Dept"]) {
				t.Error("group keys differ from Agg")
			}
			for _, c := range tc.columns {
				if !sameValues(got.Columns[c], ref.Columns[c+"_"+string(tc.fn)]) {
					t.Errorf("column %q differs from Agg %s_%s", c, c, tc.fn)
				}
			}
		})
	}
}

func TestGroupByMethodValues(t *testing.T) {
	gb := groupByDept(t)
	// Groups in key order: Eng (100, 200, null | 1, 3, 5), Ops (90 | 6), Sales (50, 70 | 2, 4).

	call := func(f func() (*dataframe.DataFrame, error)) *dataframe.DataFrame {
		t.Helper()
		df, err := f()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return df
	}

	keys := call(gb.Var).Columns["Dept"].ValuesCopy()
	if !reflect.DeepEqual(keys, []any{"Eng", "Ops", "Sales"}) {
		t.Fatalf("group keys: got %v", keys)
	}

	nan := opsVal(math.NaN())
	varDF := call(gb.Var)
	assertColumn(t, varDF, "Salary", []opsCell{opsVal(5000), nan, opsVal(200)})
	assertColumn(t, varDF, "Years", []opsCell{opsVal(4), nan, opsVal(2)})

	stdDF := call(gb.Std)
	assertColumn(t, stdDF, "Salary", []opsCell{opsVal(math.Sqrt(5000)), nan, opsVal(math.Sqrt(200))})

	medDF := call(gb.Median)
	assertColumn(t, medDF, "Salary", []opsCell{opsVal(150), opsVal(90), opsVal(60)})
	assertColumn(t, medDF, "Years", []opsCell{opsVal(3), opsVal(6), opsVal(3)})

	countDF := call(gb.Count)
	assertColumn(t, countDF, "Salary", []opsCell{opsVal(2), opsVal(1), opsVal(2)})
	assertColumn(t, countDF, "Years", []opsCell{opsVal(3), opsVal(1), opsVal(2)})
	assertColumn(t, countDF, "Name", []opsCell{opsVal(2), opsVal(1), opsVal(2)})
	assertDType(t, countDF, "Name", reflect.Int64)

	// First and Last skip nulls: Eng's last salary is 200 because row 4 is null.
	firstDF := call(gb.First)
	if got := firstDF.Columns["Name"].ValuesCopy(); !reflect.DeepEqual(got, []any{"a", "f", "b"}) {
		t.Errorf("First Name: got %v", got)
	}
	lastDF := call(gb.Last)
	assertColumn(t, lastDF, "Salary", []opsCell{opsVal(200), opsVal(90), opsVal(70)})
	if got := lastDF.Columns["Name"].ValuesCopy(); !reflect.DeepEqual(got, []any{"e", "f", "d"}) {
		t.Errorf("Last Name: got %v", got)
	}

	// Size counts rows, so Eng is 3 even though one salary and one name are null.
	sizeDF := call(gb.Size)
	if !strSliceEqual(sizeDF.ColumnOrder, []string{"Dept", "size"}) {
		t.Fatalf("Size columns: got %v", sizeDF.ColumnOrder)
	}
	assertColumn(t, sizeDF, "size", []opsCell{opsVal(3), opsVal(1), opsVal(2)})
	assertDType(t, sizeDF, "size", reflect.Int64)
}

func TestGroupByKeepsKeyTypes(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Year":  mustInt64Series(t, []int64{2024, 2023, 2024}, nil),
			"Sales": mustFloat64Series(t, []float64{1, 2, 3}, nil),
		},
		ColumnOrder: []string{"Year", "Sales"},
		Index:       []string{"0", "1", "2"},
	}
	gb, err := df.GroupBy([]string{"Year"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := gb.Sum()
	if err != nil {
		t.Fatal(err)
	}
	assertDType(t, got, "Year", reflect.Int64)
	assertColumn(t, got, "Year", []opsCell{opsVal(2023), opsVal(2024)})
	assertColumn(t, got, "Sales", []opsCell{opsVal(2), opsVal(4)})
}

func TestGroupByAggVarAndSize(t *testing.T) {
	gb := groupByDept(t)
	got, err := gb.Agg(map[string][]dataframe.AggFunc{
		"Salary": {dataframe.AggVar, dataframe.AggSize},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertColumn(t, got, "Salary_var", []opsCell{opsVal(5000), opsVal(math.NaN()), opsVal(200)})
	assertColumn(t, got, "Salary_size", []opsCell{opsVal(3), opsVal(1), opsVal(2)})
}

func TestGroupByCumcount(t *testing.T) {
	gb := groupByDept(t)
	got, err := gb.Cumcount()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Dept is Eng, Sales, Eng, Sales, Eng, Ops.
	if want := []any{int64(0), int64(0), int64(1), int64(1), int64(2), int64(0)}; !reflect.DeepEqual(got.ValuesCopy(), want) {
		t.Errorf("expected %v, got %v", want, got.ValuesCopy())
	}
}

func TestGroupByTransform(t *testing.T) {
	src := groupMethodsDF(t)
	gb, err := src.GroupBy([]string{"Dept"}, 0)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("mean broadcasts to every row", func(t *testing.T) {
		got, err := gb.Transform(dataframe.AggMean)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Aligned row-for-row with the source; the grouping column is left out.
		if !strSliceEqual(got.ColumnOrder, []string{"Salary", "Years"}) {
			t.Errorf("columns: got %v", got.ColumnOrder)
		}
		if !strSliceEqual(got.Index, src.Index) {
			t.Errorf("index: expected %v, got %v", src.Index, got.Index)
		}
		// Row 4's own salary is null, but it still receives Eng's mean.
		assertColumn(t, got, "Salary", []opsCell{opsVal(150), opsVal(60), opsVal(150), opsVal(60), opsVal(150), opsVal(90)})
		assertColumn(t, got, "Years", []opsCell{opsVal(3), opsVal(3), opsVal(3), opsVal(3), opsVal(3), opsVal(6)})
	})

	t.Run("matches Agg for every row", func(t *testing.T) {
		for _, fn := range []dataframe.AggFunc{dataframe.AggSum, dataframe.AggStd, dataframe.AggCount, dataframe.AggFirst} {
			got, err := gb.Transform(fn)
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", fn, err)
			}
			ref, err := gb.Agg(map[string][]dataframe.AggFunc{"Salary": {fn}})
			if err != nil {
				t.Fatal(err)
			}
			// Map each source row to its group's row in the Agg result.
			groupRow := map[string]int{}
			for i, k := range ref.Columns["Dept"].ValuesCopy() {
				groupRow[k.(string)] = i
			}
			for row, dept := range src.Columns["Dept"].ValuesCopy() {
				a, _ := got.Columns["Salary"].At(row)
				b, _ := ref.Columns["Salary_"+string(fn)].At(groupRow[dept.(string)])
				fa, _ := a.(float64)
				fb, _ := b.(float64)
				if !reflect.DeepEqual(a, b) && !(math.IsNaN(fa) && math.IsNaN(fb)) {
					t.Errorf("%s row %d: transform %v, Agg %v", fn, row, a, b)
				}
			}
		}
	})

	t.Run("size", func(t *testing.T) {
		got, err := gb.Transform(dataframe.AggSize)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strSliceEqual(got.ColumnOrder, []string{"size"}) {
			t.Errorf("columns: got %v", got.ColumnOrder)
		}
		assertColumn(t, got, "size", []opsCell{opsVal(3), opsVal(2), opsVal(3), opsVal(2), opsVal(3), opsVal(1)})
	})

	t.Run("unsupported function", func(t *testing.T) {
		if _, err := gb.Transform("mode"); err == nil {
			t.Error("expected an error")
		}
	})
}
