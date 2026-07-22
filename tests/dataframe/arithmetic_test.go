package dataframe_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func TestArithmeticColumns(t *testing.T) {
	t.Run("float column-column ops", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(10.0, 20.0, 30.0),
				"B": mustSeries(2.0, 4.0, 5.0),
			},
			ColumnOrder: []string{"A", "B"},
			Index:       []string{"0", "1", "2"},
		}

		add, err := df.Add("A", "B")
		if err != nil {
			t.Fatalf("Add error: %v", err)
		}
		assertFloat(t, add, 0, 12.0)
		assertFloat(t, add, 2, 35.0)

		sub, _ := df.Sub("A", "B")
		assertFloat(t, sub, 1, 16.0)

		mul, _ := df.Mul("A", "B")
		assertFloat(t, mul, 2, 150.0)

		div, _ := df.Div("A", "B")
		assertFloat(t, div, 0, 5.0)

		pow, _ := df.Pow("B", "B")
		assertFloat(t, pow, 0, 4.0) // 2^2
	})

	t.Run("int + int stays int64", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(int64(3), int64(5)),
				"B": mustSeries(int64(4), int64(6)),
			},
			ColumnOrder: []string{"A", "B"},
			Index:       []string{"0", "1"},
		}
		res, err := df.Add("A", "B")
		if err != nil {
			t.Fatalf("Add error: %v", err)
		}
		if res.DType().Kind() != reflect.Int64 {
			t.Fatalf("expected Int64 result, got %v", res.DType())
		}
		v, _ := res.At(0)
		if !valuesEqual(v, 7) {
			t.Errorf("expected 7, got %v", v)
		}
	})

	t.Run("int / int promotes to float64 (true division)", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(int64(7), int64(1)),
				"B": mustSeries(int64(2), int64(4)),
			},
			ColumnOrder: []string{"A", "B"},
			Index:       []string{"0", "1"},
		}
		res, err := df.Div("A", "B")
		if err != nil {
			t.Fatalf("Div error: %v", err)
		}
		if res.DType().Kind() != reflect.Float64 {
			t.Fatalf("expected Float64 result, got %v", res.DType())
		}
		assertFloat(t, res, 0, 3.5)
		assertFloat(t, res, 1, 0.25)
	})

	t.Run("mixed int and float promotes to float64", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(int64(3), int64(5)),
				"B": mustSeries(1.5, 2.5),
			},
			ColumnOrder: []string{"A", "B"},
			Index:       []string{"0", "1"},
		}
		res, _ := df.Add("A", "B")
		if res.DType().Kind() != reflect.Float64 {
			t.Fatalf("expected Float64 result, got %v", res.DType())
		}
		assertFloat(t, res, 0, 4.5)
	})

	t.Run("null propagation", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(1.0, nil, 3.0),
				"B": mustSeries(10.0, 20.0, nil),
			},
			ColumnOrder: []string{"A", "B"},
			Index:       []string{"0", "1", "2"},
		}
		res, _ := df.Add("A", "B")
		assertFloat(t, res, 0, 11.0)
		if !res.IsNull(1) {
			t.Error("expected null at index 1 (A null)")
		}
		if !res.IsNull(2) {
			t.Error("expected null at index 2 (B null)")
		}
	})

	t.Run("divide by zero yields Inf, not error", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(1.0, -1.0, 0.0),
				"B": mustSeries(0.0, 0.0, 0.0),
			},
			ColumnOrder: []string{"A", "B"},
			Index:       []string{"0", "1", "2"},
		}
		res, err := df.Div("A", "B")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		v0, _ := res.At(0)
		v1, _ := res.At(1)
		v2, _ := res.At(2)
		if !math.IsInf(v0.(float64), 1) {
			t.Errorf("expected +Inf, got %v", v0)
		}
		if !math.IsInf(v1.(float64), -1) {
			t.Errorf("expected -Inf, got %v", v1)
		}
		if !math.IsNaN(v2.(float64)) {
			t.Errorf("expected NaN, got %v", v2)
		}
	})

	t.Run("errors", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(1.0),
				"S": mustSeries("x"),
			},
			ColumnOrder: []string{"A", "S"},
			Index:       []string{"0"},
		}
		if _, err := df.Add("A", "Missing"); err == nil {
			t.Error("expected error for missing column")
		}
		if _, err := df.Add("A", "S"); err == nil {
			t.Error("expected error for non-numeric column")
		}
	})
}

func TestArithmeticScalar(t *testing.T) {
	t.Run("float scalar ops", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(10.0, 20.0, nil),
			},
			ColumnOrder: []string{"A"},
			Index:       []string{"0", "1", "2"},
		}
		add, _ := df.AddScalar("A", 5.0)
		assertFloat(t, add, 0, 15.0)
		if !add.IsNull(2) {
			t.Error("expected null preserved at index 2")
		}

		mul, _ := df.MulScalar("A", 2.0)
		assertFloat(t, mul, 1, 40.0)

		div, _ := df.DivScalar("A", 4.0)
		assertFloat(t, div, 0, 2.5)

		pow, _ := df.PowScalar("A", 2.0)
		assertFloat(t, pow, 0, 100.0)
	})

	t.Run("int column with whole scalar stays int", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(int64(3), int64(4)),
			},
			ColumnOrder: []string{"A"},
			Index:       []string{"0", "1"},
		}
		res, _ := df.MulScalar("A", 3.0)
		if res.DType().Kind() != reflect.Int64 {
			t.Fatalf("expected Int64 result, got %v", res.DType())
		}
		v, _ := res.At(1)
		if !valuesEqual(v, 12) {
			t.Errorf("expected 12, got %v", v)
		}
	})

	t.Run("int column with fractional scalar promotes to float", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(int64(3), int64(4)),
			},
			ColumnOrder: []string{"A"},
			Index:       []string{"0", "1"},
		}
		res, _ := df.MulScalar("A", 1.5)
		if res.DType().Kind() != reflect.Float64 {
			t.Fatalf("expected Float64 result, got %v", res.DType())
		}
		assertFloat(t, res, 0, 4.5)
	})
}

func TestComparisons(t *testing.T) {
	t.Run("numeric column-column comparisons", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(1.0, 5.0, 3.0),
				"B": mustSeries(2.0, 5.0, 1.0),
			},
			ColumnOrder: []string{"A", "B"},
			Index:       []string{"0", "1", "2"},
		}
		gt, err := df.Gt("A", "B")
		if err != nil {
			t.Fatalf("Gt error: %v", err)
		}
		if gt.DType().Kind() != reflect.Bool {
			t.Fatalf("expected Bool result, got %v", gt.DType())
		}
		assertBool(t, gt, 0, false)
		assertBool(t, gt, 1, false)
		assertBool(t, gt, 2, true)

		ge, _ := df.Ge("A", "B")
		assertBool(t, ge, 1, true)

		eq, _ := df.Eq("A", "B")
		assertBool(t, eq, 1, true)
		assertBool(t, eq, 0, false)

		ne, _ := df.Ne("A", "B")
		assertBool(t, ne, 0, true)
		assertBool(t, ne, 1, false)
	})

	t.Run("cross int/float numeric comparison", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(int64(2), int64(5)),
				"B": mustSeries(2.0, 4.5),
			},
			ColumnOrder: []string{"A", "B"},
			Index:       []string{"0", "1"},
		}
		eq, _ := df.Eq("A", "B")
		assertBool(t, eq, 0, true)
		ge, _ := df.Ge("A", "B")
		assertBool(t, ge, 1, true)
	})

	t.Run("null yields null in result", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(1.0, nil),
				"B": mustSeries(2.0, 2.0),
			},
			ColumnOrder: []string{"A", "B"},
			Index:       []string{"0", "1"},
		}
		lt, _ := df.Lt("A", "B")
		assertBool(t, lt, 0, true)
		if !lt.IsNull(1) {
			t.Error("expected null at index 1")
		}
	})

	t.Run("string ordering and equality", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries("apple", "cherry"),
				"B": mustSeries("banana", "cherry"),
			},
			ColumnOrder: []string{"A", "B"},
			Index:       []string{"0", "1"},
		}
		lt, err := df.Lt("A", "B")
		if err != nil {
			t.Fatalf("Lt error: %v", err)
		}
		assertBool(t, lt, 0, true) // apple < banana
		eq, _ := df.Eq("A", "B")
		assertBool(t, eq, 1, true)
	})

	t.Run("scalar comparisons", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Age": mustSeries(int64(20), int64(30), int64(40)),
			},
			ColumnOrder: []string{"Age"},
			Index:       []string{"0", "1", "2"},
		}
		gt, err := df.GtScalar("Age", 25)
		if err != nil {
			t.Fatalf("GtScalar error: %v", err)
		}
		assertBool(t, gt, 0, false)
		assertBool(t, gt, 1, true)

		eq, _ := df.EqScalar("Age", int64(30))
		assertBool(t, eq, 1, true)

		le, _ := df.LeScalar("Age", 30.0)
		assertBool(t, le, 0, true)
		assertBool(t, le, 2, false)
	})

	t.Run("string scalar equality", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"City": mustSeries("NYC", "LA", "NYC"),
			},
			ColumnOrder: []string{"City"},
			Index:       []string{"0", "1", "2"},
		}
		eq, _ := df.EqScalar("City", "NYC")
		assertBool(t, eq, 0, true)
		assertBool(t, eq, 1, false)
	})

	t.Run("mismatched non-numeric types are not equal", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"S": mustSeries("1", "2"),
			},
			ColumnOrder: []string{"S"},
			Index:       []string{"0", "1"},
		}
		// comparing a string column to a numeric scalar: not equal, no panic
		eq, err := df.EqScalar("S", 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		assertBool(t, eq, 0, false)
	})

	t.Run("order-compare incompatible types errors", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"S": mustSeries("a", "b"),
			},
			ColumnOrder: []string{"S"},
			Index:       []string{"0", "1"},
		}
		if _, err := df.GtScalar("S", 1); err == nil {
			t.Error("expected error ordering string against number")
		}
	})

	t.Run("nil scalar errors", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(1.0),
			},
			ColumnOrder: []string{"A"},
			Index:       []string{"0"},
		}
		if _, err := df.EqScalar("A", nil); err == nil {
			t.Error("expected error for nil scalar")
		}
	})
}

// assertFloat checks that the non-null value at index i equals want (within tolerance).
func assertFloat(t *testing.T, s collection.Series, i int, want float64) {
	t.Helper()
	if s.IsNull(i) {
		t.Fatalf("index %d is null, expected %v", i, want)
	}
	v, err := s.At(i)
	if err != nil {
		t.Fatalf("At(%d) error: %v", i, err)
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("index %d: expected float64, got %T", i, v)
	}
	if math.Abs(f-want) > 1e-9 {
		t.Errorf("index %d: expected %v, got %v", i, want, f)
	}
}

// assertBool checks that the non-null bool value at index i equals want.
func assertBool(t *testing.T, s collection.Series, i int, want bool) {
	t.Helper()
	if s.IsNull(i) {
		t.Fatalf("index %d is null, expected %v", i, want)
	}
	v, err := s.At(i)
	if err != nil {
		t.Fatalf("At(%d) error: %v", i, err)
	}
	b, ok := v.(bool)
	if !ok {
		t.Fatalf("index %d: expected bool, got %T", i, v)
	}
	if b != want {
		t.Errorf("index %d: expected %v, got %v", i, want, b)
	}
}
