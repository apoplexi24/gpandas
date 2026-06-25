package dataframe_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func TestApply(t *testing.T) {
	t.Run("element-wise transform on float column", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Salary": mustSeries(100.0, 200.0, 300.0),
			},
			ColumnOrder: []string{"Salary"},
			Index:       []string{"0", "1", "2"},
		}
		result, err := df.Apply("Salary", func(v any) any {
			return v.(float64) * 1.1
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		v0, _ := result.Columns["Salary"].At(0)
		if math.Abs(v0.(float64)-110.0) > 1e-9 {
			t.Errorf("expected ~110.0, got %v", v0)
		}
		// original unchanged
		orig, _ := df.Columns["Salary"].At(0)
		if !valuesEqual(orig, 100.0) {
			t.Errorf("original mutated: got %v", orig)
		}
	})

	t.Run("nulls passed as nil and preserved", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"V": mustSeries(1.0, nil, 3.0),
			},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1", "2"},
		}
		result, err := df.Apply("V", func(v any) any {
			if v == nil {
				return nil
			}
			return v.(float64) * 2
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !result.Columns["V"].IsNull(1) {
			t.Error("expected null preserved at index 1")
		}
		v2, _ := result.Columns["V"].At(2)
		if !valuesEqual(v2, 6.0) {
			t.Errorf("expected 6.0, got %v", v2)
		}
	})

	t.Run("type change string to int infers Int64 series", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Word": mustSeries("a", "bb", "ccc"),
			},
			ColumnOrder: []string{"Word"},
			Index:       []string{"0", "1", "2"},
		}
		result, err := df.Apply("Word", func(v any) any {
			return int64(len(v.(string)))
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Columns["Word"].DType().Kind() != reflect.Int64 {
			t.Errorf("expected Int64 series, got %v", result.Columns["Word"].DType())
		}
		v2, _ := result.Columns["Word"].At(2)
		if !valuesEqual(v2, 3) {
			t.Errorf("expected 3, got %v", v2)
		}
	})

	t.Run("errors", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"A": mustSeries(1)},
			ColumnOrder: []string{"A"},
			Index:       []string{"0"},
		}
		if _, err := df.Apply("A", nil); err == nil {
			t.Error("expected error for nil fn")
		}
		if _, err := df.Apply("Missing", func(v any) any { return v }); err == nil {
			t.Error("expected error for missing column")
		}
	})

	t.Run("mixed int and float results promote to float64", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"V": mustSeries(1, 2, 3),
			},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1", "2"},
		}
		// Return int64 for evens, float64 for odds -> should promote to float64
		result, err := df.Apply("V", func(v any) any {
			n := v.(int)
			if n%2 == 0 {
				return int64(n * 10)
			}
			return float64(n) * 1.5
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Columns["V"].DType().Kind() != reflect.Float64 {
			t.Fatalf("expected Float64 series, got %v", result.Columns["V"].DType())
		}
		// index 0 (n=1, odd) -> 1.5 ; index 1 (n=2, even) -> 20.0
		v0, _ := result.Columns["V"].At(0)
		v1, _ := result.Columns["V"].At(1)
		if v0.(float64) != 1.5 || v1.(float64) != 20.0 {
			t.Errorf("expected [1.5, 20.0, ...], got [%v, %v, ...]", v0, v1)
		}
	})
}

func TestMap(t *testing.T) {
	t.Run("replace mapped values, keep others", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Flag": mustSeries("Y", "N", "Y", "Maybe"),
			},
			ColumnOrder: []string{"Flag"},
			Index:       []string{"0", "1", "2", "3"},
		}
		result, err := df.Map("Flag", map[any]any{"Y": true, "N": false})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Y->true, N->false, Maybe stays "Maybe" => mixed types => AnySeries
		v0, _ := result.Columns["Flag"].At(0)
		v1, _ := result.Columns["Flag"].At(1)
		v3, _ := result.Columns["Flag"].At(3)
		if v0 != true || v1 != false || v3 != "Maybe" {
			t.Errorf("expected [true, false, ..., Maybe], got [%v, %v, ..., %v]", v0, v1, v3)
		}
	})

	t.Run("fully mapped homogeneous becomes typed series", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Flag": mustSeries("Y", "N", "Y"),
			},
			ColumnOrder: []string{"Flag"},
			Index:       []string{"0", "1", "2"},
		}
		result, err := df.Map("Flag", map[any]any{"Y": true, "N": false})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Columns["Flag"].DType().Kind() != reflect.Bool {
			t.Errorf("expected Bool series, got %v", result.Columns["Flag"].DType())
		}
	})

	t.Run("nil mapping errors", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"A": mustSeries(1)},
			ColumnOrder: []string{"A"},
			Index:       []string{"0"},
		}
		if _, err := df.Map("A", nil); err == nil {
			t.Error("expected error for nil mapping")
		}
	})
}

func TestApplyRow(t *testing.T) {
	t.Run("derive new column", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Salary": mustSeries(100.0, 200.0),
			},
			ColumnOrder: []string{"Salary"},
			Index:       []string{"0", "1"},
		}
		result, err := df.ApplyRow(func(row map[string]any) map[string]any {
			row["Tax"] = row["Salary"].(float64) * 0.3
			return row
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strSliceEqual(result.ColumnOrder, []string{"Salary", "Tax"}) {
			t.Errorf("expected column order [Salary, Tax], got %v", result.ColumnOrder)
		}
		tax0, _ := result.Columns["Tax"].At(0)
		if !valuesEqual(tax0, 30.0) {
			t.Errorf("expected Tax 30.0, got %v", tax0)
		}
	})

	t.Run("new keys appended sorted", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"X": mustSeries(1, 2),
			},
			ColumnOrder: []string{"X"},
			Index:       []string{"0", "1"},
		}
		result, err := df.ApplyRow(func(row map[string]any) map[string]any {
			row["Zeta"] = "z"
			row["Alpha"] = "a"
			return row
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// X first (original), then Alpha, Zeta (sorted)
		if !strSliceEqual(result.ColumnOrder, []string{"X", "Alpha", "Zeta"}) {
			t.Errorf("expected [X, Alpha, Zeta], got %v", result.ColumnOrder)
		}
	})

	t.Run("nil fn errors", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"A": mustSeries(1)},
			ColumnOrder: []string{"A"},
			Index:       []string{"0"},
		}
		if _, err := df.ApplyRow(nil); err == nil {
			t.Error("expected error for nil fn")
		}
	})
}
