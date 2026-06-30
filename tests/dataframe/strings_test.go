package dataframe_test

import (
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func strDF() *dataframe.DataFrame {
	names, _ := collection.NewStringSeriesFromData(
		[]string{"Alice", "bob", "  Charlie  ", ""}, []bool{false, false, false, true})
	return &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"Name": names},
		ColumnOrder: []string{"Name"},
		Index:       []string{"0", "1", "2", "3"},
	}
}

func TestStrAccessor(t *testing.T) {
	t.Run("lower", func(t *testing.T) {
		acc, err := strDF().Str("Name")
		if err != nil {
			t.Fatalf("Str failed: %v", err)
		}
		lower := acc.Lower()
		v0, _ := lower.At(0)
		if v0 != "alice" {
			t.Errorf("expected alice, got %v", v0)
		}
		// null preserved
		if !lower.IsNull(3) {
			t.Error("expected null preserved at index 3")
		}
	})

	t.Run("upper and strip", func(t *testing.T) {
		acc, _ := strDF().Str("Name")
		up, _ := acc.Upper().At(1)
		if up != "BOB" {
			t.Errorf("expected BOB, got %v", up)
		}
		acc2, _ := strDF().Str("Name")
		stripped, _ := acc2.Strip().At(2)
		if stripped != "Charlie" {
			t.Errorf("expected 'Charlie', got %q", stripped)
		}
	})

	t.Run("contains returns bool series", func(t *testing.T) {
		acc, _ := strDF().Str("Name")
		contains := acc.Contains("li") // Alice, Charlie (after no strip, "  Charlie  " contains li)
		v0, _ := contains.At(0)
		v1, _ := contains.At(1)
		if v0 != true || v1 != false {
			t.Errorf("expected [true, false], got [%v, %v]", v0, v1)
		}
		if !contains.IsNull(3) {
			t.Error("expected null preserved")
		}
	})

	t.Run("len", func(t *testing.T) {
		acc, _ := strDF().Str("Name")
		lengths := acc.Len()
		v1, _ := lengths.At(1) // "bob" -> 3
		if !valuesEqual(v1, 3) {
			t.Errorf("expected len 3, got %v", v1)
		}
	})

	t.Run("replace", func(t *testing.T) {
		acc, _ := strDF().Str("Name")
		replaced := acc.Replace("o", "0")
		v1, _ := replaced.At(1) // bob -> b0b
		if v1 != "b0b" {
			t.Errorf("expected b0b, got %v", v1)
		}
	})

	t.Run("integrate with Assign", func(t *testing.T) {
		df := strDF()
		acc, _ := df.Str("Name")
		if err := df.Assign("name_len", acc.Len()); err != nil {
			t.Fatalf("Assign failed: %v", err)
		}
		if _, ok := df.Columns["name_len"]; !ok {
			t.Error("expected name_len column added")
		}
	})

	t.Run("non-string column errors", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"N": mustSeries(1, 2)},
			ColumnOrder: []string{"N"},
			Index:       []string{"0", "1"},
		}
		// mustSeries builds AnySeries, not StringSeries
		if _, err := df.Str("N"); err == nil {
			t.Error("expected error for non-string column")
		}
	})
}
