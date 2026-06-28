package dataframe_test

import (
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func TestUnique(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"City": mustSeries("NYC", "LA", "NYC", nil, "LA", nil),
		},
		ColumnOrder: []string{"City"},
		Index:       []string{"0", "1", "2", "3", "4", "5"},
	}

	values, err := df.Unique("City")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// First-appearance order: NYC, LA, nil
	if len(values) != 3 {
		t.Fatalf("expected 3 unique values, got %d (%v)", len(values), values)
	}
	if values[0] != "NYC" || values[1] != "LA" || values[2] != nil {
		t.Errorf("expected [NYC, LA, nil], got %v", values)
	}

	t.Run("missing column errors", func(t *testing.T) {
		if _, err := df.Unique("Nope"); err == nil {
			t.Error("expected error for missing column")
		}
	})
}

func TestNUnique(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"City": mustSeries("NYC", "LA", "NYC", nil, "LA"),
		},
		ColumnOrder: []string{"City"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}
	n, err := df.NUnique("City")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Distinct non-null: NYC, LA -> 2
	if n != 2 {
		t.Errorf("expected 2, got %d", n)
	}
}

func dupTestDF() *dataframe.DataFrame {
	return &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries("x", "y", "x", "x", "z"),
			"B": mustSeries(1, 2, 1, 9, 3),
		},
		ColumnOrder: []string{"A", "B"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}
}

func TestDuplicated(t *testing.T) {
	t.Run("keep first on single column", func(t *testing.T) {
		mask, err := dupTestDF().Duplicated([]string{"A"}, "first")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// A: x,y,x,x,z -> dup of x at indices 2,3
		expected := []bool{false, false, true, true, false}
		for i := range expected {
			if mask[i] != expected[i] {
				t.Errorf("row %d: expected %v, got %v", i, expected[i], mask[i])
			}
		}
	})

	t.Run("keep last on single column", func(t *testing.T) {
		mask, _ := dupTestDF().Duplicated([]string{"A"}, "last")
		// x occurrences 0,2,3 -> mark 0,2 true, keep last (3)
		expected := []bool{true, false, true, false, false}
		for i := range expected {
			if mask[i] != expected[i] {
				t.Errorf("row %d: expected %v, got %v", i, expected[i], mask[i])
			}
		}
	})

	t.Run("keep none", func(t *testing.T) {
		mask, _ := dupTestDF().Duplicated([]string{"A"}, "none")
		// all x rows flagged
		expected := []bool{true, false, true, true, false}
		for i := range expected {
			if mask[i] != expected[i] {
				t.Errorf("row %d: expected %v, got %v", i, expected[i], mask[i])
			}
		}
	})

	t.Run("multi-column subset", func(t *testing.T) {
		mask, _ := dupTestDF().Duplicated([]string{"A", "B"}, "first")
		// (x,1) at 0 and 2 are dup; (x,9) at 3 is unique
		expected := []bool{false, false, true, false, false}
		for i := range expected {
			if mask[i] != expected[i] {
				t.Errorf("row %d: expected %v, got %v", i, expected[i], mask[i])
			}
		}
	})

	t.Run("invalid keep errors", func(t *testing.T) {
		if _, err := dupTestDF().Duplicated(nil, "middle"); err == nil {
			t.Error("expected error for invalid keep")
		}
	})
}

func TestDropDuplicates(t *testing.T) {
	t.Run("keep first", func(t *testing.T) {
		result, err := dupTestDF().DropDuplicates([]string{"A"}, "first")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Keep indices 0,1,4 -> x,y,z
		if result.Len() != 3 {
			t.Fatalf("expected 3 rows, got %d", result.Len())
		}
		if !strSliceEqual(result.Index, []string{"0", "1", "4"}) {
			t.Errorf("expected index [0 1 4], got %v", result.Index)
		}
	})

	t.Run("keep none removes all duplicated", func(t *testing.T) {
		result, _ := dupTestDF().DropDuplicates([]string{"A"}, "none")
		// x rows all removed -> y, z remain
		if result.Len() != 2 {
			t.Errorf("expected 2 rows, got %d", result.Len())
		}
	})
}
