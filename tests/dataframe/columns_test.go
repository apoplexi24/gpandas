package dataframe_test

import (
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func columnsTestDF() *dataframe.DataFrame {
	return &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name":   mustSeries("Alice", "Bob", "Charlie"),
			"Salary": mustSeries(100.0, 200.0, 300.0),
		},
		ColumnOrder: []string{"Name", "Salary"},
		Index:       []string{"0", "1", "2"},
	}
}

func TestAssign(t *testing.T) {
	t.Run("add new column", func(t *testing.T) {
		df := columnsTestDF()
		col, _ := collection.NewInt64SeriesFromData([]int64{10, 20, 30}, nil)
		if err := df.Assign("Age", col); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strSliceEqual(df.ColumnOrder, []string{"Name", "Salary", "Age"}) {
			t.Errorf("expected column appended, got %v", df.ColumnOrder)
		}
		v, _ := df.Columns["Age"].At(1)
		if !valuesEqual(v, 20) {
			t.Errorf("expected 20, got %v", v)
		}
	})

	t.Run("replace existing column keeps position", func(t *testing.T) {
		df := columnsTestDF()
		col, _ := collection.NewFloat64SeriesFromData([]float64{1, 2, 3}, nil)
		if err := df.Assign("Salary", col); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strSliceEqual(df.ColumnOrder, []string{"Name", "Salary"}) {
			t.Errorf("column order should be unchanged, got %v", df.ColumnOrder)
		}
		v, _ := df.Columns["Salary"].At(0)
		if !valuesEqual(v, 1.0) {
			t.Errorf("expected replaced value 1.0, got %v", v)
		}
	})

	t.Run("length mismatch errors", func(t *testing.T) {
		df := columnsTestDF()
		col, _ := collection.NewInt64SeriesFromData([]int64{1, 2}, nil)
		if err := df.Assign("Age", col); err == nil {
			t.Error("expected length mismatch error")
		}
	})

	t.Run("assign to empty DataFrame sets index", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{},
			ColumnOrder: []string{},
			Index:       []string{},
		}
		col, _ := collection.NewInt64SeriesFromData([]int64{1, 2, 3}, nil)
		if err := df.Assign("X", col); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strSliceEqual(df.Index, []string{"0", "1", "2"}) {
			t.Errorf("expected default index, got %v", df.Index)
		}
	})

	t.Run("nil series errors", func(t *testing.T) {
		df := columnsTestDF()
		if err := df.Assign("X", nil); err == nil {
			t.Error("expected error for nil series")
		}
	})
}

func TestAssignFunc(t *testing.T) {
	t.Run("derive column from rows", func(t *testing.T) {
		df := columnsTestDF()
		err := df.AssignFunc("Tax", func(row map[string]any) any {
			return row["Salary"].(float64) * 0.3
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strSliceEqual(df.ColumnOrder, []string{"Name", "Salary", "Tax"}) {
			t.Errorf("expected Tax appended, got %v", df.ColumnOrder)
		}
		v, _ := df.Columns["Tax"].At(0)
		if !valuesEqual(v, 30.0) {
			t.Errorf("expected 30.0, got %v", v)
		}
	})

	t.Run("nil fn errors", func(t *testing.T) {
		df := columnsTestDF()
		if err := df.AssignFunc("X", nil); err == nil {
			t.Error("expected error for nil fn")
		}
	})
}

func TestInsert(t *testing.T) {
	t.Run("insert at position", func(t *testing.T) {
		df := columnsTestDF()
		col, _ := collection.NewInt64SeriesFromData([]int64{1, 2, 3}, nil)
		if err := df.Insert(0, "ID", col); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strSliceEqual(df.ColumnOrder, []string{"ID", "Name", "Salary"}) {
			t.Errorf("expected ID first, got %v", df.ColumnOrder)
		}
	})

	t.Run("insert at end", func(t *testing.T) {
		df := columnsTestDF()
		col, _ := collection.NewInt64SeriesFromData([]int64{1, 2, 3}, nil)
		if err := df.Insert(2, "ID", col); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strSliceEqual(df.ColumnOrder, []string{"Name", "Salary", "ID"}) {
			t.Errorf("expected ID last, got %v", df.ColumnOrder)
		}
	})

	t.Run("duplicate name errors", func(t *testing.T) {
		df := columnsTestDF()
		col, _ := collection.NewFloat64SeriesFromData([]float64{1, 2, 3}, nil)
		if err := df.Insert(0, "Name", col); err == nil {
			t.Error("expected error for duplicate column name")
		}
	})

	t.Run("out of range loc errors", func(t *testing.T) {
		df := columnsTestDF()
		col, _ := collection.NewFloat64SeriesFromData([]float64{1, 2, 3}, nil)
		if err := df.Insert(99, "X", col); err == nil {
			t.Error("expected error for out-of-range loc")
		}
	})
}
