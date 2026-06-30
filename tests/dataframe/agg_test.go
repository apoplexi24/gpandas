package dataframe_test

import (
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func aggTestDF() *dataframe.DataFrame {
	return &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Dept":   mustSeries("Eng", "Sales", "Eng", "Sales", "Eng"),
			"Salary": mustSeries(100.0, 50.0, 200.0, 70.0, 150.0),
			"Name":   mustSeries("a", "b", "c", "d", "e"),
		},
		ColumnOrder: []string{"Dept", "Salary", "Name"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}
}

func TestGroupByAgg(t *testing.T) {
	t.Run("multiple funcs per column", func(t *testing.T) {
		gb, err := aggTestDF().GroupBy([]string{"Dept"}, 0)
		if err != nil {
			t.Fatalf("GroupBy failed: %v", err)
		}
		result, err := gb.Agg(map[string][]dataframe.AggFunc{
			"Salary": {dataframe.AggSum, dataframe.AggMean, dataframe.AggMax},
			"Name":   {dataframe.AggCount},
		})
		if err != nil {
			t.Fatalf("Agg failed: %v", err)
		}

		// Columns: Dept, Salary_sum, Salary_mean, Salary_max, Name_count
		expectedCols := []string{"Dept", "Salary_sum", "Salary_mean", "Salary_max", "Name_count"}
		if !strSliceEqual(result.ColumnOrder, expectedCols) {
			t.Fatalf("expected columns %v, got %v", expectedCols, result.ColumnOrder)
		}

		// Two groups: Eng, Sales (sorted)
		if result.Len() != 2 {
			t.Fatalf("expected 2 groups, got %d", result.Len())
		}

		// Eng: salaries 100,200,150 -> sum 450, mean 150, max 200, count 3
		dept0, _ := result.Columns["Dept"].At(0)
		sum0, _ := result.Columns["Salary_sum"].At(0)
		mean0, _ := result.Columns["Salary_mean"].At(0)
		max0, _ := result.Columns["Salary_max"].At(0)
		cnt0, _ := result.Columns["Name_count"].At(0)
		if dept0 != "Eng" {
			t.Errorf("expected first group Eng, got %v", dept0)
		}
		if !valuesEqual(sum0, 450.0) || !valuesEqual(mean0, 150.0) || !valuesEqual(max0, 200.0) {
			t.Errorf("Eng aggregates wrong: sum=%v mean=%v max=%v", sum0, mean0, max0)
		}
		if !valuesEqual(cnt0, 3) {
			t.Errorf("Eng count expected 3, got %v", cnt0)
		}
	})

	t.Run("count is int64", func(t *testing.T) {
		gb, _ := aggTestDF().GroupBy([]string{"Dept"}, 0)
		result, _ := gb.Agg(map[string][]dataframe.AggFunc{"Name": {dataframe.AggCount}})
		if result.Columns["Name_count"].DType().String() != "int64" {
			t.Errorf("expected int64 count column, got %v", result.Columns["Name_count"].DType())
		}
	})

	t.Run("errors", func(t *testing.T) {
		gb, _ := aggTestDF().GroupBy([]string{"Dept"}, 0)
		if _, err := gb.Agg(map[string][]dataframe.AggFunc{}); err == nil {
			t.Error("expected error for empty spec")
		}
		if _, err := gb.Agg(map[string][]dataframe.AggFunc{"Nope": {dataframe.AggSum}}); err == nil {
			t.Error("expected error for missing column")
		}
	})
}

func TestStackUnstackRoundTrip(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Math":    mustSeries(90.0, 80.0),
			"Science": mustSeries(85.0, 75.0),
		},
		ColumnOrder: []string{"Math", "Science"},
		Index:       []string{"Alice", "Bob"},
	}

	long, err := df.Stack()
	if err != nil {
		t.Fatalf("Stack failed: %v", err)
	}
	// 2 rows x 2 cols = 4 stacked rows
	if long.Len() != 4 {
		t.Fatalf("expected 4 stacked rows, got %d", long.Len())
	}
	if !strSliceEqual(long.ColumnOrder, []string{"index", "variable", "value"}) {
		t.Fatalf("unexpected stacked columns: %v", long.ColumnOrder)
	}

	wide, err := long.Unstack()
	if err != nil {
		t.Fatalf("Unstack failed: %v", err)
	}
	// Columns sorted: Math, Science
	if !strSliceEqual(wide.ColumnOrder, []string{"Math", "Science"}) {
		t.Fatalf("unexpected unstacked columns: %v", wide.ColumnOrder)
	}
	if !strSliceEqual(wide.Index, []string{"Alice", "Bob"}) {
		t.Errorf("expected index [Alice Bob], got %v", wide.Index)
	}
	// Alice/Math == 90
	v, _ := wide.Columns["Math"].At(0)
	if !valuesEqual(v, 90.0) {
		t.Errorf("expected Alice Math 90, got %v", v)
	}
}

func TestStackDropsNulls(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1.0, nil),
			"B": mustSeries(2.0, 3.0),
		},
		ColumnOrder: []string{"A", "B"},
		Index:       []string{"0", "1"},
	}
	long, _ := df.Stack()
	// 4 cells minus 1 null = 3 rows
	if long.Len() != 3 {
		t.Errorf("expected 3 rows after dropping null, got %d", long.Len())
	}
}

func TestSetMultiIndex(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Country": mustSeries("USA", "USA", "UK"),
			"City":    mustSeries("NYC", "LA", "London"),
			"Pop":     mustSeries(1, 2, 3),
		},
		ColumnOrder: []string{"Country", "City", "Pop"},
		Index:       []string{"0", "1", "2"},
	}
	result, err := df.SetMultiIndex([]string{"Country", "City"})
	if err != nil {
		t.Fatalf("SetMultiIndex failed: %v", err)
	}
	expected := []string{"USA_NYC", "USA_LA", "UK_London"}
	if !strSliceEqual(result.Index, expected) {
		t.Errorf("expected index %v, got %v", expected, result.Index)
	}

	t.Run("missing column errors", func(t *testing.T) {
		if _, err := df.SetMultiIndex([]string{"Nope"}); err == nil {
			t.Error("expected error for missing column")
		}
	})
}
