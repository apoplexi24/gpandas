package dataframe_test

import (
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func filterTestDF() *dataframe.DataFrame {
	return &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name": mustSeries("Alice", "Bob", "Charlie", "Diana"),
			"Age":  mustSeries(30, 25, 35, 28),
			"City": mustSeries("NYC", "LA", "NYC", "SF"),
		},
		ColumnOrder: []string{"Name", "Age", "City"},
		Index:       []string{"0", "1", "2", "3"},
	}
}

func TestFilterComparisons(t *testing.T) {
	t.Run("greater than", func(t *testing.T) {
		df := filterTestDF()
		result, err := df.Filter("Age", dataframe.GreaterThan, 28).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 2 {
			t.Fatalf("expected 2 rows, got %d", result.Len())
		}
		names := []any{}
		for i := 0; i < result.Len(); i++ {
			n, _ := result.Columns["Name"].At(i)
			names = append(names, n)
		}
		if names[0] != "Alice" || names[1] != "Charlie" {
			t.Errorf("expected [Alice, Charlie], got %v", names)
		}
	})

	t.Run("greater than uses numeric cross-type", func(t *testing.T) {
		df := filterTestDF()
		// Age column is int; compare against float64 literal
		result, err := df.Filter("Age", dataframe.GreaterThanOrEqual, 30.0).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 2 {
			t.Errorf("expected 2 rows (Alice, Charlie), got %d", result.Len())
		}
	})

	t.Run("equals string", func(t *testing.T) {
		df := filterTestDF()
		result, err := df.Filter("City", dataframe.Equals, "NYC").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 2 {
			t.Errorf("expected 2 rows, got %d", result.Len())
		}
	})

	t.Run("not equals", func(t *testing.T) {
		df := filterTestDF()
		result, err := df.Filter("City", dataframe.NotEquals, "NYC").Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 2 {
			t.Errorf("expected 2 rows, got %d", result.Len())
		}
	})

	t.Run("less than and less than or equal", func(t *testing.T) {
		df := filterTestDF()
		lt, _ := df.Filter("Age", dataframe.LessThan, 30).Result()
		if lt.Len() != 2 { // Bob(25), Diana(28)
			t.Errorf("LessThan: expected 2 rows, got %d", lt.Len())
		}
		le, _ := df.Filter("Age", dataframe.LessThanOrEqual, 30).Result()
		if le.Len() != 3 { // Bob, Diana, Alice
			t.Errorf("LessThanOrEqual: expected 3 rows, got %d", le.Len())
		}
	})

	t.Run("preserves index labels", func(t *testing.T) {
		df := filterTestDF()
		result, _ := df.Filter("City", dataframe.Equals, "NYC").Result()
		expected := []string{"0", "2"}
		if !strSliceEqual(result.Index, expected) {
			t.Errorf("expected index %v, got %v", expected, result.Index)
		}
	})
}

func TestFilterNullsExcluded(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Score": mustSeries(10.0, nil, 30.0, nil, 5.0),
		},
		ColumnOrder: []string{"Score"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}
	result, err := df.Filter("Score", dataframe.GreaterThanOrEqual, 0.0).Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Nulls never match, so only the 3 non-null values remain
	if result.Len() != 3 {
		t.Errorf("expected 3 rows, got %d", result.Len())
	}
}

func TestFilterEmptyResult(t *testing.T) {
	df := filterTestDF()
	result, err := df.Filter("Age", dataframe.GreaterThan, 1000).Result()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Len() != 0 {
		t.Errorf("expected 0 rows, got %d", result.Len())
	}
}

func TestFilterErrors(t *testing.T) {
	t.Run("nil DataFrame", func(t *testing.T) {
		var df *dataframe.DataFrame
		if _, err := df.Filter("A", dataframe.Equals, 1).Result(); err == nil {
			t.Error("expected error for nil DataFrame")
		}
	})
	t.Run("missing column", func(t *testing.T) {
		df := filterTestDF()
		if _, err := df.Filter("Missing", dataframe.Equals, 1).Result(); err == nil {
			t.Error("expected error for missing column")
		}
	})
	t.Run("invalid operator", func(t *testing.T) {
		df := filterTestDF()
		if _, err := df.Filter("Age", dataframe.FilterOp("~="), 1).Result(); err == nil {
			t.Error("expected error for invalid operator")
		}
	})
}

func TestWhere(t *testing.T) {
	t.Run("multi-column predicate", func(t *testing.T) {
		df := filterTestDF()
		result, err := df.Where(func(row map[string]any) bool {
			age, _ := toFloat(row["Age"])
			return age >= 30 && row["City"] == "NYC"
		}).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Alice(30, NYC) and Charlie(35, NYC) qualify
		if result.Len() != 2 {
			t.Fatalf("expected 2 rows, got %d", result.Len())
		}
	})

	t.Run("null passed as nil", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(1, nil, 3),
			},
			ColumnOrder: []string{"A"},
			Index:       []string{"0", "1", "2"},
		}
		result, err := df.Where(func(row map[string]any) bool {
			return row["A"] == nil
		}).Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 1 {
			t.Errorf("expected 1 row, got %d", result.Len())
		}
	})

	t.Run("nil predicate errors", func(t *testing.T) {
		df := filterTestDF()
		if _, err := df.Where(nil).Result(); err == nil {
			t.Error("expected error for nil predicate")
		}
	})
}

func TestFilterChaining(t *testing.T) {
	t.Run("chained Filter then Filter", func(t *testing.T) {
		df := filterTestDF()
		result, err := df.
			Filter("City", dataframe.Equals, "NYC").
			Filter("Age", dataframe.GreaterThan, 30).
			Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// NYC rows are Alice(30) and Charlie(35); Age > 30 keeps only Charlie
		if result.Len() != 1 {
			t.Fatalf("expected 1 row, got %d", result.Len())
		}
		name, _ := result.Columns["Name"].At(0)
		if name != "Charlie" {
			t.Errorf("expected Charlie, got %v", name)
		}
	})

	t.Run("chained Filter then Where", func(t *testing.T) {
		df := filterTestDF()
		result, err := df.
			Filter("Age", dataframe.GreaterThanOrEqual, 28).
			Where(func(row map[string]any) bool {
				return row["City"] == "NYC"
			}).
			Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Age>=28: Alice(30,NYC), Charlie(35,NYC), Diana(28,SF) -> NYC keeps Alice, Charlie
		if result.Len() != 2 {
			t.Errorf("expected 2 rows, got %d", result.Len())
		}
	})

	t.Run("error propagates and short-circuits chain", func(t *testing.T) {
		df := filterTestDF()
		_, err := df.
			Filter("City", dataframe.Equals, "NYC").
			Filter("Missing", dataframe.Equals, 1). // errors here
			Filter("Age", dataframe.GreaterThan, 0). // should be skipped
			Result()
		if err == nil {
			t.Fatal("expected error from missing column to propagate")
		}
	})

	t.Run("Err accessor", func(t *testing.T) {
		df := filterTestDF()
		chain := df.Filter("Missing", dataframe.Equals, 1)
		if chain.Err() == nil {
			t.Error("expected Err() to report the failure")
		}
	})

	t.Run("MustResult panics on error", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected MustResult to panic on error")
			}
		}()
		df := filterTestDF()
		_ = df.Filter("Missing", dataframe.Equals, 1).MustResult()
	})
}
