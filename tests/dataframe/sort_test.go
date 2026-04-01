package dataframe_test

import (
	"fmt"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// TestSortValuesSingleColumn tests sorting by a single column.
func TestSortValuesSingleColumn(t *testing.T) {
	t.Run("sort int column ascending", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Name": mustSeries("Charlie", "Alice", "Bob"),
				"Age":  mustSeries(30, 20, 25),
			},
			ColumnOrder: []string{"Name", "Age"},
			Index:       []string{"0", "1", "2"},
		}

		result, err := df.SortValues(dataframe.SortOptions{By: []string{"Age"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Expected order: Alice(20), Bob(25), Charlie(30)
		expectedNames := []any{"Alice", "Bob", "Charlie"}
		expectedAges := []any{20, 25, 30}

		for i := 0; i < result.Len(); i++ {
			name, _ := result.Columns["Name"].At(i)
			if name != expectedNames[i] {
				t.Errorf("row %d Name: expected %v, got %v", i, expectedNames[i], name)
			}
			age, _ := result.Columns["Age"].At(i)
			if !valuesEqual(age, expectedAges[i]) {
				t.Errorf("row %d Age: expected %v, got %v", i, expectedAges[i], age)
			}
		}
	})

	t.Run("sort int column descending", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Name": mustSeries("Charlie", "Alice", "Bob"),
				"Age":  mustSeries(30, 20, 25),
			},
			ColumnOrder: []string{"Name", "Age"},
			Index:       []string{"0", "1", "2"},
		}

		result, err := df.SortValues(dataframe.SortOptions{
			By:        []string{"Age"},
			Ascending: []bool{false},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Expected order: Charlie(30), Bob(25), Alice(20)
		expectedNames := []any{"Charlie", "Bob", "Alice"}
		for i := 0; i < result.Len(); i++ {
			name, _ := result.Columns["Name"].At(i)
			if name != expectedNames[i] {
				t.Errorf("row %d Name: expected %v, got %v", i, expectedNames[i], name)
			}
		}
	})

	t.Run("sort string column ascending", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Name":  mustSeries("Charlie", "Alice", "Bob"),
				"Score": mustSeries(85.0, 95.0, 90.0),
			},
			ColumnOrder: []string{"Name", "Score"},
			Index:       []string{"0", "1", "2"},
		}

		result, err := df.SortValues(dataframe.SortOptions{By: []string{"Name"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Alphabetical: Alice, Bob, Charlie
		expectedNames := []any{"Alice", "Bob", "Charlie"}
		for i := 0; i < result.Len(); i++ {
			name, _ := result.Columns["Name"].At(i)
			if name != expectedNames[i] {
				t.Errorf("row %d Name: expected %v, got %v", i, expectedNames[i], name)
			}
		}
	})

	t.Run("sort float64 column ascending", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Name":  mustSeries("Charlie", "Alice", "Bob"),
				"Score": mustSeries(85.5, 95.2, 90.1),
			},
			ColumnOrder: []string{"Name", "Score"},
			Index:       []string{"0", "1", "2"},
		}

		result, err := df.SortValues(dataframe.SortOptions{By: []string{"Score"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Expected: 85.5, 90.1, 95.2
		expectedNames := []any{"Charlie", "Bob", "Alice"}
		for i := 0; i < result.Len(); i++ {
			name, _ := result.Columns["Name"].At(i)
			if name != expectedNames[i] {
				t.Errorf("row %d Name: expected %v, got %v", i, expectedNames[i], name)
			}
		}
	})
}

// TestSortValuesMultiColumn tests sorting by multiple columns.
func TestSortValuesMultiColumn(t *testing.T) {
	t.Run("sort by two columns ascending", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Dept":   mustSeries("Sales", "Engineering", "Sales", "Engineering"),
				"Name":   mustSeries("Bob", "Alice", "Alice", "Charlie"),
				"Salary": mustSeries(50000, 80000, 60000, 75000),
			},
			ColumnOrder: []string{"Dept", "Name", "Salary"},
			Index:       []string{"0", "1", "2", "3"},
		}

		result, err := df.SortValues(dataframe.SortOptions{
			By: []string{"Dept", "Name"},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Expected: Engineering-Alice, Engineering-Charlie, Sales-Alice, Sales-Bob
		expectedDepts := []any{"Engineering", "Engineering", "Sales", "Sales"}
		expectedNames := []any{"Alice", "Charlie", "Alice", "Bob"}

		for i := 0; i < result.Len(); i++ {
			dept, _ := result.Columns["Dept"].At(i)
			name, _ := result.Columns["Name"].At(i)
			if dept != expectedDepts[i] {
				t.Errorf("row %d Dept: expected %v, got %v", i, expectedDepts[i], dept)
			}
			if name != expectedNames[i] {
				t.Errorf("row %d Name: expected %v, got %v", i, expectedNames[i], name)
			}
		}
	})

	t.Run("sort by two columns mixed ascending/descending", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Dept":   mustSeries("Sales", "Engineering", "Sales", "Engineering"),
				"Name":   mustSeries("Bob", "Alice", "Alice", "Charlie"),
				"Salary": mustSeries(50000, 80000, 60000, 75000),
			},
			ColumnOrder: []string{"Dept", "Name", "Salary"},
			Index:       []string{"0", "1", "2", "3"},
		}

		result, err := df.SortValues(dataframe.SortOptions{
			By:        []string{"Dept", "Salary"},
			Ascending: []bool{true, false}, // Dept ascending, Salary descending
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Expected: Engineering(80000), Engineering(75000), Sales(60000), Sales(50000)
		expectedNames := []any{"Alice", "Charlie", "Alice", "Bob"}
		for i := 0; i < result.Len(); i++ {
			name, _ := result.Columns["Name"].At(i)
			if name != expectedNames[i] {
				t.Errorf("row %d Name: expected %v, got %v", i, expectedNames[i], name)
			}
		}
	})
}

// TestSortValuesNullHandling tests null value placement during sorting.
func TestSortValuesNullHandling(t *testing.T) {
	t.Run("nulls last (default)", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Name":  mustSeries("Charlie", nil, "Alice", "Bob"),
				"Score": mustSeries(85.0, 70.0, 95.0, nil),
			},
			ColumnOrder: []string{"Name", "Score"},
			Index:       []string{"0", "1", "2", "3"},
		}

		result, err := df.SortValues(dataframe.SortOptions{
			By: []string{"Score"},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Non-null scores sorted: 70.0, 85.0, 95.0, then null at end
		if !result.Columns["Score"].IsNull(result.Len() - 1) {
			t.Error("expected last row to have null Score")
		}

		// First 3 should be sorted
		score0, _ := result.Columns["Score"].At(0)
		score1, _ := result.Columns["Score"].At(1)
		score2, _ := result.Columns["Score"].At(2)

		if score0 != 70.0 || score1 != 85.0 || score2 != 95.0 {
			t.Errorf("expected sorted scores [70, 85, 95], got [%v, %v, %v]", score0, score1, score2)
		}
	})

	t.Run("nulls first", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Name":  mustSeries("Charlie", nil, "Alice", "Bob"),
				"Score": mustSeries(85.0, 70.0, 95.0, nil),
			},
			ColumnOrder: []string{"Name", "Score"},
			Index:       []string{"0", "1", "2", "3"},
		}

		result, err := df.SortValues(dataframe.SortOptions{
			By:         []string{"Score"},
			NaPosition: dataframe.NaFirst,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Null should be first
		if !result.Columns["Score"].IsNull(0) {
			t.Error("expected first row to have null Score")
		}

		// Remaining sorted ascending
		score1, _ := result.Columns["Score"].At(1)
		score2, _ := result.Columns["Score"].At(2)
		score3, _ := result.Columns["Score"].At(3)

		if score1 != 70.0 || score2 != 85.0 || score3 != 95.0 {
			t.Errorf("expected sorted scores [70, 85, 95], got [%v, %v, %v]", score1, score2, score3)
		}
	})

	t.Run("multiple nulls", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Value": mustSeries(3.0, nil, 1.0, nil, 2.0),
			},
			ColumnOrder: []string{"Value"},
			Index:       []string{"0", "1", "2", "3", "4"},
		}

		result, err := df.SortValues(dataframe.SortOptions{By: []string{"Value"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Expected: 1.0, 2.0, 3.0, null, null
		v0, _ := result.Columns["Value"].At(0)
		v1, _ := result.Columns["Value"].At(1)
		v2, _ := result.Columns["Value"].At(2)
		if v0 != 1.0 || v1 != 2.0 || v2 != 3.0 {
			t.Errorf("expected [1, 2, 3], got [%v, %v, %v]", v0, v1, v2)
		}
		if !result.Columns["Value"].IsNull(3) || !result.Columns["Value"].IsNull(4) {
			t.Error("expected last two rows to be null")
		}
	})
}

// TestSortValuesIndexPreservation tests that original index labels are preserved.
func TestSortValuesIndexPreservation(t *testing.T) {
	t.Run("preserve original index", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Value": mustSeries(30, 10, 20),
			},
			ColumnOrder: []string{"Value"},
			Index:       []string{"row_c", "row_a", "row_b"},
		}

		result, err := df.SortValues(dataframe.SortOptions{By: []string{"Value"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// After sorting by Value ascending: 10, 20, 30
		// Corresponding indices: row_a, row_b, row_c
		expectedIndex := []string{"row_a", "row_b", "row_c"}
		if !strSliceEqual(result.Index, expectedIndex) {
			t.Errorf("expected index %v, got %v", expectedIndex, result.Index)
		}
	})

	t.Run("ignore index", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Value": mustSeries(30, 10, 20),
			},
			ColumnOrder: []string{"Value"},
			Index:       []string{"row_c", "row_a", "row_b"},
		}

		result, err := df.SortValues(dataframe.SortOptions{
			By:          []string{"Value"},
			IgnoreIndex: true,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expectedIndex := []string{"0", "1", "2"}
		if !strSliceEqual(result.Index, expectedIndex) {
			t.Errorf("expected index %v, got %v", expectedIndex, result.Index)
		}
	})
}

// TestSortValuesInplace tests in-place sorting.
func TestSortValuesInplace(t *testing.T) {
	t.Run("sort inplace", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Name": mustSeries("Charlie", "Alice", "Bob"),
				"Age":  mustSeries(30, 20, 25),
			},
			ColumnOrder: []string{"Name", "Age"},
			Index:       []string{"0", "1", "2"},
		}

		result, err := df.SortValues(dataframe.SortOptions{
			By:      []string{"Age"},
			Inplace: true,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result != nil {
			t.Error("expected nil result for inplace sort")
		}

		// Verify df itself is sorted
		name0, _ := df.Columns["Name"].At(0)
		name1, _ := df.Columns["Name"].At(1)
		name2, _ := df.Columns["Name"].At(2)

		if name0 != "Alice" || name1 != "Bob" || name2 != "Charlie" {
			t.Errorf("expected inplace sorted names [Alice, Bob, Charlie], got [%v, %v, %v]", name0, name1, name2)
		}
	})
}

// TestSortValuesStableSort tests that the sort is stable (preserves relative order of equal elements).
func TestSortValuesStableSort(t *testing.T) {
	t.Run("stable sort preserves insertion order for ties", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Dept": mustSeries("A", "A", "A", "B"),
				"Name": mustSeries("First", "Second", "Third", "Only"),
			},
			ColumnOrder: []string{"Dept", "Name"},
			Index:       []string{"0", "1", "2", "3"},
		}

		result, err := df.SortValues(dataframe.SortOptions{By: []string{"Dept"}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// All "A" rows should maintain their original relative order
		name0, _ := result.Columns["Name"].At(0)
		name1, _ := result.Columns["Name"].At(1)
		name2, _ := result.Columns["Name"].At(2)

		if name0 != "First" || name1 != "Second" || name2 != "Third" {
			t.Errorf("stable sort violated: expected [First, Second, Third] for dept A, got [%v, %v, %v]",
				name0, name1, name2)
		}
	})
}

// TestSortValuesSingleAscendingForMultipleColumns tests broadcast of single ascending value.
func TestSortValuesSingleAscendingForMultipleColumns(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries("b", "a", "a", "b"),
			"B": mustSeries(2, 2, 1, 1),
		},
		ColumnOrder: []string{"A", "B"},
		Index:       []string{"0", "1", "2", "3"},
	}

	result, err := df.SortValues(dataframe.SortOptions{
		By:        []string{"A", "B"},
		Ascending: []bool{false}, // Single value, should apply to both
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Descending A: b, b, a, a. Descending B within: b-2, b-1, a-2, a-1
	expectedA := []any{"b", "b", "a", "a"}
	expectedB := []any{2, 1, 2, 1}
	for i := 0; i < result.Len(); i++ {
		a, _ := result.Columns["A"].At(i)
		b, _ := result.Columns["B"].At(i)
		if a != expectedA[i] {
			t.Errorf("row %d A: expected %v, got %v", i, expectedA[i], a)
		}
		if !valuesEqual(b, expectedB[i]) {
			t.Errorf("row %d B: expected %v, got %v", i, expectedB[i], b)
		}
	}
}

// TestSortValuesEmpty tests sorting an empty DataFrame.
func TestSortValuesEmpty(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{},
	}

	result, err := df.SortValues(dataframe.SortOptions{By: []string{"A"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Len() != 0 {
		t.Errorf("expected 0 rows, got %d", result.Len())
	}
}

// TestSortValuesBoolColumn tests sorting by a boolean column.
func TestSortValuesBoolColumn(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name":   mustSeries("Alice", "Bob", "Charlie", "Diana"),
			"Active": mustSeries(true, false, true, false),
		},
		ColumnOrder: []string{"Name", "Active"},
		Index:       []string{"0", "1", "2", "3"},
	}

	result, err := df.SortValues(dataframe.SortOptions{By: []string{"Active"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// false < true, so false rows come first
	active0, _ := result.Columns["Active"].At(0)
	active1, _ := result.Columns["Active"].At(1)
	active2, _ := result.Columns["Active"].At(2)
	active3, _ := result.Columns["Active"].At(3)

	if active0 != false || active1 != false || active2 != true || active3 != true {
		t.Errorf("expected [false, false, true, true], got [%v, %v, %v, %v]",
			active0, active1, active2, active3)
	}
}

// TestSortValuesDoesNotMutateOriginal tests that non-inplace sort doesn't modify the original.
func TestSortValuesDoesNotMutateOriginal(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Value": mustSeries(3, 1, 2),
		},
		ColumnOrder: []string{"Value"},
		Index:       []string{"0", "1", "2"},
	}

	_, err := df.SortValues(dataframe.SortOptions{By: []string{"Value"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Original should be unchanged
	v0, _ := df.Columns["Value"].At(0)
	v1, _ := df.Columns["Value"].At(1)
	v2, _ := df.Columns["Value"].At(2)

	if !valuesEqual(v0, 3) || !valuesEqual(v1, 1) || !valuesEqual(v2, 2) {
		t.Errorf("original DataFrame was mutated: got [%v, %v, %v]", v0, v1, v2)
	}
}

// TestSortValuesColumnOrder tests that column order is preserved after sort.
func TestSortValuesColumnOrder(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"C": mustSeries(3, 1),
			"A": mustSeries("x", "y"),
			"B": mustSeries(true, false),
		},
		ColumnOrder: []string{"C", "A", "B"},
		Index:       []string{"0", "1"},
	}

	result, err := df.SortValues(dataframe.SortOptions{By: []string{"C"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strSliceEqual(result.ColumnOrder, []string{"C", "A", "B"}) {
		t.Errorf("expected column order [C, A, B], got %v", result.ColumnOrder)
	}
}

// TestSortValuesErrors tests error conditions.
func TestSortValuesErrors(t *testing.T) {
	t.Run("nil DataFrame", func(t *testing.T) {
		var df *dataframe.DataFrame
		_, err := df.SortValues(dataframe.SortOptions{By: []string{"A"}})
		if err == nil {
			t.Error("expected error for nil DataFrame")
		}
	})

	t.Run("empty By", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"A": mustSeries(1)},
			ColumnOrder: []string{"A"},
			Index:       []string{"0"},
		}
		_, err := df.SortValues(dataframe.SortOptions{By: []string{}})
		if err == nil {
			t.Error("expected error for empty By")
		}
	})

	t.Run("nonexistent column", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"A": mustSeries(1)},
			ColumnOrder: []string{"A"},
			Index:       []string{"0"},
		}
		_, err := df.SortValues(dataframe.SortOptions{By: []string{"NonExistent"}})
		if err == nil {
			t.Error("expected error for nonexistent column")
		}
	})

	t.Run("ascending length mismatch", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"A": mustSeries(1), "B": mustSeries(2)},
			ColumnOrder: []string{"A", "B"},
			Index:       []string{"0"},
		}
		_, err := df.SortValues(dataframe.SortOptions{
			By:        []string{"A", "B"},
			Ascending: []bool{true, false, true}, // 3 != 2
		})
		if err == nil {
			t.Error("expected error for ascending length mismatch")
		}
	})

	t.Run("invalid NaPosition", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"A": mustSeries(1)},
			ColumnOrder: []string{"A"},
			Index:       []string{"0"},
		}
		_, err := df.SortValues(dataframe.SortOptions{
			By:         []string{"A"},
			NaPosition: "invalid",
		})
		if err == nil {
			t.Error("expected error for invalid NaPosition")
		}
	})
}

// TestSortIndex tests the SortIndex method.
func TestSortIndex(t *testing.T) {
	t.Run("sort index ascending", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Value": mustSeries(30, 10, 20),
			},
			ColumnOrder: []string{"Value"},
			Index:       []string{"c", "a", "b"},
		}

		result, err := df.SortIndex(true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expectedIndex := []string{"a", "b", "c"}
		if !strSliceEqual(result.Index, expectedIndex) {
			t.Errorf("expected index %v, got %v", expectedIndex, result.Index)
		}

		// Values should follow their index
		v0, _ := result.Columns["Value"].At(0)
		v1, _ := result.Columns["Value"].At(1)
		v2, _ := result.Columns["Value"].At(2)

		if !valuesEqual(v0, 10) || !valuesEqual(v1, 20) || !valuesEqual(v2, 30) {
			t.Errorf("expected values [10, 20, 30], got [%v, %v, %v]", v0, v1, v2)
		}
	})

	t.Run("sort index descending", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Value": mustSeries(30, 10, 20),
			},
			ColumnOrder: []string{"Value"},
			Index:       []string{"c", "a", "b"},
		}

		result, err := df.SortIndex(false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expectedIndex := []string{"c", "b", "a"}
		if !strSliceEqual(result.Index, expectedIndex) {
			t.Errorf("expected index %v, got %v", expectedIndex, result.Index)
		}
	})

	t.Run("sort index nil DataFrame", func(t *testing.T) {
		var df *dataframe.DataFrame
		_, err := df.SortIndex(true)
		if err == nil {
			t.Error("expected error for nil DataFrame")
		}
	})

	t.Run("sort index empty DataFrame", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"A": mustSeries()},
			ColumnOrder: []string{"A"},
			Index:       []string{},
		}

		result, err := df.SortIndex(true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Len() != 0 {
			t.Errorf("expected 0 rows, got %d", result.Len())
		}
	})
}

// TestSortValuesLargeDataset tests sorting with a larger dataset.
func TestSortValuesLargeDataset(t *testing.T) {
	n := 1000
	data := make([]any, n)
	for i := 0; i < n; i++ {
		data[i] = float64(n - i) // Reverse order: 1000, 999, ..., 1
	}

	index := make([]string, n)
	for i := 0; i < n; i++ {
		index[i] = fmt.Sprintf("%d", i)
	}

	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Value": mustSeries(data...),
		},
		ColumnOrder: []string{"Value"},
		Index:       index,
	}

	result, err := df.SortValues(dataframe.SortOptions{By: []string{"Value"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify sorted ascending
	for i := 0; i < result.Len()-1; i++ {
		v1, _ := result.Columns["Value"].At(i)
		v2, _ := result.Columns["Value"].At(i + 1)
		if v1.(float64) > v2.(float64) {
			t.Errorf("sort order violated at index %d: %v > %v", i, v1, v2)
			break
		}
	}
}

// valuesEqual compares values accounting for type differences (int vs int64 vs float64).
func valuesEqual(a, b any) bool {
	if a == b {
		return true
	}
	// Convert both to float64 for numeric comparison
	af, aOk := toFloat(a)
	bf, bOk := toFloat(b)
	if aOk && bOk {
		return af == bf
	}
	return false
}

func toFloat(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	default:
		return 0, false
	}
}
