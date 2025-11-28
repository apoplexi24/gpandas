package dataframe_test

import (
	"reflect"
	"testing"

	"github.com/apoplexi24/gpandas"
	"github.com/apoplexi24/gpandas/dataframe"
)

// Helper function to create a test DataFrame
func createTestDataFrame(t *testing.T) *dataframe.DataFrame {
	pd := gpandas.GoPandas{}
	columns := []string{"name", "age", "city"}
	data := []gpandas.Column{
		{"Alice", "Bob", "Charlie", "David"},
		{"25", "30", "35", "28"},
		{"NYC", "LA", "SF", "Seattle"},
	}
	columnTypes := map[string]any{
		"name": gpandas.StringCol{},
		"age":  gpandas.StringCol{},
		"city": gpandas.StringCol{},
	}
	df, err := pd.DataFrame(columns, data, columnTypes)
	if err != nil {
		t.Fatalf("Failed to create test DataFrame: %v", err)
	}
	return df
}

// TestSelect tests column selection functionality
func TestSelect(t *testing.T) {
	df := createTestDataFrame(t)

	tests := []struct {
		name        string
		columns     []string
		expectError bool
		expectCols  []string
	}{
		{
			name:        "select single column",
			columns:     []string{"name"},
			expectError: false,
			expectCols:  []string{"name"},
		},
		{
			name:        "select multiple columns",
			columns:     []string{"name", "city"},
			expectError: false,
			expectCols:  []string{"name", "city"},
		},
		{
			name:        "select all columns",
			columns:     []string{"name", "age", "city"},
			expectError: false,
			expectCols:  []string{"name", "age", "city"},
		},
		{
			name:        "select non-existent column",
			columns:     []string{"salary"},
			expectError: true,
		},
		{
			name:        "select no columns",
			columns:     []string{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := df.Select(tt.columns...)
			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if !reflect.DeepEqual(result.ColumnOrder, tt.expectCols) {
				t.Errorf("expected columns %v, got %v", tt.expectCols, result.ColumnOrder)
			}

			// Verify row count matches original
			if len(result.Index) != len(df.Index) {
				t.Errorf("expected %d rows, got %d", len(df.Index), len(result.Index))
			}
		})
	}
}

// TestSelectCol tests single column selection as Series
func TestSelectCol(t *testing.T) {
	df := createTestDataFrame(t)

	t.Run("select existing column", func(t *testing.T) {
		series, err := df.SelectCol("name")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if series.Len() != 4 {
			t.Errorf("expected 4 rows, got %d", series.Len())
		}

		val, _ := series.At(0)
		if val != "Alice" {
			t.Errorf("expected 'Alice', got %v", val)
		}
	})

	t.Run("select non-existent column", func(t *testing.T) {
		_, err := df.SelectCol("salary")
		if err == nil {
			t.Error("expected error but got none")
		}
	})
}

// TestSetIndexAndResetIndex tests index management
func TestSetIndexAndResetIndex(t *testing.T) {
	df := createTestDataFrame(t)

	t.Run("set custom index", func(t *testing.T) {
		customIndex := []string{"row1", "row2", "row3", "row4"}
		err := df.SetIndex(customIndex)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if !reflect.DeepEqual(df.Index, customIndex) {
			t.Errorf("expected index %v, got %v", customIndex, df.Index)
		}
	})

	t.Run("set invalid index length", func(t *testing.T) {
		invalidIndex := []string{"row1", "row2"} // only 2 elements
		err := df.SetIndex(invalidIndex)
		if err == nil {
			t.Error("expected error for invalid index length but got none")
		}
	})

	t.Run("reset index", func(t *testing.T) {
		df.ResetIndex()
		expectedIndex := []string{"0", "1", "2", "3"}
		if !reflect.DeepEqual(df.Index, expectedIndex) {
			t.Errorf("expected index %v, got %v", expectedIndex, df.Index)
		}
	})
}

// TestLocAt tests Loc.At() for single value access
func TestLocAt(t *testing.T) {
	df := createTestDataFrame(t)
	err := df.SetIndex([]string{"A", "B", "C", "D"})
	if err != nil {
		t.Fatalf("Failed to set index: %v", err)
	}

	t.Run("valid access", func(t *testing.T) {
		val, err := df.Loc().At("A", "name")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		if val != "Alice" {
			t.Errorf("expected 'Alice', got %v", val)
		}
	})

	t.Run("invalid row label", func(t *testing.T) {
		_, err := df.Loc().At("Z", "name")
		if err == nil {
			t.Error("expected error for invalid row label but got none")
		}
	})

	t.Run("invalid column name", func(t *testing.T) {
		_, err := df.Loc().At("A", "salary")
		if err == nil {
			t.Error("expected error for invalid column name but got none")
		}
	})
}

// TestLocRow tests Loc.Row() for single row access
func TestLocRow(t *testing.T) {
	df := createTestDataFrame(t)
	err := df.SetIndex([]string{"A", "B", "C", "D"})
	if err != nil {
		t.Fatalf("Failed to set index: %v", err)
	}

	t.Run("valid row", func(t *testing.T) {
		row, err := df.Loc().Row("B")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if len(row.Index) != 1 || row.Index[0] != "B" {
			t.Errorf("expected index ['B'], got %v", row.Index)
		}

		nameVal, _ := row.Columns["name"].At(0)
		if nameVal != "Bob" {
			t.Errorf("expected 'Bob', got %v", nameVal)
		}

		ageVal, _ := row.Columns["age"].At(0)
		if ageVal != "30" {
			t.Errorf("expected '30', got %v", ageVal)
		}
	})

	t.Run("invalid row label", func(t *testing.T) {
		_, err := df.Loc().Row("Z")
		if err == nil {
			t.Error("expected error for invalid row label but got none")
		}
	})
}

// TestLocRows tests Loc.Rows() for multiple row access
func TestLocRows(t *testing.T) {
	df := createTestDataFrame(t)
	err := df.SetIndex([]string{"A", "B", "C", "D"})
	if err != nil {
		t.Fatalf("Failed to set index: %v", err)
	}

	t.Run("valid rows", func(t *testing.T) {
		rows, err := df.Loc().Rows([]string{"A", "C"})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		expectedIndex := []string{"A", "C"}
		if !reflect.DeepEqual(rows.Index, expectedIndex) {
			t.Errorf("expected index %v, got %v", expectedIndex, rows.Index)
		}

		if rows.Columns["name"].Len() != 2 {
			t.Errorf("expected 2 rows, got %d", rows.Columns["name"].Len())
		}

		val0, _ := rows.Columns["name"].At(0)
		val1, _ := rows.Columns["name"].At(1)
		if val0 != "Alice" || val1 != "Charlie" {
			t.Errorf("expected ['Alice', 'Charlie'], got [%v, %v]", val0, val1)
		}
	})

	t.Run("invalid row label", func(t *testing.T) {
		_, err := df.Loc().Rows([]string{"A", "Z"})
		if err == nil {
			t.Error("expected error for invalid row label but got none")
		}
	})
}

// TestLocCols tests Loc.Col() and Loc.Cols()
func TestLocCols(t *testing.T) {
	df := createTestDataFrame(t)

	t.Run("single column", func(t *testing.T) {
		col, err := df.Loc().Col("name")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if col.Len() != 4 {
			t.Errorf("expected 4 rows, got %d", col.Len())
		}
	})

	t.Run("multiple columns", func(t *testing.T) {
		cols, err := df.Loc().Cols([]string{"name", "city"})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		expectedCols := []string{"name", "city"}
		if !reflect.DeepEqual(cols.ColumnOrder, expectedCols) {
			t.Errorf("expected columns %v, got %v", expectedCols, cols.ColumnOrder)
		}
	})
}

// TestILocAt tests iLoc.At() for single value access
func TestILocAt(t *testing.T) {
	df := createTestDataFrame(t)

	t.Run("valid access", func(t *testing.T) {
		val, err := df.ILoc().At(0, 0)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}
		if val != "Alice" {
			t.Errorf("expected 'Alice', got %v", val)
		}
	})

	t.Run("invalid row position", func(t *testing.T) {
		_, err := df.ILoc().At(10, 0)
		if err == nil {
			t.Error("expected error for invalid row position but got none")
		}
	})

	t.Run("invalid column position", func(t *testing.T) {
		_, err := df.ILoc().At(0, 10)
		if err == nil {
			t.Error("expected error for invalid column position but got none")
		}
	})

	t.Run("negative row position", func(t *testing.T) {
		_, err := df.ILoc().At(-1, 0)
		if err == nil {
			t.Error("expected error for negative row position but got none")
		}
	})
}

// TestILocRow tests iLoc.Row() for single row access
func TestILocRow(t *testing.T) {
	df := createTestDataFrame(t)

	t.Run("valid row", func(t *testing.T) {
		row, err := df.ILoc().Row(1)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if len(row.Index) != 1 || row.Index[0] != "1" {
			t.Errorf("expected index ['1'], got %v", row.Index)
		}

		nameVal, _ := row.Columns["name"].At(0)
		if nameVal != "Bob" {
			t.Errorf("expected 'Bob', got %v", nameVal)
		}
	})

	t.Run("invalid row position", func(t *testing.T) {
		_, err := df.ILoc().Row(10)
		if err == nil {
			t.Error("expected error for invalid row position but got none")
		}
	})
}

// TestILocRows tests iLoc.Rows() for multiple row access
func TestILocRows(t *testing.T) {
	df := createTestDataFrame(t)

	t.Run("valid rows", func(t *testing.T) {
		rows, err := df.ILoc().Rows([]int{0, 2})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		expectedIndex := []string{"0", "2"}
		if !reflect.DeepEqual(rows.Index, expectedIndex) {
			t.Errorf("expected index %v, got %v", expectedIndex, rows.Index)
		}

		if rows.Columns["name"].Len() != 2 {
			t.Errorf("expected 2 rows, got %d", rows.Columns["name"].Len())
		}

		val0, _ := rows.Columns["name"].At(0)
		val1, _ := rows.Columns["name"].At(1)
		if val0 != "Alice" || val1 != "Charlie" {
			t.Errorf("expected ['Alice', 'Charlie'], got [%v, %v]", val0, val1)
		}
	})

	t.Run("invalid row position", func(t *testing.T) {
		_, err := df.ILoc().Rows([]int{0, 10})
		if err == nil {
			t.Error("expected error for invalid row position but got none")
		}
	})
}

// TestILocRange tests iLoc.Range() for row range access
func TestILocRange(t *testing.T) {
	df := createTestDataFrame(t)

	t.Run("valid range", func(t *testing.T) {
		rangeDF, err := df.ILoc().Range(1, 3)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if rangeDF.Columns["name"].Len() != 2 {
			t.Errorf("expected 2 rows, got %d", rangeDF.Columns["name"].Len())
		}

		val0, _ := rangeDF.Columns["name"].At(0)
		val1, _ := rangeDF.Columns["name"].At(1)
		if val0 != "Bob" || val1 != "Charlie" {
			t.Errorf("expected ['Bob', 'Charlie'], got [%v, %v]", val0, val1)
		}
	})

	t.Run("empty range", func(t *testing.T) {
		rangeDF, err := df.ILoc().Range(2, 2)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if rangeDF.Columns["name"].Len() != 0 {
			t.Errorf("expected 0 rows, got %d", rangeDF.Columns["name"].Len())
		}
	})

	t.Run("invalid range - start > end", func(t *testing.T) {
		_, err := df.ILoc().Range(3, 1)
		if err == nil {
			t.Error("expected error for invalid range but got none")
		}
	})

	t.Run("invalid range - out of bounds", func(t *testing.T) {
		_, err := df.ILoc().Range(0, 10)
		if err == nil {
			t.Error("expected error for out of bounds range but got none")
		}
	})
}

// TestILocCols tests iLoc.Col() and iLoc.Cols()
func TestILocCols(t *testing.T) {
	df := createTestDataFrame(t)

	t.Run("single column", func(t *testing.T) {
		col, err := df.ILoc().Col(0)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		if col.Len() != 4 {
			t.Errorf("expected 4 rows, got %d", col.Len())
		}

		val, _ := col.At(0)
		if val != "Alice" {
			t.Errorf("expected 'Alice', got %v", val)
		}
	})

	t.Run("multiple columns", func(t *testing.T) {
		cols, err := df.ILoc().Cols([]int{0, 2})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		expectedCols := []string{"name", "city"}
		if !reflect.DeepEqual(cols.ColumnOrder, expectedCols) {
			t.Errorf("expected columns %v, got %v", expectedCols, cols.ColumnOrder)
		}
	})

	t.Run("invalid column position", func(t *testing.T) {
		_, err := df.ILoc().Col(10)
		if err == nil {
			t.Error("expected error for invalid column position but got none")
		}
	})
}

// TestIndexPreservation tests that index is preserved through operations
func TestIndexPreservation(t *testing.T) {
	df := createTestDataFrame(t)
	customIndex := []string{"row1", "row2", "row3", "row4"}
	df.SetIndex(customIndex)

	t.Run("select preserves index", func(t *testing.T) {
		selected, _ := df.Select("name", "age")
		if !reflect.DeepEqual(selected.Index, customIndex) {
			t.Errorf("select did not preserve index: expected %v, got %v", customIndex, selected.Index)
		}
	})

	t.Run("loc cols preserves index", func(t *testing.T) {
		cols, _ := df.Loc().Cols([]string{"name"})
		if !reflect.DeepEqual(cols.Index, customIndex) {
			t.Errorf("Loc.Cols did not preserve index: expected %v, got %v", customIndex, cols.Index)
		}
	})

	t.Run("iloc cols preserves index", func(t *testing.T) {
		cols, _ := df.ILoc().Cols([]int{0})
		if !reflect.DeepEqual(cols.Index, customIndex) {
			t.Errorf("iLoc.Cols did not preserve index: expected %v, got %v", customIndex, cols.Index)
		}
	})
}
