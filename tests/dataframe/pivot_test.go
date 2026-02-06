package dataframe

import (
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// Helper function to create Series without error handling in tests
func mustSeries(s collection.Series, err error) collection.Series {
	if err != nil {
		panic(err)
	}
	return s
}

func TestPivotTable_Sum(t *testing.T) {
	// Create test DataFrame
	// A   B      C
	// foo one    1
	// foo one    2
	// foo two    3
	// bar one    4
	// bar two    5
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(collection.NewStringSeriesFromData([]string{"foo", "foo", "foo", "bar", "bar"}, nil)),
			"B": mustSeries(collection.NewStringSeriesFromData([]string{"one", "one", "two", "one", "two"}, nil)),
			"C": mustSeries(collection.NewFloat64SeriesFromData([]float64{1, 2, 3, 4, 5}, nil)),
		},
		ColumnOrder: []string{"A", "B", "C"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}

	// Pivot with sum aggregation
	pivot, err := df.PivotTable(dataframe.PivotTableOptions{
		Index:   []string{"A"},
		Columns: "B",
		Values:  []string{"C"},
		AggFunc: dataframe.AggSum,
	})
	if err != nil {
		t.Fatalf("PivotTable failed: %v", err)
	}

	// Expected result:
	// A   | one | two
	// bar | 4   | 5
	// foo | 3   | 3

	if pivot.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", pivot.Len())
	}

	// Check columns exist
	expectedCols := []string{"A", "one", "two"}
	for _, col := range expectedCols {
		if _, err := pivot.SelectCol(col); err != nil {
			t.Errorf("Expected column '%s' not found", col)
		}
	}

	// Check values (sorted by index key: bar, foo)
	aCol, _ := pivot.SelectCol("A")
	oneCol, _ := pivot.SelectCol("one")
	twoCol, _ := pivot.SelectCol("two")

	// Row 0: bar
	aVal0, _ := aCol.At(0)
	if aVal0 != "bar" {
		t.Errorf("Expected A[0]='bar', got '%v'", aVal0)
	}
	oneVal0, _ := oneCol.At(0)
	if oneVal0 != 4.0 {
		t.Errorf("Expected one[0]=4.0, got %v", oneVal0)
	}
	twoVal0, _ := twoCol.At(0)
	if twoVal0 != 5.0 {
		t.Errorf("Expected two[0]=5.0, got %v", twoVal0)
	}

	// Row 1: foo
	aVal1, _ := aCol.At(1)
	if aVal1 != "foo" {
		t.Errorf("Expected A[1]='foo', got '%v'", aVal1)
	}
	oneVal1, _ := oneCol.At(1)
	if oneVal1 != 3.0 { // 1 + 2 = 3
		t.Errorf("Expected one[1]=3.0, got %v", oneVal1)
	}
	twoVal1, _ := twoCol.At(1)
	if twoVal1 != 3.0 {
		t.Errorf("Expected two[1]=3.0, got %v", twoVal1)
	}
}

func TestPivotTable_Mean(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(collection.NewStringSeriesFromData([]string{"foo", "foo", "bar", "bar"}, nil)),
			"B": mustSeries(collection.NewStringSeriesFromData([]string{"one", "one", "one", "one"}, nil)),
			"C": mustSeries(collection.NewFloat64SeriesFromData([]float64{10, 20, 30, 40}, nil)),
		},
		ColumnOrder: []string{"A", "B", "C"},
		Index:       []string{"0", "1", "2", "3"},
	}

	pivot, err := df.PivotTable(dataframe.PivotTableOptions{
		Index:   []string{"A"},
		Columns: "B",
		Values:  []string{"C"},
		AggFunc: dataframe.AggMean,
	})
	if err != nil {
		t.Fatalf("PivotTable failed: %v", err)
	}

	// Expected: bar mean=35, foo mean=15
	oneCol, _ := pivot.SelectCol("one")

	// bar: (30+40)/2 = 35
	barVal, _ := oneCol.At(0)
	if barVal != 35.0 {
		t.Errorf("Expected bar mean=35.0, got %v", barVal)
	}

	// foo: (10+20)/2 = 15
	fooVal, _ := oneCol.At(1)
	if fooVal != 15.0 {
		t.Errorf("Expected foo mean=15.0, got %v", fooVal)
	}
}

func TestPivotTable_Count(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(collection.NewStringSeriesFromData([]string{"foo", "foo", "foo", "bar"}, nil)),
			"B": mustSeries(collection.NewStringSeriesFromData([]string{"one", "one", "two", "one"}, nil)),
			"C": mustSeries(collection.NewFloat64SeriesFromData([]float64{1, 2, 3, 4}, nil)),
		},
		ColumnOrder: []string{"A", "B", "C"},
		Index:       []string{"0", "1", "2", "3"},
	}

	pivot, err := df.PivotTable(dataframe.PivotTableOptions{
		Index:   []string{"A"},
		Columns: "B",
		Values:  []string{"C"},
		AggFunc: dataframe.AggCount,
	})
	if err != nil {
		t.Fatalf("PivotTable failed: %v", err)
	}

	oneCol, _ := pivot.SelectCol("one")
	twoCol, _ := pivot.SelectCol("two")

	// bar: one=1, two=null
	barOne, _ := oneCol.At(0)
	if barOne != 1.0 {
		t.Errorf("Expected bar/one count=1, got %v", barOne)
	}
	if !twoCol.IsNull(0) {
		barTwo, _ := twoCol.At(0)
		t.Errorf("Expected bar/two to be null, got %v", barTwo)
	}

	// foo: one=2, two=1
	fooOne, _ := oneCol.At(1)
	if fooOne != 2.0 {
		t.Errorf("Expected foo/one count=2, got %v", fooOne)
	}
	fooTwo, _ := twoCol.At(1)
	if fooTwo != 1.0 {
		t.Errorf("Expected foo/two count=1, got %v", fooTwo)
	}
}

func TestPivotTable_MultipleValues(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(collection.NewStringSeriesFromData([]string{"foo", "foo", "bar"}, nil)),
			"B": mustSeries(collection.NewStringSeriesFromData([]string{"one", "two", "one"}, nil)),
			"C": mustSeries(collection.NewFloat64SeriesFromData([]float64{1, 2, 3}, nil)),
			"D": mustSeries(collection.NewFloat64SeriesFromData([]float64{10, 20, 30}, nil)),
		},
		ColumnOrder: []string{"A", "B", "C", "D"},
		Index:       []string{"0", "1", "2"},
	}

	pivot, err := df.PivotTable(dataframe.PivotTableOptions{
		Index:   []string{"A"},
		Columns: "B",
		Values:  []string{"C", "D"},
		AggFunc: dataframe.AggSum,
	})
	if err != nil {
		t.Fatalf("PivotTable failed: %v", err)
	}

	// Check that we have columns C_one, C_two, D_one, D_two
	expectedCols := []string{"A", "C_one", "C_two", "D_one", "D_two"}
	for _, col := range expectedCols {
		if _, err := pivot.SelectCol(col); err != nil {
			t.Errorf("Expected column '%s' not found", col)
		}
	}

	// bar: C_one=3, D_one=30, C_two=null, D_two=null
	cOneCol, _ := pivot.SelectCol("C_one")
	dOneCol, _ := pivot.SelectCol("D_one")

	barCOne, _ := cOneCol.At(0)
	if barCOne != 3.0 {
		t.Errorf("Expected bar/C_one=3.0, got %v", barCOne)
	}
	barDOne, _ := dOneCol.At(0)
	if barDOne != 30.0 {
		t.Errorf("Expected bar/D_one=30.0, got %v", barDOne)
	}
}

func TestPivotTable_WithFillValue(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(collection.NewStringSeriesFromData([]string{"foo", "bar"}, nil)),
			"B": mustSeries(collection.NewStringSeriesFromData([]string{"one", "two"}, nil)),
			"C": mustSeries(collection.NewFloat64SeriesFromData([]float64{1, 2}, nil)),
		},
		ColumnOrder: []string{"A", "B", "C"},
		Index:       []string{"0", "1"},
	}

	pivot, err := df.PivotTable(dataframe.PivotTableOptions{
		Index:     []string{"A"},
		Columns:   "B",
		Values:    []string{"C"},
		AggFunc:   dataframe.AggSum,
		FillValue: 0.0,
	})
	if err != nil {
		t.Fatalf("PivotTable failed: %v", err)
	}

	oneCol, _ := pivot.SelectCol("one")
	twoCol, _ := pivot.SelectCol("two")

	// bar: one=0 (filled), two=2
	barOne, _ := oneCol.At(0)
	if barOne != 0.0 {
		t.Errorf("Expected bar/one=0.0 (filled), got %v", barOne)
	}

	// foo: one=1, two=0 (filled)
	fooTwo, _ := twoCol.At(1)
	if fooTwo != 0.0 {
		t.Errorf("Expected foo/two=0.0 (filled), got %v", fooTwo)
	}
}

func TestPivotTable_MinMax(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(collection.NewStringSeriesFromData([]string{"foo", "foo", "foo"}, nil)),
			"B": mustSeries(collection.NewStringSeriesFromData([]string{"one", "one", "one"}, nil)),
			"C": mustSeries(collection.NewFloat64SeriesFromData([]float64{5, 2, 8}, nil)),
		},
		ColumnOrder: []string{"A", "B", "C"},
		Index:       []string{"0", "1", "2"},
	}

	// Test Min
	pivotMin, err := df.PivotTable(dataframe.PivotTableOptions{
		Index:   []string{"A"},
		Columns: "B",
		Values:  []string{"C"},
		AggFunc: dataframe.AggMin,
	})
	if err != nil {
		t.Fatalf("PivotTable Min failed: %v", err)
	}

	oneColMin, _ := pivotMin.SelectCol("one")
	minVal, _ := oneColMin.At(0)
	if minVal != 2.0 {
		t.Errorf("Expected min=2.0, got %v", minVal)
	}

	// Test Max
	pivotMax, err := df.PivotTable(dataframe.PivotTableOptions{
		Index:   []string{"A"},
		Columns: "B",
		Values:  []string{"C"},
		AggFunc: dataframe.AggMax,
	})
	if err != nil {
		t.Fatalf("PivotTable Max failed: %v", err)
	}

	oneColMax, _ := pivotMax.SelectCol("one")
	maxVal, _ := oneColMax.At(0)
	if maxVal != 8.0 {
		t.Errorf("Expected max=8.0, got %v", maxVal)
	}
}

func TestPivotTable_ErrorCases(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(collection.NewStringSeriesFromData([]string{"foo"}, nil)),
			"B": mustSeries(collection.NewStringSeriesFromData([]string{"one"}, nil)),
			"C": mustSeries(collection.NewFloat64SeriesFromData([]float64{1}, nil)),
		},
		ColumnOrder: []string{"A", "B", "C"},
		Index:       []string{"0"},
	}

	// Missing Index
	_, err := df.PivotTable(dataframe.PivotTableOptions{
		Columns: "B",
		Values:  []string{"C"},
	})
	if err == nil {
		t.Error("Expected error for missing Index")
	}

	// Missing Columns
	_, err = df.PivotTable(dataframe.PivotTableOptions{
		Index:  []string{"A"},
		Values: []string{"C"},
	})
	if err == nil {
		t.Error("Expected error for missing Columns")
	}

	// Missing Values
	_, err = df.PivotTable(dataframe.PivotTableOptions{
		Index:   []string{"A"},
		Columns: "B",
	})
	if err == nil {
		t.Error("Expected error for missing Values")
	}

	// Non-existent column
	_, err = df.PivotTable(dataframe.PivotTableOptions{
		Index:   []string{"X"},
		Columns: "B",
		Values:  []string{"C"},
	})
	if err == nil {
		t.Error("Expected error for non-existent index column")
	}
}

// Melt Tests

func TestMelt_Basic(t *testing.T) {
	// Wide format:
	// Name  | Math | Science
	// Alice | 90   | 85
	// Bob   | 80   | 75
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name":    mustSeries(collection.NewStringSeriesFromData([]string{"Alice", "Bob"}, nil)),
			"Math":    mustSeries(collection.NewFloat64SeriesFromData([]float64{90, 80}, nil)),
			"Science": mustSeries(collection.NewFloat64SeriesFromData([]float64{85, 75}, nil)),
		},
		ColumnOrder: []string{"Name", "Math", "Science"},
		Index:       []string{"0", "1"},
	}

	melted, err := df.Melt(dataframe.MeltOptions{
		IdVars: []string{"Name"},
	})
	if err != nil {
		t.Fatalf("Melt failed: %v", err)
	}

	// Expected:
	// Name  | variable | value
	// Alice | Math     | 90
	// Alice | Science  | 85
	// Bob   | Math     | 80
	// Bob   | Science  | 75

	if melted.Len() != 4 {
		t.Errorf("Expected 4 rows, got %d", melted.Len())
	}

	// Check columns
	expectedCols := []string{"Name", "variable", "value"}
	for _, col := range expectedCols {
		if _, err := melted.SelectCol(col); err != nil {
			t.Errorf("Expected column '%s' not found", col)
		}
	}

	// Check first row
	nameCol, _ := melted.SelectCol("Name")
	varCol, _ := melted.SelectCol("variable")
	valCol, _ := melted.SelectCol("value")

	name0, _ := nameCol.At(0)
	if name0 != "Alice" {
		t.Errorf("Expected Name[0]='Alice', got '%v'", name0)
	}
	var0, _ := varCol.At(0)
	if var0 != "Math" {
		t.Errorf("Expected variable[0]='Math', got '%v'", var0)
	}
	val0, _ := valCol.At(0)
	if val0 != 90.0 {
		t.Errorf("Expected value[0]=90.0, got %v", val0)
	}
}

func TestMelt_WithValueVars(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name":    mustSeries(collection.NewStringSeriesFromData([]string{"Alice", "Bob"}, nil)),
			"Math":    mustSeries(collection.NewFloat64SeriesFromData([]float64{90, 80}, nil)),
			"Science": mustSeries(collection.NewFloat64SeriesFromData([]float64{85, 75}, nil)),
			"English": mustSeries(collection.NewFloat64SeriesFromData([]float64{88, 78}, nil)),
		},
		ColumnOrder: []string{"Name", "Math", "Science", "English"},
		Index:       []string{"0", "1"},
	}

	melted, err := df.Melt(dataframe.MeltOptions{
		IdVars:    []string{"Name"},
		ValueVars: []string{"Math", "Science"}, // Exclude English
	})
	if err != nil {
		t.Fatalf("Melt failed: %v", err)
	}

	// Should have 4 rows (2 names x 2 subjects)
	if melted.Len() != 4 {
		t.Errorf("Expected 4 rows, got %d", melted.Len())
	}

	// Verify no English values
	varCol, _ := melted.SelectCol("variable")
	for i := 0; i < melted.Len(); i++ {
		v, _ := varCol.At(i)
		if v == "English" {
			t.Error("English should not be in melted result")
		}
	}
}

func TestMelt_CustomNames(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name": mustSeries(collection.NewStringSeriesFromData([]string{"Alice"}, nil)),
			"Math": mustSeries(collection.NewFloat64SeriesFromData([]float64{90}, nil)),
		},
		ColumnOrder: []string{"Name", "Math"},
		Index:       []string{"0"},
	}

	melted, err := df.Melt(dataframe.MeltOptions{
		IdVars:    []string{"Name"},
		VarName:   "Subject",
		ValueName: "Score",
	})
	if err != nil {
		t.Fatalf("Melt failed: %v", err)
	}

	// Check custom column names exist
	if _, err := melted.SelectCol("Subject"); err != nil {
		t.Error("Expected 'Subject' column not found")
	}
	if _, err := melted.SelectCol("Score"); err != nil {
		t.Error("Expected 'Score' column not found")
	}

	// Old default names should not exist
	if _, err := melted.SelectCol("variable"); err == nil {
		t.Error("'variable' column should not exist with custom VarName")
	}
	if _, err := melted.SelectCol("value"); err == nil {
		t.Error("'value' column should not exist with custom ValueName")
	}
}

func TestMelt_NoIdVars(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(collection.NewFloat64SeriesFromData([]float64{1, 2}, nil)),
			"B": mustSeries(collection.NewFloat64SeriesFromData([]float64{3, 4}, nil)),
		},
		ColumnOrder: []string{"A", "B"},
		Index:       []string{"0", "1"},
	}

	melted, err := df.Melt(dataframe.MeltOptions{})
	if err != nil {
		t.Fatalf("Melt failed: %v", err)
	}

	// Should have 4 rows (2 rows x 2 columns)
	if melted.Len() != 4 {
		t.Errorf("Expected 4 rows, got %d", melted.Len())
	}

	// Only variable and value columns
	if len(melted.ColumnOrder) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(melted.ColumnOrder))
	}
}

func TestMelt_ErrorCases(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(collection.NewStringSeriesFromData([]string{"foo"}, nil)),
			"B": mustSeries(collection.NewFloat64SeriesFromData([]float64{1}, nil)),
		},
		ColumnOrder: []string{"A", "B"},
		Index:       []string{"0"},
	}

	// Non-existent id_vars column
	_, err := df.Melt(dataframe.MeltOptions{
		IdVars: []string{"X"},
	})
	if err == nil {
		t.Error("Expected error for non-existent id_vars column")
	}

	// Non-existent value_vars column
	_, err = df.Melt(dataframe.MeltOptions{
		IdVars:    []string{"A"},
		ValueVars: []string{"Y"},
	})
	if err == nil {
		t.Error("Expected error for non-existent value_vars column")
	}
}

func TestMelt_WithNulls(t *testing.T) {
	// Create DataFrame with null values
	mathSeries, _ := collection.NewFloat64SeriesFromData([]float64{90, 0}, []bool{false, true})
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name": mustSeries(collection.NewStringSeriesFromData([]string{"Alice", "Bob"}, nil)),
			"Math": mathSeries,
		},
		ColumnOrder: []string{"Name", "Math"},
		Index:       []string{"0", "1"},
	}

	melted, err := df.Melt(dataframe.MeltOptions{
		IdVars: []string{"Name"},
	})
	if err != nil {
		t.Fatalf("Melt failed: %v", err)
	}

	// Check that null is preserved
	valCol, _ := melted.SelectCol("value")
	if !valCol.IsNull(1) {
		t.Error("Expected null value to be preserved for Bob's Math score")
	}
}
