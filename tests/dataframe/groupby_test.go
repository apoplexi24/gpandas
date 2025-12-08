package dataframe

import (
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func must(s collection.Series, err error) collection.Series {
	if err != nil {
		panic(err)
	}
	return s
}

func TestGroupBy_Mean(t *testing.T) {
	// Create a DataFrame
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": must(collection.NewStringSeriesFromData([]string{"foo", "bar", "foo", "bar", "foo", "bar", "foo", "foo"}, nil)),
			"B": must(collection.NewStringSeriesFromData([]string{"one", "one", "two", "three", "two", "two", "one", "three"}, nil)),
			"C": must(collection.NewFloat64SeriesFromData([]float64{1, 2, 3, 4, 5, 6, 7, 8}, nil)),
			"D": must(collection.NewInt64SeriesFromData([]int64{10, 20, 30, 40, 50, 60, 70, 80}, nil)),
		},
		ColumnOrder: []string{"A", "B", "C", "D"},
		Index:       []string{"0", "1", "2", "3", "4", "5", "6", "7"},
	}

	// Group by "A"
	gb, err := df.GroupBy([]string{"A"}, 0)
	if err != nil {
		t.Fatalf("GroupBy failed: %v", err)
	}

	// Calculate Mean
	meanDF, err := gb.Mean()
	if err != nil {
		t.Fatalf("Mean failed: %v", err)
	}

	// Verify results
	// Group "bar": rows 1, 3, 5 -> C: (2+4+6)/3 = 4, D: (20+40+60)/3 = 40
	// Group "foo": rows 0, 2, 4, 6, 7 -> C: (1+3+5+7+8)/5 = 4.8, D: (10+30+50+70+80)/5 = 48

	// Check dimensions
	if meanDF.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", meanDF.Len())
	}

	// Check "A" column (group keys)
	aCol, _ := meanDF.SelectCol("A")
	// Order should be sorted: bar, foo
	val0, _ := aCol.At(0)
	if val0 != "bar" {
		t.Errorf("Expected group 'bar' at index 0, got %v", val0)
	}
	val1, _ := aCol.At(1) // foo
	if val1 != "foo" {
		t.Errorf("Expected group 'foo' at index 1, got %v", val1)
	}

	// Check "C" column
	cCol, _ := meanDF.SelectCol("C")
	valC0, _ := cCol.At(0) // bar
	if valC0 != 4.0 {
		t.Errorf("Expected mean C for bar to be 4.0, got %v", valC0)
	}
	valC1, _ := cCol.At(1) // foo
	if valC1 != 4.8 {
		t.Errorf("Expected mean C for foo to be 4.8, got %v", valC1)
	}
}

func TestGroupBy_Sum(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": must(collection.NewStringSeriesFromData([]string{"foo", "bar", "foo"}, nil)),
			"C": must(collection.NewFloat64SeriesFromData([]float64{1, 2, 3}, nil)),
		},
		ColumnOrder: []string{"A", "C"},
		Index:       []string{"0", "1", "2"},
	}

	gb, err := df.GroupBy([]string{"A"}, 0)
	if err != nil {
		t.Fatalf("GroupBy failed: %v", err)
	}

	sumDF, err := gb.Sum()
	if err != nil {
		t.Fatalf("Sum failed: %v", err)
	}

	// bar: 2
	// foo: 1+3=4
	cCol, _ := sumDF.SelectCol("C")
	valC0, _ := cCol.At(0) // bar
	if valC0 != 2.0 {
		t.Errorf("Expected sum C for bar to be 2.0, got %v", valC0)
	}
	valC1, _ := cCol.At(1) // foo
	if valC1 != 4.0 {
		t.Errorf("Expected sum C for foo to be 4.0, got %v", valC1)
	}
}

func TestGroupBy_Apply(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": must(collection.NewStringSeriesFromData([]string{"foo", "bar", "foo"}, nil)),
			"C": must(collection.NewFloat64SeriesFromData([]float64{1, 2, 3}, nil)),
		},
		ColumnOrder: []string{"A", "C"},
		Index:       []string{"0", "1", "2"},
	}

	gb, err := df.GroupBy([]string{"A"}, 0)
	if err != nil {
		t.Fatalf("GroupBy failed: %v", err)
	}

	// Apply function: return head(1) of each group
	resDF, err := gb.Apply(func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return d.Head(1), nil
	})
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}

	if resDF.Len() != 2 {
		t.Errorf("Expected 2 rows, got %d", resDF.Len())
	}

	// Check content
	// bar -> row 1 (val 2)
	// foo -> row 0 (val 1)
	cCol, _ := resDF.SelectCol("C")
	val0, _ := cCol.At(0) // bar
	if val0 != 2.0 {
		t.Errorf("Expected 2.0, got %v", val0)
	}
	val1, _ := cCol.At(1) // foo
	if val1 != 1.0 {
		t.Errorf("Expected 1.0, got %v", val1)
	}
}
