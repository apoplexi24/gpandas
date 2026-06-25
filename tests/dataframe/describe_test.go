package dataframe_test

import (
	"math"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func describeTestDF() *dataframe.DataFrame {
	return &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name":  mustSeries("a", "b", "c", "d"),
			"Score": mustSeries(10.0, 20.0, 30.0, 40.0),
			"Age":   mustSeries(1, 2, 3, 4),
		},
		ColumnOrder: []string{"Name", "Score", "Age"},
		Index:       []string{"0", "1", "2", "3"},
	}
}

func statRow(df *dataframe.DataFrame, stat string) int {
	for i := 0; i < df.Len(); i++ {
		v, _ := df.Columns["statistic"].At(i)
		if v == stat {
			return i
		}
	}
	return -1
}

func TestDescribe(t *testing.T) {
	df := describeTestDF()
	result, err := df.Describe()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// statistic column + 2 numeric columns (Score, Age); Name excluded
	expectedOrder := []string{"statistic", "Score", "Age"}
	if !strSliceEqual(result.ColumnOrder, expectedOrder) {
		t.Fatalf("expected column order %v, got %v", expectedOrder, result.ColumnOrder)
	}

	if _, ok := result.Columns["Name"]; ok {
		t.Error("non-numeric column Name should be excluded")
	}

	// count = 4
	cnt, _ := result.Columns["Score"].At(statRow(result, "count"))
	if cnt.(float64) != 4 {
		t.Errorf("expected count 4, got %v", cnt)
	}
	// mean of 10,20,30,40 = 25
	mean, _ := result.Columns["Score"].At(statRow(result, "mean"))
	if mean.(float64) != 25 {
		t.Errorf("expected mean 25, got %v", mean)
	}
	// min/max
	min, _ := result.Columns["Score"].At(statRow(result, "min"))
	max, _ := result.Columns["Score"].At(statRow(result, "max"))
	if min.(float64) != 10 || max.(float64) != 40 {
		t.Errorf("expected min 10 max 40, got %v %v", min, max)
	}
	// median (50%) = 25
	med, _ := result.Columns["Score"].At(statRow(result, "50%"))
	if med.(float64) != 25 {
		t.Errorf("expected median 25, got %v", med)
	}
	// sample std of 10,20,30,40 ≈ 12.909944
	std, _ := result.Columns["Score"].At(statRow(result, "std"))
	if math.Abs(std.(float64)-12.909944487358056) > 1e-9 {
		t.Errorf("expected std ~12.9099, got %v", std)
	}
}

func TestDescribeNoNumericColumns(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name": mustSeries("a", "b"),
		},
		ColumnOrder: []string{"Name"},
		Index:       []string{"0", "1"},
	}
	if _, err := df.Describe(); err == nil {
		t.Error("expected error when no numeric columns")
	}
}

func TestAggregationMaps(t *testing.T) {
	df := describeTestDF()

	mean := df.Mean()
	if mean["Score"] != 25 {
		t.Errorf("Mean Score: expected 25, got %v", mean["Score"])
	}
	if _, ok := mean["Name"]; ok {
		t.Error("Mean should exclude non-numeric Name")
	}

	sum := df.Sum()
	if sum["Score"] != 100 {
		t.Errorf("Sum Score: expected 100, got %v", sum["Score"])
	}

	med := df.Median()
	if med["Score"] != 25 {
		t.Errorf("Median Score: expected 25, got %v", med["Score"])
	}

	min := df.Min()
	max := df.Max()
	if min["Age"] != 1 || max["Age"] != 4 {
		t.Errorf("Min/Max Age: expected 1/4, got %v/%v", min["Age"], max["Age"])
	}
}

func TestAggregationWithNulls(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustSeries(2.0, nil, 4.0),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2"},
	}
	if df.Mean()["V"] != 3 {
		t.Errorf("Mean ignoring null: expected 3, got %v", df.Mean()["V"])
	}
	if df.Sum()["V"] != 6 {
		t.Errorf("Sum ignoring null: expected 6, got %v", df.Sum()["V"])
	}
	if df.NullCount()["V"] != 1 {
		t.Errorf("NullCount: expected 1, got %v", df.NullCount()["V"])
	}
}

func TestStdSingleValueIsNaN(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustSeries(5.0),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0"},
	}
	if !math.IsNaN(df.Std()["V"]) {
		t.Errorf("expected NaN std for single value, got %v", df.Std()["V"])
	}
}

func TestValueCounts(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"City": mustSeries("NYC", "LA", "NYC", "NYC", "LA", nil),
		},
		ColumnOrder: []string{"City"},
		Index:       []string{"0", "1", "2", "3", "4", "5"},
	}
	result, err := df.ValueCounts("City")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strSliceEqual(result.ColumnOrder, []string{"City", "count"}) {
		t.Fatalf("expected columns [City, count], got %v", result.ColumnOrder)
	}
	// 2 distinct non-null values (null excluded)
	if result.Len() != 2 {
		t.Fatalf("expected 2 rows, got %d", result.Len())
	}
	// NYC(3) should come first (descending count)
	city0, _ := result.Columns["City"].At(0)
	cnt0, _ := result.Columns["count"].At(0)
	if city0 != "NYC" || !valuesEqual(cnt0, 3) {
		t.Errorf("expected first row NYC=3, got %v=%v", city0, cnt0)
	}
	city1, _ := result.Columns["City"].At(1)
	cnt1, _ := result.Columns["count"].At(1)
	if city1 != "LA" || !valuesEqual(cnt1, 2) {
		t.Errorf("expected second row LA=2, got %v=%v", city1, cnt1)
	}
}

func TestValueCountsMissingColumn(t *testing.T) {
	df := describeTestDF()
	if _, err := df.ValueCounts("Missing"); err == nil {
		t.Error("expected error for missing column")
	}
}
