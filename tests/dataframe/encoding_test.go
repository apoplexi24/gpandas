package dataframe_test

import (
	"reflect"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// boolColumn returns a column's values, failing if it is not a null-free bool
// column.
func boolColumn(t *testing.T, df *dataframe.DataFrame, column string) []bool {
	t.Helper()
	s, ok := df.Columns[column].(*collection.BoolSeries)
	if !ok {
		t.Fatalf("column %q: expected *BoolSeries, got %T", column, df.Columns[column])
	}
	out := make([]bool, s.Len())
	for i := range out {
		if s.IsNull(i) {
			t.Fatalf("column %q row %d: unexpected null", column, i)
		}
		v, _ := s.At(i)
		out[i] = v.(bool)
	}
	return out
}

func TestGetDummiesStrings(t *testing.T) {
	city, err := collection.NewStringSeriesFromData([]string{"NYC", "LA", "", "NYC"}, []bool{false, false, true, false})
	if err != nil {
		t.Fatal(err)
	}
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Id":    mustSeries(int64(1), int64(2), int64(3), int64(4)),
			"City":  city,
			"Score": mustSeries(1.0, 2.0, 3.0, 4.0),
		},
		ColumnOrder: []string{"Id", "City", "Score"},
		Index:       []string{"w", "x", "y", "z"},
	}

	got, err := df.GetDummies("City")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Indicators take City's position, in sorted category order.
	if !strSliceEqual(got.ColumnOrder, []string{"Id", "City_LA", "City_NYC", "Score"}) {
		t.Errorf("column order: got %v", got.ColumnOrder)
	}
	if _, ok := got.Columns["City"]; ok {
		t.Error("original column should be removed")
	}
	// The null row is false everywhere.
	if want := []bool{false, true, false, false}; !reflect.DeepEqual(boolColumn(t, got, "City_LA"), want) {
		t.Errorf("City_LA: expected %v", want)
	}
	if want := []bool{true, false, false, true}; !reflect.DeepEqual(boolColumn(t, got, "City_NYC"), want) {
		t.Errorf("City_NYC: expected %v", want)
	}
	if !strSliceEqual(got.Index, df.Index) {
		t.Errorf("index: got %v", got.Index)
	}
	if _, ok := df.Columns["City"]; !ok {
		t.Error("source DataFrame was mutated")
	}
}

func TestGetDummiesNumericSortsByValue(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"X": mustInt64Series(t, []int64{10, 2, 10}, nil)},
		ColumnOrder: []string{"X"},
		Index:       []string{"0", "1", "2"},
	}
	got, err := df.GetDummies("X")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strSliceEqual(got.ColumnOrder, []string{"X_2", "X_10"}) {
		t.Errorf("expected numeric order [X_2 X_10], got %v", got.ColumnOrder)
	}
}

func TestGetDummiesCategoricalOrder(t *testing.T) {
	df := binningDF(t, []float64{3, 70, 12}, nil)

	// Categories come from the categorical, so the empty "adult" bin still gets
	// an indicator and the order follows the bins rather than the alphabet.
	binned, err := df.Cut("Value", []float64{0, 18, 65, 120}, []string{"minor", "adult", "senior"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := binned.GetDummies("Value")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strSliceEqual(got.ColumnOrder, []string{"Value_minor", "Value_adult", "Value_senior", "Id"}) {
		t.Errorf("column order: got %v", got.ColumnOrder)
	}
	if want := []bool{false, false, false}; !reflect.DeepEqual(boolColumn(t, got, "Value_adult"), want) {
		t.Errorf("Value_adult: expected %v", want)
	}
	if want := []bool{true, false, true}; !reflect.DeepEqual(boolColumn(t, got, "Value_minor"), want) {
		t.Errorf("Value_minor: expected %v", want)
	}

	// Every row has exactly one true indicator.
	for row := 0; row < 3; row++ {
		count := 0
		for _, name := range []string{"Value_minor", "Value_adult", "Value_senior"} {
			if boolColumn(t, got, name)[row] {
				count++
			}
		}
		if count != 1 {
			t.Errorf("row %d: expected exactly one true indicator, got %d", row, count)
		}
	}
}

func TestGetDummiesAllNull(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustFloat64Series(t, []float64{0, 0}, []bool{true, true}),
			"B": mustSeries("x", "y"),
		},
		ColumnOrder: []string{"A", "B"},
		Index:       []string{"0", "1"},
	}
	got, err := df.GetDummies("A")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strSliceEqual(got.ColumnOrder, []string{"B"}) {
		t.Errorf("expected only B to remain, got %v", got.ColumnOrder)
	}
}

func TestGetDummiesErrors(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Color":     mustSeries("red", "blue"),
			"Color_red": mustSeries(1.0, 0.0),
		},
		ColumnOrder: []string{"Color", "Color_red"},
		Index:       []string{"0", "1"},
	}
	if _, err := df.GetDummies("Color"); err == nil {
		t.Error("expected an error for a colliding indicator name")
	}
	if _, err := df.GetDummies("Nope"); err == nil {
		t.Error("expected an error for a missing column")
	}
}
