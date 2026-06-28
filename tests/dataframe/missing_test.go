package dataframe_test

import (
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func TestFillNA(t *testing.T) {
	t.Run("fill float column with constant", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Score": mustSeries(10.0, nil, 30.0, nil),
			},
			ColumnOrder: []string{"Score"},
			Index:       []string{"0", "1", "2", "3"},
		}
		result, err := df.FillNA(0.0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		v1, _ := result.Columns["Score"].At(1)
		v3, _ := result.Columns["Score"].At(3)
		if !valuesEqual(v1, 0.0) || !valuesEqual(v3, 0.0) {
			t.Errorf("expected nulls filled with 0, got %v and %v", v1, v3)
		}
		// original unchanged
		if !df.Columns["Score"].IsNull(1) {
			t.Error("original DataFrame was mutated")
		}
	})

	t.Run("incompatible columns left unchanged", func(t *testing.T) {
		nameSeries, _ := collection.NewStringSeriesFromData([]string{"a", "", "c"}, []bool{false, true, false})
		scoreSeries, _ := collection.NewFloat64SeriesFromData([]float64{1.0, 0, 3.0}, []bool{false, true, false})
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Name":  nameSeries,
				"Score": scoreSeries,
			},
			ColumnOrder: []string{"Name", "Score"},
			Index:       []string{"0", "1", "2"},
		}
		// Fill with a float: only Score should be affected, Name stays null
		result, err := df.FillNA(0.0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !result.Columns["Name"].IsNull(1) {
			t.Error("Name null should remain (string column incompatible with float fill)")
		}
		if result.Columns["Score"].IsNull(1) {
			t.Error("Score null should be filled")
		}
	})

	t.Run("nil fill value errors", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"A": mustSeries(1)},
			ColumnOrder: []string{"A"},
			Index:       []string{"0"},
		}
		if _, err := df.FillNA(nil); err == nil {
			t.Error("expected error for nil fill value")
		}
	})
}

func TestFillNAColumn(t *testing.T) {
	nameSeries, _ := collection.NewStringSeriesFromData([]string{"a", "", "c"}, []bool{false, true, false})
	scoreSeries, _ := collection.NewFloat64SeriesFromData([]float64{1.0, 0, 3.0}, []bool{false, true, false})
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name":  nameSeries,
			"Score": scoreSeries,
		},
		ColumnOrder: []string{"Name", "Score"},
		Index:       []string{"0", "1", "2"},
	}

	result, err := df.FillNAColumn("Name", "missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	v1, _ := result.Columns["Name"].At(1)
	if v1 != "missing" {
		t.Errorf("expected 'missing', got %v", v1)
	}
	// Score null untouched
	if !result.Columns["Score"].IsNull(1) {
		t.Error("Score column should be untouched")
	}

	t.Run("incompatible value errors", func(t *testing.T) {
		if _, err := df.FillNAColumn("Score", "notnumber"); err == nil {
			t.Error("expected error filling float column with string")
		}
	})
	t.Run("missing column errors", func(t *testing.T) {
		if _, err := df.FillNAColumn("Nope", 0.0); err == nil {
			t.Error("expected error for missing column")
		}
	})
}

func TestFillNAMethod(t *testing.T) {
	t.Run("ffill", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"V": mustSeries(1.0, nil, nil, 4.0, nil),
			},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1", "2", "3", "4"},
		}
		result, err := df.FillNAMethod("ffill")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// expected: 1, 1, 1, 4, 4
		expected := []float64{1, 1, 1, 4, 4}
		for i, exp := range expected {
			v, _ := result.Columns["V"].At(i)
			if !valuesEqual(v, exp) {
				t.Errorf("ffill row %d: expected %v, got %v", i, exp, v)
			}
		}
	})

	t.Run("bfill", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"V": mustSeries(nil, 2.0, nil, nil, 5.0),
			},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1", "2", "3", "4"},
		}
		result, err := df.FillNAMethod("bfill")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// expected: 2, 2, 5, 5, 5
		expected := []float64{2, 2, 5, 5, 5}
		for i, exp := range expected {
			v, _ := result.Columns["V"].At(i)
			if !valuesEqual(v, exp) {
				t.Errorf("bfill row %d: expected %v, got %v", i, exp, v)
			}
		}
	})

	t.Run("leading nulls stay null with ffill", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"V": mustSeries(nil, 2.0),
			},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1"},
		}
		result, _ := df.FillNAMethod("ffill")
		if !result.Columns["V"].IsNull(0) {
			t.Error("leading null should remain null after ffill")
		}
	})

	t.Run("invalid method errors", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"A": mustSeries(1)},
			ColumnOrder: []string{"A"},
			Index:       []string{"0"},
		}
		if _, err := df.FillNAMethod("zfill"); err == nil {
			t.Error("expected error for invalid method")
		}
	})
}

func TestDropNA(t *testing.T) {
	makeDF := func() *dataframe.DataFrame {
		return &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(1.0, nil, 3.0, nil),
				"B": mustSeries(nil, nil, 30.0, 40.0),
			},
			ColumnOrder: []string{"A", "B"},
			Index:       []string{"0", "1", "2", "3"},
		}
	}

	t.Run("how any", func(t *testing.T) {
		result, err := makeDF().DropNA("any", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Only row 2 (3.0, 30.0) has no nulls
		if result.Len() != 1 {
			t.Fatalf("expected 1 row, got %d", result.Len())
		}
		if !strSliceEqual(result.Index, []string{"2"}) {
			t.Errorf("expected index [2], got %v", result.Index)
		}
	})

	t.Run("how all", func(t *testing.T) {
		result, err := makeDF().DropNA("all", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Only row 1 is all-null -> dropped; 3 rows remain
		if result.Len() != 3 {
			t.Errorf("expected 3 rows, got %d", result.Len())
		}
	})

	t.Run("subset", func(t *testing.T) {
		result, err := makeDF().DropNA("any", []string{"A"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Consider only A: rows 0 and 2 are non-null
		if result.Len() != 2 {
			t.Errorf("expected 2 rows, got %d", result.Len())
		}
	})

	t.Run("invalid how errors", func(t *testing.T) {
		if _, err := makeDF().DropNA("some", nil); err == nil {
			t.Error("expected error for invalid how")
		}
	})

	t.Run("missing subset column errors", func(t *testing.T) {
		if _, err := makeDF().DropNA("any", []string{"Z"}); err == nil {
			t.Error("expected error for missing subset column")
		}
	})
}

func TestIsNANotNA(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1.0, nil, 3.0),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"0", "1", "2"},
	}

	isna := df.IsNA()
	v0, _ := isna.Columns["A"].At(0)
	v1, _ := isna.Columns["A"].At(1)
	if v0 != false || v1 != true {
		t.Errorf("IsNA expected [false, true, ...], got [%v, %v, ...]", v0, v1)
	}

	notna := df.NotNA()
	n0, _ := notna.Columns["A"].At(0)
	n1, _ := notna.Columns["A"].At(1)
	if n0 != true || n1 != false {
		t.Errorf("NotNA expected [true, false, ...], got [%v, %v, ...]", n0, n1)
	}
}
