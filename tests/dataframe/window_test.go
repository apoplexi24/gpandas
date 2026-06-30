package dataframe_test

import (
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func windowDF() *dataframe.DataFrame {
	return &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustSeries(1.0, 2.0, 3.0, 4.0, 5.0),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}
}

func TestRollingMean(t *testing.T) {
	result, err := windowDF().Rolling(3).Mean()
	if err != nil {
		t.Fatalf("Rolling.Mean failed: %v", err)
	}
	// First two are null (incomplete window)
	if !result.Columns["V"].IsNull(0) || !result.Columns["V"].IsNull(1) {
		t.Error("expected first two rolling values to be null")
	}
	// index 2: mean(1,2,3)=2 ; index 3: mean(2,3,4)=3 ; index 4: mean(3,4,5)=4
	v2, _ := result.Columns["V"].At(2)
	v3, _ := result.Columns["V"].At(3)
	v4, _ := result.Columns["V"].At(4)
	if !valuesEqual(v2, 2.0) || !valuesEqual(v3, 3.0) || !valuesEqual(v4, 4.0) {
		t.Errorf("expected [.., .., 2, 3, 4], got [%v %v %v]", v2, v3, v4)
	}
}

func TestRollingSum(t *testing.T) {
	result, _ := windowDF().Rolling(2).Sum()
	// index1: 1+2=3, index4: 4+5=9
	v1, _ := result.Columns["V"].At(1)
	v4, _ := result.Columns["V"].At(4)
	if !valuesEqual(v1, 3.0) || !valuesEqual(v4, 9.0) {
		t.Errorf("expected v1=3 v4=9, got %v %v", v1, v4)
	}
}

func TestRollingInvalidWindow(t *testing.T) {
	if _, err := windowDF().Rolling(0).Mean(); err == nil {
		t.Error("expected error for window < 1")
	}
}

func TestShift(t *testing.T) {
	t.Run("shift down by 1", func(t *testing.T) {
		result, err := windowDF().Shift(1)
		if err != nil {
			t.Fatalf("Shift failed: %v", err)
		}
		if !result.Columns["V"].IsNull(0) {
			t.Error("expected first value null after shift down")
		}
		v1, _ := result.Columns["V"].At(1)
		if !valuesEqual(v1, 1.0) {
			t.Errorf("expected v1=1, got %v", v1)
		}
	})

	t.Run("shift up by 1", func(t *testing.T) {
		result, _ := windowDF().Shift(-1)
		v0, _ := result.Columns["V"].At(0)
		if !valuesEqual(v0, 2.0) {
			t.Errorf("expected v0=2 after shift up, got %v", v0)
		}
		if !result.Columns["V"].IsNull(4) {
			t.Error("expected last value null after shift up")
		}
	})
}

func TestCumulative(t *testing.T) {
	t.Run("cumsum", func(t *testing.T) {
		result, _ := windowDF().CumSum()
		// 1,3,6,10,15
		expected := []float64{1, 3, 6, 10, 15}
		for i, exp := range expected {
			v, _ := result.Columns["V"].At(i)
			if !valuesEqual(v, exp) {
				t.Errorf("cumsum row %d: expected %v, got %v", i, exp, v)
			}
		}
	})

	t.Run("cumprod", func(t *testing.T) {
		result, _ := windowDF().CumProd()
		// 1,2,6,24,120
		v4, _ := result.Columns["V"].At(4)
		if !valuesEqual(v4, 120.0) {
			t.Errorf("expected cumprod 120, got %v", v4)
		}
	})

	t.Run("cummax with nulls skipped", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"V": mustSeries(3.0, nil, 1.0, 5.0),
			},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1", "2", "3"},
		}
		result, _ := df.CumMax()
		// 3, null, 3, 5
		if !result.Columns["V"].IsNull(1) {
			t.Error("expected null preserved at index 1")
		}
		v2, _ := result.Columns["V"].At(2)
		v3, _ := result.Columns["V"].At(3)
		if !valuesEqual(v2, 3.0) || !valuesEqual(v3, 5.0) {
			t.Errorf("expected cummax [3, null, 3, 5], got idx2=%v idx3=%v", v2, v3)
		}
	})
}
