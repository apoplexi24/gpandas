package dataframe_test

import (
	"math"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func TestPipe(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"V": mustSeries(1.0, 2.0, 3.0)},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2"},
	}
	double := func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return d.Apply("V", func(v any) any { return v.(float64) * 2 })
	}
	result, err := df.Pipe(double)
	if err != nil {
		t.Fatalf("Pipe failed: %v", err)
	}
	v0, _ := result.Columns["V"].At(0)
	if !valuesEqual(v0, 2.0) {
		t.Errorf("expected 2.0, got %v", v0)
	}

	t.Run("nil fn errors", func(t *testing.T) {
		if _, err := df.Pipe(nil); err == nil {
			t.Error("expected error for nil fn")
		}
	})
}

func TestSample(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"V": mustSeries(1, 2, 3, 4, 5)},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}

	t.Run("deterministic with seed", func(t *testing.T) {
		a, err := df.Sample(3, 42)
		if err != nil {
			t.Fatalf("Sample failed: %v", err)
		}
		if a.Len() != 3 {
			t.Fatalf("expected 3 rows, got %d", a.Len())
		}
		b, _ := df.Sample(3, 42)
		if !strSliceEqual(a.Index, b.Index) {
			t.Errorf("same seed should produce same sample: %v vs %v", a.Index, b.Index)
		}
	})

	t.Run("out of range errors", func(t *testing.T) {
		if _, err := df.Sample(10); err == nil {
			t.Error("expected error for n > rowCount")
		}
	})
}

func TestCorr(t *testing.T) {
	// y = 2x perfectly correlated; z = -x perfectly anti-correlated
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"x": mustSeries(1.0, 2.0, 3.0, 4.0),
			"y": mustSeries(2.0, 4.0, 6.0, 8.0),
			"z": mustSeries(4.0, 3.0, 2.0, 1.0),
		},
		ColumnOrder: []string{"x", "y", "z"},
		Index:       []string{"0", "1", "2", "3"},
	}
	corr, err := df.Corr()
	if err != nil {
		t.Fatalf("Corr failed: %v", err)
	}
	// Diagonal == 1
	xx, _ := corr.Columns["x"].At(0)
	if math.Abs(xx.(float64)-1.0) > 1e-9 {
		t.Errorf("corr(x,x) expected 1, got %v", xx)
	}
	// x vs y == 1
	xy, _ := corr.Columns["y"].At(0)
	if math.Abs(xy.(float64)-1.0) > 1e-9 {
		t.Errorf("corr(x,y) expected 1, got %v", xy)
	}
	// x vs z == -1
	xz, _ := corr.Columns["z"].At(0)
	if math.Abs(xz.(float64)+1.0) > 1e-9 {
		t.Errorf("corr(x,z) expected -1, got %v", xz)
	}
}

func TestCov(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"x": mustSeries(1.0, 2.0, 3.0),
		},
		ColumnOrder: []string{"x"},
		Index:       []string{"0", "1", "2"},
	}
	cov, err := df.Cov()
	if err != nil {
		t.Fatalf("Cov failed: %v", err)
	}
	// variance of 1,2,3 (ddof=1) = 1
	v, _ := cov.Columns["x"].At(0)
	if math.Abs(v.(float64)-1.0) > 1e-9 {
		t.Errorf("var(x) expected 1, got %v", v)
	}
}

func TestMergeOnMultiKey(t *testing.T) {
	left := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"year":   mustSeries(2020, 2020, 2021),
			"region": mustSeries("N", "S", "N"),
			"sales":  mustSeries(10, 20, 30),
		},
		ColumnOrder: []string{"year", "region", "sales"},
		Index:       []string{"0", "1", "2"},
	}
	right := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"year":   mustSeries(2020, 2021),
			"region": mustSeries("N", "N"),
			"target": mustSeries(100, 300),
		},
		ColumnOrder: []string{"year", "region", "target"},
		Index:       []string{"0", "1"},
	}

	result, err := left.MergeOn(right, []string{"year", "region"}, dataframe.InnerMerge)
	if err != nil {
		t.Fatalf("MergeOn failed: %v", err)
	}
	// Matches: (2020,N) and (2021,N) -> 2 rows
	if result.Len() != 2 {
		t.Fatalf("expected 2 matched rows, got %d", result.Len())
	}
	// Columns: year, region, sales, target
	if !strSliceEqual(result.ColumnOrder, []string{"year", "region", "sales", "target"}) {
		t.Errorf("unexpected columns: %v", result.ColumnOrder)
	}

	t.Run("left merge keeps unmatched", func(t *testing.T) {
		res, _ := left.MergeOn(right, []string{"year", "region"}, dataframe.LeftMerge)
		if res.Len() != 3 {
			t.Errorf("expected 3 rows for left merge, got %d", res.Len())
		}
	})

	t.Run("missing key errors", func(t *testing.T) {
		if _, err := left.MergeOn(right, []string{"nope"}, dataframe.InnerMerge); err == nil {
			t.Error("expected error for missing key")
		}
	})
}

func TestToDatetime(t *testing.T) {
	s, _ := collection.NewStringSeriesFromData([]string{"2021-01-15", "2022-06-30"}, nil)
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"d": s},
		ColumnOrder: []string{"d"},
		Index:       []string{"0", "1"},
	}
	dt, err := df.ToDatetime("d", "2006-01-02")
	if err != nil {
		t.Fatalf("ToDatetime failed: %v", err)
	}

	acc, err := dt.Dt("d")
	if err != nil {
		t.Fatalf("Dt failed: %v", err)
	}
	years := acc.Year()
	y0, _ := years.At(0)
	y1, _ := years.At(1)
	if !valuesEqual(y0, 2021) || !valuesEqual(y1, 2022) {
		t.Errorf("expected years [2021, 2022], got [%v, %v]", y0, y1)
	}
	months := acc.Month()
	m1, _ := months.At(1)
	if !valuesEqual(m1, 6) {
		t.Errorf("expected month 6, got %v", m1)
	}

	t.Run("bad parse errors", func(t *testing.T) {
		bad, _ := collection.NewStringSeriesFromData([]string{"not-a-date"}, nil)
		bdf := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"d": bad},
			ColumnOrder: []string{"d"},
			Index:       []string{"0"},
		}
		if _, err := bdf.ToDatetime("d", "2006-01-02"); err == nil {
			t.Error("expected parse error")
		}
	})
}

func TestAsCategorical(t *testing.T) {
	s, _ := collection.NewStringSeriesFromData(
		[]string{"A", "B", "A", "A", "B"}, nil)
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"grade": s},
		ColumnOrder: []string{"grade"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}
	cat, err := df.AsCategorical("grade")
	if err != nil {
		t.Fatalf("AsCategorical failed: %v", err)
	}

	// Values still readable as strings
	v0, _ := cat.Columns["grade"].At(0)
	if v0 != "A" {
		t.Errorf("expected A, got %v", v0)
	}

	cats, err := cat.Categories("grade")
	if err != nil {
		t.Fatalf("Categories failed: %v", err)
	}
	// First-appearance order: A, B
	if !strSliceEqual(cats, []string{"A", "B"}) {
		t.Errorf("expected categories [A B], got %v", cats)
	}

	t.Run("non-categorical errors", func(t *testing.T) {
		if _, err := df.Categories("grade"); err == nil {
			t.Error("expected error: original column is not categorical")
		}
	})
}
