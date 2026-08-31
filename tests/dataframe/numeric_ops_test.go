package dataframe_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// opsCell describes one expected cell: either a null, or a numeric value.
type opsCell struct {
	null bool
	val  float64
}

func opsNull() opsCell         { return opsCell{null: true} }
func opsVal(v float64) opsCell { return opsCell{val: v} }
func opsInf(sign int) opsCell  { return opsCell{val: math.Inf(sign)} }

// assertColumn checks a column against the expected cells, comparing numerically
// so int64 and float64 columns can share one assertion.
func assertColumn(t *testing.T, df *dataframe.DataFrame, column string, want []opsCell) {
	t.Helper()

	series, ok := df.Columns[column]
	if !ok {
		t.Fatalf("column %q missing from result", column)
	}
	if series.Len() != len(want) {
		t.Fatalf("column %q: expected %d rows, got %d", column, len(want), series.Len())
	}

	for i, exp := range want {
		isNull := series.IsNull(i)
		if isNull != exp.null {
			t.Errorf("column %q row %d: expected null=%v, got null=%v", column, i, exp.null, isNull)
			continue
		}
		if exp.null {
			continue
		}

		raw, err := series.At(i)
		if err != nil {
			t.Fatalf("column %q row %d: %v", column, i, err)
		}
		got, ok := opsFloat(raw)
		if !ok {
			t.Errorf("column %q row %d: value %v (%T) is not numeric", column, i, raw, raw)
			continue
		}
		switch {
		case math.IsInf(exp.val, 0):
			if got != exp.val {
				t.Errorf("column %q row %d: expected %v, got %v", column, i, exp.val, got)
			}
		case math.IsNaN(exp.val):
			if !math.IsNaN(got) {
				t.Errorf("column %q row %d: expected NaN, got %v", column, i, got)
			}
		default:
			if !approxEqual(got, exp.val) {
				t.Errorf("column %q row %d: expected %v, got %v", column, i, exp.val, got)
			}
		}
	}
}

func opsFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
}

// assertDType checks the concrete element type of a result column, which is how
// integer preservation is verified.
func assertDType(t *testing.T, df *dataframe.DataFrame, column string, want reflect.Kind) {
	t.Helper()
	series, ok := df.Columns[column]
	if !ok {
		t.Fatalf("column %q missing from result", column)
	}
	if got := series.DType().Kind(); got != want {
		t.Errorf("column %q: expected dtype %v, got %v", column, want, got)
	}
}

func mustInt64Series(t *testing.T, data []int64, mask []bool) collection.Series {
	t.Helper()
	s, err := collection.NewInt64SeriesFromData(data, mask)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return s
}

func mustFloat64Series(t *testing.T, data []float64, mask []bool) collection.Series {
	t.Helper()
	s, err := collection.NewFloat64SeriesFromData(data, mask)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return s
}

// -----------------------------------------------------------------------------
// Round
// -----------------------------------------------------------------------------

func TestRound(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name":  mustSeries("a", "b", "c"),
			"Value": mustSeries(1.234, 5.678, 3.14159),
		},
		ColumnOrder: []string{"Name", "Value"},
		Index:       []string{"0", "1", "2"},
	}

	got, err := df.Round(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertColumn(t, got, "Value", []opsCell{opsVal(1.23), opsVal(5.68), opsVal(3.14)})

	// Column order and the non-numeric column survive untouched.
	if !strSliceEqual(got.ColumnOrder, []string{"Name", "Value"}) {
		t.Errorf("expected column order [Name Value], got %v", got.ColumnOrder)
	}
	name0, _ := got.Columns["Name"].At(0)
	if name0 != "a" {
		t.Errorf("non-numeric column should pass through, got %v", name0)
	}
}

func TestRoundHalfToEven(t *testing.T) {
	// These halves are all exactly representable, so the tie-breaking rule is
	// unambiguous: pandas and NumPy round them to the nearest even number.
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustSeries(0.5, 1.5, 2.5, 3.5, -0.5, -1.5, -2.5),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3", "4", "5", "6"},
	}

	got, err := df.Round(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertColumn(t, got, "V", []opsCell{
		opsVal(0), opsVal(2), opsVal(2), opsVal(4), opsVal(0), opsVal(-2), opsVal(-2),
	})
}

func TestRoundNegativeDecimals(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustSeries(15.0, 25.0, 149.0, -15.0),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3"},
	}

	got, err := df.Round(-1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 15 -> 20 and 25 -> 20 because 1.5 and 2.5 both round to the even 2.
	assertColumn(t, got, "V", []opsCell{opsVal(20), opsVal(20), opsVal(150), opsVal(-20)})
}

func TestRoundKeepsIntegerColumns(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Count": mustInt64Series(t, []int64{7, 12, 999}, nil),
		},
		ColumnOrder: []string{"Count"},
		Index:       []string{"0", "1", "2"},
	}

	got, err := df.Round(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertDType(t, got, "Count", reflect.Int64)
	assertColumn(t, got, "Count", []opsCell{opsVal(7), opsVal(12), opsVal(999)})
}

func TestRoundPreservesNulls(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustFloat64Series(t, []float64{1.234, 0, 5.678}, []bool{false, true, false}),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2"},
	}

	got, err := df.Round(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertColumn(t, got, "V", []opsCell{opsVal(1.2), opsNull(), opsVal(5.7)})
}

// -----------------------------------------------------------------------------
// Clip
// -----------------------------------------------------------------------------

func TestClip(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustSeries(1.0, 5.0, 10.0, 15.0),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3"},
	}

	got, err := df.Clip(3, 12)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertColumn(t, got, "V", []opsCell{opsVal(3), opsVal(5), opsVal(10), opsVal(12)})
}

func TestClipOneSided(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustSeries(-5.0, 0.0, 7.0, 200.0),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3"},
	}

	// Floor only.
	floored, err := df.Clip(0, math.Inf(1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertColumn(t, floored, "V", []opsCell{opsVal(0), opsVal(0), opsVal(7), opsVal(200)})

	// Ceiling only.
	capped, err := df.Clip(math.Inf(-1), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertColumn(t, capped, "V", []opsCell{opsVal(-5), opsVal(0), opsVal(7), opsVal(100)})
}

func TestClipIntegerPreservation(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Count": mustInt64Series(t, []int64{1, 5, 10}, nil),
		},
		ColumnOrder: []string{"Count"},
		Index:       []string{"0", "1", "2"},
	}

	// Whole bounds keep the column integral.
	whole, err := df.Clip(2, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertDType(t, whole, "Count", reflect.Int64)
	assertColumn(t, whole, "Count", []opsCell{opsVal(2), opsVal(5), opsVal(8)})

	// A fractional bound would be substituted into the data, so the column is
	// promoted to float64 to represent it.
	fractional, err := df.Clip(2.5, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertDType(t, fractional, "Count", reflect.Float64)
	assertColumn(t, fractional, "Count", []opsCell{opsVal(2.5), opsVal(5), opsVal(8)})
}

func TestClipPreservesNulls(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustFloat64Series(t, []float64{-3, 0, 99}, []bool{false, true, false}),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2"},
	}

	got, err := df.Clip(0, 50)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The null is not clamped into the range; it stays null.
	assertColumn(t, got, "V", []opsCell{opsVal(0), opsNull(), opsVal(50)})
}

func TestClipInvalidBounds(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"V": mustSeries(1.0, 2.0)},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1"},
	}

	if _, err := df.Clip(10, 5); err == nil {
		t.Error("expected an error when lower exceeds upper")
	}
	if _, err := df.Clip(math.NaN(), 5); err == nil {
		t.Error("expected an error for a NaN lower bound")
	}
	if _, err := df.Clip(0, math.NaN()); err == nil {
		t.Error("expected an error for a NaN upper bound")
	}
}

// -----------------------------------------------------------------------------
// Abs
// -----------------------------------------------------------------------------

func TestAbs(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name":  mustSeries("a", "b", "c", "d"),
			"Delta": mustFloat64Series(t, []float64{-1.5, 2.5, 0, -0.25}, []bool{false, false, true, false}),
			"Count": mustInt64Series(t, []int64{-1, 2, -3, 0}, nil),
		},
		ColumnOrder: []string{"Name", "Delta", "Count"},
		Index:       []string{"0", "1", "2", "3"},
	}

	got, err := df.Abs()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertColumn(t, got, "Delta", []opsCell{opsVal(1.5), opsVal(2.5), opsNull(), opsVal(0.25)})
	assertColumn(t, got, "Count", []opsCell{opsVal(1), opsVal(2), opsVal(3), opsVal(0)})
	assertDType(t, got, "Count", reflect.Int64)
	assertDType(t, got, "Delta", reflect.Float64)

	if !strSliceEqual(got.ColumnOrder, []string{"Name", "Delta", "Count"}) {
		t.Errorf("expected column order preserved, got %v", got.ColumnOrder)
	}
}

// -----------------------------------------------------------------------------
// Diff
// -----------------------------------------------------------------------------

func TestDiff(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustSeries(10.0, 20.0, 15.0, 30.0),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3"},
	}

	got, err := df.Diff(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The first cell has no previous row, so it is null rather than zero.
	assertColumn(t, got, "V", []opsCell{opsNull(), opsVal(10), opsVal(-5), opsVal(15)})
}

func TestDiffNegativeAndMultiPeriod(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustSeries(10.0, 20.0, 15.0, 30.0),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3"},
	}

	// Negative periods look forward, vacating the tail.
	forward, err := df.Diff(-1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertColumn(t, forward, "V", []opsCell{opsVal(-10), opsVal(5), opsVal(-15), opsNull()})

	// Two periods back vacates the first two rows.
	twoBack, err := df.Diff(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertColumn(t, twoBack, "V", []opsCell{opsNull(), opsNull(), opsVal(5), opsVal(10)})

	// Zero periods compares each row with itself.
	zero, err := df.Diff(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertColumn(t, zero, "V", []opsCell{opsVal(0), opsVal(0), opsVal(0), opsVal(0)})

	// An offset larger than the DataFrame leaves nothing to compare.
	tooFar, err := df.Diff(10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertColumn(t, tooFar, "V", []opsCell{opsNull(), opsNull(), opsNull(), opsNull()})
}

func TestDiffNullPropagation(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustFloat64Series(t, []float64{10, 0, 15, 30}, []bool{false, true, false, false}),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3"},
	}

	got, err := df.Diff(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Row 1 is null itself; row 2's previous value is null. Both yield null.
	assertColumn(t, got, "V", []opsCell{opsNull(), opsNull(), opsNull(), opsVal(15)})
}

func TestDiffKeepsIntegerColumns(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Count": mustInt64Series(t, []int64{5, 9, 4}, nil),
		},
		ColumnOrder: []string{"Count"},
		Index:       []string{"0", "1", "2"},
	}

	got, err := df.Diff(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertDType(t, got, "Count", reflect.Int64)
	assertColumn(t, got, "Count", []opsCell{opsNull(), opsVal(4), opsVal(-5)})
}

func TestDiffMatchesShiftThenSubtract(t *testing.T) {
	// Acceptance criterion: Diff vacates cells exactly as Shift does.
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustFloat64Series(t, []float64{10, 20, 0, 30, 45}, []bool{false, false, true, false, false}),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}

	for _, periods := range []int{-2, -1, 1, 2} {
		diff, err := df.Diff(periods)
		if err != nil {
			t.Fatalf("Diff(%d): unexpected error: %v", periods, err)
		}
		shifted, err := df.Shift(periods)
		if err != nil {
			t.Fatalf("Shift(%d): unexpected error: %v", periods, err)
		}

		src := df.Columns["V"]
		prev := shifted.Columns["V"]
		result := diff.Columns["V"]

		for i := 0; i < src.Len(); i++ {
			wantNull := src.IsNull(i) || prev.IsNull(i)
			if result.IsNull(i) != wantNull {
				t.Errorf("Diff(%d) row %d: expected null=%v, got null=%v", periods, i, wantNull, result.IsNull(i))
				continue
			}
			if wantNull {
				continue
			}
			cur, _ := src.At(i)
			old, _ := prev.At(i)
			curF, _ := opsFloat(cur)
			oldF, _ := opsFloat(old)
			gotV, _ := result.At(i)
			gotF, _ := opsFloat(gotV)
			if !approxEqual(gotF, curF-oldF) {
				t.Errorf("Diff(%d) row %d: expected %v, got %v", periods, i, curF-oldF, gotF)
			}
		}
	}
}

// -----------------------------------------------------------------------------
// PctChange
// -----------------------------------------------------------------------------

func TestPctChange(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Price": mustSeries(100.0, 110.0, 99.0),
		},
		ColumnOrder: []string{"Price"},
		Index:       []string{"0", "1", "2"},
	}

	got, err := df.PctChange(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertColumn(t, got, "Price", []opsCell{opsNull(), opsVal(0.1), opsVal(-0.1)})
}

func TestPctChangeAlwaysFloat(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Count": mustInt64Series(t, []int64{2, 3}, nil),
		},
		ColumnOrder: []string{"Count"},
		Index:       []string{"0", "1"},
	}

	got, err := df.PctChange(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// A fractional change cannot be represented as an integer.
	assertDType(t, got, "Count", reflect.Float64)
	assertColumn(t, got, "Count", []opsCell{opsNull(), opsVal(0.5)})
}

func TestPctChangeDivisionByZero(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustSeries(0.0, 5.0, 0.0, -5.0, 0.0),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}

	got, err := df.PctChange(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Dividing by a previous value of zero yields an infinity, not an error.
	assertColumn(t, got, "V", []opsCell{
		opsNull(),
		opsInf(1),  // (5-0)/0
		opsVal(-1), // (0-5)/5
		opsInf(-1), // (-5-0)/0
		opsVal(-1), // (0-(-5))/-5
	})
}

func TestPctChangeNegativePeriods(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustSeries(100.0, 110.0),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1"},
	}

	got, err := df.PctChange(-1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// (100-110)/110
	assertColumn(t, got, "V", []opsCell{opsVal(-10.0 / 110.0), opsNull()})
}

// -----------------------------------------------------------------------------
// Rank
// -----------------------------------------------------------------------------

func TestRankMethods(t *testing.T) {
	newDF := func() *dataframe.DataFrame {
		return &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"V": mustSeries(10.0, 20.0, 20.0, 30.0)},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1", "2", "3"},
		}
	}

	cases := []struct {
		method dataframe.RankMethod
		want   []opsCell
	}{
		{dataframe.RankAverage, []opsCell{opsVal(1), opsVal(2.5), opsVal(2.5), opsVal(4)}},
		{dataframe.RankMin, []opsCell{opsVal(1), opsVal(2), opsVal(2), opsVal(4)}},
		{dataframe.RankMax, []opsCell{opsVal(1), opsVal(3), opsVal(3), opsVal(4)}},
		{dataframe.RankDense, []opsCell{opsVal(1), opsVal(2), opsVal(2), opsVal(3)}},
		{dataframe.RankFirst, []opsCell{opsVal(1), opsVal(2), opsVal(3), opsVal(4)}},
		{"", []opsCell{opsVal(1), opsVal(2.5), opsVal(2.5), opsVal(4)}}, // zero value = average
	}

	for _, c := range cases {
		got, err := newDF().Rank(c.method)
		if err != nil {
			t.Fatalf("Rank(%q): unexpected error: %v", c.method, err)
		}
		t.Run(string(c.method), func(t *testing.T) {
			assertColumn(t, got, "V", c.want)
		})
	}
}

func TestRankIsAlwaysFloat(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"Count": mustInt64Series(t, []int64{5, 1, 5}, nil)},
		ColumnOrder: []string{"Count"},
		Index:       []string{"0", "1", "2"},
	}

	got, err := df.Rank(dataframe.RankAverage)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Average ranks can be halves, so ranks are float even for integer input.
	assertDType(t, got, "Count", reflect.Float64)
	assertColumn(t, got, "Count", []opsCell{opsVal(2.5), opsVal(1), opsVal(2.5)})
}

func TestRankSkipsNulls(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"V": mustFloat64Series(t, []float64{10, 0, 20, 20}, []bool{false, true, false, false}),
		},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3"},
	}

	got, err := df.Rank(dataframe.RankAverage)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The null takes no rank, so the three real values rank 1, 2.5, 2.5.
	assertColumn(t, got, "V", []opsCell{opsVal(1), opsNull(), opsVal(2.5), opsVal(2.5)})
}

func TestRankUnsortedWithNegatives(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"V": mustSeries(3.0, -1.0, 7.0, -1.0, 0.0)},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}

	// Sorted: -1, -1, 0, 3, 7 -> the tied -1s span ranks 1 and 2.
	average, err := df.Rank(dataframe.RankAverage)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertColumn(t, average, "V", []opsCell{opsVal(4), opsVal(1.5), opsVal(5), opsVal(1.5), opsVal(3)})

	// RankFirst breaks the tie by row order: row 1 before row 3.
	first, err := df.Rank(dataframe.RankFirst)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertColumn(t, first, "V", []opsCell{opsVal(4), opsVal(1), opsVal(5), opsVal(2), opsVal(3)})
}

func TestRankInvalidMethod(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"V": mustSeries(1.0, 2.0)},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1"},
	}

	if _, err := df.Rank("median"); err == nil {
		t.Error("expected an error for an unsupported rank method")
	}
}

// -----------------------------------------------------------------------------
// Shared behaviour
// -----------------------------------------------------------------------------

func TestNumericOpsPreserveShapeAndIndex(t *testing.T) {
	index := []string{"a", "b", "c"}
	order := []string{"Label", "X", "Y"}

	newDF := func() *dataframe.DataFrame {
		return &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"Label": mustSeries("p", "q", "r"),
				"X":     mustSeries(1.5, -2.5, 3.5),
				"Y":     mustInt64Series(t, []int64{4, 5, 6}, nil),
			},
			ColumnOrder: order,
			Index:       index,
		}
	}

	ops := map[string]func(*dataframe.DataFrame) (*dataframe.DataFrame, error){
		"Round":     func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.Round(1) },
		"Clip":      func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.Clip(0, 5) },
		"Abs":       func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.Abs() },
		"Diff":      func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.Diff(1) },
		"PctChange": func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.PctChange(1) },
		"Rank":      func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { return d.Rank(dataframe.RankMin) },
	}

	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			src := newDF()
			got, err := op(src)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !strSliceEqual(got.ColumnOrder, order) {
				t.Errorf("expected column order %v, got %v", order, got.ColumnOrder)
			}
			if !strSliceEqual(got.Index, index) {
				t.Errorf("expected index %v, got %v", index, got.Index)
			}
			if got.Len() != 3 {
				t.Errorf("expected 3 rows, got %d", got.Len())
			}

			// The non-numeric column is untouched.
			label, _ := got.Columns["Label"].At(0)
			if label != "p" {
				t.Errorf("expected the Label column to pass through, got %v", label)
			}

			// The source DataFrame is never mutated.
			x0, _ := src.Columns["X"].At(0)
			if !valuesEqual(x0, 1.5) {
				t.Errorf("source DataFrame was mutated: X[0] is now %v", x0)
			}
		})
	}
}

func TestNumericOpsOnNilDataFrame(t *testing.T) {
	var df *dataframe.DataFrame

	if _, err := df.Round(2); err == nil {
		t.Error("Round: expected an error for a nil DataFrame")
	}
	if _, err := df.Clip(0, 1); err == nil {
		t.Error("Clip: expected an error for a nil DataFrame")
	}
	if _, err := df.Abs(); err == nil {
		t.Error("Abs: expected an error for a nil DataFrame")
	}
	if _, err := df.Diff(1); err == nil {
		t.Error("Diff: expected an error for a nil DataFrame")
	}
	if _, err := df.PctChange(1); err == nil {
		t.Error("PctChange: expected an error for a nil DataFrame")
	}
	if _, err := df.Rank(dataframe.RankAverage); err == nil {
		t.Error("Rank: expected an error for a nil DataFrame")
	}
}
