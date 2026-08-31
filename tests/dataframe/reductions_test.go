package dataframe_test

import (
	"math"
	"sort"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// approxEqual compares floats with a tolerance suitable for the reference values
// used below (computed independently with the textbook formulas).
func approxEqual(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9
}

// reductionsDF mirrors describeTestDF but adds a null-bearing column so that
// null exclusion is exercised by every reduction.
func reductionsDF() *dataframe.DataFrame {
	return &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name":   mustSeries("a", "b", "c", "d"),
			"Score":  mustSeries(10.0, 20.0, 30.0, 40.0),
			"Age":    mustSeries(1, 2, 3, 4),
			"Sparse": mustSeries(1.0, nil, 2.0, 4.0),
		},
		ColumnOrder: []string{"Name", "Score", "Age", "Sparse"},
		Index:       []string{"r0", "r1", "r2", "r3"},
	}
}

// -----------------------------------------------------------------------------
// Var
// -----------------------------------------------------------------------------

func TestVar(t *testing.T) {
	df := reductionsDF()
	got := df.Var()

	// Reference: sample variance (ddof=1) of 10,20,30,40 = 500/3.
	if !approxEqual(got["Score"], 166.66666666666666) {
		t.Errorf("Var Score: expected 166.666667, got %v", got["Score"])
	}
	// Integer column: variance of 1,2,3,4 = 5/3.
	if !approxEqual(got["Age"], 1.6666666666666667) {
		t.Errorf("Var Age: expected 1.666667, got %v", got["Age"])
	}
	// Null excluded: variance of 1,2,4 = 7/3.
	if !approxEqual(got["Sparse"], 2.3333333333333335) {
		t.Errorf("Var Sparse: expected 2.333333, got %v", got["Sparse"])
	}
	if _, ok := got["Name"]; ok {
		t.Error("Var should exclude the non-numeric Name column")
	}
}

func TestVarIsStdSquared(t *testing.T) {
	df := reductionsDF()
	variances := df.Var()
	stds := df.Std()

	for col, v := range variances {
		if !approxEqual(math.Sqrt(v), stds[col]) {
			t.Errorf("column %s: sqrt(Var)=%v disagrees with Std=%v", col, math.Sqrt(v), stds[col])
		}
	}
}

func TestVarSingleValueIsNaN(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"V": mustSeries(5.0, nil)},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1"},
	}
	if !math.IsNaN(df.Var()["V"]) {
		t.Errorf("expected NaN variance for a single value, got %v", df.Var()["V"])
	}
}

// -----------------------------------------------------------------------------
// Quantile
// -----------------------------------------------------------------------------

func TestQuantile(t *testing.T) {
	df := reductionsDF()

	cases := []struct {
		q    float64
		want float64
	}{
		{0.0, 10.0},
		{0.25, 17.5},
		{0.5, 25.0},
		{0.75, 32.5},
		{0.9, 37.0},
		{1.0, 40.0},
	}

	for _, c := range cases {
		got, err := df.Quantile(c.q)
		if err != nil {
			t.Fatalf("Quantile(%v): unexpected error: %v", c.q, err)
		}
		if !approxEqual(got["Score"], c.want) {
			t.Errorf("Quantile(%v) Score: expected %v, got %v", c.q, c.want, got["Score"])
		}
		if _, ok := got["Name"]; ok {
			t.Errorf("Quantile(%v) should exclude the non-numeric Name column", c.q)
		}
	}
}

func TestQuantileExcludesNulls(t *testing.T) {
	df := reductionsDF()
	got, err := df.Quantile(0.5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Median of the non-null values 1,2,4 is 2, not 1.5 (which treating the null
	// as a value would give).
	if !approxEqual(got["Sparse"], 2.0) {
		t.Errorf("Quantile(0.5) Sparse: expected 2, got %v", got["Sparse"])
	}
}

func TestQuantileMatchesMedianAndDescribe(t *testing.T) {
	df := reductionsDF()

	q50, err := df.Quantile(0.5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for col, median := range df.Median() {
		if !approxEqual(q50[col], median) {
			t.Errorf("column %s: Quantile(0.5)=%v disagrees with Median=%v", col, q50[col], median)
		}
	}

	q25, err := df.Quantile(0.25)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	summary, err := df.Describe()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	row := statRow(summary, "25%")
	if row < 0 {
		t.Fatal("Describe did not report a 25% row")
	}
	// Describe's Sparse column shares the linear-interpolation path, so the two
	// must agree even where a null is excluded.
	for _, col := range []string{"Score", "Age", "Sparse"} {
		cell, _ := summary.Columns[col].At(row)
		if !approxEqual(q25[col], cell.(float64)) {
			t.Errorf("column %s: Quantile(0.25)=%v disagrees with Describe 25%%=%v", col, q25[col], cell)
		}
	}
}

func TestQuantileInvalidQ(t *testing.T) {
	df := reductionsDF()
	for _, q := range []float64{-0.001, 1.001, math.NaN()} {
		if _, err := df.Quantile(q); err == nil {
			t.Errorf("expected an error for q=%v", q)
		}
	}
}

func TestQuantileAllNullColumnIsNaN(t *testing.T) {
	nullCol, err := collection.NewFloat64SeriesFromData([]float64{0, 0}, []bool{true, true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"V": nullCol},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1"},
	}
	got, err := df.Quantile(0.5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !math.IsNaN(got["V"]) {
		t.Errorf("expected NaN for an all-null column, got %v", got["V"])
	}
}

// -----------------------------------------------------------------------------
// Skew / Kurt
// -----------------------------------------------------------------------------

func TestSkew(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Symmetric": mustSeries(10.0, 20.0, 30.0, 40.0),
			"RightTail": mustSeries(1.0, 2.0, 3.0, 4.0, 100.0),
			"Spread":    mustSeries(2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0),
			"Sparse":    mustSeries(1.0, 2.0, nil, 2.0, 3.0, 10.0, 1.0),
		},
		ColumnOrder: []string{"Symmetric", "RightTail", "Spread", "Sparse"},
		Index:       []string{"0", "1", "2", "3", "4", "5", "6"},
	}

	got := df.Skew()

	// Reference values computed independently from the adjusted Fisher-Pearson
	// definition (equivalently scipy.stats.skew(..., bias=False)).
	checks := map[string]float64{
		"Symmetric": 0.0,
		"RightTail": 2.2323959116364573,
		"Spread":    0.8184875533567996,
		"Sparse":    2.1967478292014166, // null excluded: 1,2,2,3,10,1
	}
	for col, want := range checks {
		if !approxEqual(got[col], want) {
			t.Errorf("Skew %s: expected %v, got %v", col, want, got[col])
		}
	}
}

func TestKurt(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Uniform": mustSeries(10.0, 20.0, 30.0, 40.0),
			"Spread":  mustSeries(2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0),
			"Sparse":  mustSeries(1.0, 2.0, nil, 2.0, 3.0, 10.0, 1.0),
		},
		ColumnOrder: []string{"Uniform", "Spread", "Sparse"},
		Index:       []string{"0", "1", "2", "3", "4", "5", "6"},
	}

	got := df.Kurt()

	// Reference values computed independently from the unbiased Fisher (G2)
	// definition (equivalently scipy.stats.kurtosis(..., bias=False)).
	checks := map[string]float64{
		"Uniform": -1.2,
		"Spread":  0.940625,
		"Sparse":  5.015127318251497, // null excluded: 1,2,2,3,10,1
	}
	for col, want := range checks {
		if !approxEqual(got[col], want) {
			t.Errorf("Kurt %s: expected %v, got %v", col, want, got[col])
		}
	}
}

func TestSkewKurtUndefinedCases(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Two":      mustSeries(1.0, 2.0),           // too few for skew and kurt
			"Three":    mustSeries(1.0, 2.0, 4.0),      // enough for skew, not kurt
			"Constant": mustSeries(5.0, 5.0, 5.0, 5.0), // zero variance
		},
		ColumnOrder: []string{"Two", "Three", "Constant"},
		Index:       []string{"0", "1", "2", "3"},
	}

	skew := df.Skew()
	kurt := df.Kurt()

	if !math.IsNaN(skew["Two"]) {
		t.Errorf("Skew with 2 values: expected NaN, got %v", skew["Two"])
	}
	if math.IsNaN(skew["Three"]) {
		t.Error("Skew with 3 values: expected a value, got NaN")
	}
	if !math.IsNaN(kurt["Two"]) || !math.IsNaN(kurt["Three"]) {
		t.Errorf("Kurt with fewer than 4 values: expected NaN, got %v and %v", kurt["Two"], kurt["Three"])
	}
	if !math.IsNaN(skew["Constant"]) || !math.IsNaN(kurt["Constant"]) {
		t.Errorf("zero variance: expected NaN, got skew=%v kurt=%v", skew["Constant"], kurt["Constant"])
	}
}

// -----------------------------------------------------------------------------
// Mode
// -----------------------------------------------------------------------------

func TestMode(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"City":   mustSeries("NYC", "LA", "NYC", "NYC", "LA", nil),
			"Single": mustSeries(1.0, 2.0, 2.0, 3.0, nil, 2.0),
		},
		ColumnOrder: []string{"City", "Single"},
		Index:       []string{"0", "1", "2", "3", "4", "5"},
	}

	got := df.Mode()

	if len(got["City"]) != 1 || got["City"][0] != "NYC" {
		t.Errorf("Mode City: expected [NYC], got %v", got["City"])
	}
	if len(got["Single"]) != 1 || !valuesEqual(got["Single"][0], 2.0) {
		t.Errorf("Mode Single: expected [2], got %v", got["Single"])
	}
}

func TestModeTiesSortedAscending(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			// 30 and 10 both appear twice; 20 appears once.
			"Nums":  mustSeries(30.0, 10.0, 20.0, 30.0, 10.0),
			"Words": mustSeries("pear", "apple", "pear", "apple", "fig"),
		},
		ColumnOrder: []string{"Nums", "Words"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}

	got := df.Mode()

	if len(got["Nums"]) != 2 || !valuesEqual(got["Nums"][0], 10.0) || !valuesEqual(got["Nums"][1], 30.0) {
		t.Errorf("Mode Nums: expected [10 30] ascending, got %v", got["Nums"])
	}
	if len(got["Words"]) != 2 || got["Words"][0] != "apple" || got["Words"][1] != "pear" {
		t.Errorf("Mode Words: expected [apple pear] ascending, got %v", got["Words"])
	}
}

func TestModeExcludesNulls(t *testing.T) {
	// The null appears more often than any value, but must never be the mode.
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"V": mustSeries(nil, nil, nil, 7.0)},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1", "2", "3"},
	}
	got := df.Mode()
	if len(got["V"]) != 1 || !valuesEqual(got["V"][0], 7.0) {
		t.Errorf("Mode V: expected [7], got %v", got["V"])
	}
}

func TestModeAllNullColumnIsEmpty(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"V": mustSeries(nil, nil)},
		ColumnOrder: []string{"V"},
		Index:       []string{"0", "1"},
	}
	got := df.Mode()
	modes, ok := got["V"]
	if !ok {
		t.Fatal("Mode should report every column, including all-null ones")
	}
	if len(modes) != 0 {
		t.Errorf("expected no modes for an all-null column, got %v", modes)
	}
}

func TestModeCoversBoolAndNonNumericColumns(t *testing.T) {
	flags, err := collection.NewBoolSeriesFromData([]bool{true, false, true}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Flag": flags,
			"Name": mustSeries("a", "b", "b"),
		},
		ColumnOrder: []string{"Flag", "Name"},
		Index:       []string{"0", "1", "2"},
	}

	got := df.Mode()
	if len(got["Flag"]) != 1 || got["Flag"][0] != true {
		t.Errorf("Mode Flag: expected [true], got %v", got["Flag"])
	}
	if len(got["Name"]) != 1 || got["Name"][0] != "b" {
		t.Errorf("Mode Name: expected [b], got %v", got["Name"])
	}
}

// -----------------------------------------------------------------------------
// IdxMax / IdxMin
// -----------------------------------------------------------------------------

func TestIdxMaxIdxMin(t *testing.T) {
	df := reductionsDF()

	maxLabels := df.IdxMax()
	minLabels := df.IdxMin()

	if maxLabels["Score"] != "r3" || minLabels["Score"] != "r0" {
		t.Errorf("Score: expected max r3 / min r0, got %v / %v", maxLabels["Score"], minLabels["Score"])
	}
	if maxLabels["Age"] != "r3" || minLabels["Age"] != "r0" {
		t.Errorf("Age: expected max r3 / min r0, got %v / %v", maxLabels["Age"], minLabels["Age"])
	}
	// Sparse is 1, null, 2, 4 — the null at r1 is skipped.
	if maxLabels["Sparse"] != "r3" || minLabels["Sparse"] != "r0" {
		t.Errorf("Sparse: expected max r3 / min r0, got %v / %v", maxLabels["Sparse"], minLabels["Sparse"])
	}
	if _, ok := maxLabels["Name"]; ok {
		t.Error("IdxMax should exclude the non-numeric Name column")
	}
}

func TestIdxMaxAgreesWithMaxValue(t *testing.T) {
	df := reductionsDF()
	maxes := df.Max()
	labels := df.IdxMax()

	for col, label := range labels {
		row := -1
		for i, l := range df.Index {
			if l == label {
				row = i
				break
			}
		}
		if row < 0 {
			t.Fatalf("column %s: label %q not present in the index", col, label)
		}
		val, _ := df.Columns[col].At(row)
		f, _ := val.(float64)
		if iv, ok := val.(int); ok {
			f = float64(iv)
		}
		if !approxEqual(f, maxes[col]) {
			t.Errorf("column %s: IdxMax points at %v but Max is %v", col, f, maxes[col])
		}
	}
}

func TestIdxExtremeTiesKeepFirstRow(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"V": mustSeries(5.0, 1.0, 5.0, 1.0)},
		ColumnOrder: []string{"V"},
		Index:       []string{"a", "b", "c", "d"},
	}
	if got := df.IdxMax()["V"]; got != "a" {
		t.Errorf("IdxMax tie: expected first row 'a', got %v", got)
	}
	if got := df.IdxMin()["V"]; got != "b" {
		t.Errorf("IdxMin tie: expected first row 'b', got %v", got)
	}
}

func TestIdxExtremeSkipsNaN(t *testing.T) {
	withNaN, err := collection.NewFloat64SeriesFromData([]float64{1, math.NaN(), 3}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"V": withNaN},
		ColumnOrder: []string{"V"},
		Index:       []string{"a", "b", "c"},
	}
	if got := df.IdxMax()["V"]; got != "c" {
		t.Errorf("IdxMax with NaN: expected 'c', got %v", got)
	}
	if got := df.IdxMin()["V"]; got != "a" {
		t.Errorf("IdxMin with NaN: expected 'a', got %v", got)
	}
}

func TestIdxExtremeOmitsUnlabelableColumns(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"V": mustSeries(1.0, 2.0), "Empty": mustSeries(nil, nil)},
		ColumnOrder: []string{"V", "Empty"},
		Index:       []string{"a", "b"},
	}
	if _, ok := df.IdxMax()["Empty"]; ok {
		t.Error("an all-null column has no label to report and should be omitted")
	}
	if _, ok := df.IdxMin()["Empty"]; ok {
		t.Error("an all-null column has no label to report and should be omitted")
	}
}

func TestIdxExtremeFallsBackToRowNumber(t *testing.T) {
	// Index deliberately shorter than the column.
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"V": mustSeries(1.0, 2.0, 9.0)},
		ColumnOrder: []string{"V"},
		Index:       []string{"a"},
	}
	if got := df.IdxMax()["V"]; got != "2" {
		t.Errorf("expected the row-number fallback '2', got %v", got)
	}
	if got := df.IdxMin()["V"]; got != "a" {
		t.Errorf("expected the index label 'a', got %v", got)
	}
}

// -----------------------------------------------------------------------------
// Any / All
// -----------------------------------------------------------------------------

func TestAnyAllBoolColumns(t *testing.T) {
	mixed, err := collection.NewBoolSeriesFromData([]bool{true, false, true}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	allTrue, err := collection.NewBoolSeriesFromData([]bool{true, true, true}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	allFalse, err := collection.NewBoolSeriesFromData([]bool{false, false, false}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Mixed":    mixed,
			"AllTrue":  allTrue,
			"AllFalse": allFalse,
		},
		ColumnOrder: []string{"Mixed", "AllTrue", "AllFalse"},
		Index:       []string{"0", "1", "2"},
	}

	any := df.Any()
	all := df.All()

	if !any["Mixed"] || all["Mixed"] {
		t.Errorf("Mixed: expected any=true all=false, got any=%v all=%v", any["Mixed"], all["Mixed"])
	}
	if !any["AllTrue"] || !all["AllTrue"] {
		t.Errorf("AllTrue: expected any=true all=true, got any=%v all=%v", any["AllTrue"], all["AllTrue"])
	}
	if any["AllFalse"] || all["AllFalse"] {
		t.Errorf("AllFalse: expected any=false all=false, got any=%v all=%v", any["AllFalse"], all["AllFalse"])
	}
}

func TestAnyAllNumericTruthiness(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Zeros":    mustSeries(0.0, 0, 0.0),
			"SomeZero": mustSeries(0.0, 3.0, 0.0),
			"NonZero":  mustSeries(1, -2, 3),
		},
		ColumnOrder: []string{"Zeros", "SomeZero", "NonZero"},
		Index:       []string{"0", "1", "2"},
	}

	any := df.Any()
	all := df.All()

	if any["Zeros"] || all["Zeros"] {
		t.Errorf("Zeros: expected any=false all=false, got any=%v all=%v", any["Zeros"], all["Zeros"])
	}
	if !any["SomeZero"] || all["SomeZero"] {
		t.Errorf("SomeZero: expected any=true all=false, got any=%v all=%v", any["SomeZero"], all["SomeZero"])
	}
	if !any["NonZero"] || !all["NonZero"] {
		t.Errorf("NonZero: expected any=true all=true (negatives are truthy), got any=%v all=%v", any["NonZero"], all["NonZero"])
	}
}

func TestAnyAllSkipNullsAndNaN(t *testing.T) {
	withNaN, err := collection.NewFloat64SeriesFromData([]float64{math.NaN(), 0, math.NaN()}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	allNull, err := collection.NewBoolSeriesFromData([]bool{false, false}, []bool{true, true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			// 1 is truthy, the null must not make All false.
			"NullTrue": mustSeries(1.0, nil, 2.0),
			"NaNs":     withNaN,
		},
		ColumnOrder: []string{"NullTrue", "NaNs"},
		Index:       []string{"0", "1", "2"},
	}

	if !df.Any()["NullTrue"] || !df.All()["NullTrue"] {
		t.Errorf("NullTrue: expected any=true all=true, got any=%v all=%v", df.Any()["NullTrue"], df.All()["NullTrue"])
	}
	// NaNs are skipped, leaving only the 0, so any=false and all=false.
	if df.Any()["NaNs"] || df.All()["NaNs"] {
		t.Errorf("NaNs: expected any=false all=false, got any=%v all=%v", df.Any()["NaNs"], df.All()["NaNs"])
	}

	emptyDF := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{"Empty": allNull},
		ColumnOrder: []string{"Empty"},
		Index:       []string{"0", "1"},
	}
	// An empty selection: any=false, all=true, as in pandas.
	if emptyDF.Any()["Empty"] {
		t.Error("all-null column: expected any=false")
	}
	if !emptyDF.All()["Empty"] {
		t.Error("all-null column: expected all=true")
	}
}

func TestAnyAllOmitNonBooleanColumns(t *testing.T) {
	df := reductionsDF()
	if _, ok := df.Any()["Name"]; ok {
		t.Error("Any should omit string columns")
	}
	if _, ok := df.All()["Name"]; ok {
		t.Error("All should omit string columns")
	}
	// Numeric columns are still covered.
	keys := make([]string, 0, len(df.Any()))
	for k := range df.Any() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if !strSliceEqual(keys, []string{"Age", "Score", "Sparse"}) {
		t.Errorf("expected Any over [Age Score Sparse], got %v", keys)
	}
}
