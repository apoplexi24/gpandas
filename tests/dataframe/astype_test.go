package dataframe_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func TestAsType(t *testing.T) {
	t.Run("string to float", func(t *testing.T) {
		s, _ := collection.NewStringSeriesFromData([]string{"1.5", "2.0", "3.25"}, nil)
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"V": s},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1", "2"},
		}
		result, err := df.AsType("V", dataframe.FloatCol{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Columns["V"].DType().Kind() != reflect.Float64 {
			t.Fatalf("expected float64 series, got %v", result.Columns["V"].DType())
		}
		v0, _ := result.Columns["V"].At(0)
		if !valuesEqual(v0, 1.5) {
			t.Errorf("expected 1.5, got %v", v0)
		}
	})

	t.Run("string to int", func(t *testing.T) {
		s, _ := collection.NewStringSeriesFromData([]string{"10", "20", "30"}, nil)
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"V": s},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1", "2"},
		}
		result, err := df.AsType("V", dataframe.IntCol{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Columns["V"].DType().Kind() != reflect.Int64 {
			t.Fatalf("expected int64 series, got %v", result.Columns["V"].DType())
		}
		v1, _ := result.Columns["V"].At(1)
		if !valuesEqual(v1, 20) {
			t.Errorf("expected 20, got %v", v1)
		}
	})

	t.Run("float to int truncates", func(t *testing.T) {
		s, _ := collection.NewFloat64SeriesFromData([]float64{1.9, 2.1, 3.5}, nil)
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"V": s},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1", "2"},
		}
		result, _ := df.AsType("V", dataframe.IntCol{})
		v0, _ := result.Columns["V"].At(0)
		if !valuesEqual(v0, 1) {
			t.Errorf("expected truncated 1, got %v", v0)
		}
	})

	t.Run("int to string", func(t *testing.T) {
		s, _ := collection.NewInt64SeriesFromData([]int64{1, 2, 3}, nil)
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"V": s},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1", "2"},
		}
		result, _ := df.AsType("V", dataframe.StringCol{})
		v0, _ := result.Columns["V"].At(0)
		if v0 != "1" {
			t.Errorf("expected \"1\", got %v", v0)
		}
	})

	t.Run("string to bool", func(t *testing.T) {
		s, _ := collection.NewStringSeriesFromData([]string{"true", "false", "1"}, nil)
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"V": s},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1", "2"},
		}
		result, _ := df.AsType("V", dataframe.BoolCol{})
		v0, _ := result.Columns["V"].At(0)
		v1, _ := result.Columns["V"].At(1)
		if v0 != true || v1 != false {
			t.Errorf("expected [true, false], got [%v, %v]", v0, v1)
		}
	})

	t.Run("string alias", func(t *testing.T) {
		s, _ := collection.NewStringSeriesFromData([]string{"1.5", "2.5"}, nil)
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"V": s},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1"},
		}
		result, err := df.AsType("V", "float64")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Columns["V"].DType().Kind() != reflect.Float64 {
			t.Errorf("expected float64 via alias")
		}
	})

	t.Run("nulls preserved", func(t *testing.T) {
		s, _ := collection.NewStringSeriesFromData([]string{"1", "", "3"}, []bool{false, true, false})
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"V": s},
			ColumnOrder: []string{"V"},
			Index:       []string{"0", "1", "2"},
		}
		result, err := df.AsType("V", dataframe.IntCol{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !result.Columns["V"].IsNull(1) {
			t.Error("expected null preserved")
		}
	})

	t.Run("unparseable value errors", func(t *testing.T) {
		s, _ := collection.NewStringSeriesFromData([]string{"abc"}, nil)
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"V": s},
			ColumnOrder: []string{"V"},
			Index:       []string{"0"},
		}
		if _, err := df.AsType("V", dataframe.FloatCol{}); err == nil {
			t.Error("expected error converting 'abc' to float")
		}
	})

	t.Run("missing column and bad target", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{"V": mustSeries(1)},
			ColumnOrder: []string{"V"},
			Index:       []string{"0"},
		}
		if _, err := df.AsType("Nope", dataframe.IntCol{}); err == nil {
			t.Error("expected error for missing column")
		}
		if _, err := df.AsType("V", 42); err == nil {
			t.Error("expected error for unsupported target type")
		}
	})
}

func TestDTypes(t *testing.T) {
	fs, _ := collection.NewFloat64SeriesFromData([]float64{1}, nil)
	is, _ := collection.NewInt64SeriesFromData([]int64{1}, nil)
	ss, _ := collection.NewStringSeriesFromData([]string{"a"}, nil)
	bs, _ := collection.NewBoolSeriesFromData([]bool{true}, nil)
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"F": fs, "I": is, "S": ss, "B": bs,
		},
		ColumnOrder: []string{"F", "I", "S", "B"},
		Index:       []string{"0"},
	}
	types := df.DTypes()
	if types["F"] != "float64" || types["I"] != "int64" || types["S"] != "string" || types["B"] != "bool" {
		t.Errorf("unexpected dtypes: %v", types)
	}
}

func TestInfo(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name":  mustSeries("a", nil, "c"),
			"Score": mustSeries(1.0, 2.0, 3.0),
		},
		ColumnOrder: []string{"Name", "Score"},
		Index:       []string{"0", "1", "2"},
	}
	info := df.Info()
	if !strings.Contains(info, "3 rows x 2 columns") {
		t.Errorf("info missing shape line: %s", info)
	}
	if !strings.Contains(info, "Name") || !strings.Contains(info, "Score") {
		t.Errorf("info missing column names: %s", info)
	}
	// Name has 2 non-null (one null)
	if !strings.Contains(info, "2 non-null") {
		t.Errorf("info missing non-null count: %s", info)
	}
}
