package collection_test

import (
	"reflect"
	"testing"

	"github.com/apoplexi24/gpandas/utils/collection"
)

func TestSeriesBasic(t *testing.T) {
	s, err := collection.NewSeriesWithData(nil, []any{1, 2, 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Len() != 3 {
		t.Fatalf("expected len 3, got %d", s.Len())
	}
	// Integers are stored as int64
	if s.DType() != reflect.TypeOf(int64(0)) {
		t.Fatalf("expected dtype int64, got %v", s.DType())
	}
	v, _ := s.At(1)
	if v.(int64) != 2 {
		t.Fatalf("expected value 2 at index 1, got %v", v)
	}
}

func TestSeriesAppendAndSet(t *testing.T) {
	s := collection.NewSeriesOfType(reflect.TypeOf(""), 0)
	if err := s.Append("a"); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if err := s.Append("b"); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if s.Len() != 2 {
		t.Fatalf("expected len 2, got %d", s.Len())
	}
	if err := s.Set(1, "c"); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	v, _ := s.At(1)
	if v.(string) != "c" {
		t.Fatalf("expected value 'c' at index 1, got %v", v)
	}
}

func TestSeriesTypeEnforcement(t *testing.T) {
	s := collection.NewSeriesOfType(reflect.TypeOf(true), 0)
	if err := s.Append(true); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if err := s.Append(1); err == nil {
		t.Fatalf("expected type mismatch error when appending int to bool series")
	}
}

func TestSeriesNullHandling(t *testing.T) {
	// Test creating series with null values
	s, err := collection.NewSeriesWithData(nil, []any{1.0, nil, 3.0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Test length
	if s.Len() != 3 {
		t.Fatalf("expected len 3, got %d", s.Len())
	}

	// Test IsNull
	if s.IsNull(0) {
		t.Error("expected index 0 to not be null")
	}
	if !s.IsNull(1) {
		t.Error("expected index 1 to be null")
	}
	if s.IsNull(2) {
		t.Error("expected index 2 to not be null")
	}

	// Test NullCount
	if s.NullCount() != 1 {
		t.Errorf("expected null count 1, got %d", s.NullCount())
	}

	// Test At returns nil for null values
	v, err := s.At(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != nil {
		t.Errorf("expected nil for null value, got %v", v)
	}
}

func TestSeriesAppendNull(t *testing.T) {
	s := collection.NewFloat64Series(0)
	
	// Append some values
	s.Append(1.0)
	s.AppendNull()
	s.Append(3.0)

	if s.Len() != 3 {
		t.Fatalf("expected len 3, got %d", s.Len())
	}

	if !s.IsNull(1) {
		t.Error("expected index 1 to be null after AppendNull")
	}

	if s.NullCount() != 1 {
		t.Errorf("expected null count 1, got %d", s.NullCount())
	}
}

func TestSeriesSetNull(t *testing.T) {
	s, _ := collection.NewFloat64SeriesFromData([]float64{1.0, 2.0, 3.0}, nil)

	// Set a value to null
	err := s.SetNull(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !s.IsNull(1) {
		t.Error("expected index 1 to be null after SetNull")
	}

	// The value at index 1 should now return nil
	v, _ := s.At(1)
	if v != nil {
		t.Errorf("expected nil for null value, got %v", v)
	}
}

func TestSeriesMaskCopy(t *testing.T) {
	s, _ := collection.NewSeriesWithData(nil, []any{1, nil, 3})

	mask := s.MaskCopy()
	if len(mask) != 3 {
		t.Fatalf("expected mask len 3, got %d", len(mask))
	}
	if mask[0] != false {
		t.Error("expected mask[0] to be false")
	}
	if mask[1] != true {
		t.Error("expected mask[1] to be true")
	}
	if mask[2] != false {
		t.Error("expected mask[2] to be false")
	}

	// Modifying the copy should not affect the original
	mask[0] = true
	if s.IsNull(0) {
		t.Error("modifying MaskCopy should not affect original series")
	}
}

func TestSeriesValuesCopy(t *testing.T) {
	s, _ := collection.NewSeriesWithData(nil, []any{"a", nil, "c"})

	values := s.ValuesCopy()
	if len(values) != 3 {
		t.Fatalf("expected values len 3, got %d", len(values))
	}
	if values[0].(string) != "a" {
		t.Errorf("expected values[0] to be 'a', got %v", values[0])
	}
	if values[1] != nil {
		t.Errorf("expected values[1] to be nil, got %v", values[1])
	}
	if values[2].(string) != "c" {
		t.Errorf("expected values[2] to be 'c', got %v", values[2])
	}
}

func TestTypedSeriesAccess(t *testing.T) {
	// Test Float64Series typed access
	fs, _ := collection.NewFloat64SeriesFromData([]float64{1.0, 2.0, 3.0}, nil)
	fv, _ := fs.Float64Value(1)
	if fv != 2.0 {
		t.Errorf("expected Float64Value(1) to be 2.0, got %v", fv)
	}

	fvals := fs.Float64Values()
	if len(fvals) != 3 || fvals[0] != 1.0 {
		t.Errorf("expected Float64Values to return [1.0, 2.0, 3.0], got %v", fvals)
	}

	// Test Int64Series typed access
	is, _ := collection.NewInt64SeriesFromData([]int64{10, 20, 30}, nil)
	iv, _ := is.Int64Value(1)
	if iv != 20 {
		t.Errorf("expected Int64Value(1) to be 20, got %v", iv)
	}

	// Test StringSeries typed access
	ss, _ := collection.NewStringSeriesFromData([]string{"a", "b", "c"}, nil)
	sv, _ := ss.StringValue(1)
	if sv != "b" {
		t.Errorf("expected StringValue(1) to be 'b', got %v", sv)
	}

	// Test BoolSeries typed access
	bs, _ := collection.NewBoolSeriesFromData([]bool{true, false, true}, nil)
	bv, _ := bs.BoolValue(1)
	if bv != false {
		t.Errorf("expected BoolValue(1) to be false, got %v", bv)
	}
}

func TestNewSeriesFromValues(t *testing.T) {
	// Test with mixed int types (all should convert to int64)
	s1, err := collection.NewSeriesFromValues([]any{1, 2, 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s1.DType() != reflect.TypeOf(int64(0)) {
		t.Errorf("expected dtype int64, got %v", s1.DType())
	}

	// Test with float64 values
	s2, err := collection.NewSeriesFromValues([]any{1.0, 2.0, 3.0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s2.DType() != reflect.TypeOf(float64(0)) {
		t.Errorf("expected dtype float64, got %v", s2.DType())
	}

	// Test with string values
	s3, err := collection.NewSeriesFromValues([]any{"a", "b", "c"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s3.DType() != reflect.TypeOf("") {
		t.Errorf("expected dtype string, got %v", s3.DType())
	}

	// Test with bool values
	s4, err := collection.NewSeriesFromValues([]any{true, false, true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s4.DType() != reflect.TypeOf(true) {
		t.Errorf("expected dtype bool, got %v", s4.DType())
	}

	// Test with all nil values
	s5, err := collection.NewSeriesFromValues([]any{nil, nil, nil})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s5.NullCount() != 3 {
		t.Errorf("expected null count 3, got %d", s5.NullCount())
	}
}
