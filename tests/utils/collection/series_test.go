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
	if s.DType() != reflect.TypeOf(1) {
		t.Fatalf("expected dtype int, got %v", s.DType())
	}
	v, _ := s.At(1)
	if v.(int) != 2 {
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
