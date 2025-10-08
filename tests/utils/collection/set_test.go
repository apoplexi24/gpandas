package collection_test

import (
	"testing"

	"github.com/apoplexi24/gpandas/utils/collection"
)

func TestNewSet(t *testing.T) {
	tests := []struct {
		name        string
		initialSize []int
		expectError bool
	}{
		{
			name:        "create empty set",
			initialSize: nil,
			expectError: false,
		},
		{
			name:        "create set with initial size",
			initialSize: []int{5},
			expectError: false,
		},
		{
			name:        "negative initial size",
			initialSize: []int{-1},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := collection.NewSet[int](tt.initialSize...)
			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if s == nil {
					t.Error("expected non-nil set")
				}
			}
		})
	}
}

func TestSetOperations(t *testing.T) {
	t.Run("Add and Has", func(t *testing.T) {
		s, _ := collection.NewSet[int]()

		// Test Add
		if err := s.Add(1); err != nil {
			t.Errorf("failed to add new value: %v", err)
		}

		// Test duplicate Add
		if err := s.Add(1); err == nil {
			t.Error("expected error when adding duplicate value")
		}

		// Test Has
		if !s.Has(1) {
			t.Error("Has returned false for existing value")
		}
		if s.Has(2) {
			t.Error("Has returned true for non-existing value")
		}
	})

	t.Run("AddMulti", func(t *testing.T) {
		s, _ := collection.NewSet[int]()

		// Test adding multiple values
		if err := s.AddMulti(1, 2, 3); err != nil {
			t.Errorf("failed to add multiple values: %v", err)
		}

		// Test adding duplicates
		if err := s.AddMulti(1, 4); err == nil {
			t.Error("expected error when adding duplicate values")
		}
	})

	t.Run("Filter", func(t *testing.T) {
		s, _ := collection.NewSet[int]()
		s.AddMulti(1, 2, 3, 4, 5)

		filtered, err := s.Filter(func(v int) bool {
			return v%2 == 0
		})

		if err != nil {
			t.Errorf("filter failed: %v", err)
		}

		if !filtered.Has(2) || !filtered.Has(4) {
			t.Error("filtered set missing expected values")
		}
		if filtered.Has(1) || filtered.Has(3) || filtered.Has(5) {
			t.Error("filtered set contains unexpected values")
		}
	})

	t.Run("Set Operations", func(t *testing.T) {
		s1, _ := collection.NewSet[int]()
		s2, _ := collection.NewSet[int]()

		s1.AddMulti(1, 2, 3)
		s2.AddMulti(2, 3, 4)

		// Test Union
		union, err := s1.Union(s2)
		if err != nil {
			t.Errorf("union failed: %v", err)
		}
		for _, v := range []int{1, 2, 3, 4} {
			if !union.Has(v) {
				t.Errorf("union missing value: %d", v)
			}
		}

		// Test Intersect
		intersect, err := s1.Intersect(s2)
		if err != nil {
			t.Errorf("intersect failed: %v", err)
		}
		for _, v := range []int{2, 3} {
			if !intersect.Has(v) {
				t.Errorf("intersect missing value: %d", v)
			}
		}

		// Test Difference
		diff, err := s1.Difference(s2)
		if err != nil {
			t.Errorf("difference failed: %v", err)
		}
		if !diff.Has(1) || diff.Has(2) || diff.Has(3) || diff.Has(4) {
			t.Error("difference contains incorrect values")
		}
	})

	t.Run("Compare", func(t *testing.T) {
		s1, _ := collection.NewSet[int]()
		s2, _ := collection.NewSet[int]()
		s3, _ := collection.NewSet[int]()

		s1.AddMulti(1, 2, 3)
		s2.AddMulti(1, 2, 3)
		s3.AddMulti(1, 2, 4)

		// Test equal sets
		equal, diff := s1.Compare(s2)
		if !equal || diff != nil {
			t.Error("Compare failed for equal sets")
		}

		// Test unequal sets
		equal, _ = s1.Compare(s3)
		if equal {
			t.Error("Compare failed for unequal sets")
		}
	})
}

func TestSetConversions(t *testing.T) {
	t.Run("ToSlice", func(t *testing.T) {
		s, _ := collection.NewSet[int]()
		s.AddMulti(1, 2, 3)

		slice, err := collection.ToSlice(s)
		if err != nil {
			t.Errorf("ToSlice failed: %v", err)
		}

		if len(slice) != 3 {
			t.Errorf("expected slice length 3, got %d", len(slice))
		}

		sliceSet, _ := collection.ToSet(slice)
		equal, _ := s.Compare(sliceSet)
		if !equal {
			t.Error("set->slice->set conversion failed to preserve values")
		}
	})

	t.Run("ToSet", func(t *testing.T) {
		slice := []int{1, 2, 3, 3} // Note duplicate

		set, err := collection.ToSet(slice)
		if err != nil {
			t.Errorf("ToSet failed: %v", err)
		}

		if len(set) != 3 {
			t.Error("ToSet failed to remove duplicates")
		}

		for _, v := range []int{1, 2, 3} {
			if !set.Has(v) {
				t.Errorf("set missing value: %d", v)
			}
		}
	})
}
