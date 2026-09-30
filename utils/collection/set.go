package collection

import "errors"

// Set is an alias for a map representing an unordered collection of unique elements of type T.
// Type T must be comparable.
type Set[T comparable] map[T]struct{}

// NewSet creates and initializes a new empty Set.
// If an initial size is provided, the set will be pre-allocated with that size.
// The initial size must be a non-negative integer.
func NewSet[T comparable](initialSize ...int) (Set[T], error) {
	if len(initialSize) > 0 {
		if initialSize[0] < 0 {
			return nil, errors.New("initialSize must be a non-negative integer")
		}
		return make(Set[T], initialSize[0]), nil
	}
	return make(Set[T]), nil
}

// Has returns true if the value exists in the set.
func (s Set[T]) Has(v T) bool {
	_, ok := s[v]
	return ok
}

// Add inserts a value into the set only if it does not already exist.
func (s Set[T]) Add(v T) error {
	if !s.Has(v) {
		s[v] = struct{}{}
		return nil
	}
	return errors.New("value already exists in the set")
}

// AddMulti inserts multiple values into the set only if they do not already exist.
// It iterates over the provided values and attempts to add each one to the set.
// If any value already exists in the set, the function returns an error.
// If all values are successfully added, the function returns nil.
func (s Set[T]) AddMulti(val ...T) error {
	for _, v := range val {
		err := s.Add(v)
		if err != nil {
			return err
		}
	}
	return nil
}

// FilterFunc defines a function type that returns true if a value should be included in filtered results.
type FilterFunc[T any] func(v T) bool

// Filter applies a filter function to each element of the set and returns a new set containing only the elements that pass the filter.
// The filter function P is applied to each element of the set. If the function returns true for an element, it is included in the resulting set.
// If the function returns false, the element is not included.
// The function returns a new Set containing the filtered elements and an error if the operation fails.
func (s Set[T]) Filter(P FilterFunc[T]) (Set[T], error) {
	res, err := NewSet[T]()
	if err != nil {
		return nil, err
	}
	for v := range s {
		if !P(v) {
			continue
		}
		res[v] = struct{}{}
	}
	return res, nil
}

// Union creates a new Set containing all elements from both the current set and the input set.
// It iterates over the elements of both sets and adds them to a new Set, ensuring no duplicates.
// The resulting Set contains all unique elements from both input sets in an arbitrary order.
//
// Parameters:
//
//	s2: The input Set[T] to union with the current set.
//
// Returns:
//
//	A Set of type Set[T] containing all unique elements from both input sets.
//	An error if the operation fails.
func (s Set[T]) Union(s2 Set[T]) (Set[T], error) {
	res, err := NewSet[T]()
	if err != nil {
		return nil, err
	}
	for v := range s {
		res[v] = struct{}{}
	}
	for v := range s2 {
		res[v] = struct{}{}
	}
	return res, nil
}

// Slice converts a Set of type T into a slice of type []T.
// It iterates over the elements of the set and appends them to a slice.
// The resulting slice contains all the elements from the set in an arbitrary order.
//
// Parameters:
//
//	s: A Set[T] from which to create the slice.
//
// Returns:
//
//	A slice of type []T containing all elements from the input set.
func ToSlice[T comparable](s Set[T]) ([]T, error) {
	result_slice := make([]T, 0, len(s))
	for k := range s {
		result_slice = append(result_slice, k)
	}
	return result_slice, nil
}

// ToSet converts a slice of type []T into a Set of type Set[T].
// It iterates over the elements of the slice and adds them to a Set.
// The resulting Set contains all the elements from the input slice in an arbitrary order.
//
// Parameters:
//
//	slice: A slice of type []T from which to create the Set.
//
// Returns:
//
//	A Set of type Set[T] containing all elements from the input slice.
func ToSet[T comparable](slice []T) (Set[T], error) {
	result_set, err := NewSet[T]()
	if err != nil {
		return nil, err
	}
	for i := range slice {
		result_set[slice[i]] = struct{}{}
	}
	return result_set, nil
}

// Intersect creates a new Set containing elements that exist in both the current set and the input set.
// It iterates over the elements of the current set and checks if they exist in the input set.
// If an element exists in both sets, it is added to the resulting set.
// The resulting Set contains all the elements that are common to both input sets in an arbitrary order.
//
// Parameters:
//
//	s2: The input Set[T] to intersect with the current set.
//
// Returns:
//
//	A Set of type Set[T] containing all elements that are common to both input sets.
//	An error if the operation fails.
func (s Set[T]) Intersect(s2 Set[T]) (Set[T], error) {
	res, err := NewSet[T]()
	if err != nil {
		return nil, err
	}
	for v := range s {
		if _, ok := s2[v]; !ok {
			continue
		}
		res[v] = struct{}{}
	}
	return res, nil
}

// Difference creates a new Set containing elements that exist in the current set but not in the input set.
// It iterates over the elements of the current set and checks if they do not exist in the input set.
// If an element does not exist in the input set, it is added to the resulting set.
// The resulting Set contains all the elements that are unique to the current set in an arbitrary order.
//
// Parameters:
//
//	s2: The input Set[T] to find the difference with the current set.
//
// Returns:
//
//	A Set of type Set[T] containing all elements that are unique to the current set.
//	An error if the operation fails.
func (s Set[T]) Difference(s2 Set[T]) (Set[T], error) {
	res, err := NewSet[T]()
	if err != nil {
		return nil, err
	}
	for v := range s {
		if _, ok := s2[v]; ok {
			continue
		}
		res[v] = struct{}{}
	}
	return res, nil
}

// Compare checks if two sets are equal by comparing their lengths and ensuring all elements in the first set exist in the second set.
// It first checks if the lengths of the two sets are equal. If not, it returns false and nil.
// Then, it iterates over the elements of the first set and checks if each element exists in the second set.
// If an element does not exist in the second set, it returns false and the non-existent element.
// If all elements in the first set exist in the second set, it returns true and nil.
//
// Parameters:
//
//	s2: The input Set[T] to compare with the current set.
//
// Returns:
//
//	A boolean indicating if the sets are equal.
//	An any value representing the first element that does not exist in the second set if the sets are not equal, or nil if they are equal.
func (s Set[T]) Compare(s2 Set[T]) (bool, any) {
	// Check if lengths are the same
	if len(s) != len(s2) {
		return false, nil
	}

	// Check if all keys in s are in s2
	for item := range s {
		if _, exists := s2[item]; !exists {
			return false, item
		}
	}

	return true, nil
}
