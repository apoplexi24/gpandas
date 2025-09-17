package collection

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// Series is a concurrency-safe, homogeneously-typed vector of values.
// Values are stored as []any but enforced to share the same concrete type (dtype).
type Series struct {
	mu    sync.RWMutex
	dtype reflect.Type
	data  []any
}

// NewSeriesOfType creates an empty Series with a declared dtype and optional capacity.
func NewSeriesOfType(dtype reflect.Type, capacity int) *Series {
	return &Series{dtype: dtype, data: make([]any, 0, capacity)}
}

// NewSeriesWithData creates a Series from provided values. If dtype is nil, it is inferred
// from the first non-nil value and then enforced across all values.
func NewSeriesWithData(dtype reflect.Type, values []any) (*Series, error) {
	if dtype == nil {
		for i := range values {
			if values[i] != nil {
				dtype = reflect.TypeOf(values[i])
				break
			}
		}
	}

	s := &Series{dtype: dtype, data: make([]any, len(values))}
	for i := range values {
		if values[i] != nil && s.dtype != nil && reflect.TypeOf(values[i]) != s.dtype {
			return nil, fmt.Errorf("type mismatch at index %d: expected %v, got %v", i, s.dtype, reflect.TypeOf(values[i]))
		}
		s.data[i] = values[i]
	}
	return s, nil
}

// Len returns the number of elements in the Series.
func (s *Series) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// DType returns the element concrete type of the Series.
func (s *Series) DType() reflect.Type {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dtype
}

// At returns the value at index i.
func (s *Series) At(i int) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.data) {
		return nil, errors.New("index out of range")
	}
	return s.data[i], nil
}

// MustAt returns the value at index i and panics if out of range. Intended for internal use.
func (s *Series) MustAt(i int) any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data[i]
}

// Append adds a value to the Series, enforcing dtype.
func (s *Series) Append(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dtype == nil && v != nil {
		s.dtype = reflect.TypeOf(v)
	}
	if v != nil && s.dtype != nil && reflect.TypeOf(v) != s.dtype {
		return fmt.Errorf("type mismatch: expected %v, got %v", s.dtype, reflect.TypeOf(v))
	}
	s.data = append(s.data, v)
	return nil
}

// Set sets the value at index i, enforcing dtype.
func (s *Series) Set(i int, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}
	if s.dtype == nil && v != nil {
		s.dtype = reflect.TypeOf(v)
	}
	if v != nil && s.dtype != nil && reflect.TypeOf(v) != s.dtype {
		return fmt.Errorf("type mismatch: expected %v, got %v", s.dtype, reflect.TypeOf(v))
	}
	s.data[i] = v
	return nil
}

// ValuesCopy returns a shallow copy of the underlying data slice.
func (s *Series) ValuesCopy() []any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]any, len(s.data))
	copy(out, s.data)
	return out
}
