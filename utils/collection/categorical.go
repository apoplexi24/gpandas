package collection

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// CategoricalSeries is a memory-efficient series for string values drawn from a
// limited set of categories. Values are stored as small integer codes into a
// shared category list. A code of -1 represents null.
type CategoricalSeries struct {
	mu         sync.RWMutex
	codes      []int32
	categories []string
	catIndex   map[string]int32
}

// NewCategoricalSeriesFromStrings builds a CategoricalSeries from string values
// and an optional null mask (true = null). Categories are discovered in order of
// first appearance.
func NewCategoricalSeriesFromStrings(values []string, mask []bool) (*CategoricalSeries, error) {
	if mask != nil && len(values) != len(mask) {
		return nil, errors.New("values and mask length mismatch")
	}
	s := &CategoricalSeries{
		codes:      make([]int32, len(values)),
		categories: make([]string, 0),
		catIndex:   make(map[string]int32),
	}
	for i, v := range values {
		if mask != nil && mask[i] {
			s.codes[i] = -1
			continue
		}
		s.codes[i] = s.codeFor(v)
	}
	return s, nil
}

// codeFor returns the code for a category, registering it if new. Caller must
// hold no lock (used during construction) or the write lock.
func (s *CategoricalSeries) codeFor(v string) int32 {
	if code, ok := s.catIndex[v]; ok {
		return code
	}
	code := int32(len(s.categories))
	s.categories = append(s.categories, v)
	s.catIndex[v] = code
	return code
}

// Categories returns the distinct categories in code order.
func (s *CategoricalSeries) Categories() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.categories))
	copy(out, s.categories)
	return out
}

// Codes returns a copy of the integer codes (-1 for null).
func (s *CategoricalSeries) Codes() []int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]int32, len(s.codes))
	copy(out, s.codes)
	return out
}

func (s *CategoricalSeries) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.codes)
}

// DType reports string, since categorical values are surfaced as strings.
func (s *CategoricalSeries) DType() reflect.Type {
	return reflect.TypeOf("")
}

func (s *CategoricalSeries) At(i int) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.codes) {
		return nil, errors.New("index out of range")
	}
	if s.codes[i] < 0 {
		return nil, nil
	}
	return s.categories[s.codes[i]], nil
}

func (s *CategoricalSeries) IsNull(i int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.codes) {
		return true
	}
	return s.codes[i] < 0
}

func (s *CategoricalSeries) NullCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, c := range s.codes {
		if c < 0 {
			count++
		}
	}
	return count
}

func (s *CategoricalSeries) Set(i int, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.codes) {
		return errors.New("index out of range")
	}
	if v == nil {
		s.codes[i] = -1
		return nil
	}
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("type mismatch: expected string, got %T", v)
	}
	s.codes[i] = s.codeFor(str)
	return nil
}

func (s *CategoricalSeries) SetNull(i int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.codes) {
		return errors.New("index out of range")
	}
	s.codes[i] = -1
	return nil
}

func (s *CategoricalSeries) Append(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v == nil {
		s.codes = append(s.codes, -1)
		return nil
	}
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("type mismatch: expected string, got %T", v)
	}
	s.codes = append(s.codes, s.codeFor(str))
	return nil
}

func (s *CategoricalSeries) AppendNull() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.codes = append(s.codes, -1)
}

func (s *CategoricalSeries) ValuesCopy() []any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]any, len(s.codes))
	for i, c := range s.codes {
		if c < 0 {
			out[i] = nil
		} else {
			out[i] = s.categories[c]
		}
	}
	return out
}

func (s *CategoricalSeries) MaskCopy() []bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]bool, len(s.codes))
	for i, c := range s.codes {
		out[i] = c < 0
	}
	return out
}

func (s *CategoricalSeries) Slice(start, end int) (Series, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if start < 0 || end > len(s.codes) || start > end {
		return nil, errors.New("invalid slice bounds")
	}
	// Rebuild from the string values to keep categories compact and independent.
	values := make([]string, 0, end-start)
	mask := make([]bool, 0, end-start)
	for i := start; i < end; i++ {
		if s.codes[i] < 0 {
			values = append(values, "")
			mask = append(mask, true)
		} else {
			values = append(values, s.categories[s.codes[i]])
			mask = append(mask, false)
		}
	}
	return NewCategoricalSeriesFromStrings(values, mask)
}
