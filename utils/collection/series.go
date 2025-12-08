package collection

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// Series is the interface for all typed series implementations.
// Each implementation stores data in a strongly-typed slice with a boolean mask for null values.
type Series interface {
	// Len returns the number of elements in the series.
	Len() int

	// DType returns the reflect.Type of the elements in the series.
	DType() reflect.Type

	// At returns the value at index i. Returns nil if the value is null.
	At(i int) (any, error)

	// IsNull returns true if the value at index i is null.
	IsNull(i int) bool

	// NullCount returns the number of null values in the series.
	NullCount() int

	// Set sets the value at index i. Use nil to set a null value.
	Set(i int, v any) error

	// SetNull sets the value at index i to null.
	SetNull(i int) error

	// Append adds a value to the end of the series. Use nil to append a null value.
	Append(v any) error

	// AppendNull appends a null value to the series.
	AppendNull()

	// ValuesCopy returns a shallow copy of the underlying data as []any.
	// Null values are represented as nil.
	ValuesCopy() []any

	// MaskCopy returns a copy of the null mask.
	MaskCopy() []bool

	// Slice returns a new Series containing elements from start (inclusive) to end (exclusive).
	Slice(start, end int) (Series, error)
}

// NewSeriesOfType creates a new Series based on the provided reflect.Type.
func NewSeriesOfType(t reflect.Type, capacity int) Series {
	switch t.Kind() {
	case reflect.Float64:
		return NewFloat64Series(capacity)
	case reflect.Int64, reflect.Int:
		return NewInt64Series(capacity)
	case reflect.String:
		return NewStringSeries(capacity)
	case reflect.Bool:
		return NewBoolSeries(capacity)
	default:
		// Fallback to AnySeries
		return NewAnySeries(capacity)
	}
}

// NewSeriesOfTypeWithSize creates a new Series with the specified size (length).
func NewSeriesOfTypeWithSize(t reflect.Type, size int) Series {
	switch t.Kind() {
	case reflect.Float64:
		s, _ := NewFloat64SeriesFromData(make([]float64, size), nil)
		return s
	case reflect.Int64, reflect.Int:
		s, _ := NewInt64SeriesFromData(make([]int64, size), nil)
		return s
	case reflect.String:
		s, _ := NewStringSeriesFromData(make([]string, size), nil)
		return s
	case reflect.Bool:
		s, _ := NewBoolSeriesFromData(make([]bool, size), nil)
		return s
	default:
		s, _ := NewAnySeriesFromData(make([]any, size), nil)
		return s
	}
}

// NewSeriesWithData creates a typed series from a slice of any values.
// It attempts to convert values to the target type.
func NewSeriesWithData(t reflect.Type, values []any) (Series, error) {
	n := len(values)
	s := NewSeriesOfType(t, n)

	for _, v := range values {
		if v == nil {
			s.AppendNull()
		} else {
			if err := s.Append(v); err != nil {
				return nil, err
			}
		}
	}
	return s, nil
}

// -----------------------------------------------------------------------------
// AnySeries
// -----------------------------------------------------------------------------

// AnySeries is a generic series for any values with null support.
type AnySeries struct {
	mu   sync.RWMutex
	data []any
	mask []bool // true = null
}

// NewAnySeries creates a new empty AnySeries with optional capacity.
func NewAnySeries(capacity int) *AnySeries {
	return &AnySeries{
		data: make([]any, 0, capacity),
		mask: make([]bool, 0, capacity),
	}
}

// NewAnySeriesFromData creates an AnySeries from values and mask.
func NewAnySeriesFromData(data []any, mask []bool) (*AnySeries, error) {
	if mask != nil && len(data) != len(mask) {
		return nil, errors.New("data and mask length mismatch")
	}
	dataCopy := make([]any, len(data))
	copy(dataCopy, data)

	var maskCopy []bool
	if mask != nil {
		maskCopy = make([]bool, len(mask))
		copy(maskCopy, mask)
	} else {
		maskCopy = make([]bool, len(data))
	}

	return &AnySeries{data: dataCopy, mask: maskCopy}, nil
}

func (s *AnySeries) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *AnySeries) DType() reflect.Type {
	return reflect.TypeOf((*any)(nil)).Elem()
}

func (s *AnySeries) At(i int) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.data) {
		return nil, errors.New("index out of range")
	}
	if s.mask[i] {
		return nil, nil
	}
	return s.data[i], nil
}

func (s *AnySeries) IsNull(i int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.mask) {
		return true
	}
	return s.mask[i]
}

func (s *AnySeries) NullCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, isNull := range s.mask {
		if isNull {
			count++
		}
	}
	return count
}

func (s *AnySeries) Set(i int, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}
	if v == nil {
		s.mask[i] = true
		s.data[i] = nil
		return nil
	}
	s.data[i] = v
	s.mask[i] = false
	return nil
}

func (s *AnySeries) SetNull(i int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}
	s.mask[i] = true
	s.data[i] = nil
	return nil
}

func (s *AnySeries) Append(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v == nil {
		s.data = append(s.data, nil)
		s.mask = append(s.mask, true)
		return nil
	}
	s.data = append(s.data, v)
	s.mask = append(s.mask, false)
	return nil
}

func (s *AnySeries) AppendNull() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append(s.data, nil)
	s.mask = append(s.mask, true)
}

func (s *AnySeries) ValuesCopy() []any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]any, len(s.data))
	for i, v := range s.data {
		if s.mask[i] {
			out[i] = nil
		} else {
			out[i] = v
		}
	}
	return out
}

func (s *AnySeries) MaskCopy() []bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]bool, len(s.mask))
	copy(out, s.mask)
	return out
}

func (s *AnySeries) Slice(start, end int) (Series, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if start < 0 || end > len(s.data) || start > end {
		return nil, errors.New("invalid slice bounds")
	}

	newData := make([]any, end-start)
	copy(newData, s.data[start:end])

	newMask := make([]bool, end-start)
	copy(newMask, s.mask[start:end])

	return &AnySeries{
		data: newData,
		mask: newMask,
	}, nil
}

// -----------------------------------------------------------------------------
// Float64Series
// -----------------------------------------------------------------------------

// Float64Series is a high-performance series for float64 values with null support.
type Float64Series struct {
	mu   sync.RWMutex
	data []float64
	mask []bool // true = null
}

// NewFloat64Series creates a new empty Float64Series with optional capacity.
func NewFloat64Series(capacity int) *Float64Series {
	return &Float64Series{
		data: make([]float64, 0, capacity),
		mask: make([]bool, 0, capacity),
	}
}

// NewFloat64SeriesFromData creates a Float64Series from values and mask.
// If mask is nil, all values are treated as non-null.
func NewFloat64SeriesFromData(data []float64, mask []bool) (*Float64Series, error) {
	if mask != nil && len(data) != len(mask) {
		return nil, errors.New("data and mask length mismatch")
	}
	dataCopy := make([]float64, len(data))
	copy(dataCopy, data)

	var maskCopy []bool
	if mask != nil {
		maskCopy = make([]bool, len(mask))
		copy(maskCopy, mask)
	} else {
		maskCopy = make([]bool, len(data))
	}

	return &Float64Series{data: dataCopy, mask: maskCopy}, nil
}

func (s *Float64Series) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *Float64Series) DType() reflect.Type {
	return reflect.TypeOf(float64(0))
}

func (s *Float64Series) At(i int) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.data) {
		return nil, errors.New("index out of range")
	}
	if s.mask[i] {
		return nil, nil
	}
	return s.data[i], nil
}

func (s *Float64Series) IsNull(i int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.mask) {
		return true
	}
	return s.mask[i]
}

func (s *Float64Series) NullCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, isNull := range s.mask {
		if isNull {
			count++
		}
	}
	return count
}

func (s *Float64Series) Set(i int, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}
	if v == nil {
		s.mask[i] = true
		s.data[i] = 0
		return nil
	}
	val, ok := v.(float64)
	if !ok {
		return fmt.Errorf("type mismatch: expected float64, got %T", v)
	}
	s.data[i] = val
	s.mask[i] = false
	return nil
}

func (s *Float64Series) SetNull(i int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}
	s.mask[i] = true
	s.data[i] = 0
	return nil
}

func (s *Float64Series) Append(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v == nil {
		s.data = append(s.data, 0)
		s.mask = append(s.mask, true)
		return nil
	}
	val, ok := v.(float64)
	if !ok {
		return fmt.Errorf("type mismatch: expected float64, got %T", v)
	}
	s.data = append(s.data, val)
	s.mask = append(s.mask, false)
	return nil
}

func (s *Float64Series) AppendNull() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append(s.data, 0)
	s.mask = append(s.mask, true)
}

func (s *Float64Series) ValuesCopy() []any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]any, len(s.data))
	for i, v := range s.data {
		if s.mask[i] {
			out[i] = nil
		} else {
			out[i] = v
		}
	}
	return out
}

func (s *Float64Series) MaskCopy() []bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]bool, len(s.mask))
	copy(out, s.mask)
	return out
}

func (s *Float64Series) Slice(start, end int) (Series, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if start < 0 || end > len(s.data) || start > end {
		return nil, errors.New("invalid slice bounds")
	}

	newData := make([]float64, end-start)
	copy(newData, s.data[start:end])

	newMask := make([]bool, end-start)
	copy(newMask, s.mask[start:end])

	return &Float64Series{
		data: newData,
		mask: newMask,
	}, nil
}

// Float64Value returns the raw float64 value at index i (ignores null mask).
func (s *Float64Series) Float64Value(i int) (float64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.data) {
		return 0, errors.New("index out of range")
	}
	return s.data[i], nil
}

// Float64Values returns a copy of the raw float64 data slice.
func (s *Float64Series) Float64Values() []float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]float64, len(s.data))
	copy(out, s.data)
	return out
}

// -----------------------------------------------------------------------------
// Int64Series
// -----------------------------------------------------------------------------

// Int64Series is a high-performance series for int64 values with null support.
type Int64Series struct {
	mu   sync.RWMutex
	data []int64
	mask []bool // true = null
}

// NewInt64Series creates a new empty Int64Series with optional capacity.
func NewInt64Series(capacity int) *Int64Series {
	return &Int64Series{
		data: make([]int64, 0, capacity),
		mask: make([]bool, 0, capacity),
	}
}

// NewInt64SeriesFromData creates an Int64Series from values and mask.
func NewInt64SeriesFromData(data []int64, mask []bool) (*Int64Series, error) {
	if mask != nil && len(data) != len(mask) {
		return nil, errors.New("data and mask length mismatch")
	}
	dataCopy := make([]int64, len(data))
	copy(dataCopy, data)

	var maskCopy []bool
	if mask != nil {
		maskCopy = make([]bool, len(mask))
		copy(maskCopy, mask)
	} else {
		maskCopy = make([]bool, len(data))
	}

	return &Int64Series{data: dataCopy, mask: maskCopy}, nil
}

func (s *Int64Series) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *Int64Series) DType() reflect.Type {
	return reflect.TypeOf(int64(0))
}

func (s *Int64Series) At(i int) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.data) {
		return nil, errors.New("index out of range")
	}
	if s.mask[i] {
		return nil, nil
	}
	return s.data[i], nil
}

func (s *Int64Series) IsNull(i int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.mask) {
		return true
	}
	return s.mask[i]
}

func (s *Int64Series) NullCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, isNull := range s.mask {
		if isNull {
			count++
		}
	}
	return count
}

func (s *Int64Series) Set(i int, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}
	if v == nil {
		s.mask[i] = true
		s.data[i] = 0
		return nil
	}
	val, ok := v.(int64)
	if !ok {
		return fmt.Errorf("type mismatch: expected int64, got %T", v)
	}
	s.data[i] = val
	s.mask[i] = false
	return nil
}

func (s *Int64Series) SetNull(i int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}
	s.mask[i] = true
	s.data[i] = 0
	return nil
}

func (s *Int64Series) Append(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v == nil {
		s.data = append(s.data, 0)
		s.mask = append(s.mask, true)
		return nil
	}
	val, ok := v.(int64)
	if !ok {
		return fmt.Errorf("type mismatch: expected int64, got %T", v)
	}
	s.data = append(s.data, val)
	s.mask = append(s.mask, false)
	return nil
}

func (s *Int64Series) AppendNull() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append(s.data, 0)
	s.mask = append(s.mask, true)
}

func (s *Int64Series) ValuesCopy() []any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]any, len(s.data))
	for i, v := range s.data {
		if s.mask[i] {
			out[i] = nil
		} else {
			out[i] = v
		}
	}
	return out
}

func (s *Int64Series) MaskCopy() []bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]bool, len(s.mask))
	copy(out, s.mask)
	return out
}

func (s *Int64Series) Slice(start, end int) (Series, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if start < 0 || end > len(s.data) || start > end {
		return nil, errors.New("invalid slice bounds")
	}

	newData := make([]int64, end-start)
	copy(newData, s.data[start:end])

	newMask := make([]bool, end-start)
	copy(newMask, s.mask[start:end])

	return &Int64Series{
		data: newData,
		mask: newMask,
	}, nil
}

// Int64Value returns the raw int64 value at index i (ignores null mask).
func (s *Int64Series) Int64Value(i int) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.data) {
		return 0, errors.New("index out of range")
	}
	return s.data[i], nil
}

// Int64Values returns a copy of the raw int64 data slice.
func (s *Int64Series) Int64Values() []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]int64, len(s.data))
	copy(out, s.data)
	return out
}

// -----------------------------------------------------------------------------
// StringSeries
// -----------------------------------------------------------------------------

// StringSeries is a high-performance series for string values with null support.
type StringSeries struct {
	mu   sync.RWMutex
	data []string
	mask []bool // true = null
}

// NewStringSeries creates a new empty StringSeries with optional capacity.
func NewStringSeries(capacity int) *StringSeries {
	return &StringSeries{
		data: make([]string, 0, capacity),
		mask: make([]bool, 0, capacity),
	}
}

// NewStringSeriesFromData creates a StringSeries from values and mask.
func NewStringSeriesFromData(data []string, mask []bool) (*StringSeries, error) {
	if mask != nil && len(data) != len(mask) {
		return nil, errors.New("data and mask length mismatch")
	}
	dataCopy := make([]string, len(data))
	copy(dataCopy, data)

	var maskCopy []bool
	if mask != nil {
		maskCopy = make([]bool, len(mask))
		copy(maskCopy, mask)
	} else {
		maskCopy = make([]bool, len(data))
	}

	return &StringSeries{data: dataCopy, mask: maskCopy}, nil
}

func (s *StringSeries) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *StringSeries) DType() reflect.Type {
	return reflect.TypeOf("")
}

func (s *StringSeries) At(i int) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.data) {
		return nil, errors.New("index out of range")
	}
	if s.mask[i] {
		return nil, nil
	}
	return s.data[i], nil
}

func (s *StringSeries) IsNull(i int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.mask) {
		return true
	}
	return s.mask[i]
}

func (s *StringSeries) NullCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, isNull := range s.mask {
		if isNull {
			count++
		}
	}
	return count
}

func (s *StringSeries) Set(i int, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}
	if v == nil {
		s.mask[i] = true
		s.data[i] = ""
		return nil
	}
	val, ok := v.(string)
	if !ok {
		return fmt.Errorf("type mismatch: expected string, got %T", v)
	}
	s.data[i] = val
	s.mask[i] = false
	return nil
}

func (s *StringSeries) SetNull(i int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}
	s.mask[i] = true
	s.data[i] = ""
	return nil
}

func (s *StringSeries) Append(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v == nil {
		s.data = append(s.data, "")
		s.mask = append(s.mask, true)
		return nil
	}
	val, ok := v.(string)
	if !ok {
		return fmt.Errorf("type mismatch: expected string, got %T", v)
	}
	s.data = append(s.data, val)
	s.mask = append(s.mask, false)
	return nil
}

func (s *StringSeries) AppendNull() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append(s.data, "")
	s.mask = append(s.mask, true)
}

func (s *StringSeries) ValuesCopy() []any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]any, len(s.data))
	for i, v := range s.data {
		if s.mask[i] {
			out[i] = nil
		} else {
			out[i] = v
		}
	}
	return out
}

func (s *StringSeries) MaskCopy() []bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]bool, len(s.mask))
	copy(out, s.mask)
	return out
}

func (s *StringSeries) Slice(start, end int) (Series, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if start < 0 || end > len(s.data) || start > end {
		return nil, errors.New("invalid slice bounds")
	}

	newData := make([]string, end-start)
	copy(newData, s.data[start:end])

	newMask := make([]bool, end-start)
	copy(newMask, s.mask[start:end])

	return &StringSeries{
		data: newData,
		mask: newMask,
	}, nil
}

// StringValue returns the raw string value at index i (ignores null mask).
func (s *StringSeries) StringValue(i int) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.data) {
		return "", errors.New("index out of range")
	}
	return s.data[i], nil
}

// StringValues returns a copy of the raw string data slice.
func (s *StringSeries) StringValues() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, len(s.data))
	copy(out, s.data)
	return out
}

// -----------------------------------------------------------------------------
// BoolSeries
// -----------------------------------------------------------------------------

// BoolSeries is a high-performance series for bool values with null support.
type BoolSeries struct {
	mu   sync.RWMutex
	data []bool
	mask []bool // true = null
}

// NewBoolSeries creates a new empty BoolSeries with optional capacity.
func NewBoolSeries(capacity int) *BoolSeries {
	return &BoolSeries{
		data: make([]bool, 0, capacity),
		mask: make([]bool, 0, capacity),
	}
}

// NewBoolSeriesFromData creates a BoolSeries from values and mask.
func NewBoolSeriesFromData(data []bool, mask []bool) (*BoolSeries, error) {
	if mask != nil && len(data) != len(mask) {
		return nil, errors.New("data and mask length mismatch")
	}
	dataCopy := make([]bool, len(data))
	copy(dataCopy, data)

	var maskCopy []bool
	if mask != nil {
		maskCopy = make([]bool, len(mask))
		copy(maskCopy, mask)
	} else {
		maskCopy = make([]bool, len(data))
	}

	return &BoolSeries{data: dataCopy, mask: maskCopy}, nil
}

func (s *BoolSeries) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *BoolSeries) DType() reflect.Type {
	return reflect.TypeOf(true)
}

func (s *BoolSeries) At(i int) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.data) {
		return nil, errors.New("index out of range")
	}
	if s.mask[i] {
		return nil, nil
	}
	return s.data[i], nil
}

func (s *BoolSeries) IsNull(i int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.mask) {
		return true
	}
	return s.mask[i]
}

func (s *BoolSeries) NullCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, isNull := range s.mask {
		if isNull {
			count++
		}
	}
	return count
}

func (s *BoolSeries) Set(i int, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}
	if v == nil {
		s.mask[i] = true
		s.data[i] = false
		return nil
	}
	val, ok := v.(bool)
	if !ok {
		return fmt.Errorf("type mismatch: expected bool, got %T", v)
	}
	s.data[i] = val
	s.mask[i] = false
	return nil
}

func (s *BoolSeries) SetNull(i int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}
	s.mask[i] = true
	s.data[i] = false
	return nil
}

func (s *BoolSeries) Append(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v == nil {
		s.data = append(s.data, false)
		s.mask = append(s.mask, true)
		return nil
	}
	val, ok := v.(bool)
	if !ok {
		return fmt.Errorf("type mismatch: expected bool, got %T", v)
	}
	s.data = append(s.data, val)
	s.mask = append(s.mask, false)
	return nil
}

func (s *BoolSeries) AppendNull() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append(s.data, false)
	s.mask = append(s.mask, true)
}

func (s *BoolSeries) ValuesCopy() []any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]any, len(s.data))
	for i, v := range s.data {
		if s.mask[i] {
			out[i] = nil
		} else {
			out[i] = v
		}
	}
	return out
}

func (s *BoolSeries) MaskCopy() []bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]bool, len(s.mask))
	copy(out, s.mask)
	return out
}

func (s *BoolSeries) Slice(start, end int) (Series, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if start < 0 || end > len(s.data) || start > end {
		return nil, errors.New("invalid slice bounds")
	}

	newData := make([]bool, end-start)
	copy(newData, s.data[start:end])

	newMask := make([]bool, end-start)
	copy(newMask, s.mask[start:end])

	return &BoolSeries{
		data: newData,
		mask: newMask,
	}, nil
}
