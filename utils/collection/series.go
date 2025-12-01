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

// BoolValue returns the raw bool value at index i (ignores null mask).
func (s *BoolSeries) BoolValue(i int) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.data) {
		return false, errors.New("index out of range")
	}
	return s.data[i], nil
}

// BoolValues returns a copy of the raw bool data slice.
func (s *BoolSeries) BoolValues() []bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]bool, len(s.data))
	copy(out, s.data)
	return out
}

// -----------------------------------------------------------------------------
// AnySeries - fallback for heterogeneous data
// -----------------------------------------------------------------------------

// AnySeries is a fallback series for heterogeneous data types (like pandas object dtype).
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
		// Infer mask from nil values
		maskCopy = make([]bool, len(data))
		for i, v := range data {
			if v == nil {
				maskCopy[i] = true
			}
		}
	}

	return &AnySeries{data: dataCopy, mask: maskCopy}, nil
}

func (s *AnySeries) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *AnySeries) DType() reflect.Type {
	// AnySeries has no specific dtype
	return nil
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

// -----------------------------------------------------------------------------
// Factory Functions
// -----------------------------------------------------------------------------

// NewSeriesFromValues creates a typed Series from a slice of any values.
// It infers the type from the first non-nil value and creates the appropriate typed series.
// If all values are nil or empty, it creates an AnySeries.
// Integer types (int, int8, int16, int32, int64) are all stored as Int64Series.
// Float types (float32, float64) are all stored as Float64Series.
func NewSeriesFromValues(values []any) (Series, error) {
	if len(values) == 0 {
		return NewAnySeries(0), nil
	}

	// Find first non-nil value to infer type
	var inferredType reflect.Type
	for _, v := range values {
		if v != nil {
			inferredType = reflect.TypeOf(v)
			break
		}
	}

	if inferredType == nil {
		// All values are nil, use AnySeries
		mask := make([]bool, len(values))
		for i := range mask {
			mask[i] = true
		}
		return NewAnySeriesFromData(values, mask)
	}

	// Create appropriate typed series based on inferred type
	switch inferredType.Kind() {
	case reflect.Float64, reflect.Float32:
		data := make([]float64, len(values))
		mask := make([]bool, len(values))
		for i, v := range values {
			if v == nil {
				mask[i] = true
			} else {
				rv := reflect.ValueOf(v)
				if rv.Kind() >= reflect.Float32 && rv.Kind() <= reflect.Float64 {
					data[i] = rv.Float()
				} else {
					return nil, fmt.Errorf("type mismatch at index %d: expected float, got %T", i, v)
				}
			}
		}
		return NewFloat64SeriesFromData(data, mask)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		data := make([]int64, len(values))
		mask := make([]bool, len(values))
		for i, v := range values {
			if v == nil {
				mask[i] = true
			} else {
				rv := reflect.ValueOf(v)
				if rv.Kind() >= reflect.Int && rv.Kind() <= reflect.Int64 {
					data[i] = rv.Int()
				} else {
					return nil, fmt.Errorf("type mismatch at index %d: expected int, got %T", i, v)
				}
			}
		}
		return NewInt64SeriesFromData(data, mask)

	case reflect.String:
		data := make([]string, len(values))
		mask := make([]bool, len(values))
		for i, v := range values {
			if v == nil {
				mask[i] = true
			} else if s, ok := v.(string); ok {
				data[i] = s
			} else {
				return nil, fmt.Errorf("type mismatch at index %d: expected string, got %T", i, v)
			}
		}
		return NewStringSeriesFromData(data, mask)

	case reflect.Bool:
		data := make([]bool, len(values))
		mask := make([]bool, len(values))
		for i, v := range values {
			if v == nil {
				mask[i] = true
			} else if b, ok := v.(bool); ok {
				data[i] = b
			} else {
				return nil, fmt.Errorf("type mismatch at index %d: expected bool, got %T", i, v)
			}
		}
		return NewBoolSeriesFromData(data, mask)

	default:
		// Fallback to AnySeries for other types
		return NewAnySeriesFromData(values, nil)
	}
}

// NewSeriesOfType creates a new empty Series of the specified type with optional capacity.
func NewSeriesOfType(dtype reflect.Type, capacity int) Series {
	if dtype == nil {
		return NewAnySeries(capacity)
	}

	switch dtype.Kind() {
	case reflect.Float64:
		return NewFloat64Series(capacity)
	case reflect.Int64:
		return NewInt64Series(capacity)
	case reflect.String:
		return NewStringSeries(capacity)
	case reflect.Bool:
		return NewBoolSeries(capacity)
	default:
		return NewAnySeries(capacity)
	}
}

// NewSeriesWithData creates a Series from values with explicit dtype.
// If dtype is nil, it infers the type from the first non-nil value.
// Integer types are stored as Int64Series, float types as Float64Series.
func NewSeriesWithData(dtype reflect.Type, values []any) (Series, error) {
	if dtype == nil {
		return NewSeriesFromValues(values)
	}

	switch dtype.Kind() {
	case reflect.Float64, reflect.Float32:
		data := make([]float64, len(values))
		mask := make([]bool, len(values))
		for i, v := range values {
			if v == nil {
				mask[i] = true
			} else {
				rv := reflect.ValueOf(v)
				if rv.Kind() >= reflect.Float32 && rv.Kind() <= reflect.Float64 {
					data[i] = rv.Float()
				} else {
					return nil, fmt.Errorf("type mismatch at index %d: expected float, got %T", i, v)
				}
			}
		}
		return NewFloat64SeriesFromData(data, mask)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		data := make([]int64, len(values))
		mask := make([]bool, len(values))
		for i, v := range values {
			if v == nil {
				mask[i] = true
			} else {
				rv := reflect.ValueOf(v)
				if rv.Kind() >= reflect.Int && rv.Kind() <= reflect.Int64 {
					data[i] = rv.Int()
				} else {
					return nil, fmt.Errorf("type mismatch at index %d: expected int, got %T", i, v)
				}
			}
		}
		return NewInt64SeriesFromData(data, mask)

	case reflect.String:
		data := make([]string, len(values))
		mask := make([]bool, len(values))
		for i, v := range values {
			if v == nil {
				mask[i] = true
			} else if s, ok := v.(string); ok {
				data[i] = s
			} else {
				return nil, fmt.Errorf("type mismatch at index %d: expected string, got %T", i, v)
			}
		}
		return NewStringSeriesFromData(data, mask)

	case reflect.Bool:
		data := make([]bool, len(values))
		mask := make([]bool, len(values))
		for i, v := range values {
			if v == nil {
				mask[i] = true
			} else if b, ok := v.(bool); ok {
				data[i] = b
			} else {
				return nil, fmt.Errorf("type mismatch at index %d: expected bool, got %T", i, v)
			}
		}
		return NewBoolSeriesFromData(data, mask)

	default:
		return NewAnySeriesFromData(values, nil)
	}
}
