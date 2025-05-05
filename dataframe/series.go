package dataframe

import (
	"errors"
	"fmt"
	"gpandas/utils/nullable"
	"reflect"
)

// Series is an interface for type-agnostic column operations
type Series interface {
	// Basic properties
	Len() int
	Name() string
	SetName(name string)

	// Value access
	IsNull(i int) bool
	GetValue(i int) any
	SetValue(i int, v any) error
	GetType() reflect.Type

	// Serialization
	String() string

	// Create a new series with the same type but empty data
	EmptyCopy(size int) Series

	// Create a full copy of this series
	Copy() Series
}

// IntSeries represents a column of nullable int64 values
type IntSeries struct {
	name string
	data []nullable.NullableInt
}

// FloatSeries represents a column of nullable float64 values
type FloatSeries struct {
	name string
	data []nullable.NullableFloat
}

// StringSeries represents a column of nullable string values
type StringSeries struct {
	name string
	data []nullable.NullableString
}

// BoolSeries represents a column of nullable bool values
type BoolSeries struct {
	name string
	data []nullable.NullableBool
}

// NewIntSeries creates a new series of nullable int values
func NewIntSeries(name string, size int) *IntSeries {
	return &IntSeries{
		name: name,
		data: make([]nullable.NullableInt, size),
	}
}

// NewFloatSeries creates a new series of nullable float values
func NewFloatSeries(name string, size int) *FloatSeries {
	return &FloatSeries{
		name: name,
		data: make([]nullable.NullableFloat, size),
	}
}

// NewStringSeries creates a new series of nullable string values
func NewStringSeries(name string, size int) *StringSeries {
	return &StringSeries{
		name: name,
		data: make([]nullable.NullableString, size),
	}
}

// NewBoolSeries creates a new series of nullable bool values
func NewBoolSeries(name string, size int) *BoolSeries {
	return &BoolSeries{
		name: name,
		data: make([]nullable.NullableBool, size),
	}
}

// Implementation of Series interface for IntSeries

func (s *IntSeries) Len() int {
	return len(s.data)
}

func (s *IntSeries) Name() string {
	return s.name
}

func (s *IntSeries) SetName(name string) {
	s.name = name
}

func (s *IntSeries) IsNull(i int) bool {
	if i < 0 || i >= len(s.data) {
		return true
	}
	return !s.data[i].Valid
}

func (s *IntSeries) GetValue(i int) any {
	if i < 0 || i >= len(s.data) || !s.data[i].Valid {
		return nil
	}
	return s.data[i].Value
}

func (s *IntSeries) SetValue(i int, v any) error {
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}

	if v == nil {
		s.data[i] = nullable.NullableInt{Valid: false}
		return nil
	}

	switch val := v.(type) {
	case int:
		s.data[i] = nullable.FromInt64(int64(val))
	case int64:
		s.data[i] = nullable.FromInt64(val)
	case int32:
		s.data[i] = nullable.FromInt64(int64(val))
	case nullable.NullableInt:
		s.data[i] = val
	default:
		return fmt.Errorf("cannot convert %T to int64", v)
	}

	return nil
}

func (s *IntSeries) GetType() reflect.Type {
	return reflect.TypeOf(int64(0))
}

func (s *IntSeries) String() string {
	result := fmt.Sprintf("IntSeries(%s, %d elements): [", s.name, len(s.data))
	maxDisplay := 10
	numItems := len(s.data)
	if numItems == 0 {
		return result + "]"
	}

	for i := 0; i < numItems && i < maxDisplay; i++ {
		if i > 0 {
			result += ", "
		}
		result += s.data[i].String()
	}

	if numItems > maxDisplay {
		result += fmt.Sprintf(", ... (%d more)", numItems-maxDisplay)
	}

	return result + "]"
}

func (s *IntSeries) EmptyCopy(size int) Series {
	return NewIntSeries(s.name, size)
}

func (s *IntSeries) Copy() Series {
	copy := NewIntSeries(s.name, len(s.data))
	for i, v := range s.data {
		copy.data[i] = v
	}
	return copy
}

// Implementation of Series interface for FloatSeries

func (s *FloatSeries) Len() int {
	return len(s.data)
}

func (s *FloatSeries) Name() string {
	return s.name
}

func (s *FloatSeries) SetName(name string) {
	s.name = name
}

func (s *FloatSeries) IsNull(i int) bool {
	if i < 0 || i >= len(s.data) {
		return true
	}
	return !s.data[i].Valid
}

func (s *FloatSeries) GetValue(i int) any {
	if i < 0 || i >= len(s.data) || !s.data[i].Valid {
		return nil
	}
	return s.data[i].Value
}

func (s *FloatSeries) SetValue(i int, v any) error {
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}

	if v == nil {
		s.data[i] = nullable.NullableFloat{Valid: false}
		return nil
	}

	switch val := v.(type) {
	case float64:
		s.data[i] = nullable.FromFloat64(val)
	case float32:
		s.data[i] = nullable.FromFloat64(float64(val))
	case int:
		s.data[i] = nullable.FromFloat64(float64(val))
	case int64:
		s.data[i] = nullable.FromFloat64(float64(val))
	case nullable.NullableFloat:
		s.data[i] = val
	default:
		return fmt.Errorf("cannot convert %T to float64", v)
	}

	return nil
}

func (s *FloatSeries) GetType() reflect.Type {
	return reflect.TypeOf(float64(0))
}

func (s *FloatSeries) String() string {
	result := fmt.Sprintf("FloatSeries(%s, %d elements): [", s.name, len(s.data))
	maxDisplay := 10
	numItems := len(s.data)
	if numItems == 0 {
		return result + "]"
	}

	for i := 0; i < numItems && i < maxDisplay; i++ {
		if i > 0 {
			result += ", "
		}
		result += s.data[i].String()
	}

	if numItems > maxDisplay {
		result += fmt.Sprintf(", ... (%d more)", numItems-maxDisplay)
	}

	return result + "]"
}

func (s *FloatSeries) EmptyCopy(size int) Series {
	return NewFloatSeries(s.name, size)
}

func (s *FloatSeries) Copy() Series {
	copy := NewFloatSeries(s.name, len(s.data))
	for i, v := range s.data {
		copy.data[i] = v
	}
	return copy
}

// Implementation of Series interface for StringSeries

func (s *StringSeries) Len() int {
	return len(s.data)
}

func (s *StringSeries) Name() string {
	return s.name
}

func (s *StringSeries) SetName(name string) {
	s.name = name
}

func (s *StringSeries) IsNull(i int) bool {
	if i < 0 || i >= len(s.data) {
		return true
	}
	return !s.data[i].Valid
}

func (s *StringSeries) GetValue(i int) any {
	if i < 0 || i >= len(s.data) || !s.data[i].Valid {
		return nil
	}
	return s.data[i].Value
}

func (s *StringSeries) SetValue(i int, v any) error {
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}

	if v == nil {
		s.data[i] = nullable.NullableString{Valid: false}
		return nil
	}

	switch val := v.(type) {
	case string:
		s.data[i] = nullable.FromString(val)
	case nullable.NullableString:
		s.data[i] = val
	default:
		// Convert any value to string using fmt.Sprintf
		s.data[i] = nullable.FromString(fmt.Sprintf("%v", val))
	}

	return nil
}

func (s *StringSeries) GetType() reflect.Type {
	return reflect.TypeOf("")
}

func (s *StringSeries) String() string {
	result := fmt.Sprintf("StringSeries(%s, %d elements): [", s.name, len(s.data))
	maxDisplay := 10
	numItems := len(s.data)
	if numItems == 0 {
		return result + "]"
	}

	for i := 0; i < numItems && i < maxDisplay; i++ {
		if i > 0 {
			result += ", "
		}
		result += s.data[i].String()
	}

	if numItems > maxDisplay {
		result += fmt.Sprintf(", ... (%d more)", numItems-maxDisplay)
	}

	return result + "]"
}

func (s *StringSeries) EmptyCopy(size int) Series {
	return NewStringSeries(s.name, size)
}

func (s *StringSeries) Copy() Series {
	copy := NewStringSeries(s.name, len(s.data))
	for i, v := range s.data {
		copy.data[i] = v
	}
	return copy
}

// Implementation of Series interface for BoolSeries

func (s *BoolSeries) Len() int {
	return len(s.data)
}

func (s *BoolSeries) Name() string {
	return s.name
}

func (s *BoolSeries) SetName(name string) {
	s.name = name
}

func (s *BoolSeries) IsNull(i int) bool {
	if i < 0 || i >= len(s.data) {
		return true
	}
	return !s.data[i].Valid
}

func (s *BoolSeries) GetValue(i int) any {
	if i < 0 || i >= len(s.data) || !s.data[i].Valid {
		return nil
	}
	return s.data[i].Value
}

func (s *BoolSeries) SetValue(i int, v any) error {
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}

	if v == nil {
		s.data[i] = nullable.NullableBool{Valid: false}
		return nil
	}

	switch val := v.(type) {
	case bool:
		s.data[i] = nullable.FromBool(val)
	case nullable.NullableBool:
		s.data[i] = val
	default:
		return fmt.Errorf("cannot convert %T to bool", v)
	}

	return nil
}

func (s *BoolSeries) GetType() reflect.Type {
	return reflect.TypeOf(bool(false))
}

func (s *BoolSeries) String() string {
	result := fmt.Sprintf("BoolSeries(%s, %d elements): [", s.name, len(s.data))
	maxDisplay := 10
	numItems := len(s.data)
	if numItems == 0 {
		return result + "]"
	}

	for i := 0; i < numItems && i < maxDisplay; i++ {
		if i > 0 {
			result += ", "
		}
		result += s.data[i].String()
	}

	if numItems > maxDisplay {
		result += fmt.Sprintf(", ... (%d more)", numItems-maxDisplay)
	}

	return result + "]"
}

func (s *BoolSeries) EmptyCopy(size int) Series {
	return NewBoolSeries(s.name, size)
}

func (s *BoolSeries) Copy() Series {
	copy := NewBoolSeries(s.name, len(s.data))
	for i, v := range s.data {
		copy.data[i] = v
	}
	return copy
}

// SeriesType represents the data type of a series
type SeriesType int

const (
	IntType SeriesType = iota
	FloatType
	StringType
	BoolType
	UnknownType
)

// CreateSeries creates a new series of the specified type and name
func CreateSeries(seriesType SeriesType, name string, size int) Series {
	switch seriesType {
	case IntType:
		return NewIntSeries(name, size)
	case FloatType:
		return NewFloatSeries(name, size)
	case StringType:
		return NewStringSeries(name, size)
	case BoolType:
		return NewBoolSeries(name, size)
	default:
		return NewStringSeries(name, size) // Default to string for unknown types
	}
}

// CreateSeriesFromData creates a new series based on the data provided
func CreateSeriesFromData(name string, data []any) Series {
	if len(data) == 0 {
		return NewStringSeries(name, 0)
	}

	// Infer type from the first non-nil value
	var seriesType SeriesType = UnknownType
	for _, v := range data {
		if v != nil {
			switch v.(type) {
			case int, int32, int64:
				seriesType = IntType
			case float32, float64:
				seriesType = FloatType
			case bool:
				seriesType = BoolType
			case string:
				seriesType = StringType
			default:
				seriesType = StringType // Default to string for complex types
			}
			break
		}
	}

	// If all values are nil, default to string
	if seriesType == UnknownType {
		seriesType = StringType
	}

	series := CreateSeries(seriesType, name, len(data))

	// Fill the series with data
	for i, v := range data {
		if err := series.SetValue(i, v); err != nil {
			// If we can't set a value, try with string representation
			if seriesType != StringType {
				seriesType = StringType
				stringSeries := NewStringSeries(name, len(data))
				for j := 0; j < i; j++ {
					stringSeries.SetValue(j, series.GetValue(j))
				}
				series = stringSeries
				series.SetValue(i, v)
			}
		}
	}

	return series
}
