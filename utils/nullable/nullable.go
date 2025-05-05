package nullable

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
)

// NullableInt represents an int64 that may be null
type NullableInt struct {
	Value int64
	Valid bool
}

// NullableFloat represents a float64 that may be null
type NullableFloat struct {
	Value float64
	Valid bool
}

// NullableString represents a string that may be null
type NullableString struct {
	Value string
	Valid bool
}

// NullableBool represents a bool that may be null
type NullableBool struct {
	Value bool
	Valid bool
}

// Null returns a null value of any nullable type
func Null[T any]() T {
	var t T
	return t
}

// FromInt64 creates a nullable int from a value
func FromInt64(v int64) NullableInt {
	return NullableInt{Value: v, Valid: true}
}

// FromNullableInt64 creates a nullable int from a sql.NullInt64
func FromNullableInt64(v sql.NullInt64) NullableInt {
	return NullableInt{Value: v.Int64, Valid: v.Valid}
}

// FromFloat64 creates a nullable float from a value
func FromFloat64(v float64) NullableFloat {
	return NullableFloat{Value: v, Valid: true}
}

// FromNullableFloat64 creates a nullable float from a sql.NullFloat64
func FromNullableFloat64(v sql.NullFloat64) NullableFloat {
	return NullableFloat{Value: v.Float64, Valid: v.Valid}
}

// FromString creates a nullable string from a value
func FromString(v string) NullableString {
	return NullableString{Value: v, Valid: true}
}

// FromNullableString creates a nullable string from a sql.NullString
func FromNullableString(v sql.NullString) NullableString {
	return NullableString{Value: v.String, Valid: v.Valid}
}

// FromBool creates a nullable bool from a value
func FromBool(v bool) NullableBool {
	return NullableBool{Value: v, Valid: true}
}

// FromNullableBool creates a nullable bool from a sql.NullBool
func FromNullableBool(v sql.NullBool) NullableBool {
	return NullableBool{Value: v.Bool, Valid: v.Valid}
}

// FromAny attempts to create the appropriate nullable type from an interface value
func FromAny(v any) any {
	if v == nil {
		return nil
	}
	
	switch val := v.(type) {
	case int:
		return FromInt64(int64(val))
	case int32:
		return FromInt64(int64(val))
	case int64:
		return FromInt64(val)
	case float32:
		return FromFloat64(float64(val))
	case float64:
		return FromFloat64(val)
	case string:
		return FromString(val)
	case bool:
		return FromBool(val)
	case sql.NullInt64:
		return FromNullableInt64(val)
	case sql.NullFloat64:
		return FromNullableFloat64(val)
	case sql.NullString:
		return FromNullableString(val)
	case sql.NullBool:
		return FromNullableBool(val)
	default:
		// For unknown types, we'll try to convert using string representation
		return FromString(fmt.Sprintf("%v", val))
	}
}

// MarshalJSON implements json.Marshaler for NullableInt
func (n NullableInt) MarshalJSON() ([]byte, error) {
	if !n.Valid {
		return []byte("null"), nil
	}
	return json.Marshal(n.Value)
}

// UnmarshalJSON implements json.Unmarshaler for NullableInt
func (n *NullableInt) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		n.Valid = false
		return nil
	}
	
	var value int64
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	
	n.Value = value
	n.Valid = true
	return nil
}

// ParseInt attempts to convert a string to a NullableInt
func ParseInt(s string) (NullableInt, error) {
	if s == "" {
		return NullableInt{Valid: false}, nil
	}
	
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return NullableInt{Valid: false}, err
	}
	
	return NullableInt{Value: val, Valid: true}, nil
}

// ParseFloat attempts to convert a string to a NullableFloat
func ParseFloat(s string) (NullableFloat, error) {
	if s == "" {
		return NullableFloat{Valid: false}, nil
	}
	
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return NullableFloat{Valid: false}, err
	}
	
	return NullableFloat{Value: val, Valid: true}, nil
}

// ParseBool attempts to convert a string to a NullableBool
func ParseBool(s string) (NullableBool, error) {
	if s == "" {
		return NullableBool{Valid: false}, nil
	}
	
	val, err := strconv.ParseBool(s)
	if err != nil {
		return NullableBool{Valid: false}, err
	}
	
	return NullableBool{Value: val, Valid: true}, nil
}

// String methods for pretty printing
func (n NullableInt) String() string {
	if !n.Valid {
		return "NULL"
	}
	return fmt.Sprintf("%d", n.Value)
}

func (n NullableFloat) String() string {
	if !n.Valid {
		return "NULL"
	}
	return fmt.Sprintf("%g", n.Value)
}

func (n NullableString) String() string {
	if !n.Valid {
		return "NULL"
	}
	return n.Value
}

func (n NullableBool) String() string {
	if !n.Valid {
		return "NULL"
	}
	return fmt.Sprintf("%t", n.Value)
} 