package plot

import (
	"fmt"
	"reflect"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// convertToStringSlice converts a Series to []string for axis labels.
// Null values are skipped during conversion.
// Returns an error if the Series contains non-string values or if value extraction fails.
func convertToStringSlice(s collection.Series) ([]string, error) {
	if s == nil {
		return nil, fmt.Errorf("series is nil")
	}

	result := make([]string, 0, s.Len()-s.NullCount())

	for i := 0; i < s.Len(); i++ {
		// Skip null values
		if s.IsNull(i) {
			continue
		}

		// Get value at index
		val, err := s.At(i)
		if err != nil {
			return nil, fmt.Errorf("failed to get value at index %d: %w", i, err)
		}

		// Convert to string
		strVal, ok := val.(string)
		if !ok {
			return nil, fmt.Errorf("value at index %d has type %T, expected string", i, val)
		}

		result = append(result, strVal)
	}

	return result, nil
}

// validateNumericSeries checks if a Series contains numeric data (int64 or float64).
// Returns an error if the Series is nil or contains non-numeric types.
func validateNumericSeries(s collection.Series) error {
	if s == nil {
		return fmt.Errorf("series is nil")
	}

	dtype := s.DType()
	if dtype != reflect.TypeOf(int64(0)) && dtype != reflect.TypeOf(float64(0)) {
		return fmt.Errorf("series has type %v, expected numeric type (int64 or float64)", dtype)
	}

	return nil
}

// convertToFloat64Slice converts a numeric Series to []float64.
// Null values are skipped during conversion.
// Int64 values are converted to float64.
// Returns an error if the Series is not numeric or if value extraction fails.
func convertToFloat64Slice(s collection.Series) ([]float64, error) {
	if err := validateNumericSeries(s); err != nil {
		return nil, err
	}

	result := make([]float64, 0, s.Len()-s.NullCount())
	dtype := s.DType()

	for i := 0; i < s.Len(); i++ {
		// Skip null values
		if s.IsNull(i) {
			continue
		}

		// Get value at index
		val, err := s.At(i)
		if err != nil {
			return nil, fmt.Errorf("failed to get value at index %d: %w", i, err)
		}

		// Convert based on dtype
		var floatVal float64
		if dtype == reflect.TypeOf(int64(0)) {
			intVal, ok := val.(int64)
			if !ok {
				return nil, fmt.Errorf("value at index %d has type %T, expected int64", i, val)
			}
			floatVal = float64(intVal)
		} else {
			fVal, ok := val.(float64)
			if !ok {
				return nil, fmt.Errorf("value at index %d has type %T, expected float64", i, val)
			}
			floatVal = fVal
		}

		result = append(result, floatVal)
	}

	return result, nil
}

// Test-only exports for unit testing
// These functions are exported with "ForTest" suffix to make them accessible to tests
// while keeping the original functions unexported for internal use only.

// ConvertToStringSliceForTest is a test-only export of convertToStringSlice
func ConvertToStringSliceForTest(s collection.Series) ([]string, error) {
	return convertToStringSlice(s)
}

// ValidateNumericSeriesForTest is a test-only export of validateNumericSeries
func ValidateNumericSeriesForTest(s collection.Series) error {
	return validateNumericSeries(s)
}

// ConvertToFloat64SliceForTest is a test-only export of convertToFloat64Slice
func ConvertToFloat64SliceForTest(s collection.Series) ([]float64, error) {
	return convertToFloat64Slice(s)
}
