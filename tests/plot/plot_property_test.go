package gpandas_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// Helper generators for property-based testing

// genNumericSeries generates a random numeric Series (int64 or float64)
func genNumericSeries() gopter.Gen {
	return gen.OneGenOf(
		// Generate int64 Series
		gen.SliceOf(gen.Int64()).Map(func(values []int64) collection.Series {
			series, _ := collection.NewInt64SeriesFromData(values, nil)
			return series
		}),
		// Generate float64 Series
		gen.SliceOf(gen.Float64()).Map(func(values []float64) collection.Series {
			series, _ := collection.NewFloat64SeriesFromData(values, nil)
			return series
		}),
	)
}

// genStringSeries generates a random string Series
func genStringSeries() gopter.Gen {
	return gen.SliceOf(gen.AlphaString()).Map(func(values []string) collection.Series {
		series, _ := collection.NewStringSeriesFromData(values, nil)
		return series
	})
}

// genSeriesWithNulls generates a Series with random null values
func genSeriesWithNulls(minLen, maxLen int) gopter.Gen {
	return gen.IntRange(minLen, maxLen).FlatMap(func(length interface{}) gopter.Gen {
		len := length.(int)
		return gen.SliceOfN(len, gen.Float64()).FlatMap(func(values interface{}) gopter.Gen {
			vals := values.([]float64)
			// Generate random null positions
			return gen.SliceOf(gen.IntRange(0, len-1)).Map(func(nullIndices interface{}) collection.Series {
				indices := nullIndices.([]int)
				// Create null mask
				nullMask := make([]bool, len)
				for _, idx := range indices {
					if idx < len {
						nullMask[idx] = true
					}
				}
				series, _ := collection.NewFloat64SeriesFromData(vals, nullMask)
				return series
			})
		}, reflect.TypeOf([]float64{}))
	}, reflect.TypeOf(0))
}


// Property 5: Null Value Exclusion
// For any Series containing null values, when converted to chart data, the resulting
// chart data array should contain only non-null values and should have length equal to
// (Series.Len() - Series.NullCount()).
// **Validates: Requirements 1.7, 2.7, 3.7**
func TestProperty5_NullValueExclusion(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("null values excluded from float64 conversion", prop.ForAll(
		func(values []float64, nullIndices []int) bool {
			// Skip empty data
			if len(values) == 0 {
				return true
			}

			// Create null mask
			nullMask := make([]bool, len(values))
			expectedNullCount := 0
			for _, idx := range nullIndices {
				if idx >= 0 && idx < len(values) {
					if !nullMask[idx] { // Only count if not already marked
						nullMask[idx] = true
						expectedNullCount++
					}
				}
			}

			// Create Series with nulls
			series, err := collection.NewFloat64SeriesFromData(values, nullMask)
			if err != nil {
				return true // Skip this case
			}

			// Convert to float64 slice
			result, err := plot.ConvertToFloat64SliceForTest(series)
			if err != nil {
				t.Logf("Conversion failed: %v", err)
				return false
			}

			// Verify length matches non-null count
			expectedLen := series.Len() - series.NullCount()
			if len(result) != expectedLen {
				t.Logf("Expected length %d, got %d", expectedLen, len(result))
				return false
			}

			// Verify no null values in result (all values should be valid floats)
			for i, val := range result {
				if math.IsNaN(val) {
					t.Logf("Found NaN at index %d", i)
					return false
				}
			}

			return true
		},
		gen.SliceOfN(20, gen.Float64()),
		gen.SliceOf(gen.IntRange(0, 19)),
	))

	properties.Property("null values excluded from string conversion", prop.ForAll(
		func(values []string, nullIndices []int) bool {
			// Skip empty data
			if len(values) == 0 {
				return true
			}

			// Create null mask
			nullMask := make([]bool, len(values))
			for _, idx := range nullIndices {
				if idx >= 0 && idx < len(values) {
					nullMask[idx] = true
				}
			}

			// Create Series with nulls
			series, err := collection.NewStringSeriesFromData(values, nullMask)
			if err != nil {
				return true // Skip this case
			}

			// Convert to string slice
			result, err := plot.ConvertToStringSliceForTest(series)
			if err != nil {
				t.Logf("Conversion failed: %v", err)
				return false
			}

			// Verify length matches non-null count
			expectedLen := series.Len() - series.NullCount()
			if len(result) != expectedLen {
				t.Logf("Expected length %d, got %d", expectedLen, len(result))
				return false
			}

			return true
		},
		gen.SliceOfN(20, gen.AlphaString()),
		gen.SliceOf(gen.IntRange(0, 19)),
	))

	properties.TestingRun(t)
}

// Property 9: Numeric Series Extraction
// For any numeric Series (int64 or float64), extracting values for chart data
// should produce numeric values that can be plotted without type errors.
// **Validates: Requirements 5.1**
func TestProperty9_NumericSeriesExtraction(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("int64 series extracts to float64 values", prop.ForAll(
		func(values []int64) bool {
			// Skip empty data
			if len(values) == 0 {
				return true
			}

			// Create int64 Series
			series, err := collection.NewInt64SeriesFromData(values, nil)
			if err != nil {
				return true // Skip this case
			}

			// Validate it's numeric
			err = plot.ValidateNumericSeriesForTest(series)
			if err != nil {
				t.Logf("Validation failed: %v", err)
				return false
			}

			// Convert to float64 slice
			result, err := plot.ConvertToFloat64SliceForTest(series)
			if err != nil {
				t.Logf("Conversion failed: %v", err)
				return false
			}

			// Verify length matches
			if len(result) != len(values) {
				t.Logf("Expected length %d, got %d", len(values), len(result))
				return false
			}

			// Verify values are correct
			for i, val := range result {
				expected := float64(values[i])
				if val != expected {
					t.Logf("Value mismatch at index %d: expected %f, got %f", i, expected, val)
					return false
				}
			}

			return true
		},
		gen.SliceOf(gen.Int64()),
	))

	properties.Property("float64 series extracts to float64 values", prop.ForAll(
		func(values []float64) bool {
			// Skip empty data
			if len(values) == 0 {
				return true
			}

			// Create float64 Series
			series, err := collection.NewFloat64SeriesFromData(values, nil)
			if err != nil {
				return true // Skip this case
			}

			// Validate it's numeric
			err = plot.ValidateNumericSeriesForTest(series)
			if err != nil {
				t.Logf("Validation failed: %v", err)
				return false
			}

			// Convert to float64 slice
			result, err := plot.ConvertToFloat64SliceForTest(series)
			if err != nil {
				t.Logf("Conversion failed: %v", err)
				return false
			}

			// Verify length matches
			if len(result) != len(values) {
				t.Logf("Expected length %d, got %d", len(values), len(result))
				return false
			}

			// Verify values are correct (handle NaN specially)
			for i, val := range result {
				expected := values[i]
				if math.IsNaN(expected) && math.IsNaN(val) {
					continue // Both NaN, OK
				}
				if val != expected {
					t.Logf("Value mismatch at index %d: expected %f, got %f", i, expected, val)
					return false
				}
			}

			return true
		},
		gen.SliceOf(gen.Float64()),
	))

	properties.Property("string series fails numeric validation", prop.ForAll(
		func(values []string) bool {
			// Skip empty data
			if len(values) == 0 {
				return true
			}

			// Create string Series
			series, err := collection.NewStringSeriesFromData(values, nil)
			if err != nil {
				return true // Skip this case
			}

			// Validate it's numeric - should fail
			err = plot.ValidateNumericSeriesForTest(series)
			
			// Should return an error
			return err != nil
		},
		gen.SliceOf(gen.AlphaString()),
	))

	properties.TestingRun(t)
}

// Property 10: String Series Extraction
// For any string Series used for labels or x-axis, extracting values should produce
// string values that can be used as chart labels without type errors.
// **Validates: Requirements 5.2**
func TestProperty10_StringSeriesExtraction(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("string series extracts to string values", prop.ForAll(
		func(values []string) bool {
			// Skip empty data
			if len(values) == 0 {
				return true
			}

			// Create string Series
			series, err := collection.NewStringSeriesFromData(values, nil)
			if err != nil {
				return true // Skip this case
			}

			// Convert to string slice
			result, err := plot.ConvertToStringSliceForTest(series)
			if err != nil {
				t.Logf("Conversion failed: %v", err)
				return false
			}

			// Verify length matches
			if len(result) != len(values) {
				t.Logf("Expected length %d, got %d", len(values), len(result))
				return false
			}

			// Verify values are correct
			for i, val := range result {
				if val != values[i] {
					t.Logf("Value mismatch at index %d: expected %s, got %s", i, values[i], val)
					return false
				}
			}

			return true
		},
		gen.SliceOf(gen.AlphaString()),
	))

	properties.Property("numeric series fails string conversion", prop.ForAll(
		func(values []int64) bool {
			// Skip empty data
			if len(values) == 0 {
				return true
			}

			// Create int64 Series
			series, err := collection.NewInt64SeriesFromData(values, nil)
			if err != nil {
				return true // Skip this case
			}

			// Convert to string slice - should fail
			_, err = plot.ConvertToStringSliceForTest(series)
			
			// Should return an error
			return err != nil
		},
		gen.SliceOf(gen.Int64()),
	))

	properties.TestingRun(t)
}

// Property 11: Data Precision Preservation
// For any numeric value in a Series, after conversion to chart data and back
// (if extractable), the numeric value should be preserved within floating-point
// precision limits.
// **Validates: Requirements 5.6**
func TestProperty11_DataPrecisionPreservation(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("int64 values preserve precision after conversion", prop.ForAll(
		func(values []int64) bool {
			// Skip empty data
			if len(values) == 0 {
				return true
			}

			// Create int64 Series
			series, err := collection.NewInt64SeriesFromData(values, nil)
			if err != nil {
				return true // Skip this case
			}

			// Convert to float64 slice
			result, err := plot.ConvertToFloat64SliceForTest(series)
			if err != nil {
				t.Logf("Conversion failed: %v", err)
				return false
			}

			// Verify precision is preserved
			for i, val := range result {
				expected := float64(values[i])
				if val != expected {
					t.Logf("Precision lost at index %d: expected %f, got %f", i, expected, val)
					return false
				}
			}

			return true
		},
		gen.SliceOf(gen.Int64()),
	))

	properties.Property("float64 values preserve precision after conversion", prop.ForAll(
		func(values []float64) bool {
			// Skip empty data
			if len(values) == 0 {
				return true
			}

			// Filter out special values that might cause issues
			filtered := make([]float64, 0, len(values))
			for _, v := range values {
				if !math.IsNaN(v) && !math.IsInf(v, 0) {
					filtered = append(filtered, v)
				}
			}

			if len(filtered) == 0 {
				return true // Skip if all values were special
			}

			// Create float64 Series
			series, err := collection.NewFloat64SeriesFromData(filtered, nil)
			if err != nil {
				return true // Skip this case
			}

			// Convert to float64 slice
			result, err := plot.ConvertToFloat64SliceForTest(series)
			if err != nil {
				t.Logf("Conversion failed: %v", err)
				return false
			}

			// Verify precision is preserved (exact match for float64)
			for i, val := range result {
				expected := filtered[i]
				if val != expected {
					t.Logf("Precision lost at index %d: expected %f, got %f", i, expected, val)
					return false
				}
			}

			return true
		},
		gen.SliceOf(gen.Float64()),
	))

	properties.TestingRun(t)
}
