package gpandas_test

import (
	"testing"

	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// TestConvertToStringSlice_NullHandling tests that null values are excluded
func TestConvertToStringSlice_NullHandling(t *testing.T) {
	// Create a string series with some null values
	data := []string{"a", "b", "", "d", ""}
	mask := []bool{false, false, true, false, true} // indices 2 and 4 are null
	series, err := collection.NewStringSeriesFromData(data, mask)
	if err != nil {
		t.Fatalf("Failed to create series: %v", err)
	}

	// Convert to string slice
	result, err := plot.ConvertToStringSliceForTest(series)
	if err != nil {
		t.Fatalf("convertToStringSlice failed: %v", err)
	}

	// Verify null values were excluded
	expectedLen := series.Len() - series.NullCount()
	if len(result) != expectedLen {
		t.Errorf("Expected length %d (excluding nulls), got %d", expectedLen, len(result))
	}

	// Verify the non-null values are present
	expected := []string{"a", "b", "d"}
	if len(result) != len(expected) {
		t.Fatalf("Expected %d values, got %d", len(expected), len(result))
	}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("At index %d: expected %q, got %q", i, v, result[i])
		}
	}
}

// TestConvertToFloat64Slice_NullHandling tests that null values are excluded from numeric series
func TestConvertToFloat64Slice_NullHandling(t *testing.T) {
	// Create a float64 series with some null values
	data := []float64{1.5, 2.5, 0.0, 4.5, 0.0}
	mask := []bool{false, false, true, false, true} // indices 2 and 4 are null
	series, err := collection.NewFloat64SeriesFromData(data, mask)
	if err != nil {
		t.Fatalf("Failed to create series: %v", err)
	}

	// Convert to float64 slice
	result, err := plot.ConvertToFloat64SliceForTest(series)
	if err != nil {
		t.Fatalf("convertToFloat64Slice failed: %v", err)
	}

	// Verify null values were excluded
	expectedLen := series.Len() - series.NullCount()
	if len(result) != expectedLen {
		t.Errorf("Expected length %d (excluding nulls), got %d", expectedLen, len(result))
	}

	// Verify the non-null values are present
	expected := []float64{1.5, 2.5, 4.5}
	if len(result) != len(expected) {
		t.Fatalf("Expected %d values, got %d", len(expected), len(result))
	}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("At index %d: expected %f, got %f", i, v, result[i])
		}
	}
}

// TestConvertToFloat64Slice_Int64Series tests conversion from int64 to float64
func TestConvertToFloat64Slice_Int64Series(t *testing.T) {
	// Create an int64 series with some null values
	data := []int64{10, 20, 0, 40, 0}
	mask := []bool{false, false, true, false, true} // indices 2 and 4 are null
	series, err := collection.NewInt64SeriesFromData(data, mask)
	if err != nil {
		t.Fatalf("Failed to create series: %v", err)
	}

	// Convert to float64 slice
	result, err := plot.ConvertToFloat64SliceForTest(series)
	if err != nil {
		t.Fatalf("convertToFloat64Slice failed: %v", err)
	}

	// Verify null values were excluded and int64 converted to float64
	expectedLen := series.Len() - series.NullCount()
	if len(result) != expectedLen {
		t.Errorf("Expected length %d (excluding nulls), got %d", expectedLen, len(result))
	}

	// Verify the non-null values are present and converted
	expected := []float64{10.0, 20.0, 40.0}
	if len(result) != len(expected) {
		t.Fatalf("Expected %d values, got %d", len(expected), len(result))
	}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("At index %d: expected %f, got %f", i, v, result[i])
		}
	}
}

// TestValidateNumericSeries tests numeric type validation
func TestValidateNumericSeries(t *testing.T) {
	tests := []struct {
		name      string
		series    collection.Series
		shouldErr bool
	}{
		{
			name:      "float64 series is valid",
			series:    collection.NewFloat64Series(0),
			shouldErr: false,
		},
		{
			name:      "int64 series is valid",
			series:    collection.NewInt64Series(0),
			shouldErr: false,
		},
		{
			name:      "string series is invalid",
			series:    collection.NewStringSeries(0),
			shouldErr: true,
		},
		{
			name:      "bool series is invalid",
			series:    collection.NewBoolSeries(0),
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := plot.ValidateNumericSeriesForTest(tt.series)
			if tt.shouldErr && err == nil {
				t.Error("Expected error but got nil")
			}
			if !tt.shouldErr && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}
