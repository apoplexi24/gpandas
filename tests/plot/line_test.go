package gpandas_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// TestRenderLine_ValidData tests successful line chart generation with valid data
func TestRenderLine_ValidData(t *testing.T) {
	// Create test data
	xLabels := []string{"Jan", "Feb", "Mar", "Apr"}
	yValues := []float64{10.5, 20.3, 15.7, 30.2}

	xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
	if err != nil {
		t.Fatalf("Failed to create xSeries: %v", err)
	}

	ySeries, err := collection.NewFloat64SeriesFromData(yValues, nil)
	if err != nil {
		t.Fatalf("Failed to create ySeries: %v", err)
	}

	// Create temporary output file
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_line.html")

	opts := &plot.ChartOptions{
		Title:      "Test Line Chart",
		Width:      800,
		Height:     600,
		OutputPath: outputPath,
	}

	// Render line chart
	err = plot.RenderLine(xSeries, []collection.Series{ySeries}, []string{"Series 1"}, opts)
	if err != nil {
		t.Fatalf("RenderLine failed: %v", err)
	}

	// Verify file was created
	fileInfo, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Output file not created: %v", err)
	}

	// Verify file is not empty
	if fileInfo.Size() == 0 {
		t.Fatal("Output file is empty")
	}
}

// TestRenderLine_MultiSeries tests multi-series line chart with multiple y-columns
func TestRenderLine_MultiSeries(t *testing.T) {
	// Create test data with multiple series
	xLabels := []string{"Q1", "Q2", "Q3", "Q4"}
	y1Values := []float64{100.0, 120.0, 110.0, 130.0}
	y2Values := []float64{80.0, 90.0, 95.0, 100.0}
	y3Values := []int64{50, 60, 55, 70}

	xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
	if err != nil {
		t.Fatalf("Failed to create xSeries: %v", err)
	}

	y1Series, err := collection.NewFloat64SeriesFromData(y1Values, nil)
	if err != nil {
		t.Fatalf("Failed to create y1Series: %v", err)
	}

	y2Series, err := collection.NewFloat64SeriesFromData(y2Values, nil)
	if err != nil {
		t.Fatalf("Failed to create y2Series: %v", err)
	}

	y3Series, err := collection.NewInt64SeriesFromData(y3Values, nil)
	if err != nil {
		t.Fatalf("Failed to create y3Series: %v", err)
	}

	// Create temporary output file
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_line_multi.html")

	opts := &plot.ChartOptions{
		Title:      "Multi-Series Line Chart",
		Width:      900,
		Height:     600,
		OutputPath: outputPath,
	}

	// Render line chart with multiple series
	ySeriesList := []collection.Series{y1Series, y2Series, y3Series}
	seriesNames := []string{"Product A", "Product B", "Product C"}

	err = plot.RenderLine(xSeries, ySeriesList, seriesNames, opts)
	if err != nil {
		t.Fatalf("RenderLine failed with multiple series: %v", err)
	}

	// Verify file was created
	fileInfo, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Output file not created: %v", err)
	}

	// Verify file is not empty
	if fileInfo.Size() == 0 {
		t.Fatal("Output file is empty")
	}
}

// TestRenderLine_Int64Values tests line chart generation with int64 values
func TestRenderLine_Int64Values(t *testing.T) {
	// Create test data with int64
	xLabels := []string{"Week 1", "Week 2", "Week 3", "Week 4"}
	yValues := []int64{1000, 1500, 1200, 1800}

	xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
	if err != nil {
		t.Fatalf("Failed to create xSeries: %v", err)
	}

	ySeries, err := collection.NewInt64SeriesFromData(yValues, nil)
	if err != nil {
		t.Fatalf("Failed to create ySeries: %v", err)
	}

	// Create temporary output file
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_line_int64.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render line chart
	err = plot.RenderLine(xSeries, []collection.Series{ySeries}, []string{"Sales"}, opts)
	if err != nil {
		t.Fatalf("RenderLine failed with int64 values: %v", err)
	}

	// Verify file was created
	_, err = os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Output file not created: %v", err)
	}
}

// TestRenderLine_NilXSeries tests error handling for nil xSeries
func TestRenderLine_NilXSeries(t *testing.T) {
	yValues := []float64{10.5, 20.3, 15.7}
	ySeries, err := collection.NewFloat64SeriesFromData(yValues, nil)
	if err != nil {
		t.Fatalf("Failed to create ySeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_line.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render line chart with nil xSeries
	err = plot.RenderLine(nil, []collection.Series{ySeries}, []string{"Series 1"}, opts)
	if err == nil {
		t.Fatal("Expected error for nil xSeries, got nil")
	}
}

// TestRenderLine_EmptyYSeriesList tests error handling for empty ySeriesList
func TestRenderLine_EmptyYSeriesList(t *testing.T) {
	xLabels := []string{"A", "B", "C"}
	xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
	if err != nil {
		t.Fatalf("Failed to create xSeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_line.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render line chart with empty ySeriesList
	err = plot.RenderLine(xSeries, []collection.Series{}, []string{}, opts)
	if err == nil {
		t.Fatal("Expected error for empty ySeriesList, got nil")
	}
}

// TestRenderLine_NilYSeries tests error handling for nil ySeries in list
func TestRenderLine_NilYSeries(t *testing.T) {
	xLabels := []string{"A", "B", "C"}
	xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
	if err != nil {
		t.Fatalf("Failed to create xSeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_line.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render line chart with nil ySeries in list
	err = plot.RenderLine(xSeries, []collection.Series{nil}, []string{"Series 1"}, opts)
	if err == nil {
		t.Fatal("Expected error for nil ySeries in list, got nil")
	}
}

// TestRenderLine_IncompatibleTypes tests error handling for incompatible types
func TestRenderLine_IncompatibleTypes(t *testing.T) {
	// Create test data with string ySeries (incompatible)
	xLabels := []string{"A", "B", "C"}
	yLabels := []string{"X", "Y", "Z"}

	xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
	if err != nil {
		t.Fatalf("Failed to create xSeries: %v", err)
	}

	ySeries, err := collection.NewStringSeriesFromData(yLabels, nil)
	if err != nil {
		t.Fatalf("Failed to create ySeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_line.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render line chart with incompatible types
	err = plot.RenderLine(xSeries, []collection.Series{ySeries}, []string{"Series 1"}, opts)
	if err == nil {
		t.Fatal("Expected error for incompatible types, got nil")
	}
}

// TestRenderLine_MismatchedSeriesNames tests error handling when seriesNames length doesn't match ySeriesList
func TestRenderLine_MismatchedSeriesNames(t *testing.T) {
	xLabels := []string{"A", "B", "C"}
	yValues := []float64{10.5, 20.3, 15.7}

	xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
	if err != nil {
		t.Fatalf("Failed to create xSeries: %v", err)
	}

	ySeries, err := collection.NewFloat64SeriesFromData(yValues, nil)
	if err != nil {
		t.Fatalf("Failed to create ySeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_line.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render line chart with mismatched seriesNames length
	err = plot.RenderLine(xSeries, []collection.Series{ySeries}, []string{"Series 1", "Series 2"}, opts)
	if err == nil {
		t.Fatal("Expected error for mismatched seriesNames length, got nil")
	}
}

// TestRenderLine_NullValues tests null value handling in Series data
func TestRenderLine_NullValues(t *testing.T) {
	// Create test data with null values
	xLabels := []string{"A", "B", "C", "D", "E"}
	yValues := []float64{10.5, 20.3, 15.7, 30.2, 25.1}
	yMask := []bool{false, true, false, false, true} // B and E are null

	xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
	if err != nil {
		t.Fatalf("Failed to create xSeries: %v", err)
	}

	ySeries, err := collection.NewFloat64SeriesFromData(yValues, yMask)
	if err != nil {
		t.Fatalf("Failed to create ySeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_line_nulls.html")

	opts := &plot.ChartOptions{
		Title:      "Line Chart with Nulls",
		OutputPath: outputPath,
	}

	// Render line chart
	err = plot.RenderLine(xSeries, []collection.Series{ySeries}, []string{"Data"}, opts)
	if err != nil {
		t.Fatalf("RenderLine failed with null values: %v", err)
	}

	// Verify file was created
	_, err = os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Output file not created: %v", err)
	}
}

// TestRenderLine_MultiSeriesWithNulls tests multi-series line chart with null values
func TestRenderLine_MultiSeriesWithNulls(t *testing.T) {
	// Create test data with multiple series containing null values
	xLabels := []string{"Jan", "Feb", "Mar", "Apr", "May"}
	y1Values := []float64{100.0, 120.0, 110.0, 130.0, 140.0}
	y1Mask := []bool{false, false, true, false, false} // Mar is null
	y2Values := []int64{80, 90, 95, 100, 105}
	y2Mask := []bool{false, true, false, false, true} // Feb and May are null

	xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
	if err != nil {
		t.Fatalf("Failed to create xSeries: %v", err)
	}

	y1Series, err := collection.NewFloat64SeriesFromData(y1Values, y1Mask)
	if err != nil {
		t.Fatalf("Failed to create y1Series: %v", err)
	}

	y2Series, err := collection.NewInt64SeriesFromData(y2Values, y2Mask)
	if err != nil {
		t.Fatalf("Failed to create y2Series: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_line_multi_nulls.html")

	opts := &plot.ChartOptions{
		Title:      "Multi-Series with Nulls",
		OutputPath: outputPath,
	}

	// Render line chart
	err = plot.RenderLine(xSeries, []collection.Series{y1Series, y2Series}, []string{"Series A", "Series B"}, opts)
	if err != nil {
		t.Fatalf("RenderLine failed with multi-series and null values: %v", err)
	}

	// Verify file was created
	_, err = os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Output file not created: %v", err)
	}
}

// TestRenderLine_EmptyOutputPath tests error handling for empty output path
func TestRenderLine_EmptyOutputPath(t *testing.T) {
	xLabels := []string{"A", "B", "C"}
	yValues := []float64{10.5, 20.3, 15.7}

	xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
	if err != nil {
		t.Fatalf("Failed to create xSeries: %v", err)
	}

	ySeries, err := collection.NewFloat64SeriesFromData(yValues, nil)
	if err != nil {
		t.Fatalf("Failed to create ySeries: %v", err)
	}

	opts := &plot.ChartOptions{
		OutputPath: "", // Empty output path
	}

	// Render line chart
	err = plot.RenderLine(xSeries, []collection.Series{ySeries}, []string{"Series 1"}, opts)
	if err == nil {
		t.Fatal("Expected error for empty output path, got nil")
	}
}

// TestRenderLine_DefaultOptions tests that default options are applied correctly
func TestRenderLine_DefaultOptions(t *testing.T) {
	xLabels := []string{"A", "B", "C"}
	yValues := []float64{10.5, 20.3, 15.7}

	xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
	if err != nil {
		t.Fatalf("Failed to create xSeries: %v", err)
	}

	ySeries, err := collection.NewFloat64SeriesFromData(yValues, nil)
	if err != nil {
		t.Fatalf("Failed to create ySeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_line_defaults.html")

	// Use options without Width and Height to test defaults
	opts := &plot.ChartOptions{
		OutputPath: outputPath,
		// Width and Height not specified, should use defaults
	}

	// Render line chart
	err = plot.RenderLine(xSeries, []collection.Series{ySeries}, []string{"Data"}, opts)
	if err != nil {
		t.Fatalf("RenderLine failed with default options: %v", err)
	}

	// Verify file was created
	_, err = os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Output file not created: %v", err)
	}
}
