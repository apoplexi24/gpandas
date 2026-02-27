package gpandas_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// TestRenderBar_ValidData tests successful bar chart generation with valid data
func TestRenderBar_ValidData(t *testing.T) {
	// Create test data
	xLabels := []string{"A", "B", "C", "D"}
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
	outputPath := filepath.Join(tmpDir, "test_bar.html")

	opts := &plot.ChartOptions{
		Title:      "Test Bar Chart",
		Width:      800,
		Height:     600,
		OutputPath: outputPath,
	}

	// Render bar chart
	err = plot.RenderBar(xSeries, ySeries, opts)
	if err != nil {
		t.Fatalf("RenderBar failed: %v", err)
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

// TestRenderBar_Int64Values tests bar chart generation with int64 values
func TestRenderBar_Int64Values(t *testing.T) {
	// Create test data with int64
	xLabels := []string{"Jan", "Feb", "Mar"}
	yValues := []int64{100, 200, 150}

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
	outputPath := filepath.Join(tmpDir, "test_bar_int64.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render bar chart
	err = plot.RenderBar(xSeries, ySeries, opts)
	if err != nil {
		t.Fatalf("RenderBar failed with int64 values: %v", err)
	}

	// Verify file was created
	_, err = os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Output file not created: %v", err)
	}
}

// TestRenderBar_NilXSeries tests error handling for nil xSeries
func TestRenderBar_NilXSeries(t *testing.T) {
	yValues := []float64{10.5, 20.3, 15.7}
	ySeries, err := collection.NewFloat64SeriesFromData(yValues, nil)
	if err != nil {
		t.Fatalf("Failed to create ySeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_bar.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render bar chart with nil xSeries
	err = plot.RenderBar(nil, ySeries, opts)
	if err == nil {
		t.Fatal("Expected error for nil xSeries, got nil")
	}
}

// TestRenderBar_NilYSeries tests error handling for nil ySeries
func TestRenderBar_NilYSeries(t *testing.T) {
	xLabels := []string{"A", "B", "C"}
	xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
	if err != nil {
		t.Fatalf("Failed to create xSeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_bar.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render bar chart with nil ySeries
	err = plot.RenderBar(xSeries, nil, opts)
	if err == nil {
		t.Fatal("Expected error for nil ySeries, got nil")
	}
}

// TestRenderBar_IncompatibleTypes tests error handling for incompatible types
func TestRenderBar_IncompatibleTypes(t *testing.T) {
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
	outputPath := filepath.Join(tmpDir, "test_bar.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render bar chart with incompatible types
	err = plot.RenderBar(xSeries, ySeries, opts)
	if err == nil {
		t.Fatal("Expected error for incompatible types, got nil")
	}
}

// TestRenderBar_NullValues tests null value handling in Series data
func TestRenderBar_NullValues(t *testing.T) {
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
	outputPath := filepath.Join(tmpDir, "test_bar_nulls.html")

	opts := &plot.ChartOptions{
		Title:      "Bar Chart with Nulls",
		OutputPath: outputPath,
	}

	// Render bar chart
	err = plot.RenderBar(xSeries, ySeries, opts)
	if err != nil {
		t.Fatalf("RenderBar failed with null values: %v", err)
	}

	// Verify file was created
	_, err = os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Output file not created: %v", err)
	}
}

// TestRenderBar_EmptyOutputPath tests error handling for empty output path
func TestRenderBar_EmptyOutputPath(t *testing.T) {
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

	// Render bar chart
	err = plot.RenderBar(xSeries, ySeries, opts)
	if err == nil {
		t.Fatal("Expected error for empty output path, got nil")
	}
}

// TestRenderBar_DefaultOptions tests that default options are applied correctly
func TestRenderBar_DefaultOptions(t *testing.T) {
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
	outputPath := filepath.Join(tmpDir, "test_bar_defaults.html")

	// Use nil options to test defaults
	opts := &plot.ChartOptions{
		OutputPath: outputPath,
		// Width and Height not specified, should use defaults
	}

	// Render bar chart
	err = plot.RenderBar(xSeries, ySeries, opts)
	if err != nil {
		t.Fatalf("RenderBar failed with default options: %v", err)
	}

	// Verify file was created
	_, err = os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Output file not created: %v", err)
	}
}
