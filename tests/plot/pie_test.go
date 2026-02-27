package gpandas_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// TestRenderPie_ValidData tests successful pie chart generation with valid data
func TestRenderPie_ValidData(t *testing.T) {
	// Create test data
	labels := []string{"Category A", "Category B", "Category C", "Category D"}
	values := []float64{25.5, 30.3, 20.7, 23.5}

	labelSeries, err := collection.NewStringSeriesFromData(labels, nil)
	if err != nil {
		t.Fatalf("Failed to create labelSeries: %v", err)
	}

	valueSeries, err := collection.NewFloat64SeriesFromData(values, nil)
	if err != nil {
		t.Fatalf("Failed to create valueSeries: %v", err)
	}

	// Create temporary output file
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_pie.html")

	opts := &plot.ChartOptions{
		Title:      "Test Pie Chart",
		Width:      800,
		Height:     600,
		OutputPath: outputPath,
	}

	// Render pie chart
	err = plot.RenderPie(labelSeries, valueSeries, opts)
	if err != nil {
		t.Fatalf("RenderPie failed: %v", err)
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

// TestRenderPie_Int64Values tests pie chart generation with int64 values
func TestRenderPie_Int64Values(t *testing.T) {
	// Create test data with int64
	labels := []string{"Q1", "Q2", "Q3", "Q4"}
	values := []int64{1000, 1500, 1200, 1800}

	labelSeries, err := collection.NewStringSeriesFromData(labels, nil)
	if err != nil {
		t.Fatalf("Failed to create labelSeries: %v", err)
	}

	valueSeries, err := collection.NewInt64SeriesFromData(values, nil)
	if err != nil {
		t.Fatalf("Failed to create valueSeries: %v", err)
	}

	// Create temporary output file
	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_pie_int64.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render pie chart
	err = plot.RenderPie(labelSeries, valueSeries, opts)
	if err != nil {
		t.Fatalf("RenderPie failed with int64 values: %v", err)
	}

	// Verify file was created
	_, err = os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Output file not created: %v", err)
	}
}

// TestRenderPie_NilLabelSeries tests error handling for nil labelSeries
func TestRenderPie_NilLabelSeries(t *testing.T) {
	values := []float64{25.5, 30.3, 20.7}
	valueSeries, err := collection.NewFloat64SeriesFromData(values, nil)
	if err != nil {
		t.Fatalf("Failed to create valueSeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_pie.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render pie chart with nil labelSeries
	err = plot.RenderPie(nil, valueSeries, opts)
	if err == nil {
		t.Fatal("Expected error for nil labelSeries, got nil")
	}
}

// TestRenderPie_NilValueSeries tests error handling for nil valueSeries
func TestRenderPie_NilValueSeries(t *testing.T) {
	labels := []string{"A", "B", "C"}
	labelSeries, err := collection.NewStringSeriesFromData(labels, nil)
	if err != nil {
		t.Fatalf("Failed to create labelSeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_pie.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render pie chart with nil valueSeries
	err = plot.RenderPie(labelSeries, nil, opts)
	if err == nil {
		t.Fatal("Expected error for nil valueSeries, got nil")
	}
}

// TestRenderPie_IncompatibleTypes tests error handling for incompatible types
func TestRenderPie_IncompatibleTypes(t *testing.T) {
	// Create test data with string valueSeries (incompatible)
	labels := []string{"A", "B", "C"}
	values := []string{"X", "Y", "Z"}

	labelSeries, err := collection.NewStringSeriesFromData(labels, nil)
	if err != nil {
		t.Fatalf("Failed to create labelSeries: %v", err)
	}

	valueSeries, err := collection.NewStringSeriesFromData(values, nil)
	if err != nil {
		t.Fatalf("Failed to create valueSeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_pie.html")

	opts := &plot.ChartOptions{
		OutputPath: outputPath,
	}

	// Render pie chart with incompatible types
	err = plot.RenderPie(labelSeries, valueSeries, opts)
	if err == nil {
		t.Fatal("Expected error for incompatible types, got nil")
	}
}

// TestRenderPie_NullValues tests null value handling in Series data
func TestRenderPie_NullValues(t *testing.T) {
	// Create test data with null values
	labels := []string{"A", "B", "C", "D", "E"}
	values := []float64{10.5, 20.3, 15.7, 30.2, 25.1}
	valueMask := []bool{false, true, false, false, true} // B and E are null

	labelSeries, err := collection.NewStringSeriesFromData(labels, nil)
	if err != nil {
		t.Fatalf("Failed to create labelSeries: %v", err)
	}

	valueSeries, err := collection.NewFloat64SeriesFromData(values, valueMask)
	if err != nil {
		t.Fatalf("Failed to create valueSeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_pie_nulls.html")

	opts := &plot.ChartOptions{
		Title:      "Pie Chart with Nulls",
		OutputPath: outputPath,
	}

	// Render pie chart
	err = plot.RenderPie(labelSeries, valueSeries, opts)
	if err != nil {
		t.Fatalf("RenderPie failed with null values: %v", err)
	}

	// Verify file was created
	_, err = os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Output file not created: %v", err)
	}
}

// TestRenderPie_EmptyOutputPath tests error handling for empty output path
func TestRenderPie_EmptyOutputPath(t *testing.T) {
	labels := []string{"A", "B", "C"}
	values := []float64{10.5, 20.3, 15.7}

	labelSeries, err := collection.NewStringSeriesFromData(labels, nil)
	if err != nil {
		t.Fatalf("Failed to create labelSeries: %v", err)
	}

	valueSeries, err := collection.NewFloat64SeriesFromData(values, nil)
	if err != nil {
		t.Fatalf("Failed to create valueSeries: %v", err)
	}

	opts := &plot.ChartOptions{
		OutputPath: "", // Empty output path
	}

	// Render pie chart
	err = plot.RenderPie(labelSeries, valueSeries, opts)
	if err == nil {
		t.Fatal("Expected error for empty output path, got nil")
	}
}

// TestRenderPie_DefaultOptions tests that default options are applied correctly
func TestRenderPie_DefaultOptions(t *testing.T) {
	labels := []string{"A", "B", "C"}
	values := []float64{10.5, 20.3, 15.7}

	labelSeries, err := collection.NewStringSeriesFromData(labels, nil)
	if err != nil {
		t.Fatalf("Failed to create labelSeries: %v", err)
	}

	valueSeries, err := collection.NewFloat64SeriesFromData(values, nil)
	if err != nil {
		t.Fatalf("Failed to create valueSeries: %v", err)
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "test_pie_defaults.html")

	// Use options without Width and Height to test defaults
	opts := &plot.ChartOptions{
		OutputPath: outputPath,
		// Width and Height not specified, should use defaults
	}

	// Render pie chart
	err = plot.RenderPie(labelSeries, valueSeries, opts)
	if err != nil {
		t.Fatalf("RenderPie failed with default options: %v", err)
	}

	// Verify file was created
	_, err = os.Stat(outputPath)
	if err != nil {
		t.Fatalf("Output file not created: %v", err)
	}
}
