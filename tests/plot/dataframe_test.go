package gpandas_test

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// Helper function to create a test DataFrame with string and numeric columns
func createTestDataFrame(t *testing.T) *dataframe.DataFrame {
	t.Helper()

	categories, err := collection.NewStringSeriesFromData(
		[]string{"A", "B", "C", "D"},
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to create categories series: %v", err)
	}

	values, err := collection.NewFloat64SeriesFromData(
		[]float64{10.0, 20.0, 30.0, 40.0},
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to create values series: %v", err)
	}

	return &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"category": categories,
			"value":    values,
		},
		ColumnOrder: []string{"category", "value"},
		Index:       []string{"0", "1", "2", "3"},
	}
}

// Test PlotBar with valid DataFrame and columns
func TestDataFrame_PlotBar_ValidData(t *testing.T) {
	df := createTestDataFrame(t)

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "bar_chart.html")

	opts := &plot.ChartOptions{
		Title:      "Test Bar Chart",
		Width:      800,
		Height:     600,
		OutputPath: outputPath,
	}

	err := df.PlotBar("category", "value", opts)
	if err != nil {
		t.Fatalf("PlotBar failed: %v", err)
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

	// Verify file contains HTML content
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	if !strings.Contains(string(content), "html") {
		t.Fatal("Output file does not contain HTML content")
	}
}

// Test PlotPie with valid DataFrame and columns
func TestDataFrame_PlotPie_ValidData(t *testing.T) {
	df := createTestDataFrame(t)

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "pie_chart.html")

	opts := &plot.ChartOptions{
		Title:      "Test Pie Chart",
		Width:      800,
		Height:     600,
		OutputPath: outputPath,
	}

	err := df.PlotPie("category", "value", opts)
	if err != nil {
		t.Fatalf("PlotPie failed: %v", err)
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

// Test PlotLine with valid DataFrame and single y-column
func TestDataFrame_PlotLine_SingleYColumn(t *testing.T) {
	df := createTestDataFrame(t)

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "line_chart.html")

	opts := &plot.ChartOptions{
		Title:      "Test Line Chart",
		Width:      800,
		Height:     600,
		OutputPath: outputPath,
	}

	err := df.PlotLine("category", []string{"value"}, opts)
	if err != nil {
		t.Fatalf("PlotLine failed: %v", err)
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

// Test PlotLine with multiple y-columns
func TestDataFrame_PlotLine_MultipleYColumns(t *testing.T) {
	// Create DataFrame with multiple numeric columns
	categories, err := collection.NewStringSeriesFromData(
		[]string{"Jan", "Feb", "Mar", "Apr"},
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to create categories series: %v", err)
	}

	sales, err := collection.NewFloat64SeriesFromData(
		[]float64{100.0, 150.0, 200.0, 180.0},
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to create sales series: %v", err)
	}

	costs, err := collection.NewFloat64SeriesFromData(
		[]float64{60.0, 80.0, 120.0, 100.0},
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to create costs series: %v", err)
	}

	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"month": categories,
			"sales": sales,
			"costs": costs,
		},
		ColumnOrder: []string{"month", "sales", "costs"},
		Index:       []string{"0", "1", "2", "3"},
	}

	tmpDir := t.TempDir()
	outputPath := filepath.Join(tmpDir, "multi_line_chart.html")

	opts := &plot.ChartOptions{
		Title:      "Sales vs Costs",
		Width:      800,
		Height:     600,
		OutputPath: outputPath,
	}

	err = df.PlotLine("month", []string{"sales", "costs"}, opts)
	if err != nil {
		t.Fatalf("PlotLine with multiple y-columns failed: %v", err)
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

// Test error handling for empty DataFrame
func TestDataFrame_PlotBar_EmptyDataFrame(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns:     map[string]collection.Series{},
		ColumnOrder: []string{},
		Index:       []string{},
	}

	tmpDir := t.TempDir()
	opts := &plot.ChartOptions{
		OutputPath: filepath.Join(tmpDir, "test.html"),
	}

	err := df.PlotBar("x", "y", opts)
	if err == nil {
		t.Fatal("Expected error for empty DataFrame, got nil")
	}

	if !strings.Contains(err.Error(), "empty") {
		t.Fatalf("Expected error message to contain 'empty', got: %v", err)
	}

	if !strings.Contains(err.Error(), "PlotBar") {
		t.Fatalf("Expected error message to contain 'PlotBar' context, got: %v", err)
	}
}

// Test error handling for missing columns
func TestDataFrame_PlotBar_MissingColumn(t *testing.T) {
	df := createTestDataFrame(t)

	tmpDir := t.TempDir()
	opts := &plot.ChartOptions{
		OutputPath: filepath.Join(tmpDir, "test.html"),
	}

	// Test with missing x column
	err := df.PlotBar("nonexistent", "value", opts)
	if err == nil {
		t.Fatal("Expected error for missing x column, got nil")
	}

	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Expected error message to contain 'not found', got: %v", err)
	}

	if !strings.Contains(err.Error(), "nonexistent") {
		t.Fatalf("Expected error message to contain column name 'nonexistent', got: %v", err)
	}

	// Test with missing y column
	err = df.PlotBar("category", "nonexistent", opts)
	if err == nil {
		t.Fatal("Expected error for missing y column, got nil")
	}

	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Expected error message to contain 'not found', got: %v", err)
	}
}

// Test error handling for PlotPie with missing columns
func TestDataFrame_PlotPie_MissingColumn(t *testing.T) {
	df := createTestDataFrame(t)

	tmpDir := t.TempDir()
	opts := &plot.ChartOptions{
		OutputPath: filepath.Join(tmpDir, "test.html"),
	}

	err := df.PlotPie("nonexistent", "value", opts)
	if err == nil {
		t.Fatal("Expected error for missing label column, got nil")
	}

	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Expected error message to contain 'not found', got: %v", err)
	}

	if !strings.Contains(err.Error(), "PlotPie") {
		t.Fatalf("Expected error message to contain 'PlotPie' context, got: %v", err)
	}
}

// Test error handling for PlotLine with missing columns
func TestDataFrame_PlotLine_MissingColumn(t *testing.T) {
	df := createTestDataFrame(t)

	tmpDir := t.TempDir()
	opts := &plot.ChartOptions{
		OutputPath: filepath.Join(tmpDir, "test.html"),
	}

	// Test with missing x column
	err := df.PlotLine("nonexistent", []string{"value"}, opts)
	if err == nil {
		t.Fatal("Expected error for missing x column, got nil")
	}

	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Expected error message to contain 'not found', got: %v", err)
	}

	// Test with missing y column
	err = df.PlotLine("category", []string{"nonexistent"}, opts)
	if err == nil {
		t.Fatal("Expected error for missing y column, got nil")
	}

	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("Expected error message to contain 'not found', got: %v", err)
	}

	if !strings.Contains(err.Error(), "PlotLine") {
		t.Fatalf("Expected error message to contain 'PlotLine' context, got: %v", err)
	}
}

// Test thread safety with concurrent plotting (race detector enabled)
func TestDataFrame_ConcurrentPlotting(t *testing.T) {
	df := createTestDataFrame(t)

	tmpDir := t.TempDir()

	// Launch multiple concurrent plot operations
	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			opts := &plot.ChartOptions{
				OutputPath: filepath.Join(tmpDir, "chart_"+string(rune('0'+idx))+".html"),
			}

			// Alternate between different plot types
			switch idx % 3 {
			case 0:
				_ = df.PlotBar("category", "value", opts)
			case 1:
				_ = df.PlotPie("category", "value", opts)
			case 2:
				_ = df.PlotLine("category", []string{"value"}, opts)
			}
		}(i)
	}

	wg.Wait()

	// If we reach here without panic or race condition, test passes
	// The race detector will catch any data races during test execution
}

// Test PlotLine with empty yCols
func TestDataFrame_PlotLine_EmptyYCols(t *testing.T) {
	df := createTestDataFrame(t)

	tmpDir := t.TempDir()
	opts := &plot.ChartOptions{
		OutputPath: filepath.Join(tmpDir, "test.html"),
	}

	err := df.PlotLine("category", []string{}, opts)
	if err == nil {
		t.Fatal("Expected error for empty yCols, got nil")
	}

	if !strings.Contains(err.Error(), "empty") || !strings.Contains(err.Error(), "PlotLine") {
		t.Fatalf("Expected error message about empty yCols with PlotLine context, got: %v", err)
	}
}

// Test nil DataFrame
func TestDataFrame_PlotBar_NilDataFrame(t *testing.T) {
	var df *dataframe.DataFrame

	tmpDir := t.TempDir()
	opts := &plot.ChartOptions{
		OutputPath: filepath.Join(tmpDir, "test.html"),
	}

	err := df.PlotBar("x", "y", opts)
	if err == nil {
		t.Fatal("Expected error for nil DataFrame, got nil")
	}

	if !strings.Contains(err.Error(), "nil") {
		t.Fatalf("Expected error message to contain 'nil', got: %v", err)
	}
}
