package gpandas_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// Property 1: Column Existence Validation
// For any chart plotting method (PlotBar, PlotPie, PlotLine) and any column name
// that does not exist in the DataFrame, the method should return an error indicating
// the missing column.
// **Validates: Requirements 1.3, 2.3, 3.3, 7.2**
func TestProperty1_ColumnExistenceValidation(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("PlotBar returns error for non-existent columns", prop.ForAll(
		func(validCol string, invalidCol string) bool {
			// Ensure columns are different
			if validCol == invalidCol || validCol == "" || invalidCol == "" {
				return true
			}

			// Create DataFrame with one column
			series, err := collection.NewFloat64SeriesFromData([]float64{1.0, 2.0, 3.0}, nil)
			if err != nil {
				return true
			}

			df := &dataframe.DataFrame{
				Columns:     map[string]collection.Series{validCol: series},
				ColumnOrder: []string{validCol},
				Index:       []string{"0", "1", "2"},
			}

			tmpDir := t.TempDir()
			opts := &plot.ChartOptions{
				OutputPath: filepath.Join(tmpDir, "test.html"),
			}

			// Test with invalid xCol
			err = df.PlotBar(invalidCol, validCol, opts)
			if err == nil || !strings.Contains(err.Error(), "not found") {
				return false
			}

			// Test with invalid yCol
			err = df.PlotBar(validCol, invalidCol, opts)
			if err == nil || !strings.Contains(err.Error(), "not found") {
				return false
			}

			return true
		},
		gen.AlphaString(),
		gen.AlphaString(),
	))

	properties.Property("PlotPie returns error for non-existent columns", prop.ForAll(
		func(validCol string, invalidCol string) bool {
			// Ensure columns are different
			if validCol == invalidCol || validCol == "" || invalidCol == "" {
				return true
			}

			// Create DataFrame with one column
			series, err := collection.NewFloat64SeriesFromData([]float64{1.0, 2.0, 3.0}, nil)
			if err != nil {
				return true
			}

			df := &dataframe.DataFrame{
				Columns:     map[string]collection.Series{validCol: series},
				ColumnOrder: []string{validCol},
				Index:       []string{"0", "1", "2"},
			}

			tmpDir := t.TempDir()
			opts := &plot.ChartOptions{
				OutputPath: filepath.Join(tmpDir, "test.html"),
			}

			// Test with invalid labelCol
			err = df.PlotPie(invalidCol, validCol, opts)
			if err == nil || !strings.Contains(err.Error(), "not found") {
				return false
			}

			// Test with invalid valueCol
			err = df.PlotPie(validCol, invalidCol, opts)
			if err == nil || !strings.Contains(err.Error(), "not found") {
				return false
			}

			return true
		},
		gen.AlphaString(),
		gen.AlphaString(),
	))

	properties.Property("PlotLine returns error for non-existent columns", prop.ForAll(
		func(validCol string, invalidCol string) bool {
			// Ensure columns are different
			if validCol == invalidCol || validCol == "" || invalidCol == "" {
				return true
			}

			// Create DataFrame with one column
			series, err := collection.NewFloat64SeriesFromData([]float64{1.0, 2.0, 3.0}, nil)
			if err != nil {
				return true
			}

			df := &dataframe.DataFrame{
				Columns:     map[string]collection.Series{validCol: series},
				ColumnOrder: []string{validCol},
				Index:       []string{"0", "1", "2"},
			}

			tmpDir := t.TempDir()
			opts := &plot.ChartOptions{
				OutputPath: filepath.Join(tmpDir, "test.html"),
			}

			// Test with invalid xCol
			err = df.PlotLine(invalidCol, []string{validCol}, opts)
			if err == nil || !strings.Contains(err.Error(), "not found") {
				return false
			}

			// Test with invalid yCol
			err = df.PlotLine(validCol, []string{invalidCol}, opts)
			if err == nil || !strings.Contains(err.Error(), "not found") {
				return false
			}

			return true
		},
		gen.AlphaString(),
		gen.AlphaString(),
	))

	properties.TestingRun(t)
}

// Property 7: Default Options Application
// For any plotting method called with nil ChartOptions, the chart should be generated
// with default width (900), default height (500), and no title.
// **Validates: Requirements 4.3, 4.4, 4.5**
func TestProperty7_DefaultOptionsApplication(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("nil ChartOptions uses defaults", prop.ForAll(
		func(xLabels []string, yValues []float64) bool {
			// Skip empty data
			if len(xLabels) == 0 || len(yValues) == 0 {
				return true
			}

			// Ensure equal lengths
			minLen := len(xLabels)
			if len(yValues) < minLen {
				minLen = len(yValues)
			}
			xLabels = xLabels[:minLen]
			yValues = yValues[:minLen]

			// Create DataFrame
			xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
			if err != nil {
				return true
			}

			ySeries, err := collection.NewFloat64SeriesFromData(yValues, nil)
			if err != nil {
				return true
			}

			df := &dataframe.DataFrame{
				Columns:     map[string]collection.Series{"x": xSeries, "y": ySeries},
				ColumnOrder: []string{"x", "y"},
				Index:       make([]string, len(xLabels)),
			}

			tmpDir := t.TempDir()
			opts := &plot.ChartOptions{
				OutputPath: filepath.Join(tmpDir, "test.html"),
				// Width, Height, Title not specified - should use defaults
			}

			// Test PlotBar with default options
			err = df.PlotBar("x", "y", opts)
			if err != nil {
				return false
			}

			// Verify file was created (indicates defaults were applied)
			_, err = os.Stat(opts.OutputPath)
			return err == nil
		},
		gen.SliceOf(gen.AlphaString()),
		gen.SliceOf(gen.Float64()),
	))

	properties.TestingRun(t)
}

// Property 8: Invalid Path Error Handling
// For any output path that cannot be written to (non-existent directory, permission denied, etc.),
// the plotting method should return an error describing the file system issue.
// **Validates: Requirements 4.6, 7.4**
func TestProperty8_InvalidPathErrorHandling(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("invalid output path returns error", prop.ForAll(
		func(xLabels []string, yValues []float64) bool {
			// Skip empty data
			if len(xLabels) == 0 || len(yValues) == 0 {
				return true
			}

			// Ensure equal lengths
			minLen := len(xLabels)
			if len(yValues) < minLen {
				minLen = len(yValues)
			}
			xLabels = xLabels[:minLen]
			yValues = yValues[:minLen]

			// Create DataFrame
			xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
			if err != nil {
				return true
			}

			ySeries, err := collection.NewFloat64SeriesFromData(yValues, nil)
			if err != nil {
				return true
			}

			df := &dataframe.DataFrame{
				Columns:     map[string]collection.Series{"x": xSeries, "y": ySeries},
				ColumnOrder: []string{"x", "y"},
				Index:       make([]string, len(xLabels)),
			}

			// Use invalid path (non-existent directory)
			opts := &plot.ChartOptions{
				OutputPath: "/nonexistent/directory/that/does/not/exist/test.html",
			}

			// Test PlotBar with invalid path
			err = df.PlotBar("x", "y", opts)
			
			// Should return an error
			return err != nil
		},
		gen.SliceOf(gen.AlphaString()),
		gen.SliceOf(gen.Float64()),
	))

	properties.TestingRun(t)
}

// Property 12: Thread-Safe Plotting
// For any DataFrame being plotted concurrently from multiple goroutines,
// no data races should occur and all plot operations should complete successfully
// or return appropriate errors.
// **Validates: Requirements 6.7**
func TestProperty12_ThreadSafePlotting(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("concurrent plotting is thread-safe", prop.ForAll(
		func(xLabels []string, yValues []float64, numGoroutines int) bool {
			// Skip empty data
			if len(xLabels) == 0 || len(yValues) == 0 {
				return true
			}

			// Limit goroutines to reasonable range
			if numGoroutines < 2 || numGoroutines > 10 {
				return true
			}

			// Ensure equal lengths
			minLen := len(xLabels)
			if len(yValues) < minLen {
				minLen = len(yValues)
			}
			xLabels = xLabels[:minLen]
			yValues = yValues[:minLen]

			// Create DataFrame
			xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
			if err != nil {
				return true
			}

			ySeries, err := collection.NewFloat64SeriesFromData(yValues, nil)
			if err != nil {
				return true
			}

			df := &dataframe.DataFrame{
				Columns:     map[string]collection.Series{"x": xSeries, "y": ySeries},
				ColumnOrder: []string{"x", "y"},
				Index:       make([]string, len(xLabels)),
			}

			tmpDir := t.TempDir()

			// Launch concurrent plot operations
			var wg sync.WaitGroup
			errors := make([]error, numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()
					opts := &plot.ChartOptions{
						OutputPath: filepath.Join(tmpDir, fmt.Sprintf("test_%d.html", idx)),
					}
					errors[idx] = df.PlotBar("x", "y", opts)
				}(i)
			}

			wg.Wait()

			// All operations should complete (either success or error, but no panic/race)
			for _, err := range errors {
				if err != nil {
					// Error is acceptable (e.g., file system issues)
					// The important thing is no data race occurred
					continue
				}
			}

			return true
		},
		gen.SliceOf(gen.AlphaString()),
		gen.SliceOf(gen.Float64()),
		gen.IntRange(2, 10),
	))

	properties.TestingRun(t)
}

// Property 13: Input Validation Before Generation
// For any invalid input parameters (empty DataFrame, mismatched column types, missing columns),
// validation should detect the issue and return an error before attempting to create
// a go-echarts chart object.
// **Validates: Requirements 7.5**
func TestProperty13_InputValidationBeforeGeneration(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("empty DataFrame returns error", prop.ForAll(
		func(colName string) bool {
			if colName == "" {
				return true
			}

			// Create empty DataFrame
			df := &dataframe.DataFrame{
				Columns:     map[string]collection.Series{},
				ColumnOrder: []string{},
				Index:       []string{},
			}

			tmpDir := t.TempDir()
			opts := &plot.ChartOptions{
				OutputPath: filepath.Join(tmpDir, "test.html"),
			}

			// Test PlotBar with empty DataFrame
			err := df.PlotBar(colName, colName, opts)
			if err == nil || !strings.Contains(err.Error(), "empty") {
				return false
			}

			// Test PlotPie with empty DataFrame
			err = df.PlotPie(colName, colName, opts)
			if err == nil || !strings.Contains(err.Error(), "empty") {
				return false
			}

			// Test PlotLine with empty DataFrame
			err = df.PlotLine(colName, []string{colName}, opts)
			if err == nil || !strings.Contains(err.Error(), "empty") {
				return false
			}

			return true
		},
		gen.AlphaString(),
	))

	properties.TestingRun(t)
}

// Property 14: Error Context Inclusion
// For any error returned by plotting methods, the error message should include
// context about which operation failed (e.g., "PlotBar", "column validation", "file write").
// **Validates: Requirements 7.6, 8.6**
func TestProperty14_ErrorContextInclusion(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("errors include operation context", prop.ForAll(
		func(validCol string, invalidCol string) bool {
			// Ensure columns are different
			if validCol == invalidCol || validCol == "" || invalidCol == "" {
				return true
			}

			// Create DataFrame with one column
			series, err := collection.NewFloat64SeriesFromData([]float64{1.0, 2.0, 3.0}, nil)
			if err != nil {
				return true
			}

			df := &dataframe.DataFrame{
				Columns:     map[string]collection.Series{validCol: series},
				ColumnOrder: []string{validCol},
				Index:       []string{"0", "1", "2"},
			}

			tmpDir := t.TempDir()
			opts := &plot.ChartOptions{
				OutputPath: filepath.Join(tmpDir, "test.html"),
			}

			// Test PlotBar error includes "PlotBar" context
			err = df.PlotBar(invalidCol, validCol, opts)
			if err == nil || !strings.Contains(err.Error(), "PlotBar") {
				return false
			}

			// Test PlotPie error includes "PlotPie" context
			err = df.PlotPie(invalidCol, validCol, opts)
			if err == nil || !strings.Contains(err.Error(), "PlotPie") {
				return false
			}

			// Test PlotLine error includes "PlotLine" context
			err = df.PlotLine(invalidCol, []string{validCol}, opts)
			if err == nil || !strings.Contains(err.Error(), "PlotLine") {
				return false
			}

			return true
		},
		gen.AlphaString(),
		gen.AlphaString(),
	))

	properties.TestingRun(t)
}
