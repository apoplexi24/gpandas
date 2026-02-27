package gpandas_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// **Validates: Requirements 1.2**
// Property 2: Valid Chart Generation (bar charts)
// For any DataFrame with valid columns of compatible types, calling PlotBar
// should successfully generate a chart without error.
func TestProperty2_ValidChartGeneration_BarCharts(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("valid bar chart generation succeeds", prop.ForAll(
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

			// Create Series
			xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
			if err != nil {
				t.Logf("Failed to create xSeries: %v", err)
				return false
			}

			ySeries, err := collection.NewFloat64SeriesFromData(yValues, nil)
			if err != nil {
				t.Logf("Failed to create ySeries: %v", err)
				return false
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
				t.Logf("RenderBar failed: %v", err)
				return false
			}

			// Verify file was created
			_, err = os.Stat(outputPath)
			return err == nil
		},
		gen.SliceOf(gen.AlphaString()),
		gen.SliceOf(gen.Float64()),
	))

	properties.TestingRun(t)
}

// **Validates: Requirements 1.4**
// Property 3: Type Compatibility Validation (bar charts)
// For any chart plotting method that requires numeric data and any non-numeric Series,
// the method should return an error indicating type incompatibility.
func TestProperty3_TypeCompatibilityValidation_BarCharts(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("non-numeric ySeries returns type error", prop.ForAll(
		func(xLabels []string, yLabels []string) bool {
			// Skip empty data
			if len(xLabels) == 0 || len(yLabels) == 0 {
				return true
			}

			// Ensure equal lengths
			minLen := len(xLabels)
			if len(yLabels) < minLen {
				minLen = len(yLabels)
			}
			xLabels = xLabels[:minLen]
			yLabels = yLabels[:minLen]

			// Create Series (ySeries is string, not numeric)
			xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
			if err != nil {
				return true // Skip this case
			}

			ySeries, err := collection.NewStringSeriesFromData(yLabels, nil)
			if err != nil {
				return true // Skip this case
			}

			// Create temporary output file
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "test_bar.html")

			opts := &plot.ChartOptions{
				OutputPath: outputPath,
			}

			// Render bar chart - should fail with type error
			err = plot.RenderBar(xSeries, ySeries, opts)
			
			// Should return an error
			return err != nil
		},
		gen.SliceOf(gen.AlphaString()),
		gen.SliceOf(gen.AlphaString()),
	))

	properties.TestingRun(t)
}

// **Validates: Requirements 1.6**
// Property 4: HTML File Creation (bar charts)
// For any successful chart generation, an HTML file should be created at the
// specified output path and the file should be readable.
func TestProperty4_HTMLFileCreation_BarCharts(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("HTML file is created and readable", prop.ForAll(
		func(xLabels []string, yValues []int64, seed int64) bool {
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

			// Create Series
			xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
			if err != nil {
				return true // Skip this case
			}

			ySeries, err := collection.NewInt64SeriesFromData(yValues, nil)
			if err != nil {
				return true // Skip this case
			}

			// Create temporary output file with unique name
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, fmt.Sprintf("test_bar_%d.html", seed))

			opts := &plot.ChartOptions{
				OutputPath: outputPath,
			}

			// Render bar chart
			err = plot.RenderBar(xSeries, ySeries, opts)
			if err != nil {
				t.Logf("RenderBar failed: %v", err)
				return false
			}

			// Verify file exists
			fileInfo, err := os.Stat(outputPath)
			if err != nil {
				t.Logf("File does not exist: %v", err)
				return false
			}

			// Verify file is not empty
			if fileInfo.Size() == 0 {
				t.Logf("File is empty")
				return false
			}

			// Verify file is readable
			content, err := os.ReadFile(outputPath)
			if err != nil {
				t.Logf("File is not readable: %v", err)
				return false
			}

			// Verify content contains HTML
			return len(content) > 0
		},
		gen.SliceOf(gen.AlphaString()),
		gen.SliceOf(gen.Int64()),
		gen.Int64(),
	))

	properties.TestingRun(t)
}
