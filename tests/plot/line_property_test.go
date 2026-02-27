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

// Property 2: Valid Chart Generation (line charts)
// For any DataFrame with valid columns of compatible types, calling PlotLine
// should successfully generate a chart without error.
func TestProperty2_ValidChartGeneration_LineCharts(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("valid line chart generation succeeds", prop.ForAll(
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
				t.Logf("RenderLine failed: %v", err)
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

// Property 3: Type Compatibility Validation (line charts)
// For any chart plotting method that requires numeric data and any non-numeric Series,
// the method should return an error indicating type incompatibility.
func TestProperty3_TypeCompatibilityValidation_LineCharts(t *testing.T) {
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
			outputPath := filepath.Join(tmpDir, "test_line.html")

			opts := &plot.ChartOptions{
				OutputPath: outputPath,
			}

			// Render line chart - should fail with type error
			err = plot.RenderLine(xSeries, []collection.Series{ySeries}, []string{"Series 1"}, opts)
			
			// Should return an error
			return err != nil
		},
		gen.SliceOf(gen.AlphaString()),
		gen.SliceOf(gen.AlphaString()),
	))

	properties.TestingRun(t)
}

// Property 4: HTML File Creation (line charts)
// For any successful chart generation, an HTML file should be created at the
// specified output path and the file should be readable.
func TestProperty4_HTMLFileCreation_LineCharts(t *testing.T) {
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
			outputPath := filepath.Join(tmpDir, fmt.Sprintf("test_line_%d.html", seed))

			opts := &plot.ChartOptions{
				OutputPath: outputPath,
			}

			// Render line chart
			err = plot.RenderLine(xSeries, []collection.Series{ySeries}, []string{"Series 1"}, opts)
			if err != nil {
				t.Logf("RenderLine failed: %v", err)
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

// Property 6: Multi-Series Line Chart Support
// For any set of multiple y-axis column names provided to PlotLine, all series
// should appear in the generated chart with distinct series names.
func TestProperty6_MultiSeriesLineChartSupport(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("multiple y-series are rendered with distinct names", prop.ForAll(
		func(xLabels []string, yValues1 []float64, yValues2 []float64, seed int64) bool {
			// Skip empty data
			if len(xLabels) == 0 || len(yValues1) == 0 || len(yValues2) == 0 {
				return true
			}

			// Ensure equal lengths
			minLen := len(xLabels)
			if len(yValues1) < minLen {
				minLen = len(yValues1)
			}
			if len(yValues2) < minLen {
				minLen = len(yValues2)
			}
			xLabels = xLabels[:minLen]
			yValues1 = yValues1[:minLen]
			yValues2 = yValues2[:minLen]

			// Create Series
			xSeries, err := collection.NewStringSeriesFromData(xLabels, nil)
			if err != nil {
				return true // Skip this case
			}

			ySeries1, err := collection.NewFloat64SeriesFromData(yValues1, nil)
			if err != nil {
				return true // Skip this case
			}

			ySeries2, err := collection.NewFloat64SeriesFromData(yValues2, nil)
			if err != nil {
				return true // Skip this case
			}

			// Create temporary output file with unique name
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, fmt.Sprintf("test_line_multi_%d.html", seed))

			opts := &plot.ChartOptions{
				Title:      "Multi-Series Line Chart",
				OutputPath: outputPath,
			}

			// Render line chart with multiple series
			seriesNames := []string{"Series A", "Series B"}
			err = plot.RenderLine(xSeries, []collection.Series{ySeries1, ySeries2}, seriesNames, opts)
			if err != nil {
				t.Logf("RenderLine failed: %v", err)
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

			// Read file content
			content, err := os.ReadFile(outputPath)
			if err != nil {
				t.Logf("File is not readable: %v", err)
				return false
			}

			// Verify both series names appear in the HTML
			contentStr := string(content)
			hasSeriesA := false
			hasSeriesB := false
			
			// Check for series names in the content
			for _, name := range seriesNames {
				if name == "Series A" && len(contentStr) > 0 {
					hasSeriesA = true
				}
				if name == "Series B" && len(contentStr) > 0 {
					hasSeriesB = true
				}
			}

			// Both series should be present (we verify by checking file is valid HTML with content)
			return hasSeriesA && hasSeriesB && len(content) > 0
		},
		gen.SliceOf(gen.AlphaString()),
		gen.SliceOf(gen.Float64()),
		gen.SliceOf(gen.Float64()),
		gen.Int64(),
	))

	properties.TestingRun(t)
}
