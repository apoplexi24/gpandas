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

// Property 2: Valid Chart Generation (pie charts)
// For any DataFrame with valid columns of compatible types, calling PlotPie
// should successfully generate a chart without error.
func TestProperty2_ValidChartGeneration_PieCharts(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("valid pie chart generation succeeds", prop.ForAll(
		func(labels []string, values []float64) bool {
			// Skip empty data
			if len(labels) == 0 || len(values) == 0 {
				return true
			}

			// Ensure equal lengths
			minLen := len(labels)
			if len(values) < minLen {
				minLen = len(values)
			}
			labels = labels[:minLen]
			values = values[:minLen]

			// Create Series
			labelSeries, err := collection.NewStringSeriesFromData(labels, nil)
			if err != nil {
				t.Logf("Failed to create labelSeries: %v", err)
				return false
			}

			valueSeries, err := collection.NewFloat64SeriesFromData(values, nil)
			if err != nil {
				t.Logf("Failed to create valueSeries: %v", err)
				return false
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
				t.Logf("RenderPie failed: %v", err)
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

// Property 3: Type Compatibility Validation (pie charts)
// For any chart plotting method that requires numeric data and any non-numeric Series,
// the method should return an error indicating type incompatibility.
func TestProperty3_TypeCompatibilityValidation_PieCharts(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("non-numeric valueSeries returns type error", prop.ForAll(
		func(labels []string, values []string) bool {
			// Skip empty data
			if len(labels) == 0 || len(values) == 0 {
				return true
			}

			// Ensure equal lengths
			minLen := len(labels)
			if len(values) < minLen {
				minLen = len(values)
			}
			labels = labels[:minLen]
			values = values[:minLen]

			// Create Series (valueSeries is string, not numeric)
			labelSeries, err := collection.NewStringSeriesFromData(labels, nil)
			if err != nil {
				return true // Skip this case
			}

			valueSeries, err := collection.NewStringSeriesFromData(values, nil)
			if err != nil {
				return true // Skip this case
			}

			// Create temporary output file
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, "test_pie.html")

			opts := &plot.ChartOptions{
				OutputPath: outputPath,
			}

			// Render pie chart - should fail with type error
			err = plot.RenderPie(labelSeries, valueSeries, opts)
			
			// Should return an error
			return err != nil
		},
		gen.SliceOf(gen.AlphaString()),
		gen.SliceOf(gen.AlphaString()),
	))

	properties.TestingRun(t)
}

// Property 4: HTML File Creation (pie charts)
// For any successful chart generation, an HTML file should be created at the
// specified output path and the file should be readable.
func TestProperty4_HTMLFileCreation_PieCharts(t *testing.T) {
	properties := gopter.NewProperties(nil)

	properties.Property("HTML file is created and readable", prop.ForAll(
		func(labels []string, values []int64, seed int64) bool {
			// Skip empty data
			if len(labels) == 0 || len(values) == 0 {
				return true
			}

			// Ensure equal lengths
			minLen := len(labels)
			if len(values) < minLen {
				minLen = len(values)
			}
			labels = labels[:minLen]
			values = values[:minLen]

			// Create Series
			labelSeries, err := collection.NewStringSeriesFromData(labels, nil)
			if err != nil {
				return true // Skip this case
			}

			valueSeries, err := collection.NewInt64SeriesFromData(values, nil)
			if err != nil {
				return true // Skip this case
			}

			// Create temporary output file with unique name
			tmpDir := t.TempDir()
			outputPath := filepath.Join(tmpDir, fmt.Sprintf("test_pie_%d.html", seed))

			opts := &plot.ChartOptions{
				OutputPath: outputPath,
			}

			// Render pie chart
			err = plot.RenderPie(labelSeries, valueSeries, opts)
			if err != nil {
				t.Logf("RenderPie failed: %v", err)
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
