package plot

import (
	"fmt"
	"os"

	"github.com/apoplexi24/gpandas/utils/collection"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

// RenderBar generates a bar chart from Series data.
// xSeries provides the x-axis labels (typically string Series).
// ySeries provides the y-axis values (must be numeric: int64 or float64).
// opts configures chart appearance and output location.
//
// Returns an error if:
// - Either Series is nil
// - ySeries is not numeric (int64 or float64)
// - Data conversion fails
// - File write fails
func RenderBar(xSeries, ySeries collection.Series, chartOpts *ChartOptions) error {
	// Validate input Series
	if xSeries == nil {
		return fmt.Errorf("RenderBar: xSeries is nil")
	}
	if ySeries == nil {
		return fmt.Errorf("RenderBar: ySeries is nil")
	}

	// Validate ySeries is numeric
	if err := validateNumericSeries(ySeries); err != nil {
		return fmt.Errorf("RenderBar: ySeries validation failed: %w", err)
	}

	// Apply default options
	chartOpts = applyDefaultOptions(chartOpts)

	// Validate output path is specified
	if chartOpts.OutputPath == "" {
		return fmt.Errorf("RenderBar: output path is required in ChartOptions")
	}

	// Convert xSeries to string labels
	xLabels, err := convertToStringSlice(xSeries)
	if err != nil {
		return fmt.Errorf("RenderBar: failed to convert xSeries to labels: %w", err)
	}

	// Convert ySeries to bar data
	barData, err := convertToBarData(ySeries)
	if err != nil {
		return fmt.Errorf("RenderBar: failed to convert ySeries to bar data: %w", err)
	}

	// Create bar chart
	bar := charts.NewBar()

	// Set global options
	bar.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title: chartOpts.Title,
			Top:   "5%",
		}),
		charts.WithInitializationOpts(opts.Initialization{
			Width:  fmt.Sprintf("%dpx", chartOpts.Width),
			Height: fmt.Sprintf("%dpx", chartOpts.Height),
			Theme:  chartOpts.Theme,
		}),
		charts.WithLegendOpts(opts.Legend{
			Top: "10%",
		}),
	)

	// Set x-axis data
	bar.SetXAxis(xLabels)

	// Add y-axis series
	bar.AddSeries("", barData)

	// Render to HTML file
	f, err := os.Create(chartOpts.OutputPath)
	if err != nil {
		return fmt.Errorf("RenderBar: failed to create output file: %w", err)
	}
	defer f.Close()

	if err := bar.Render(f); err != nil {
		return fmt.Errorf("RenderBar: failed to render chart: %w", err)
	}

	return nil
}

// convertToBarData converts a numeric Series to []opts.BarData.
// Null values are skipped during conversion.
// Int64 values are converted to float64 for consistency.
// Returns an error if the Series is not numeric or if value extraction fails.
func convertToBarData(s collection.Series) ([]opts.BarData, error) {
	// Get float64 values (handles both int64 and float64 Series)
	values, err := convertToFloat64Slice(s)
	if err != nil {
		return nil, err
	}

	// Convert to BarData format
	result := make([]opts.BarData, len(values))
	for i, v := range values {
		result[i] = opts.BarData{Value: v}
	}

	return result, nil
}
