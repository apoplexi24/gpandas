package plot

import (
	"fmt"
	"os"

	"github.com/apoplexi24/gpandas/utils/collection"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

// RenderLine generates a line chart from Series data.
// xSeries provides the x-axis labels (typically string Series).
// ySeriesList provides one or more y-axis series (must be numeric: int64 or float64).
// seriesNames provides names for each y-axis series (must match length of ySeriesList).
// chartOpts configures chart appearance and output location.
//
// Returns an error if:
// - xSeries is nil
// - ySeriesList is empty or contains nil Series
// - Any ySeries is not numeric (int64 or float64)
// - seriesNames length doesn't match ySeriesList length
// - Data conversion fails
// - File write fails
func RenderLine(xSeries collection.Series, ySeriesList []collection.Series, seriesNames []string, chartOpts *ChartOptions) error {
	// Validate input Series
	if xSeries == nil {
		return fmt.Errorf("RenderLine: xSeries is nil")
	}
	if len(ySeriesList) == 0 {
		return fmt.Errorf("RenderLine: ySeriesList is empty")
	}
	if len(seriesNames) != len(ySeriesList) {
		return fmt.Errorf("RenderLine: seriesNames length (%d) does not match ySeriesList length (%d)", 
			len(seriesNames), len(ySeriesList))
	}

	// Validate all ySeries are non-nil and numeric
	for i, ySeries := range ySeriesList {
		if ySeries == nil {
			return fmt.Errorf("RenderLine: ySeries at index %d is nil", i)
		}
		if err := validateNumericSeries(ySeries); err != nil {
			return fmt.Errorf("RenderLine: ySeries at index %d validation failed: %w", i, err)
		}
	}

	// Apply default options
	chartOpts = applyDefaultOptions(chartOpts)

	// Validate output path is specified
	if chartOpts.OutputPath == "" {
		return fmt.Errorf("RenderLine: output path is required in ChartOptions")
	}

	// Convert xSeries to string labels
	xLabels, err := convertToStringSlice(xSeries)
	if err != nil {
		return fmt.Errorf("RenderLine: failed to convert xSeries to labels: %w", err)
	}

	// Create line chart
	line := charts.NewLine()

	// Set global options
	line.SetGlobalOptions(
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
	line.SetXAxis(xLabels)

	// Add each y-axis series
	for i, ySeries := range ySeriesList {
		// Convert ySeries to line data
		lineData, err := convertToLineData(ySeries)
		if err != nil {
			return fmt.Errorf("RenderLine: failed to convert ySeries at index %d to line data: %w", i, err)
		}

		// Add series with its name
		line.AddSeries(seriesNames[i], lineData)
	}

	// Render to HTML file
	f, err := os.Create(chartOpts.OutputPath)
	if err != nil {
		return fmt.Errorf("RenderLine: failed to create output file: %w", err)
	}
	defer f.Close()

	if err := line.Render(f); err != nil {
		return fmt.Errorf("RenderLine: failed to render chart: %w", err)
	}

	return nil
}

// convertToLineData converts a numeric Series to []opts.LineData.
// Null values are skipped during conversion.
// Int64 values are converted to float64 for consistency.
// Returns an error if the Series is not numeric or if value extraction fails.
func convertToLineData(s collection.Series) ([]opts.LineData, error) {
	// Get float64 values (handles both int64 and float64 Series)
	values, err := convertToFloat64Slice(s)
	if err != nil {
		return nil, err
	}

	// Convert to LineData format
	result := make([]opts.LineData, len(values))
	for i, v := range values {
		result[i] = opts.LineData{Value: v}
	}

	return result, nil
}
