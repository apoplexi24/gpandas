package plot

import (
	"fmt"
	"os"
	"reflect"

	"github.com/apoplexi24/gpandas/utils/collection"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

// RenderPie generates a pie chart from Series data.
// labelSeries provides the pie slice labels (typically string Series).
// valueSeries provides the pie slice values (must be numeric: int64 or float64).
// chartOpts configures chart appearance and output location.
//
// Returns an error if:
// - Either Series is nil
// - valueSeries is not numeric (int64 or float64)
// - Data conversion fails
// - File write fails
func RenderPie(labelSeries, valueSeries collection.Series, chartOpts *ChartOptions) error {
	// Validate input Series
	if labelSeries == nil {
		return fmt.Errorf("RenderPie: labelSeries is nil")
	}
	if valueSeries == nil {
		return fmt.Errorf("RenderPie: valueSeries is nil")
	}

	// Validate valueSeries is numeric
	if err := validateNumericSeries(valueSeries); err != nil {
		return fmt.Errorf("RenderPie: valueSeries validation failed: %w", err)
	}

	// Apply default options
	chartOpts = applyDefaultOptions(chartOpts)

	// Validate output path is specified
	if chartOpts.OutputPath == "" {
		return fmt.Errorf("RenderPie: output path is required in ChartOptions")
	}

	// Convert to pie data
	pieData, err := convertToPieData(labelSeries, valueSeries)
	if err != nil {
		return fmt.Errorf("RenderPie: failed to convert to pie data: %w", err)
	}

	// Create pie chart
	pie := charts.NewPie()

	// Set global options
	pie.SetGlobalOptions(
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
			Top:    "10%",
			Orient: "vertical",
			Right:  "10%",
		}),
	)

	// Add pie series
	pie.AddSeries("", pieData)

	// Render to HTML file
	f, err := os.Create(chartOpts.OutputPath)
	if err != nil {
		return fmt.Errorf("RenderPie: failed to create output file: %w", err)
	}
	defer f.Close()

	if err := pie.Render(f); err != nil {
		return fmt.Errorf("RenderPie: failed to render chart: %w", err)
	}

	return nil
}

// convertToPieData converts label and value Series to []opts.PieData.
// Null values are skipped during conversion (both label and value must be non-null).
// Int64 values are converted to float64 for consistency.
// Returns an error if valueSeries is not numeric or if value extraction fails.
func convertToPieData(labelSeries, valueSeries collection.Series) ([]opts.PieData, error) {
	if labelSeries == nil {
		return nil, fmt.Errorf("labelSeries is nil")
	}
	if valueSeries == nil {
		return nil, fmt.Errorf("valueSeries is nil")
	}

	// Validate valueSeries is numeric
	if err := validateNumericSeries(valueSeries); err != nil {
		return nil, err
	}

	// Ensure both Series have the same length
	if labelSeries.Len() != valueSeries.Len() {
		return nil, fmt.Errorf("labelSeries length (%d) does not match valueSeries length (%d)", 
			labelSeries.Len(), valueSeries.Len())
	}

	result := make([]opts.PieData, 0, labelSeries.Len())
	dtype := valueSeries.DType()

	for i := 0; i < labelSeries.Len(); i++ {
		// Skip if either label or value is null
		if labelSeries.IsNull(i) || valueSeries.IsNull(i) {
			continue
		}

		// Get label
		labelVal, err := labelSeries.At(i)
		if err != nil {
			return nil, fmt.Errorf("failed to get label at index %d: %w", i, err)
		}
		label, ok := labelVal.(string)
		if !ok {
			return nil, fmt.Errorf("label at index %d has type %T, expected string", i, labelVal)
		}

		// Get value
		valueVal, err := valueSeries.At(i)
		if err != nil {
			return nil, fmt.Errorf("failed to get value at index %d: %w", i, err)
		}

		// Convert to float64 based on dtype
		var floatVal float64
		if dtype == reflect.TypeOf(int64(0)) {
			intVal, ok := valueVal.(int64)
			if !ok {
				return nil, fmt.Errorf("value at index %d has type %T, expected int64", i, valueVal)
			}
			floatVal = float64(intVal)
		} else {
			fVal, ok := valueVal.(float64)
			if !ok {
				return nil, fmt.Errorf("value at index %d has type %T, expected float64", i, valueVal)
			}
			floatVal = fVal
		}

		result = append(result, opts.PieData{
			Name:  label,
			Value: floatVal,
		})
	}

	return result, nil
}

// Test-only exports for unit testing

// ConvertToPieDataForTest is a test-only export of convertToPieData
func ConvertToPieDataForTest(labelSeries, valueSeries collection.Series) ([]opts.PieData, error) {
	return convertToPieData(labelSeries, valueSeries)
}
