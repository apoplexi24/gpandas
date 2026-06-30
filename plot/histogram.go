package plot

import (
	"fmt"
	"math"
	"os"

	"github.com/apoplexi24/gpandas/utils/collection"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

// RenderHistogram generates a histogram (frequency bar chart) from a numeric
// Series by binning its values into the given number of equal-width bins.
// Null values are skipped. If bins <= 0, a default of 10 is used.
//
// Returns an error if the Series is nil or non-numeric, has no non-null values,
// or if rendering fails.
func RenderHistogram(series collection.Series, bins int, chartOpts *ChartOptions) error {
	if series == nil {
		return fmt.Errorf("RenderHistogram: series is nil")
	}
	if err := validateNumericSeries(series); err != nil {
		return fmt.Errorf("RenderHistogram: series validation failed: %w", err)
	}
	if bins <= 0 {
		bins = 10
	}

	chartOpts = applyDefaultOptions(chartOpts)
	if chartOpts.OutputPath == "" {
		return fmt.Errorf("RenderHistogram: output path is required in ChartOptions")
	}

	values, err := convertToFloat64Slice(series)
	if err != nil {
		return fmt.Errorf("RenderHistogram: %w", err)
	}
	if len(values) == 0 {
		return fmt.Errorf("RenderHistogram: no non-null values to plot")
	}

	// Determine range.
	min, max := values[0], values[0]
	for _, v := range values[1:] {
		min = math.Min(min, v)
		max = math.Max(max, v)
	}

	width := (max - min) / float64(bins)
	if width == 0 {
		// All values identical: single bin.
		width = 1
		bins = 1
	}

	counts := make([]int, bins)
	for _, v := range values {
		idx := int((v - min) / width)
		if idx >= bins {
			idx = bins - 1 // include the max value in the last bin
		}
		if idx < 0 {
			idx = 0
		}
		counts[idx]++
	}

	labels := make([]string, bins)
	barData := make([]opts.BarData, bins)
	for i := 0; i < bins; i++ {
		lo := min + float64(i)*width
		hi := lo + width
		labels[i] = fmt.Sprintf("%.2f-%.2f", lo, hi)
		barData[i] = opts.BarData{Value: counts[i]}
	}

	bar := charts.NewBar()
	bar.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: chartOpts.Title, Top: "5%"}),
		charts.WithInitializationOpts(opts.Initialization{
			Width:  fmt.Sprintf("%dpx", chartOpts.Width),
			Height: fmt.Sprintf("%dpx", chartOpts.Height),
			Theme:  chartOpts.Theme,
		}),
	)
	bar.SetXAxis(labels).AddSeries("frequency", barData)

	f, err := os.Create(chartOpts.OutputPath)
	if err != nil {
		return fmt.Errorf("RenderHistogram: failed to create output file: %w", err)
	}
	defer f.Close()
	if err := bar.Render(f); err != nil {
		return fmt.Errorf("RenderHistogram: failed to render chart: %w", err)
	}
	return nil
}
