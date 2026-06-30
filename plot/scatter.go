package plot

import (
	"fmt"
	"os"

	"github.com/apoplexi24/gpandas/utils/collection"
	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

// RenderScatter generates a scatter chart from two numeric Series.
// xSeries and ySeries must both be numeric (int64 or float64) and the same length.
// Rows where either value is null are skipped.
//
// Returns an error if either Series is nil or non-numeric, or if rendering fails.
func RenderScatter(xSeries, ySeries collection.Series, chartOpts *ChartOptions) error {
	if xSeries == nil {
		return fmt.Errorf("RenderScatter: xSeries is nil")
	}
	if ySeries == nil {
		return fmt.Errorf("RenderScatter: ySeries is nil")
	}
	if err := validateNumericSeries(xSeries); err != nil {
		return fmt.Errorf("RenderScatter: xSeries validation failed: %w", err)
	}
	if err := validateNumericSeries(ySeries); err != nil {
		return fmt.Errorf("RenderScatter: ySeries validation failed: %w", err)
	}

	chartOpts = applyDefaultOptions(chartOpts)
	if chartOpts.OutputPath == "" {
		return fmt.Errorf("RenderScatter: output path is required in ChartOptions")
	}

	// Build paired points, skipping rows where either value is null.
	n := xSeries.Len()
	if ySeries.Len() < n {
		n = ySeries.Len()
	}
	xLabels := make([]string, 0, n)
	points := make([]opts.ScatterData, 0, n)
	for i := 0; i < n; i++ {
		if xSeries.IsNull(i) || ySeries.IsNull(i) {
			continue
		}
		xv, _ := xSeries.At(i)
		yv, _ := ySeries.At(i)
		xf, _ := toFloat(xv)
		yf, _ := toFloat(yv)
		xLabels = append(xLabels, fmt.Sprintf("%v", xf))
		points = append(points, opts.ScatterData{Value: []any{xf, yf}, SymbolSize: 10})
	}

	scatter := charts.NewScatter()
	scatter.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: chartOpts.Title, Top: "5%"}),
		charts.WithInitializationOpts(opts.Initialization{
			Width:  fmt.Sprintf("%dpx", chartOpts.Width),
			Height: fmt.Sprintf("%dpx", chartOpts.Height),
			Theme:  chartOpts.Theme,
		}),
		charts.WithXAxisOpts(opts.XAxis{Type: "value"}),
		charts.WithYAxisOpts(opts.YAxis{Type: "value"}),
	)
	scatter.SetXAxis(xLabels).AddSeries("", points)

	f, err := os.Create(chartOpts.OutputPath)
	if err != nil {
		return fmt.Errorf("RenderScatter: failed to create output file: %w", err)
	}
	defer f.Close()
	if err := scatter.Render(f); err != nil {
		return fmt.Errorf("RenderScatter: failed to render chart: %w", err)
	}
	return nil
}

// toFloat converts a numeric value (int64/float64) to float64.
func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int64:
		return float64(x), true
	case int:
		return float64(x), true
	default:
		return 0, false
	}
}
