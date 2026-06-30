package plot

import (
	"fmt"
	"math"
	"os"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
)

// RenderHeatmap generates a heatmap from a grid of numeric values. xLabels and
// yLabels name the columns and rows, and matrix[row][col] holds each cell value
// (NaN cells are omitted). This is typically used to visualize a correlation
// matrix.
//
// Returns an error if dimensions are inconsistent or rendering fails.
func RenderHeatmap(xLabels, yLabels []string, matrix [][]float64, chartOpts *ChartOptions) error {
	if len(xLabels) == 0 || len(yLabels) == 0 {
		return fmt.Errorf("RenderHeatmap: xLabels and yLabels must be non-empty")
	}
	if len(matrix) != len(yLabels) {
		return fmt.Errorf("RenderHeatmap: matrix has %d rows, expected %d", len(matrix), len(yLabels))
	}

	chartOpts = applyDefaultOptions(chartOpts)
	if chartOpts.OutputPath == "" {
		return fmt.Errorf("RenderHeatmap: output path is required in ChartOptions")
	}

	min, max := math.Inf(1), math.Inf(-1)
	data := make([]opts.HeatMapData, 0, len(xLabels)*len(yLabels))
	for r := range yLabels {
		if len(matrix[r]) != len(xLabels) {
			return fmt.Errorf("RenderHeatmap: row %d has %d cols, expected %d", r, len(matrix[r]), len(xLabels))
		}
		for c := range xLabels {
			v := matrix[r][c]
			if math.IsNaN(v) {
				continue
			}
			min = math.Min(min, v)
			max = math.Max(max, v)
			// echarts heatmap expects [x, y, value]
			data = append(data, opts.HeatMapData{Value: [3]any{c, r, v}})
		}
	}
	if math.IsInf(min, 1) {
		min, max = 0, 1
	}

	hm := charts.NewHeatMap()
	hm.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{Title: chartOpts.Title, Top: "5%"}),
		charts.WithInitializationOpts(opts.Initialization{
			Width:  fmt.Sprintf("%dpx", chartOpts.Width),
			Height: fmt.Sprintf("%dpx", chartOpts.Height),
			Theme:  chartOpts.Theme,
		}),
		charts.WithXAxisOpts(opts.XAxis{Type: "category", Data: xLabels}),
		charts.WithYAxisOpts(opts.YAxis{Type: "category", Data: yLabels}),
		charts.WithVisualMapOpts(opts.VisualMap{
			Calculable: opts.Bool(true),
			Min:        float32(min),
			Max:        float32(max),
		}),
	)
	hm.SetXAxis(xLabels).AddSeries("", data)

	f, err := os.Create(chartOpts.OutputPath)
	if err != nil {
		return fmt.Errorf("RenderHeatmap: failed to create output file: %w", err)
	}
	defer f.Close()
	if err := hm.Render(f); err != nil {
		return fmt.Errorf("RenderHeatmap: failed to render chart: %w", err)
	}
	return nil
}
