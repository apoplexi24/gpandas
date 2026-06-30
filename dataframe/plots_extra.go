package dataframe

import (
	"errors"
	"fmt"
	"math"

	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// PlotScatter creates a scatter chart from two numeric columns.
// xCol and yCol must both reference numeric columns. opts configures appearance
// and output location.
//
// Example:
//
//	opts := &plot.ChartOptions{Title: "Height vs Weight", OutputPath: "out/scatter.html"}
//	err := df.PlotScatter("height", "weight", opts)
func (df *DataFrame) PlotScatter(xCol, yCol string, opts *plot.ChartOptions) error {
	if df == nil {
		return errors.New("PlotScatter: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	xSeries, ok := df.Columns[xCol]
	if !ok {
		return fmt.Errorf("PlotScatter: column '%s' not found", xCol)
	}
	ySeries, ok := df.Columns[yCol]
	if !ok {
		return fmt.Errorf("PlotScatter: column '%s' not found", yCol)
	}

	if err := plot.RenderScatter(xSeries, ySeries, opts); err != nil {
		return fmt.Errorf("PlotScatter: %w", err)
	}
	return nil
}

// PlotHistogram creates a histogram of a numeric column using the given number
// of bins (a non-positive bins value defaults to 10).
//
// Example:
//
//	opts := &plot.ChartOptions{Title: "Age Distribution", OutputPath: "out/hist.html"}
//	err := df.PlotHistogram("age", 20, opts)
func (df *DataFrame) PlotHistogram(column string, bins int, opts *plot.ChartOptions) error {
	if df == nil {
		return errors.New("PlotHistogram: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return fmt.Errorf("PlotHistogram: column '%s' not found", column)
	}

	if err := plot.RenderHistogram(series, bins, opts); err != nil {
		return fmt.Errorf("PlotHistogram: %w", err)
	}
	return nil
}

// PlotHeatmap creates a heatmap of the DataFrame's numeric columns. Columns
// become the x-axis, row index labels become the y-axis, and each numeric cell
// is colored by value. This pairs naturally with Corr() to visualize a
// correlation matrix.
//
// Example:
//
//	corr, _ := df.Corr()
//	opts := &plot.ChartOptions{Title: "Correlation", OutputPath: "out/heatmap.html"}
//	err := corr.PlotHeatmap(opts)
func (df *DataFrame) PlotHeatmap(opts *plot.ChartOptions) error {
	if df == nil {
		return errors.New("PlotHeatmap: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	// Numeric columns become the x-axis.
	xLabels := make([]string, 0, len(df.ColumnOrder))
	cols := make([]collection.Series, 0, len(df.ColumnOrder))
	for _, name := range df.ColumnOrder {
		if isNumericSeries(df.Columns[name]) {
			xLabels = append(xLabels, name)
			cols = append(cols, df.Columns[name])
		}
	}
	if len(xLabels) == 0 {
		return errors.New("PlotHeatmap: no numeric columns to plot")
	}

	rowCount := cols[0].Len()
	yLabels := make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		if i < len(df.Index) {
			yLabels[i] = df.Index[i]
		} else {
			yLabels[i] = fmt.Sprintf("%d", i)
		}
	}

	// Build the value matrix (row-major), with NaN for nulls.
	matrix := make([][]float64, rowCount)
	for r := 0; r < rowCount; r++ {
		matrix[r] = make([]float64, len(cols))
		for c, s := range cols {
			if s.IsNull(r) {
				matrix[r][c] = math.NaN()
				continue
			}
			v, _ := s.At(r)
			if f, ok := toFloat64(v); ok {
				matrix[r][c] = f
			} else {
				matrix[r][c] = math.NaN()
			}
		}
	}

	if err := plot.RenderHeatmap(xLabels, yLabels, matrix, opts); err != nil {
		return fmt.Errorf("PlotHeatmap: %w", err)
	}
	return nil
}
