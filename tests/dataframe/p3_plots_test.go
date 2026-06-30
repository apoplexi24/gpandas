package dataframe_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func plotDF() *dataframe.DataFrame {
	x, _ := collection.NewFloat64SeriesFromData([]float64{1, 2, 3, 4, 5}, nil)
	y, _ := collection.NewFloat64SeriesFromData([]float64{2, 4, 6, 8, 10}, nil)
	return &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"x": x,
			"y": y,
		},
		ColumnOrder: []string{"x", "y"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}
}

func TestPlotScatter(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "gpandas_plot")
	defer os.RemoveAll(tmpDir)

	out := filepath.Join(tmpDir, "scatter.html")
	err := plotDF().PlotScatter("x", "y", &plot.ChartOptions{Title: "Scatter", OutputPath: out})
	if err != nil {
		t.Fatalf("PlotScatter failed: %v", err)
	}
	if _, err := os.Stat(out); err != nil {
		t.Errorf("expected scatter output file: %v", err)
	}

	t.Run("missing column errors", func(t *testing.T) {
		err := plotDF().PlotScatter("x", "nope", &plot.ChartOptions{OutputPath: out})
		if err == nil {
			t.Error("expected error for missing column")
		}
	})
}

func TestPlotHistogram(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "gpandas_plot")
	defer os.RemoveAll(tmpDir)

	out := filepath.Join(tmpDir, "hist.html")
	err := plotDF().PlotHistogram("y", 4, &plot.ChartOptions{Title: "Hist", OutputPath: out})
	if err != nil {
		t.Fatalf("PlotHistogram failed: %v", err)
	}
	if _, err := os.Stat(out); err != nil {
		t.Errorf("expected histogram output file: %v", err)
	}
}

func TestPlotHeatmap(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "gpandas_plot")
	defer os.RemoveAll(tmpDir)

	// Heatmap of a correlation matrix
	corr, err := plotDF().Corr()
	if err != nil {
		t.Fatalf("Corr failed: %v", err)
	}
	out := filepath.Join(tmpDir, "heat.html")
	if err := corr.PlotHeatmap(&plot.ChartOptions{Title: "Corr", OutputPath: out}); err != nil {
		t.Fatalf("PlotHeatmap failed: %v", err)
	}
	if _, err := os.Stat(out); err != nil {
		t.Errorf("expected heatmap output file: %v", err)
	}
}
