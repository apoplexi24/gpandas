package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func main() {
	// Create sample DataFrame for demonstration
	categories := []string{"Q1", "Q2", "Q3", "Q4"}
	categorySeries, err := collection.NewStringSeriesFromData(categories, nil)
	if err != nil {
		log.Fatalf("Failed to create category series: %v", err)
	}
	
	revenue := []float64{125.5, 148.3, 167.2, 189.7}
	revenueSeries, err := collection.NewFloat64SeriesFromData(revenue, nil)
	if err != nil {
		log.Fatalf("Failed to create revenue series: %v", err)
	}
	
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Quarter": categorySeries,
			"Revenue": revenueSeries,
		},
		ColumnOrder: []string{"Quarter", "Revenue"},
		Index:       []string{"0", "1", "2", "3"},
	}
	
	fmt.Println("Sample DataFrame:")
	fmt.Println(df.String())
	fmt.Println()
	
	// Example 1: Using default options (nil ChartOptions)
	// When nil is passed, the chart will use default values:
	// - Width: 900 pixels
	// - Height: 500 pixels
	// - Title: empty (no title)
	// - Theme: default go-echarts theme
	fmt.Println("Example 1: Default options (nil ChartOptions)")
	err = df.PlotBar("Quarter", "Revenue", nil)
	if err != nil {
		// Note: This will fail because OutputPath is required
		// This demonstrates that OutputPath must always be provided
		fmt.Printf("Expected error (OutputPath required): %v\n", err)
	}
	fmt.Println()
	
	// Example 2: Minimal custom options (only OutputPath)
	// All other fields will use default values
	fmt.Println("Example 2: Minimal options (only OutputPath)")
	minimalOpts := &plot.ChartOptions{
		OutputPath: "examples/plot/output/customization_minimal.html",
	}
	
	err = df.PlotBar("Quarter", "Revenue", minimalOpts)
	if err != nil {
		log.Fatalf("Failed to create chart with minimal options: %v", err)
	}
	fmt.Printf("Chart created with default width (900) and height (500): %s\n", minimalOpts.OutputPath)
	fmt.Println()
	
	// Example 3: Fully customized bar chart
	// Demonstrates all ChartOptions fields
	fmt.Println("Example 3: Fully customized bar chart")
	customBarOpts := &plot.ChartOptions{
		Title:      "Quarterly Revenue Report - Bar Chart",
		Width:      1200,  // Custom width
		Height:     700,   // Custom height
		OutputPath: "examples/plot/output/customization_bar.html",
		Theme:      "dark", // Custom theme (if supported by go-echarts)
	}
	
	err = df.PlotBar("Quarter", "Revenue", customBarOpts)
	if err != nil {
		log.Fatalf("Failed to create customized bar chart: %v", err)
	}
	fmt.Printf("Customized bar chart created: %s\n", customBarOpts.OutputPath)
	fmt.Println()
	
	// Example 4: Fully customized pie chart
	fmt.Println("Example 4: Fully customized pie chart")
	customPieOpts := &plot.ChartOptions{
		Title:      "Quarterly Revenue Distribution - Pie Chart",
		Width:      800,
		Height:     800,  // Square dimensions work well for pie charts
		OutputPath: "examples/plot/output/customization_pie.html",
		Theme:      "light",
	}
	
	err = df.PlotPie("Quarter", "Revenue", customPieOpts)
	if err != nil {
		log.Fatalf("Failed to create customized pie chart: %v", err)
	}
	fmt.Printf("Customized pie chart created: %s\n", customPieOpts.OutputPath)
	fmt.Println()
	
	// Example 5: Fully customized line chart
	fmt.Println("Example 5: Fully customized line chart")
	customLineOpts := &plot.ChartOptions{
		Title:      "Quarterly Revenue Trend - Line Chart",
		Width:      1400,  // Wide format for time series
		Height:     600,
		OutputPath: "examples/plot/output/customization_line.html",
		Theme:      "vintage",
	}
	
	err = df.PlotLine("Quarter", []string{"Revenue"}, customLineOpts)
	if err != nil {
		log.Fatalf("Failed to create customized line chart: %v", err)
	}
	fmt.Printf("Customized line chart created: %s\n", customLineOpts.OutputPath)
	fmt.Println()
	
	// Summary of customization options
	fmt.Println("=== Chart Customization Summary ===")
	fmt.Println("ChartOptions fields:")
	fmt.Println("  - Title:      Chart title displayed at the top")
	fmt.Println("  - Width:      Chart width in pixels (default: 900)")
	fmt.Println("  - Height:     Chart height in pixels (default: 500)")
	fmt.Println("  - OutputPath: File path for HTML output (required)")
	fmt.Println("  - Theme:      Chart theme/style (optional, e.g., 'light', 'dark', 'vintage')")
	fmt.Println()
	fmt.Println("All HTML files have been created in examples/plot/output/")
	fmt.Println("Open them in your browser to see the different customization options.")
}
