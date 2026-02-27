package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func main() {
	// Create sample DataFrame with time series data
	// This example shows temperature and humidity readings over time
	
	// Define the time points (x-axis)
	months := []string{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
	monthSeries, err := collection.NewStringSeriesFromData(months, nil)
	if err != nil {
		log.Fatalf("Failed to create month series: %v", err)
	}
	
	// Define temperature values (first y-axis series)
	temperatures := []float64{5.2, 7.1, 12.3, 16.8, 21.5, 25.3, 28.1, 27.4, 23.2, 17.5, 11.2, 6.8}
	tempSeries, err := collection.NewFloat64SeriesFromData(temperatures, nil)
	if err != nil {
		log.Fatalf("Failed to create temperature series: %v", err)
	}
	
	// Define humidity values (second y-axis series)
	humidity := []float64{78.5, 72.3, 68.1, 65.4, 62.8, 58.2, 55.6, 57.3, 63.5, 70.2, 75.8, 80.1}
	humiditySeries, err := collection.NewFloat64SeriesFromData(humidity, nil)
	if err != nil {
		log.Fatalf("Failed to create humidity series: %v", err)
	}
	
	// Create DataFrame from the series
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Month":       monthSeries,
			"Temperature": tempSeries,
			"Humidity":    humiditySeries,
		},
		ColumnOrder: []string{"Month", "Temperature", "Humidity"},
		Index:       []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"},
	}
	
	fmt.Println("Sample DataFrame:")
	fmt.Println(df.String())
	fmt.Println()
	
	// Example 1: Single series line chart (Temperature only)
	opts1 := &plot.ChartOptions{
		Title:      "Monthly Temperature Trend",
		Width:      1000,
		Height:     500,
		OutputPath: "examples/plot/output/line_chart_single.html",
	}
	
	// Generate line chart with single y-column
	// PlotLine takes the x-axis column name, slice of y-axis column names, and chart options
	err = df.PlotLine("Month", []string{"Temperature"}, opts1)
	if err != nil {
		log.Fatalf("Failed to create single-series line chart: %v", err)
	}
	
	fmt.Printf("Single-series line chart created at: %s\n", opts1.OutputPath)
	
	// Example 2: Multi-series line chart (Temperature and Humidity)
	opts2 := &plot.ChartOptions{
		Title:      "Monthly Temperature and Humidity Trends",
		Width:      1000,
		Height:     500,
		OutputPath: "examples/plot/output/line_chart_multi.html",
	}
	
	// Generate line chart with multiple y-columns for comparison
	// This demonstrates plotting multiple series on the same chart
	err = df.PlotLine("Month", []string{"Temperature", "Humidity"}, opts2)
	if err != nil {
		log.Fatalf("Failed to create multi-series line chart: %v", err)
	}
	
	fmt.Printf("Multi-series line chart created at: %s\n", opts2.OutputPath)
	fmt.Println()
	fmt.Println("Open the HTML files in your browser to view the interactive charts.")
}
