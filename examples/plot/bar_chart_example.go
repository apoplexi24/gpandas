package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func main() {
	// Create sample DataFrame with categorical and numeric data
	// This example shows monthly sales data for different products
	
	// Define the product categories (x-axis)
	products := []string{"Laptop", "Phone", "Tablet", "Monitor", "Keyboard"}
	productSeries, err := collection.NewStringSeriesFromData(products, nil)
	if err != nil {
		log.Fatalf("Failed to create product series: %v", err)
	}
	
	// Define the sales values (y-axis)
	sales := []int64{45, 78, 32, 56, 23}
	salesSeries, err := collection.NewInt64SeriesFromData(sales, nil)
	if err != nil {
		log.Fatalf("Failed to create sales series: %v", err)
	}
	
	// Create DataFrame from the series
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Product": productSeries,
			"Sales":   salesSeries,
		},
		ColumnOrder: []string{"Product", "Sales"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}
	
	fmt.Println("Sample DataFrame:")
	fmt.Println(df.String())
	fmt.Println()
	
	// Configure chart options with custom title, dimensions, and output path
	opts := &plot.ChartOptions{
		Title:      "Monthly Product Sales",
		Width:      1000,
		Height:     600,
		OutputPath: "examples/plot/output/bar_chart.html",
	}
	
	// Generate the bar chart
	// PlotBar takes the x-axis column name, y-axis column name, and chart options
	err = df.PlotBar("Product", "Sales", opts)
	if err != nil {
		log.Fatalf("Failed to create bar chart: %v", err)
	}
	
	fmt.Printf("Bar chart successfully created at: %s\n", opts.OutputPath)
	fmt.Println("Open the HTML file in your browser to view the interactive chart.")
}
