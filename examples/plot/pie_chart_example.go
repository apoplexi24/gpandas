package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func main() {
	// Create sample DataFrame with labels and values
	// This example shows market share distribution across different companies
	
	// Define the company labels
	companies := []string{"Company A", "Company B", "Company C", "Company D", "Company E"}
	companySeries, err := collection.NewStringSeriesFromData(companies, nil)
	if err != nil {
		log.Fatalf("Failed to create company series: %v", err)
	}
	
	// Define the market share percentages (values)
	marketShare := []float64{35.5, 28.3, 18.7, 12.1, 5.4}
	shareSeries, err := collection.NewFloat64SeriesFromData(marketShare, nil)
	if err != nil {
		log.Fatalf("Failed to create market share series: %v", err)
	}
	
	// Create DataFrame from the series
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Company":     companySeries,
			"MarketShare": shareSeries,
		},
		ColumnOrder: []string{"Company", "MarketShare"},
		Index:       []string{"0", "1", "2", "3", "4"},
	}
	
	fmt.Println("Sample DataFrame:")
	fmt.Println(df.String())
	fmt.Println()
	
	// Configure chart options with custom title, dimensions, and output path
	opts := &plot.ChartOptions{
		Title:      "Market Share Distribution",
		Width:      900,
		Height:     600,
		OutputPath: "examples/plot/output/pie_chart.html",
	}
	
	// Generate the pie chart
	// PlotPie takes the label column name, value column name, and chart options
	err = df.PlotPie("Company", "MarketShare", opts)
	if err != nil {
		log.Fatalf("Failed to create pie chart: %v", err)
	}
	
	fmt.Printf("Pie chart successfully created at: %s\n", opts.OutputPath)
	fmt.Println("Open the HTML file in your browser to view the interactive chart.")
}
