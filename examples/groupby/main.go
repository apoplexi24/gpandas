package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas"
)

func main() {
	gp := gpandas.GoPandas{}

	// Create a sample DataFrame
	columns := []string{"Category", "Value", "Quantity"}
	data := []gpandas.Column{
		{"A", "B", "A", "B", "A", "C"},
		{10.0, 20.0, 30.0, 40.0, 50.0, 60.0},
		{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)},
	}
	types := map[string]any{
		"Category": gpandas.StringCol{},
		"Value":    gpandas.FloatCol{},
		"Quantity": gpandas.IntCol{},
	}

	df, err := gp.DataFrame(columns, data, types)
	if err != nil {
		log.Fatalf("Failed to create DataFrame: %v", err)
	}

	fmt.Println("Original DataFrame:")
	fmt.Println(df)

	// Group by 'Category'
	gb, err := df.GroupBy([]string{"Category"}, 0)
	if err != nil {
		log.Fatalf("Failed to group by Category: %v", err)
	}

	// Calculate Mean
	meanDF, err := gb.Mean()
	if err != nil {
		log.Fatalf("Failed to calculate Mean: %v", err)
	}
	fmt.Println("\nMean by Category:")
	fmt.Println(meanDF)

	// Calculate Sum
	sumDF, err := gb.Sum()
	if err != nil {
		log.Fatalf("Failed to calculate Sum: %v", err)
	}
	fmt.Println("\nSum by Category:")
	fmt.Println(sumDF)

	// Calculate Min
	minDF, err := gb.Min()
	if err != nil {
		log.Fatalf("Failed to calculate Min: %v", err)
	}
	fmt.Println("\nMin by Category:")
	fmt.Println(minDF)

	// Calculate Max
	maxDF, err := gb.Max()
	if err != nil {
		log.Fatalf("Failed to calculate Max: %v", err)
	}
	fmt.Println("\nMax by Category:")
	fmt.Println(maxDF)
}
