package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas"
	"github.com/apoplexi24/gpandas/dataframe"
)

func main() {
	gp := gpandas.GoPandas{}

	// Create a sample DataFrame
	columns := []string{"Category", "Value", "Quantity", "Rep"}
	data := []gpandas.Column{
		{"A", "B", "A", "B", "A", "C"},
		{10.0, 20.0, 30.0, 40.0, 50.0, 60.0},
		{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)},
		{"ann", "bob", "cat", "dan", nil, "fay"},
	}
	types := map[string]any{
		"Category": gpandas.StringCol{},
		"Value":    gpandas.FloatCol{},
		"Quantity": gpandas.IntCol{},
		"Rep":      gpandas.StringCol{},
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

	// Spread and middle: Std and Var are NaN for C, which has a single row.
	stdDF, err := gb.Std()
	if err != nil {
		log.Fatalf("Failed to calculate Std: %v", err)
	}
	fmt.Println("\nStd by Category (NaN for a one-row group):")
	fmt.Println(stdDF)

	varDF, err := gb.Var()
	if err != nil {
		log.Fatalf("Failed to calculate Var: %v", err)
	}
	fmt.Println("\nVar by Category:")
	fmt.Println(varDF)

	medianDF, err := gb.Median()
	if err != nil {
		log.Fatalf("Failed to calculate Median: %v", err)
	}
	fmt.Println("\nMedian by Category:")
	fmt.Println(medianDF)

	// Count skips nulls, Size does not. Rep has a null in group A.
	countDF, err := gb.Count()
	if err != nil {
		log.Fatalf("Failed to calculate Count: %v", err)
	}
	fmt.Println("\nCount by Category (non-null values; Rep is short one in A):")
	fmt.Println(countDF)

	sizeDF, err := gb.Size()
	if err != nil {
		log.Fatalf("Failed to calculate Size: %v", err)
	}
	fmt.Println("\nSize by Category (rows, nulls included):")
	fmt.Println(sizeDF)

	firstDF, err := gb.First()
	if err != nil {
		log.Fatalf("Failed to calculate First: %v", err)
	}
	fmt.Println("\nFirst by Category:")
	fmt.Println(firstDF)

	lastDF, err := gb.Last()
	if err != nil {
		log.Fatalf("Failed to calculate Last: %v", err)
	}
	fmt.Println("\nLast by Category (A skips its null Rep and reports cat):")
	fmt.Println(lastDF)

	// Cumcount and Transform return results aligned with the original rows,
	// so they can be added straight back with Assign.
	nth, err := gb.Cumcount()
	if err != nil {
		log.Fatalf("Failed to calculate Cumcount: %v", err)
	}
	if err := df.Assign("NthInCategory", nth); err != nil {
		log.Fatalf("Assign failed: %v", err)
	}

	means, err := gb.Transform(dataframe.AggMean)
	if err != nil {
		log.Fatalf("Failed to Transform: %v", err)
	}
	if err := df.Assign("CategoryMean", means.Columns["Value"]); err != nil {
		log.Fatalf("Assign failed: %v", err)
	}
	gap, err := df.Sub("Value", "CategoryMean")
	if err != nil {
		log.Fatalf("Sub failed: %v", err)
	}
	if err := df.Assign("VsMean", gap); err != nil {
		log.Fatalf("Assign failed: %v", err)
	}
	fmt.Println("\nCumcount and Transform(AggMean) added back row-for-row:")
	fmt.Println(df)
}
