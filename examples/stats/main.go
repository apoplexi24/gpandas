package main

import (
	"fmt"
	"log"
	"math"

	"github.com/apoplexi24/gpandas"
)

func main() {
	gp := gpandas.GoPandas{}

	// A small store dataset. The nulls in Rating and Returns show that every
	// reduction excludes missing values rather than treating them as zero.
	columns := []string{"Product", "Price", "Units", "Rating", "InStock"}
	data := []gpandas.Column{
		{"Widget", "Gadget", "Doohickey", "Gizmo", "Thingamajig", "Whatsit"},
		{9.99, 24.50, 9.99, 149.00, 24.50, 12.75},
		{int64(120), int64(45), int64(120), int64(3), int64(60), int64(0)},
		{4.5, 3.0, 4.5, nil, 5.0, 2.0},
		{true, true, true, false, true, false},
	}
	types := map[string]any{
		"Product": gpandas.StringCol{},
		"Price":   gpandas.FloatCol{},
		"Units":   gpandas.IntCol{},
		"Rating":  gpandas.FloatCol{},
		"InStock": gpandas.BoolCol{},
	}

	df, err := gp.DataFrame(columns, data, types)
	if err != nil {
		log.Fatalf("Failed to create DataFrame: %v", err)
	}

	// Label the rows by product so IdxMax/IdxMin return something readable.
	names := make([]string, len(data[0]))
	for i, v := range data[0] {
		names[i] = v.(string)
	}
	df.Index = names

	fmt.Println("=== Original DataFrame ===")
	fmt.Println(df)

	numericCols := []string{"Price", "Units", "Rating"}

	// ---------------------------------------------------------------
	// 1. Var: sample variance (ddof=1), the square of Std
	// ---------------------------------------------------------------
	fmt.Println("=== Var (sample, ddof=1) vs Std ===")
	variances := df.Var()
	stds := df.Std()
	for _, col := range numericCols {
		fmt.Printf("  %-8s var=%12.4f  std=%9.4f  sqrt(var)=%9.4f\n",
			col, variances[col], stds[col], math.Sqrt(variances[col]))
	}
	fmt.Println()

	// ---------------------------------------------------------------
	// 2. Quantile: linear interpolation, same as Describe's 25%/50%/75%
	// ---------------------------------------------------------------
	fmt.Println("=== Quantile (linear interpolation) ===")
	for _, q := range []float64{0.25, 0.5, 0.9} {
		quantiles, err := df.Quantile(q)
		if err != nil {
			log.Fatalf("Quantile(%v) failed: %v", q, err)
		}
		fmt.Printf("  q=%.2f ->", q)
		for _, col := range numericCols {
			fmt.Printf("  %s=%.4f", col, quantiles[col])
		}
		fmt.Println()
	}
	// q outside [0, 1] is an error rather than a panic.
	if _, err := df.Quantile(1.5); err != nil {
		fmt.Printf("  Quantile(1.5) rejected: %v\n", err)
	}
	fmt.Println()

	// ---------------------------------------------------------------
	// 3. Skew / Kurt: shape of the distribution
	// ---------------------------------------------------------------
	fmt.Println("=== Skew / Kurt (unbiased, Fisher) ===")
	skew := df.Skew()
	kurt := df.Kurt()
	for _, col := range numericCols {
		fmt.Printf("  %-8s skew=%9.4f  kurt=%9.4f\n", col, skew[col], kurt[col])
	}
	fmt.Println("  (NaN means too few values, or zero variance)")
	fmt.Println()

	// ---------------------------------------------------------------
	// 4. Mode: most frequent value(s), all columns, ties included
	// ---------------------------------------------------------------
	fmt.Println("=== Mode (ties returned together, sorted ascending) ===")
	modes := df.Mode()
	for _, col := range df.ColumnOrder {
		fmt.Printf("  %-8s %v\n", col, modes[col])
	}
	fmt.Println()

	// ---------------------------------------------------------------
	// 5. IdxMax / IdxMin: index label of the extreme value
	// ---------------------------------------------------------------
	fmt.Println("=== IdxMax / IdxMin (index labels, ties keep the first row) ===")
	maxLabels := df.IdxMax()
	minLabels := df.IdxMin()
	for _, col := range numericCols {
		fmt.Printf("  %-8s max at %-12s min at %s\n", col, maxLabels[col], minLabels[col])
	}
	fmt.Println()

	// ---------------------------------------------------------------
	// 6. Any / All: boolean reductions over bool and numeric columns
	// ---------------------------------------------------------------
	fmt.Println("=== Any / All (bool columns; numeric non-zero is truthy) ===")
	any := df.Any()
	all := df.All()
	for _, col := range []string{"InStock", "Units"} {
		fmt.Printf("  %-8s any=%-6v all=%v\n", col, any[col], all[col])
	}
	fmt.Println("  Units has a zero, so All(Units) is false.")
	fmt.Println("  Product is a string column, so Any/All omit it entirely.")
}
