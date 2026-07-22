package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas"
)

func main() {
	gp := gpandas.GoPandas{}

	columns := []string{"Product", "Q1", "Q2", "Price"}
	data := []gpandas.Column{
		{"Widget", "Gadget", "Gizmo", "Doohickey"},
		{int64(100), int64(80), int64(120), int64(60)},
		{int64(150), int64(90), int64(110), int64(75)},
		{9.99, 14.50, 7.25, 19.00},
	}
	types := map[string]any{
		"Product": gpandas.StringCol{},
		"Q1":      gpandas.IntCol{},
		"Q2":      gpandas.IntCol{},
		"Price":   gpandas.FloatCol{},
	}

	df, err := gp.DataFrame(columns, data, types)
	if err != nil {
		log.Fatalf("Failed to create DataFrame: %v", err)
	}

	fmt.Println("=== Original DataFrame ===")
	fmt.Println(df)

	// ---------------------------------------------------------------
	// 1. Column-column arithmetic
	// ---------------------------------------------------------------
	// Total units = Q1 + Q2 (int + int stays int64)
	total, err := df.Add("Q1", "Q2")
	if err != nil {
		log.Fatalf("Add failed: %v", err)
	}
	if err := df.Assign("TotalUnits", total); err != nil {
		log.Fatalf("Assign failed: %v", err)
	}

	// Growth = Q2 - Q1
	growth, _ := df.Sub("Q2", "Q1")
	_ = df.Assign("Growth", growth)

	fmt.Println("=== After Add (TotalUnits) and Sub (Growth) ===")
	fmt.Println(df)

	// ---------------------------------------------------------------
	// 2. Column-scalar arithmetic
	// ---------------------------------------------------------------
	// Revenue = TotalUnits * Price (int * float -> float64)
	revenue, err := df.Mul("TotalUnits", "Price")
	if err != nil {
		log.Fatalf("Mul failed: %v", err)
	}
	_ = df.Assign("Revenue", revenue)

	// Apply a 10% discount to Price
	discounted, _ := df.MulScalar("Price", 0.90)
	_ = df.Assign("DiscountPrice", discounted)

	fmt.Println("=== After Mul (Revenue) and MulScalar (DiscountPrice, -10%) ===")
	fmt.Println(df)

	// ---------------------------------------------------------------
	// 3. Comparisons produce boolean columns
	// ---------------------------------------------------------------
	// Which products grew quarter over quarter?
	grew, err := df.Gt("Q2", "Q1")
	if err != nil {
		log.Fatalf("Gt failed: %v", err)
	}
	_ = df.Assign("Grew", grew)

	// Which are premium priced (Price > 10)?
	premium, _ := df.GtScalar("Price", 10.0)
	_ = df.Assign("Premium", premium)

	fmt.Println("=== After Gt (Grew) and GtScalar (Premium, Price > 10) ===")
	fmt.Println(df)

	// ---------------------------------------------------------------
	// 4. Null propagation
	// ---------------------------------------------------------------
	nullCols := []string{"X", "Y"}
	nullData := []gpandas.Column{
		{1.0, nil, 3.0},
		{10.0, 20.0, nil},
	}
	nullTypes := map[string]any{
		"X": gpandas.FloatCol{},
		"Y": gpandas.FloatCol{},
	}
	ndf, err := gp.DataFrame(nullCols, nullData, nullTypes)
	if err != nil {
		log.Fatalf("Failed to create null DataFrame: %v", err)
	}

	sum, _ := ndf.Add("X", "Y")
	_ = ndf.Assign("X_plus_Y", sum)

	fmt.Println("=== Null propagation: a null in either operand yields a null result ===")
	fmt.Println(ndf)
}
