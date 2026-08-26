package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas"
	"github.com/apoplexi24/gpandas/dataframe"
)

func main() {
	gp := gpandas.GoPandas{}

	// Create a sample employee DataFrame
	columns := []string{"Name", "Department", "Age", "Salary"}
	data := []gpandas.Column{
		{"Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"},
		{"Engineering", "Sales", "Engineering", "Sales", "Marketing", "Engineering"},
		{int64(30), int64(25), int64(35), int64(28), int64(32), int64(27)},
		{95000.0, 55000.0, 105000.0, 62000.0, 72000.0, 88000.0},
	}
	types := map[string]any{
		"Name":       gpandas.StringCol{},
		"Department": gpandas.StringCol{},
		"Age":        gpandas.IntCol{},
		"Salary":     gpandas.FloatCol{},
	}

	df, err := gp.DataFrame(columns, data, types)
	if err != nil {
		log.Fatalf("Failed to create DataFrame: %v", err)
	}

	fmt.Println("=== Original DataFrame ===")
	fmt.Println(df)

	// ---------------------------------------------------------------
	// 1. Isin: keep rows whose value is in a set
	// ---------------------------------------------------------------
	inDepts, err := df.Isin("Department", []any{"Engineering", "Marketing"}).Result()
	if err != nil {
		log.Fatalf("Isin failed: %v", err)
	}
	fmt.Println("=== Isin: Department in [Engineering, Marketing] ===")
	fmt.Println(inDepts)

	// Numeric members match across int, int64 and float64.
	inAges, err := df.Isin("Age", []any{25, 30.0, int64(35)}).Result()
	if err != nil {
		log.Fatalf("Isin failed: %v", err)
	}
	fmt.Println("=== Isin: Age in [25, 30, 35] (mixed numeric literals) ===")
	fmt.Println(inAges)

	// ---------------------------------------------------------------
	// 2. Between: keep rows inside a range
	// ---------------------------------------------------------------
	midCareer, err := df.Between("Age", 27, 32, dataframe.InclusiveBoth).Result()
	if err != nil {
		log.Fatalf("Between failed: %v", err)
	}
	fmt.Println("=== Between: 27 <= Age <= 32 (inclusive both) ===")
	fmt.Println(midCareer)

	// Bounds can be excluded individually.
	exclusive, err := df.Between("Age", 27, 32, dataframe.InclusiveNeither).Result()
	if err != nil {
		log.Fatalf("Between failed: %v", err)
	}
	fmt.Println("=== Between: 27 < Age < 32 (inclusive neither) ===")
	fmt.Println(exclusive)

	leftOnly, err := df.Between("Salary", 62000.0, 95000.0, dataframe.InclusiveLeft).Result()
	if err != nil {
		log.Fatalf("Between failed: %v", err)
	}
	fmt.Println("=== Between: 62000 <= Salary < 95000 (inclusive left) ===")
	fmt.Println(leftOnly)

	// ---------------------------------------------------------------
	// 3. Nlargest / Nsmallest: top-k without a full sort
	// ---------------------------------------------------------------
	topEarners, err := df.Nlargest(3, "Salary").Result()
	if err != nil {
		log.Fatalf("Nlargest failed: %v", err)
	}
	fmt.Println("=== Nlargest: top 3 by Salary (largest first) ===")
	fmt.Println(topEarners)

	youngest, err := df.Nsmallest(2, "Age").Result()
	if err != nil {
		log.Fatalf("Nsmallest failed: %v", err)
	}
	fmt.Println("=== Nsmallest: 2 youngest (smallest first) ===")
	fmt.Println(youngest)

	// ---------------------------------------------------------------
	// 4. Chaining with the existing Filter/Where helpers
	// ---------------------------------------------------------------
	chained, err := df.
		Isin("Department", []any{"Engineering", "Sales"}).
		Between("Age", 25, 30, dataframe.InclusiveBoth).
		Nlargest(2, "Salary").
		Result()
	if err != nil {
		log.Fatalf("chained selection failed: %v", err)
	}
	fmt.Println("=== Chained: Engineering/Sales, 25-30, top 2 by Salary ===")
	fmt.Println(chained)

	// Mixing these helpers with Filter and Where works the same way.
	mixed, err := df.
		Filter("Salary", dataframe.GreaterThan, 60000.0).
		Isin("Department", []any{"Engineering"}).
		Where(func(row map[string]any) bool {
			age, _ := row["Age"].(int64)
			return age < 35
		}).
		Result()
	if err != nil {
		log.Fatalf("mixed selection failed: %v", err)
	}
	fmt.Println("=== Mixed: Filter + Isin + Where ===")
	fmt.Println(mixed)

	// ---------------------------------------------------------------
	// 5. Errors are deferred to the terminal call
	// ---------------------------------------------------------------
	if _, err := df.Isin("Missing", []any{1}).Nlargest(2, "Salary").Result(); err != nil {
		fmt.Println("=== Deferred error surfaced by Result() ===")
		fmt.Println(err)
	}
}
