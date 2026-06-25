package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas"
	"github.com/apoplexi24/gpandas/dataframe"
)

func main() {
	gp := gpandas.GoPandas{}

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
	// 1. Boolean filtering (chainable, error-deferred)
	// ---------------------------------------------------------------
	highEarners, err := df.Filter("Salary", dataframe.GreaterThan, 80000.0).Result()
	if err != nil {
		log.Fatalf("Filter failed: %v", err)
	}
	fmt.Println("=== Filter: Salary > 80,000 ===")
	fmt.Println(highEarners)

	// Chained conditions: Engineering AND Salary > 90,000
	chained, err := df.
		Filter("Department", dataframe.Equals, "Engineering").
		Filter("Salary", dataframe.GreaterThan, 90000.0).
		Result()
	if err != nil {
		log.Fatalf("chained Filter failed: %v", err)
	}
	fmt.Println("=== Filter: Department == Engineering AND Salary > 90,000 ===")
	fmt.Println(chained)

	// Multi-condition predicate via Where
	youngEngineers, err := df.Where(func(row map[string]any) bool {
		age, _ := row["Age"].(int64)
		return age < 32 && row["Department"] == "Engineering"
	}).Result()
	if err != nil {
		log.Fatalf("Where failed: %v", err)
	}
	fmt.Println("=== Where: Age < 32 AND Department == Engineering ===")
	fmt.Println(youngEngineers)

	// ---------------------------------------------------------------
	// 2. Summary statistics
	// ---------------------------------------------------------------
	summary, err := df.Describe()
	if err != nil {
		log.Fatalf("Describe failed: %v", err)
	}
	fmt.Println("=== Describe (numeric columns) ===")
	fmt.Println(summary)

	fmt.Println("=== Column aggregations ===")
	fmt.Printf("Mean:   %v\n", df.Mean())
	fmt.Printf("Sum:    %v\n", df.Sum())
	fmt.Printf("Std:    %v\n", df.Std())
	fmt.Printf("Median: %v\n", df.Median())
	fmt.Printf("Nulls:  %v\n\n", df.NullCount())

	counts, err := df.ValueCounts("Department")
	if err != nil {
		log.Fatalf("ValueCounts failed: %v", err)
	}
	fmt.Println("=== ValueCounts: Department ===")
	fmt.Println(counts)

	// ---------------------------------------------------------------
	// 3. Apply / Map transformations
	// ---------------------------------------------------------------
	raised, err := df.Apply("Salary", func(v any) any {
		if v == nil {
			return nil
		}
		return v.(float64) * 1.10 // 10% raise
	})
	if err != nil {
		log.Fatalf("Apply failed: %v", err)
	}
	fmt.Println("=== Apply: 10% raise on Salary ===")
	fmt.Println(raised)

	abbreviated, err := df.Map("Department", map[any]any{
		"Engineering": "ENG",
		"Sales":       "SAL",
		"Marketing":   "MKT",
	})
	if err != nil {
		log.Fatalf("Map failed: %v", err)
	}
	fmt.Println("=== Map: abbreviate Department ===")
	fmt.Println(abbreviated)

	withTax, err := df.ApplyRow(func(row map[string]any) map[string]any {
		row["Tax"] = row["Salary"].(float64) * 0.30
		return row
	})
	if err != nil {
		log.Fatalf("ApplyRow failed: %v", err)
	}
	fmt.Println("=== ApplyRow: derive Tax column (30% of Salary) ===")
	fmt.Println(withTax)
}
