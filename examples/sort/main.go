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
	// 1. Sort by a single column (ascending)
	// ---------------------------------------------------------------
	sorted, err := df.SortValues(dataframe.SortOptions{
		By: []string{"Salary"},
	})
	if err != nil {
		log.Fatalf("SortValues failed: %v", err)
	}
	fmt.Println("=== Sorted by Salary (ascending) ===")
	fmt.Println(sorted)

	// ---------------------------------------------------------------
	// 2. Sort by a single column (descending)
	// ---------------------------------------------------------------
	sorted, err = df.SortValues(dataframe.SortOptions{
		By:        []string{"Age"},
		Ascending: []bool{false},
	})
	if err != nil {
		log.Fatalf("SortValues failed: %v", err)
	}
	fmt.Println("=== Sorted by Age (descending) ===")
	fmt.Println(sorted)

	// ---------------------------------------------------------------
	// 3. Sort by multiple columns with mixed order
	// ---------------------------------------------------------------
	sorted, err = df.SortValues(dataframe.SortOptions{
		By:        []string{"Department", "Salary"},
		Ascending: []bool{true, false}, // Dept ascending, Salary descending within each dept
	})
	if err != nil {
		log.Fatalf("SortValues failed: %v", err)
	}
	fmt.Println("=== Sorted by Department (asc) then Salary (desc) ===")
	fmt.Println(sorted)

	// ---------------------------------------------------------------
	// 4. Sort by Name (alphabetical)
	// ---------------------------------------------------------------
	sorted, err = df.SortValues(dataframe.SortOptions{
		By: []string{"Name"},
	})
	if err != nil {
		log.Fatalf("SortValues failed: %v", err)
	}
	fmt.Println("=== Sorted by Name (alphabetical) ===")
	fmt.Println(sorted)

	// ---------------------------------------------------------------
	// 5. Sort with index reset
	// ---------------------------------------------------------------
	sorted, err = df.SortValues(dataframe.SortOptions{
		By:          []string{"Salary"},
		Ascending:   []bool{false},
		IgnoreIndex: true,
	})
	if err != nil {
		log.Fatalf("SortValues failed: %v", err)
	}
	fmt.Println("=== Sorted by Salary (desc) with index reset ===")
	fmt.Println(sorted)

	// ---------------------------------------------------------------
	// 6. Sort by index
	// ---------------------------------------------------------------
	// First, set a custom index
	err = df.SetIndex([]string{"f", "b", "e", "a", "d", "c"})
	if err != nil {
		log.Fatalf("SetIndex failed: %v", err)
	}

	indexSorted, err := df.SortIndex(true)
	if err != nil {
		log.Fatalf("SortIndex failed: %v", err)
	}
	fmt.Println("=== Sorted by Index (ascending) ===")
	fmt.Println(indexSorted)
}
