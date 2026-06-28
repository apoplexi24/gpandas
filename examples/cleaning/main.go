package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas"
	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func main() {
	gp := gpandas.GoPandas{}

	// Build a DataFrame with some missing values and duplicates.
	columns := []string{"Name", "Department", "Age", "Salary"}
	data := []gpandas.Column{
		{"Alice", "Bob", "Charlie", "Charlie", "Eve", "Frank"},
		{"Engineering", "Sales", "Engineering", "Engineering", "Marketing", nil},
		{int64(30), int64(25), int64(35), int64(35), nil, int64(27)},
		{95000.0, nil, 105000.0, 105000.0, 72000.0, 88000.0},
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
	// 1. Inspect types and structure
	// ---------------------------------------------------------------
	fmt.Println("=== Info ===")
	fmt.Println(df.Info())
	fmt.Printf("DTypes: %v\n\n", df.DTypes())

	// ---------------------------------------------------------------
	// 2. Missing data: detect, fill, drop
	// ---------------------------------------------------------------
	fmt.Println("=== IsNA (null mask) ===")
	fmt.Println(df.IsNA())

	filled, err := df.FillNAColumn("Salary", 0.0)
	if err != nil {
		log.Fatalf("FillNAColumn failed: %v", err)
	}
	fmt.Println("=== FillNAColumn: Salary nulls -> 0 ===")
	fmt.Println(filled)

	dropped, err := df.DropNA("any", nil)
	if err != nil {
		log.Fatalf("DropNA failed: %v", err)
	}
	fmt.Println("=== DropNA: rows with any null removed ===")
	fmt.Println(dropped)

	// ---------------------------------------------------------------
	// 3. Deduplication
	// ---------------------------------------------------------------
	deduped, err := df.DropDuplicates([]string{"Name"}, "first")
	if err != nil {
		log.Fatalf("DropDuplicates failed: %v", err)
	}
	fmt.Println("=== DropDuplicates by Name (keep first) ===")
	fmt.Println(deduped)

	depts, err := df.Unique("Department")
	if err != nil {
		log.Fatalf("Unique failed: %v", err)
	}
	count, _ := df.NUnique("Department")
	fmt.Printf("Unique departments: %v (nunique excluding null: %d)\n\n", depts, count)

	// ---------------------------------------------------------------
	// 4. Adding columns
	// ---------------------------------------------------------------
	bonus, _ := collection.NewFloat64SeriesFromData(
		[]float64{5000, 3000, 7000, 7000, 4000, 3500}, nil)
	if err := df.Assign("Bonus", bonus); err != nil {
		log.Fatalf("Assign failed: %v", err)
	}

	if err := df.AssignFunc("HighEarner", func(row map[string]any) any {
		salary, ok := row["Salary"].(float64)
		return ok && salary >= 95000
	}); err != nil {
		log.Fatalf("AssignFunc failed: %v", err)
	}

	ids, _ := collection.NewInt64SeriesFromData([]int64{1, 2, 3, 4, 5, 6}, nil)
	if err := df.Insert(0, "ID", ids); err != nil {
		log.Fatalf("Insert failed: %v", err)
	}

	fmt.Println("=== After Assign/AssignFunc/Insert ===")
	fmt.Println(df)

	// ---------------------------------------------------------------
	// 5. Type casting
	// ---------------------------------------------------------------
	casted, err := df.AsType("Age", dataframe.FloatCol{})
	if err != nil {
		log.Fatalf("AsType failed: %v", err)
	}
	fmt.Println("=== AsType: Age int64 -> float64 ===")
	fmt.Printf("Age dtype is now: %s\n", casted.DTypes()["Age"])
}
