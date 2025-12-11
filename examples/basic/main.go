package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas"
	"github.com/apoplexi24/gpandas/dataframe"
)

func main() {
	gp := gpandas.GoPandas{}

	// Create a DataFrame
	columns := []string{"Name", "Age", "City", "Salary"}
	data := []gpandas.Column{
		{"John", "Jane", "Doe", "Smith", "Brown"},
		{int64(25), int64(30), int64(22), int64(35), int64(28)},
		{"New York", "London", "Paris", "Tokyo", "Berlin"},
		{50000.0, 60000.0, 45000.0, 70000.0, 55000.0},
	}
	types := map[string]any{
		"Name":   gpandas.StringCol{},
		"Age":    gpandas.IntCol{},
		"City":   gpandas.StringCol{},
		"Salary": gpandas.FloatCol{},
	}

	df, err := gp.DataFrame(columns, data, types)
	if err != nil {
		log.Fatalf("Failed to create DataFrame: %v", err)
	}

	fmt.Println("Original DataFrame:")
	fmt.Println(df)

	// Head
	fmt.Println("\nHead(3):")
	fmt.Println(df.Head(3))

	// Tail
	fmt.Println("\nTail(2):")
	fmt.Println(df.Tail(2))

	// Select columns
	selected, err := df.Select("Name", "Salary")
	if err != nil {
		log.Fatalf("Select failed: %v", err)
	}
	fmt.Println("\nSelected 'Name' and 'Salary':")
	fmt.Println(selected)

	// Drop columns
	dropped, err := df.Drop(dataframe.DropOptions{Columns: []string{"City"}})
	if err != nil {
		log.Fatalf("Drop failed: %v", err)
	}
	fmt.Println("\nDropped 'City':")
	fmt.Println(dropped)

	// Rename columns
	// Note: Rename modifies in place
	err = df.Rename(map[string]string{"Salary": "Annual Income"})
	if err != nil {
		log.Fatalf("Rename failed: %v", err)
	}
	fmt.Println("\nRenamed 'Salary' to 'Annual Income':")
	fmt.Println(df)

	// ToCSV
	csvStr, err := df.ToCSV("")
	if err != nil {
		log.Fatalf("ToCSV failed: %v", err)
	}
	fmt.Println("\nCSV Output:")
	fmt.Println(csvStr)
}
