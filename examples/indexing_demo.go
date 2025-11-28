package main

import (
	"fmt"

	"github.com/apoplexi24/gpandas"
)

func main() {
	// Create a sample DataFrame
	pd := gpandas.GoPandas{}
	columns := []string{"name", "age", "city"}
	data := []gpandas.Column{
		{"Alice", "Bob", "Charlie", "David"},
		{int64(25), int64(30), int64(35), int64(28)},
		{"NYC", "LA", "SF", "Seattle"},
	}
	columnTypes := map[string]any{
		"name": gpandas.StringCol{},
		"age":  gpandas.IntCol{},
		"city": gpandas.StringCol{},
	}

	df, err := pd.DataFrame(columns, data, columnTypes)
	if err != nil {
		panic(err)
	}

	fmt.Println("=== Original DataFrame ===")
	fmt.Println(df.String())

	// Example 1: Column Selection
	fmt.Println("\n=== Select Columns: name and city ===")
	selectedCols, _ := df.Select("name", "city")
	fmt.Println(selectedCols.String())

	// Example 2: Get single column as Series
	fmt.Println("\n=== Get 'age' column as Series ===")
	ageSeries, _ := df.SelectCol("age")
	fmt.Printf("Length: %d, First value: %v\n", ageSeries.Len(), ageSeries.MustAt(0))

	// Example 3: Set custom index
	fmt.Println("\n=== Set Custom Index ===")
	df.SetIndex([]string{"person1", "person2", "person3", "person4"})
	fmt.Println(df.String())

	// Example 4: Loc - Label-based indexing
	fmt.Println("\n=== Loc: Get value at ('person2', 'name') ===")
	val, _ := df.Loc().At("person2", "name")
	fmt.Printf("Value: %v\n", val)

	fmt.Println("\n=== Loc: Get row 'person3' ===")
	row, _ := df.Loc().Row("person3")
	fmt.Println(row.String())

	fmt.Println("\n=== Loc: Get rows 'person1' and 'person3' ===")
	rows, _ := df.Loc().Rows([]string{"person1", "person3"})
	fmt.Println(rows.String())

	// Example 5: Reset and use ILoc
	fmt.Println("\n=== Reset Index and Use ILoc ===")
	df.ResetIndex()

	fmt.Println("\n=== ILoc: Get value at position (1, 2) ===")
	val2, _ := df.ILoc().At(1, 2)
	fmt.Printf("Value: %v\n", val2)

	fmt.Println("\n=== ILoc: Get row at position 0 ===")
	row2, _ := df.ILoc().Row(0)
	fmt.Println(row2.String())

	fmt.Println("\n=== ILoc: Get rows at positions [0, 2] ===")
	rows2, _ := df.ILoc().Rows([]int{0, 2})
	fmt.Println(rows2.String())

	fmt.Println("\n=== ILoc: Get range [1:3) ===")
	rangeDF, _ := df.ILoc().Range(1, 3)
	fmt.Println(rangeDF.String())

	fmt.Println("\n=== ILoc: Get columns at positions [0, 2] ===")
	cols, _ := df.ILoc().Cols([]int{0, 2})
	fmt.Println(cols.String())
}
