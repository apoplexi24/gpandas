package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas"
	"github.com/apoplexi24/gpandas/dataframe"
)

func main() {
	gp := gpandas.GoPandas{}

	// Create first DataFrame
	cols1 := []string{"A", "B"}
	data1 := []gpandas.Column{
		{int64(1), int64(2)},
		{int64(3), int64(4)},
	}
	types1 := map[string]any{
		"A": gpandas.IntCol{},
		"B": gpandas.IntCol{},
	}
	df1, err := gp.DataFrame(cols1, data1, types1)
	if err != nil {
		log.Fatalf("Failed to create df1: %v", err)
	}

	// Create second DataFrame
	cols2 := []string{"A", "B"}
	data2 := []gpandas.Column{
		{int64(5), int64(6)},
		{int64(7), int64(8)},
	}
	types2 := map[string]any{
		"A": gpandas.IntCol{},
		"B": gpandas.IntCol{},
	}
	df2, err := gp.DataFrame(cols2, data2, types2)
	if err != nil {
		log.Fatalf("Failed to create df2: %v", err)
	}

	fmt.Println("DataFrame 1:")
	fmt.Println(df1)
	fmt.Println("\nDataFrame 2:")
	fmt.Println(df2)

	// Concat along rows (Axis 0)
	concatRows, err := gpandas.Concat([]*dataframe.DataFrame{df1, df2})
	if err != nil {
		log.Fatalf("Concat rows failed: %v", err)
	}
	fmt.Println("\nConcat along rows (Axis 0):")
	fmt.Println(concatRows)

	// Concat along columns (Axis 1)
	// For this, let's create a DataFrame with different columns
	cols3 := []string{"C", "D"}
	data3 := []gpandas.Column{
		{int64(9), int64(10)},
		{int64(11), int64(12)},
	}
	types3 := map[string]any{
		"C": gpandas.IntCol{},
		"D": gpandas.IntCol{},
	}
	df3, err := gp.DataFrame(cols3, data3, types3)
	if err != nil {
		log.Fatalf("Failed to create df3: %v", err)
	}

	fmt.Println("\nDataFrame 3:")
	fmt.Println(df3)

	concatCols, err := gpandas.Concat([]*dataframe.DataFrame{df1, df3}, gpandas.ConcatOptions{Axis: gpandas.AxisColumns})
	if err != nil {
		log.Fatalf("Concat columns failed: %v", err)
	}
	fmt.Println("\nConcat along columns (Axis 1):")
	fmt.Println(concatCols)
}
