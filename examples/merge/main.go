package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas"
	"github.com/apoplexi24/gpandas/dataframe"
)

func main() {
	gp := gpandas.GoPandas{}

	// Create first DataFrame (Employees)
	cols1 := []string{"ID", "Name", "DepartmentID"}
	data1 := []gpandas.Column{
		{int64(1), int64(2), int64(3), int64(4)},
		{"Alice", "Bob", "Charlie", "David"},
		{int64(101), int64(102), int64(101), int64(103)},
	}
	types1 := map[string]any{
		"ID":           gpandas.IntCol{},
		"Name":         gpandas.StringCol{},
		"DepartmentID": gpandas.IntCol{},
	}
	df1, err := gp.DataFrame(cols1, data1, types1)
	if err != nil {
		log.Fatalf("Failed to create df1: %v", err)
	}

	// Create second DataFrame (Departments)
	cols2 := []string{"DeptID", "DeptName"}
	data2 := []gpandas.Column{
		{int64(101), int64(102), int64(104)},
		{"HR", "Engineering", "Marketing"},
	}
	types2 := map[string]any{
		"DeptID":   gpandas.IntCol{},
		"DeptName": gpandas.StringCol{},
	}
	df2, err := gp.DataFrame(cols2, data2, types2)
	if err != nil {
		log.Fatalf("Failed to create df2: %v", err)
	}

	// Rename DepartmentID in df1 to match DeptID in df2 for easier merging,
	// or we can just merge on different names if supported?
	// The API df.Merge(other, on, how) takes a single 'on' column name,
	// implying the column must exist in both.
	// So let's rename DeptID in df2 to DepartmentID.
	err = df2.Rename(map[string]string{"DeptID": "DepartmentID"})
	if err != nil {
		log.Fatalf("Failed to rename column: %v", err)
	}

	fmt.Println("DataFrame 1 (Employees):")
	fmt.Println(df1)
	fmt.Println("\nDataFrame 2 (Departments):")
	fmt.Println(df2)

	// Inner Merge
	inner, err := df1.Merge(df2, "DepartmentID", dataframe.InnerMerge)
	if err != nil {
		log.Fatalf("Inner merge failed: %v", err)
	}
	fmt.Println("\nInner Merge (Employees with Departments):")
	fmt.Println(inner)

	// Left Merge
	left, err := df1.Merge(df2, "DepartmentID", dataframe.LeftMerge)
	if err != nil {
		log.Fatalf("Left merge failed: %v", err)
	}
	fmt.Println("\nLeft Merge (All Employees, matched Departments):")
	fmt.Println(left)

	// Right Merge
	right, err := df1.Merge(df2, "DepartmentID", dataframe.RightMerge)
	if err != nil {
		log.Fatalf("Right merge failed: %v", err)
	}
	fmt.Println("\nRight Merge (All Departments, matched Employees):")
	fmt.Println(right)

	// Full Merge
	full, err := df1.Merge(df2, "DepartmentID", dataframe.FullMerge)
	if err != nil {
		log.Fatalf("Full merge failed: %v", err)
	}
	fmt.Println("\nFull Merge (All Employees and Departments):")
	fmt.Println(full)
}
