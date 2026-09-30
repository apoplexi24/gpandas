package main

import (
	"fmt"
	"log"
	"math"

	"github.com/apoplexi24/gpandas"
	"github.com/apoplexi24/gpandas/dataframe"
)

func main() {
	gp := gpandas.GoPandas{}

	// Customer records with a missing city and a missing income, to show how
	// each step treats nulls.
	columns := []string{"Id", "Age", "City", "Income"}
	data := []gpandas.Column{
		{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8)},
		{int64(14), int64(23), int64(35), int64(47), int64(52), int64(68), int64(71), int64(29)},
		{"NYC", "LA", "NYC", "Austin", nil, "LA", "NYC", "Austin"},
		{12000.0, 48000.0, 61000.0, nil, 95000.0, 40000.0, 33000.0, 72000.0},
	}
	types := map[string]any{
		"Id":     gpandas.IntCol{},
		"Age":    gpandas.IntCol{},
		"City":   gpandas.StringCol{},
		"Income": gpandas.FloatCol{},
	}

	df, err := gp.DataFrame(columns, data, types)
	if err != nil {
		log.Fatalf("Failed to create DataFrame: %v", err)
	}

	fmt.Println("=== Original DataFrame ===")
	fmt.Println(df)

	// ---------------------------------------------------------------
	// 1. Cut: fixed, named bins
	// ---------------------------------------------------------------
	// Bins are right-closed, (low, high]. Infinite outer edges catch every age.
	ageBins := []float64{math.Inf(-1), 17, 34, 64, math.Inf(1)}
	ageLabels := []string{"minor", "young", "middle", "senior"}
	byAge, err := df.Cut("Age", ageBins, ageLabels)
	if err != nil {
		log.Fatalf("Cut failed: %v", err)
	}
	fmt.Println("=== Cut(Age): each age replaced by its group ===")
	fmt.Println(byAge)

	// Without labels, each bin is named by its interval.
	decades, err := df.Cut("Age", []float64{10, 30, 50, 70}, nil)
	if err != nil {
		log.Fatalf("Cut failed: %v", err)
	}
	fmt.Println("=== Cut(Age) with interval labels: 71 is past the top edge, so it is null ===")
	fmt.Println(decades)

	// ---------------------------------------------------------------
	// 2. Qcut: bins holding equal numbers of rows
	// ---------------------------------------------------------------
	quartiles, err := byAge.Qcut("Income", 4, []string{"Q1", "Q2", "Q3", "Q4"})
	if err != nil {
		log.Fatalf("Qcut failed: %v", err)
	}
	fmt.Println("=== Qcut(Income, 4): quartiles; the missing income stays null ===")
	fmt.Println(quartiles)

	edges, err := df.Qcut("Income", 4, nil)
	if err != nil {
		log.Fatalf("Qcut failed: %v", err)
	}
	cats, _ := edges.Categories("Income")
	fmt.Println("Quartile edges as interval labels:", cats)
	fmt.Println()

	// A column with too few distinct values cannot make distinct bins, so this
	// is an error rather than a silent reduction to fewer bins.
	flags, err := gp.DataFrame(
		[]string{"Flag"},
		[]gpandas.Column{{0.0, 0.0, 0.0, 1.0}},
		map[string]any{"Flag": gpandas.FloatCol{}},
	)
	if err != nil {
		log.Fatalf("Failed to create DataFrame: %v", err)
	}
	if _, err := flags.Qcut("Flag", 4, nil); err != nil {
		fmt.Printf("Qcut on a mostly-constant column rejected: %v\n\n", err)
	}

	// ---------------------------------------------------------------
	// 3. GetDummies: one-hot encoding
	// ---------------------------------------------------------------
	encoded, err := quartiles.GetDummies("City")
	if err != nil {
		log.Fatalf("GetDummies failed: %v", err)
	}
	fmt.Println("=== GetDummies(City): one bool column per city; the null row is all false ===")
	fmt.Println(encoded)

	// On a binned column the indicators follow bin order and include empty
	// bins, so the feature set does not depend on which values happen to occur.
	encoded, err = encoded.GetDummies("Age")
	if err != nil {
		log.Fatalf("GetDummies failed: %v", err)
	}
	fmt.Println("=== GetDummies(Age): one column per age group, in bin order ===")
	fmt.Println(encoded)

	// ---------------------------------------------------------------
	// 4. Binned columns work with the rest of the API
	// ---------------------------------------------------------------
	counts, err := byAge.ValueCounts("Age")
	if err != nil {
		log.Fatalf("ValueCounts failed: %v", err)
	}
	fmt.Println("=== ValueCounts on the age groups ===")
	fmt.Println(counts)

	gb, err := byAge.GroupBy([]string{"Age"}, 0)
	if err != nil {
		log.Fatalf("GroupBy failed: %v", err)
	}
	avgIncome, err := gb.Agg(map[string][]dataframe.AggFunc{"Income": {dataframe.AggMean}})
	if err != nil {
		log.Fatalf("Agg failed: %v", err)
	}
	fmt.Println("=== Mean income per age group ===")
	fmt.Println(avgIncome)
}
