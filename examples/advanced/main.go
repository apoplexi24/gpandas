package main

import (
	"fmt"
	"log"

	"github.com/apoplexi24/gpandas"
	"github.com/apoplexi24/gpandas/dataframe"
)

func main() {
	gp := gpandas.GoPandas{}

	df, err := gp.DataFrame(
		[]string{"Department", "Name", "Salary"},
		[]gpandas.Column{
			{"Eng", "Sales", "Eng", "Sales", "Eng"},
			{"alice", "bob", "charlie", "diana", "eve"},
			{100.0, 50.0, 200.0, 70.0, 150.0},
		},
		map[string]any{
			"Department": gpandas.StringCol{},
			"Name":       gpandas.StringCol{},
			"Salary":     gpandas.FloatCol{},
		},
	)
	if err != nil {
		log.Fatalf("DataFrame failed: %v", err)
	}

	fmt.Println("=== Original ===")
	fmt.Println(df)

	// ---------------------------------------------------------------
	// 1. Flexible GroupBy aggregation
	// ---------------------------------------------------------------
	gb, _ := df.GroupBy([]string{"Department"}, 0)
	agg, err := gb.Agg(map[string][]dataframe.AggFunc{
		"Salary": {dataframe.AggSum, dataframe.AggMean, dataframe.AggMax},
		"Name":   {dataframe.AggCount},
	})
	if err != nil {
		log.Fatalf("Agg failed: %v", err)
	}
	fmt.Println("=== GroupBy.Agg (sum/mean/max salary, count names) ===")
	fmt.Println(agg)

	// ---------------------------------------------------------------
	// 2. Window functions
	// ---------------------------------------------------------------
	roll, _ := df.Rolling(2).Mean()
	fmt.Println("=== Rolling(2).Mean ===")
	fmt.Println(roll)

	cum, _ := df.CumSum()
	fmt.Println("=== CumSum ===")
	fmt.Println(cum)

	shifted, _ := df.Shift(1)
	fmt.Println("=== Shift(1) ===")
	fmt.Println(shifted)

	// ---------------------------------------------------------------
	// 3. String methods
	// ---------------------------------------------------------------
	acc, _ := df.Str("Name")
	if err := df.Assign("NameUpper", acc.Upper()); err != nil {
		log.Fatalf("Assign failed: %v", err)
	}
	fmt.Println("=== Str().Upper() added as NameUpper ===")
	fmt.Println(df)

	// ---------------------------------------------------------------
	// 4. Reshape: Stack / Unstack
	// ---------------------------------------------------------------
	scores, _ := gp.DataFrame(
		[]string{"Math", "Science"},
		[]gpandas.Column{
			{90.0, 80.0},
			{85.0, 75.0},
		},
		map[string]any{"Math": gpandas.FloatCol{}, "Science": gpandas.FloatCol{}},
	)
	_ = scores.SetIndex([]string{"Alice", "Bob"})

	long, _ := scores.Stack()
	fmt.Println("=== Stack (wide -> long) ===")
	fmt.Println(long)

	wide, _ := long.Unstack()
	fmt.Println("=== Unstack (long -> wide) ===")
	fmt.Println(wide)

	// ---------------------------------------------------------------
	// 5. JSON I/O
	// ---------------------------------------------------------------
	jsonStr, _ := agg.ToJSON("")
	fmt.Println("=== ToJSON (records) ===")
	fmt.Println(jsonStr)
}
