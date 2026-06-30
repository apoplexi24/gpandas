package main

import (
	"fmt"
	"log"
	"os"

	"github.com/apoplexi24/gpandas"
	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func main() {
	gp := gpandas.GoPandas{}

	df, err := gp.DataFrame(
		[]string{"Region", "Date", "Sales", "Units"},
		[]gpandas.Column{
			{"N", "S", "N", "S", "N"},
			{"2021-01-05", "2021-01-06", "2021-02-10", "2021-02-11", "2021-03-01"},
			{100.0, 50.0, 200.0, 70.0, 150.0},
			{int64(3), int64(5), int64(2), int64(8), int64(4)},
		},
		map[string]any{
			"Region": gpandas.StringCol{},
			"Date":   gpandas.StringCol{},
			"Sales":  gpandas.FloatCol{},
			"Units":  gpandas.IntCol{},
		},
	)
	if err != nil {
		log.Fatalf("DataFrame failed: %v", err)
	}

	fmt.Println("=== Original ===")
	fmt.Println(df)

	// 1. Correlation / covariance
	corr, _ := df.Corr()
	fmt.Println("=== Corr (Sales, Units) ===")
	fmt.Println(corr)

	// 2. Sample
	sample, _ := df.Sample(3, 42)
	fmt.Println("=== Sample(3, seed=42) ===")
	fmt.Println(sample)

	// 3. Pipe
	piped, _ := df.Pipe(func(d *dataframe.DataFrame) (*dataframe.DataFrame, error) {
		return d.Filter("Sales", dataframe.GreaterThan, 80.0).Result()
	})
	fmt.Println("=== Pipe(Filter Sales > 80) ===")
	fmt.Println(piped)

	// 4. DateTime extraction
	dated, _ := df.ToDatetime("Date", "2006-01-02")
	acc, _ := dated.Dt("Date")
	_ = dated.Assign("Month", acc.Month())
	fmt.Println("=== ToDatetime + Dt().Month() ===")
	fmt.Println(dated)

	// 5. Categorical
	cat, _ := df.AsCategorical("Region")
	cats, _ := cat.Categories("Region")
	fmt.Printf("=== AsCategorical(Region): categories = %v ===\n\n", cats)

	// 6. Multi-key merge
	targets := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Region": mustString("N", "S"),
			"Target": mustFloat(500, 200),
		},
		ColumnOrder: []string{"Region", "Target"},
		Index:       []string{"0", "1"},
	}
	merged, _ := df.MergeOn(targets, []string{"Region"}, dataframe.LeftMerge)
	fmt.Println("=== MergeOn(Region) ===")
	fmt.Println(merged)

	// 7. Parquet round-trip
	_ = os.MkdirAll("output", 0o755)
	if err := df.ToParquet("output/sales.parquet"); err != nil {
		log.Fatalf("ToParquet failed: %v", err)
	}
	loaded, _ := gp.Read_parquet("output/sales.parquet")
	fmt.Println("=== Parquet round-trip (dtypes) ===")
	fmt.Printf("%v\n\n", loaded.DTypes())

	// 8. Plots
	if err := df.PlotScatter("Units", "Sales", &plot.ChartOptions{
		Title: "Sales vs Units", OutputPath: "output/scatter.html",
	}); err != nil {
		log.Fatalf("PlotScatter failed: %v", err)
	}
	if err := df.PlotHistogram("Sales", 5, &plot.ChartOptions{
		Title: "Sales Distribution", OutputPath: "output/hist.html",
	}); err != nil {
		log.Fatalf("PlotHistogram failed: %v", err)
	}
	if err := corr.PlotHeatmap(&plot.ChartOptions{
		Title: "Correlation", OutputPath: "output/heatmap.html",
	}); err != nil {
		log.Fatalf("PlotHeatmap failed: %v", err)
	}
	fmt.Println("=== Charts written to output/ (scatter.html, hist.html, heatmap.html) ===")
}

func mustString(vals ...string) collection.Series {
	s, _ := collection.NewStringSeriesFromData(vals, nil)
	return s
}

func mustFloat(vals ...float64) collection.Series {
	s, _ := collection.NewFloat64SeriesFromData(vals, nil)
	return s
}
