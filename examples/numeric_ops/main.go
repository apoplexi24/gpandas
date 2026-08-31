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

	// A daily price series. The null in Price on day 4 shows how every operation
	// carries missing data through instead of inventing a value for it.
	columns := []string{"Date", "Price", "Volume", "Sentiment"}
	data := []gpandas.Column{
		{"2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09"},
		{101.4567, 103.5, 99.25, nil, 104.875, 104.875},
		{int64(1200), int64(1450), int64(980), int64(0), int64(1610), int64(1450)},
		{0.35, -0.12, -0.68, 0.05, 0.42, -0.42},
	}
	types := map[string]any{
		"Date":      gpandas.StringCol{},
		"Price":     gpandas.FloatCol{},
		"Volume":    gpandas.IntCol{},
		"Sentiment": gpandas.FloatCol{},
	}

	df, err := gp.DataFrame(columns, data, types)
	if err != nil {
		log.Fatalf("Failed to create DataFrame: %v", err)
	}

	fmt.Println("=== Original DataFrame ===")
	fmt.Println(df)

	// ---------------------------------------------------------------
	// 1. Round: halfway values go to the nearest even number
	// ---------------------------------------------------------------
	rounded, err := df.Round(2)
	if err != nil {
		log.Fatalf("Round failed: %v", err)
	}
	fmt.Println("=== Round(2): Volume stays int64, the null stays null ===")
	fmt.Println(rounded)

	// Negative decimals round to the left of the decimal point.
	coarse, err := df.Round(-2)
	if err != nil {
		log.Fatalf("Round failed: %v", err)
	}
	fmt.Println("=== Round(-2): rounded to the nearest hundred ===")
	fmt.Println(coarse)
	fmt.Println("Sentiment shows -0 because IEEE floats keep the sign of a value")
	fmt.Println("that rounds to zero. NumPy and pandas print it the same way.")
	fmt.Println()

	// ---------------------------------------------------------------
	// 2. Clip: bound values to a range
	// ---------------------------------------------------------------
	bounded, err := df.Clip(100, 104)
	if err != nil {
		log.Fatalf("Clip failed: %v", err)
	}
	fmt.Println("=== Clip(100, 104): every numeric column is bounded ===")
	fmt.Println(bounded)
	fmt.Println("The bounds apply to all numeric columns, so Volume and Sentiment")
	fmt.Println("are clamped to the price range too. Clip one column at a time by")
	fmt.Println("selecting it first if that is not what you want.")
	fmt.Println()

	// Pass an infinity to leave one side unbounded. Here every numeric column
	// gets a floor of zero while its upper end is left alone.
	floored, err := df.Clip(0, math.Inf(1))
	if err != nil {
		log.Fatalf("Clip failed: %v", err)
	}
	fmt.Println("=== Clip(0, +Inf): a floor with no ceiling ===")
	fmt.Println(floored)

	// A reversed range is a mistake rather than an empty result, so it errors.
	if _, err := df.Clip(10, 5); err != nil {
		fmt.Printf("Clip(10, 5) rejected: %v\n\n", err)
	}

	// ---------------------------------------------------------------
	// 3. Abs: magnitude, ignoring direction
	// ---------------------------------------------------------------
	magnitudes, err := df.Abs()
	if err != nil {
		log.Fatalf("Abs failed: %v", err)
	}
	fmt.Println("=== Abs(): Sentiment loses its sign ===")
	fmt.Println(magnitudes)

	// ---------------------------------------------------------------
	// 4. Diff: change from an earlier row
	// ---------------------------------------------------------------
	delta, err := df.Diff(1)
	if err != nil {
		log.Fatalf("Diff failed: %v", err)
	}
	fmt.Println("=== Diff(1): row 0 has no predecessor, so it is null ===")
	fmt.Println(delta)

	// Negative periods look forward and vacate the tail instead of the head.
	ahead, err := df.Diff(-1)
	if err != nil {
		log.Fatalf("Diff failed: %v", err)
	}
	fmt.Println("=== Diff(-1): compares each row with the next one ===")
	fmt.Println(ahead)

	// ---------------------------------------------------------------
	// 5. PctChange: fractional change from an earlier row
	// ---------------------------------------------------------------
	growth, err := df.PctChange(1)
	if err != nil {
		log.Fatalf("PctChange failed: %v", err)
	}
	fmt.Println("=== PctChange(1): always float64; Volume divides by zero on day 5 ===")
	fmt.Println(growth)

	// Scale it into percentage points with the Sprint 1 scalar kernels.
	pctPoints, err := growth.MulScalar("Price", 100)
	if err != nil {
		log.Fatalf("MulScalar failed: %v", err)
	}
	if err := growth.Assign("PricePct", pctPoints); err != nil {
		log.Fatalf("Assign failed: %v", err)
	}
	fmt.Println("=== PctChange scaled to percentage points via MulScalar ===")
	fmt.Println(growth)

	// ---------------------------------------------------------------
	// 6. Rank: the five tie-breaking methods side by side
	// ---------------------------------------------------------------
	// Volume is 1200, 1450, 980, 0, 1610, 1450, so 1450 is tied.
	display, err := gp.DataFrame(
		[]string{"Date", "Volume"},
		[]gpandas.Column{data[0], data[2]},
		map[string]any{"Date": gpandas.StringCol{}, "Volume": gpandas.IntCol{}},
	)
	if err != nil {
		log.Fatalf("Failed to create DataFrame: %v", err)
	}

	methods := []dataframe.RankMethod{
		dataframe.RankAverage,
		dataframe.RankMin,
		dataframe.RankMax,
		dataframe.RankDense,
		dataframe.RankFirst,
	}
	for _, method := range methods {
		ranked, err := df.Rank(method)
		if err != nil {
			log.Fatalf("Rank(%s) failed: %v", method, err)
		}
		if err := display.Assign(string(method), ranked.Columns["Volume"]); err != nil {
			log.Fatalf("Assign failed: %v", err)
		}
	}

	fmt.Println("=== Rank: Volume has a tie (1450 appears twice) ===")
	fmt.Println(display)
	fmt.Println("average splits the tied ranks, min and max push both to one end,")
	fmt.Println("dense leaves no gap after the tie, and first breaks it by row order.")
}
