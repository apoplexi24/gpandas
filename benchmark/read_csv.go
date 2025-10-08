package main

import (
	"fmt"

	"github.com/apoplexi24/gpandas"

	"time"

	"github.com/joho/godotenv"
)

func read_csv_test() {
	envFile, _ := godotenv.Read(".env")
	table_id := envFile["TABLEID"]
	start := time.Now()
	gp := gpandas.GoPandas{}
	_, err := gp.Read_csv("./" + table_id + ".csv")
	if err != nil {
		fmt.Printf("Error reading CSV: %v\n", err)
		return
	}
	elapsed := time.Since(start)
	fmt.Printf("%f\n", elapsed.Seconds())

}
