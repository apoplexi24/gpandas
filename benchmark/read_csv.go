package main

import (
	"fmt"
	"gpandas"
	"time"
)

func main() {
	start := time.Now()
	gp := gpandas.GoPandas{}
	_, err := gp.Read_csv("./customers-2000000.csv")
	if err != nil {
		fmt.Printf("Error reading CSV: %v\n", err)
		return
	}
	elapsed := time.Since(start)
	fmt.Printf("%f\n", elapsed.Seconds())

}
