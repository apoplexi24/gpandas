package main

import (
	"fmt"
	"gpandas"
	"time"

	"github.com/joho/godotenv"
)

func funcname() {
	envFile, _ := godotenv.Read(".env")
	table_id := envFile["TABLEID"]
	start := time.Now()
	gp := gpandas.GoPandas{}
	df, err := gp.From_gbq("SELECT * FROM "+table_id, "jm-ebg")
	if err != nil {
		fmt.Printf("Error reading from gbq: %v\n", err)
		return
	}
	fmt.Println(df)
	end := time.Since(start)
	fmt.Printf("%f\n", end.Seconds())
}
