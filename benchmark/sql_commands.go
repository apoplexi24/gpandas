package main

import (
	"fmt"
	"time"

	"github.com/apoplexi24/gpandas"

	"github.com/joho/godotenv"
)

func sql_commands_test() {
	envFile, _ := godotenv.Read(".env")
	table_id := envFile["TABLEID"]
	start := time.Now()
	gp := gpandas.GoPandas{}
	df, err := gp.From_gbq("SELECT distinct(Card_Name) FROM `"+table_id+".NewsFeed.NewsFeedCardsTracker`", "jm-ebg")
	if err != nil {
		fmt.Printf("Error while querying => %v", err)
	}
	fmt.Printf("##########")
	fmt.Printf("%v", df.String())
	fmt.Printf("##########")
	elapsed := time.Since(start)
	fmt.Printf("%f\n", elapsed.Seconds())
}
