package main

import (
	"fmt"
	"strconv"
	"time"

	pgsql "go-db-update/pkg/configs/db/postgres"
	goroutine_util "go-db-update/pkg/utils/goroutine"
	util "go-db-update/pkg/utils/util"
	"database/sql"
)

type Batch struct {
	limit int
	offset int
}

func generateChannel(limit int, iteration int) (<-chan Batch) {
	c := make(chan Batch)
	go func() {
		for i:=0; i<iteration; i++ {
			offset := i * limit
			c <- Batch{ offset: offset, limit: limit	}
		}
		close(c)
	}()
	return c
}

func getData(db *sql.DB) (int) {
	sqlStatement := "SELECT COUNT(1) AS total FROM table_updates"
	rows, _ := db.Query(sqlStatement)

	dataCount := 0

	for rows.Next(){
		err := rows.Scan(&dataCount)
    if err != nil {
			fmt.Println(err)
		}
	}

	return dataCount
}

func updateDB(timestamp time.Time, in <-chan Batch, db *sql.DB) (<-chan string) {
	c := make(chan string)
	sqlStatement := "UPDATE table_updates SET timestamp=$1 WHERE uuid IN (SELECT uuid FROM table_updates ORDER BY uuid LIMIT $2 OFFSET $3)"
	go func() {
		for b := range in {
			limit := b.limit
			offset := b.offset
		
			_, err := db.Exec(sqlStatement, timestamp, limit, offset)

			if err != nil {
				fmt.Println("error update ==>", err)
			}
			
			c <- "finish offset " + strconv.Itoa(b.offset)
		}
		close(c)
	}()

	return c
}

func method1(timestamp time.Time, limit int, iteration int, db *sql.DB) {
	gen := generateChannel(limit, iteration)

	var arr []<-chan string
	for i:=0; i<iteration; i++ {
		arr = append(arr, updateDB(timestamp, gen, db))
	}

	out := goroutine_util.Merge(arr)
	for i:=0; i<iteration; i++ {
		fmt.Println("Result: ", <-out)
	}
}

func method2(timestamp time.Time, limit int, iteration int, db *sql.DB) {
	gen := generateChannel(limit * iteration, 1)

	var arr []<-chan string
	for i:=0; i<iteration; i++ {
		arr = append(arr, updateDB(timestamp, gen, db))
	}

	out := goroutine_util.Merge(arr)
	for i:=0; i<iteration; i++ {
		<-out
	}
}

func main() {
	util.LoadENV()
	
	defer func() {
		if err := recover(); err != nil {
				time.Sleep(time.Millisecond * time.Duration(1000))
				fmt.Println("Exception: ", err)
				main()
		}
	}()

	db, _ := pgsql.PGConnect()

	totalData := getData(db)

	for {
		timestamp := time.Now()
		
		limit := 1000
		iteration := totalData / limit

		start := time.Now()
			method1(timestamp, limit, iteration, db)
			// method2(timestamp, limit, iteration, db)
		end := time.Now()
		elapsed := end.Sub(start)
	
		fmt.Println("update time elapsed ==>", elapsed)
		util.PrintMemUsage()

		time.Sleep(time.Millisecond * time.Duration(60000))
	}

	defer pgsql.PGClose(db)
}
