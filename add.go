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
	start int
	max int
}

func generateChannel(iteration int, numBatch int) (<-chan Batch) {
	c := make(chan Batch)
	go func() {
		for i:=1; i<=iteration; i++ {
			max := i * numBatch
			c <- Batch{ start: (max - numBatch) + 1, max: max	}
		}
		close(c)
	}()
	return c
}

func insertDB(in <-chan Batch, db *sql.DB) (<-chan string) {
	c := make(chan string)
	sqlStatement := "INSERT INTO table_updates VALUES (uuid_generate_v4(), $1, now())"
	go func() {
		for b := range in {
			start := b.start
			maxRowInsert := b.max
			for start <= maxRowInsert {
				db.Exec(sqlStatement, "product title " + strconv.Itoa(start))
				start++
			}
			c <- "finish" + strconv.Itoa(b.start)
		}
		close(c)
	}()

	return c
}

func method1 (iteration int, numBatch int, db *sql.DB) {
	gen := generateChannel(iteration, numBatch)
	for _ = range insertDB(gen, db) {
		// fmt.Println(n)
	}
}

func method2 (iteration int, numBatch int, db *sql.DB) {
	gen := generateChannel(iteration, numBatch)
	output := insertDB(gen, db)

	for i:=0; i<iteration; i++ {
		<-output
	}
}

func method3 (iteration int, numBatch int, db *sql.DB) {
	gen := generateChannel(iteration, numBatch)

	var arr []<-chan string
	for i:=0; i<iteration; i++ {
		arr = append(arr, insertDB(gen, db))
	}

	out := goroutine_util.Merge(arr)
	for i:=0; i<iteration; i++ {
		<-out
	}
}

func main() {
	util.LoadENV()
	
	db, _ := pgsql.PGConnect()

	iteration := 10
	numBatch := 1000
	
	start := time.Now()
		// method1(iteration, numBatch, db)
		// method2(iteration, numBatch, db)
		method3(iteration, numBatch, db)
	end := time.Now()
	elapsed := end.Sub(start)

	fmt.Println("add time elapsed ==>", elapsed)
	util.PrintMemUsage()

	defer pgsql.PGClose(db)
}
