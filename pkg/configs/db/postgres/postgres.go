package postgres

import (
  "database/sql"
	"fmt"
	"os"

  _ "github.com/lib/pq"
)

var (
	host     = ""
  port     = 5432
  user     = ""
  password = ""
  dbname   = ""
)

func initProps() {
	host     = os.Getenv("DBHOST")
  port     = 5432
  user     = os.Getenv("DBUSERNAME")
  password = os.Getenv("DBPASSWORD")
  dbname   = os.Getenv("DBNAME")
}

func PGConnect() (*sql.DB, error) {
	initProps()

  psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
    "password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
		return nil, err
	}

  err = db.Ping()
  if err != nil {
    panic(err)
		return nil, err
  }
	
	return db, nil
}

func PGClose(db *sql.DB) {
	db.Close()
}