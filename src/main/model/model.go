package model

import (
	"database/sql"
    _ "github.com/go-sql-driver/mysql" // import your used driver
)

type model interface {
	Create() (int64,error)
}

var db *sql.DB
var err error

func init() {
	db, err = sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/test?charset=utf8")
	if err != nil {
		panic(err)
	}
}