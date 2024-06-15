package main

import (
	"database/sql"

	_ "github.com/snowflakedb/gosnowflake"

	"log"
)

func main() {
	db, err := sql.Open("snowflake", "test:test@snowflake.localhost.localstack.cloud:4566/test?account=test")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err1 := db.Query("SELECT 123, 'test', false")
    if err1 != nil {
		log.Fatal("err1", err1)
	}
    defer rows.Close()

    err2 := rows.Next()
    if err2 != true {
		log.Fatal("err2", err2)
	}

    var v1 int
    var v2 string
    var v3 bool
    rows.Scan(&v1, &v2, &v3)
    println(v1)
    println(v2)
    println(v3)
}
