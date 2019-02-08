package main

import (
	"database/sql"
	_ "github.com/lib/pq"
	"log"
)

func main() {
	// connect
	connection_string := "postgres://denis:assword@localhost/go_test_db?sslmode=disable"
	db, err := sql.Open("postgres", connection_string)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// ping
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// drop table
	_, err = db.Exec("drop table artists")
	if err != nil {
		log.Fatal(err)
	}

	// create table
	_, err = db.Exec(`create table if not exists artists(
		id serial primary key,
		name varchar (50) unique not null
	)`)
	if err != nil {
		log.Fatal(err)
	}

	// insert
	name := "mark morgan"
	result, err := db.Exec("insert into artists (name) values ($1) returning id", name)
	if err != nil {
		log.Fatal(err)
	}

	rows_count, err := result.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("inserted %d rows", rows_count)

	// select
	rows, err := db.Query("select name from artists")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("artist: %q", name)
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

	// update
	tx, err := db.Begin()
	_, err = db.Exec("update artists SET name = $1 WHERE id = $2", "mm", 1)
	err = tx.Commit()

	// select one
	var artist_name string
	id := 1
	err = db.QueryRow("select name from artists where id = $1", id).Scan(&artist_name)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Println("table is empty")
		} else {
			log.Fatal(err)
		}
	}
	log.Println("selected artist:", artist_name)

	// delete
	id = 1
	result, err = db.Exec("delete from artists where id = $1", id)
	if err != nil {
		log.Fatal(err)
	}
	rows_count, err = result.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("deleted %d rows", rows_count)
}
