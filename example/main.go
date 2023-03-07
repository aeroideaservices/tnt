package main

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/aeroideaservices/tnt"
)

func main() {
	db, _ := sql.Open("tnt", "tarantool://user:password@localhost:3301")
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `CREATE TABLE modules (name STRING, size INTEGER, purpose STRING, PRIMARY KEY (name));`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `CREATE INDEX size ON modules (size);`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `CREATE UNIQUE INDEX purpose ON modules (purpose);`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.ExecContext(ctx, `
	INSERT INTO modules VALUES ('box', 1432, 'Database Management'),
	('clock', 188, 'Seconds'), ('crypto', 4, 'Cryptography');
	`)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, `SELECT size FROM modules WHERE name = ?;`, "clock")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var size int
		err = rows.Scan(&size)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("size: %d\n", size)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
