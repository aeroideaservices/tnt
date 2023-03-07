# tnt

[Tarantool](https://www.tarantool.io/en/) driver for
Go's [database/sql](https://golang.org/pkg/database/sql/) package 
based on [go-tarantool](https://github.com/tarantool/go-tarantool) library.

## Example of usage 

```go
package main

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/aeroideaservices/tnt"
)

func main() {
	db := sql.Open("tnt", "tarantool://user:password@localhost:3301")
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
```

## Принцип работы

В двух словах, все здесь нужно, что бы имплементировать [интерфейс](https://pkg.go.dev/database/sql/driver@go1.20.1#Driver)
драйвера из библиотеки [database/sql](https://golang.org/pkg/database/sql/). 

- Основная часть кода находится в файле `driver.go`
- Часть, отвечающая за транзакции в файле `transaction.go`
- Часть, отвечающая за сторки, которые мы получаем через SELECT `rows.go`
- Часть, отвечающая за "выражения", а в нашем случае также и за фактическое обращение к тарантулу `stmt.go`

Также в `stmt.go` находится часть, связанная с разобром аргуменов в SQL запросе и их касты для нестандартных типов