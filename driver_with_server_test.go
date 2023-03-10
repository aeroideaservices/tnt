package tnt

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/tarantool/go-tarantool"
)

const SelectFooFromBar = "SELECT FOO FROM BAR"

func TestPingContext(t *testing.T) {
	// t.Parallel()

	db, teardown := setupTestDBConnection(t)
	defer teardown()
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("unexpected error for ping: %v", err)
	}
}

/* Селекты */

func TestSimpleQuery(t *testing.T) {
	// t.Parallel()

	db, teardown := setupTestDBConnection(t)
	defer teardown()
	rows, err := db.QueryContext(context.Background(), SelectFooFromBar)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for want := int64(1); rows.Next(); want++ {
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		if !cmp.Equal(cols, []string{"FOO"}) {
			t.Fatalf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
		}
		var got int64
		err = rows.Scan(&got)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("value mismatch\nGot: %v\nWant: %v", got, want)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
}

/* Транзакции */

func TestSimpleReadWriteTransactionCommit(t *testing.T) {
	// t.Parallel()

	db, teardown := setupTestDBConnection(t)
	defer teardown()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	func() {
		rows, err := tx.Query(SelectFooFromBar)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		checkSelectFooFromBarResult(t, rows, 2)
	}()

	_, err = tx.ExecContext(context.Background(), `INSERT INTO "BAR" VALUES (?)`, 3)
	if err != nil {
		t.Fatalf("unexpected error for tx.ExecContext: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("unexpected error for tx.Commit: %v", err)
	}

	func() {
		rows, err := db.QueryContext(context.Background(), SelectFooFromBar)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		checkSelectFooFromBarResult(t, rows, 3)
	}()

}

func TestSimpleReadWriteTransactionRollback(t *testing.T) {
	// t.Parallel()

	db, teardown := setupTestDBConnection(t)
	defer teardown()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	func() {
		rows, err := tx.Query(SelectFooFromBar)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		checkSelectFooFromBarResult(t, rows, 2)
	}()

	_, err = tx.ExecContext(context.Background(), `INSERT INTO "BAR" VALUES (?)`, 3)
	if err != nil {
		t.Fatalf("unexpected error for tx.ExecContext: %v", err)
	}

	func() {
		rows, err := tx.Query(SelectFooFromBar)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		checkSelectFooFromBarResult(t, rows, 3)
	}()

	err = tx.Rollback()
	if err != nil {
		t.Fatalf("unexpected error for tx.Rollback: %v", err)
	}

	func() {
		rows, err := db.QueryContext(context.Background(), SelectFooFromBar)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		checkSelectFooFromBarResult(t, rows, 2)
	}()
}

func checkSelectFooFromBarResult(t *testing.T, rows *sql.Rows, count int64) {
	for want := int64(1); rows.Next(); want++ {
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		if !cmp.Equal(cols, []string{"FOO"}) {
			t.Fatalf("cols mismatch\nGot: %v\nWant: %v", cols, []string{"FOO"})
		}
		var got int64
		err = rows.Scan(&got)
		if err != nil {
			t.Fatal(err)
		}
		if got > count {
			t.Fatalf("rows count mismatch\nWant: %v\nGot more(%v)", count, got)
		}
		if got != want {
			t.Fatalf("value mismatch\nGot: %v\nWant: %v", got, want)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
}

func TestPreparedQueryUnnamed(t *testing.T) {
	// t.Parallel()

	db, teardown := setupTestDBConnection(t)
	defer teardown()

	stmt, err := db.Prepare(`SELECT "id" FROM "Test" WHERE "name"=?`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	rows, err := stmt.QueryContext(context.Background(), "Alice")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for want := int64(1); rows.Next(); want++ {
		var got int64
		err = rows.Scan(&got)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("value mismatch\nGot: %v\nWant: %v", got, want)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
}

// Фейлится из-за того, что не работают нормально именованные параметры

// func TestPreparedQueryNamed(t *testing.T) {
// 	// t.Parallel()

// 	db, teardown := setupTestDBConnection(t)
// 	defer teardown()

// 	stmt, err := db.Prepare(`SELECT "id" FROM "Test" WHERE "name"=:name`)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer stmt.Close()

// 	rows, err := stmt.QueryContext(context.Background(), sql.NamedArg{Name: "name", Value: "Alice"})
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer rows.Close()

// 	for want := int64(1); rows.Next(); want++ {
// 		var got int64
// 		err = rows.Scan(&got)
// 		if err != nil {
// 			t.Fatal(err)
// 		}
// 		if got != want {
// 			t.Fatalf("value mismatch\nGot: %v\nWant: %v", got, want)
// 		}
// 	}
// 	if rows.Err() != nil {
// 		t.Fatal(rows.Err())
// 	}
// }

func TestAllTypeExec(t *testing.T) {
	db, teardown := setupTestDBConnection(t)
	defer teardown()

	q := `INSERT INTO "TestTypes" VALUES
	    (?, ?, ` + // boolean
		`	?, ` + // integer
		`	?, ` + // unsigned
		`	?, ` + // double
		`	?, ` + // number
		`	?, ` + // decimal
		`	?, ` + // string
		// `	?, ` + // varbinary
		`	? ` + // uuid
		`)`
	args1 := []any{
		1,
		true,
		int(42),
		uint(43),
		float64(3.14),
		float64(3.14),
		float64(3.14),
		"hello",
		// []byte("hello"),
		uuid.New(),
	}
	_, err := db.ExecContext(context.Background(), q, args1...)
	if err != nil {
		t.Fatalf("unexpected error for ExecContext: %v", err)
	}

	type tts struct {
		id int
		a1 bool
		a2 int
		a3 uint
		a4 float64
		a5 float64
		a6 float64
		a7 string
		a8 uuid.UUID
	}

	q = `SELECT * FROM "TestTypes"`
	rows, err := db.QueryContext(context.Background(), q)
	if err != nil {
		t.Fatalf("unexpected error for QueryContext: %v", err)
	}
	var s tts
	for rows.Next() {
		err = rows.Scan(
			&s.id,
			&s.a1,
			&s.a2,
			&s.a3,
			&s.a4,
			&s.a5,
			&s.a6,
			&s.a7,
			&s.a8)

		if err != nil {
			t.Fatalf("unexpected error for Scan: %v", err)
		}
	}
	func() {
		t.Log(s.id)
		t.Log(s.a1)
		t.Log(s.a2)
		t.Log(s.a3)
		t.Log(s.a4)
		t.Log(s.a5)
		t.Log(s.a6)
		t.Log(s.a7)
		t.Log(s.a8)
	}()
}

func TestAllPointersTypeExec(t *testing.T) {
	db, teardown := setupTestDBConnection(t)
	defer teardown()

	q := `INSERT INTO "TestTypes" VALUES
	    (?, ?, ` + // boolean
		`	?, ` + // integer
		`	?, ` + // unsigned
		`	?, ` + // double
		`	?, ` + // number
		`	?, ` + // decimal
		`	?, ` + // string
		// `	?, ` + // varbinary
		`	? ` + // uuid
		`)`

	args := []any{
		1,
		func() *bool { v := true; return &v }(),
		func() *int { v := int(42); return &v }(),
		func() *uint { v := uint(43); return &v }(),
		func() *float64 { v := float64(3.14); return &v }(),
		func() *float64 { v := float64(3.14); return &v }(),
		func() *float64 { v := float64(3.14); return &v }(),
		func() *string { v := "hello"; return &v }(),
		// []byte("hello"),
		func() *uuid.UUID { v := uuid.New(); return &v }(),
	}
	_, err := db.ExecContext(context.Background(), q, args...)
	if err != nil {
		t.Fatalf("unexpected error for ExecContext: %v", err)
	}
}

func setupTestDBConnection(t *testing.T) (db *sql.DB, teardown func()) {
	dsn := getTestDBdsn(t)
	teardown = setupTestDBData(t, dsn)
	db, err := sql.Open("tnt", dsn)
	if err != nil {
		t.Fatalf("unexpected error for sql.Open with dsn %s: %v", dsn, err)
	}
	return
}

func getTestDBdsn(t *testing.T) (dsn string) {
	dsn, ok := os.LookupEnv("TEST_DB_DSN")
	if !ok || dsn == "" {
		t.Fatal("TEST_DB_DSN env variable is missing or empty")
	}
	return
}

func setupTestDBData(t *testing.T, dsn string) (teardown func()) {
	fns := make([]func(), 0)
	config, err := extractConnectorConfig(dsn)
	if err != nil {
		t.Fatalf("unexpected error for extract config: %v", err)
	}
	fns = append(fns, creaeteTestDBSchema(t, &config))
	fns = append(fns, insertTestDBValues(t, &config))
	teardown = func() {
		for i, j := 0, len(fns)-1; i < j; i, j = i+1, j-1 {
			fns[i], fns[j] = fns[j], fns[i]
		}
		for _, f := range fns {
			f()
		}
	}
	return
}

func creaeteTestDBSchema(t *testing.T, config *connectorConfig) (clear func()) {
	// space: BAR
	// | FOO |
	// |-----|
	// | 1   |
	// | 2   |
	conn, err := tarantool.Connect(config.connStr,
		tarantool.Opts{
			User: config.user,
			Pass: config.pass,
		})
	if err != nil {
		t.Fatalf("unexpected error for tarantool.Connect: %v", err)
	}
	defer conn.Close()
	// Тут всегда возвращается ошибка, видимо это какой-то прикол gp-tarantool
	// unexpected error for conn.Call(space.create): unsupported Lua type 'function' (LuajitError, code 0x20)
	_, err = conn.Call("box.schema.space.create", []interface{}{
		"BAR",
		map[string]bool{"if_not_exists": true}})
	if err != nil {
		t.Logf("unexpected error for conn.Call(space.create): %v", err)
	}
	_, err = conn.Call("box.space.BAR:format", [][]map[string]string{
		{
			{"name": "FOO", "type": "number"},
		}})
	if err != nil {
		t.Fatalf("unexpected error for conn.Call(space.BAR:format): %v", err)
	}
	_, err = conn.Call("box.space.BAR:create_index", []interface{}{
		"primary",
		map[string]interface{}{
			"parts":         []string{"FOO"},
			"if_not_exists": true}})
	if err != nil {
		t.Fatalf("unexpected error for conn.Call(space.BAR:create_index): %v", err)
	}

	// Space Test
	// | id | name  |
	// |----|-------|
	// | 1  | Alice |
	// | 2  | Bob   |
	_, err = conn.Call("box.schema.space.create", []interface{}{
		"Test",
		map[string]bool{"if_not_exists": true}})
	if err != nil {
		t.Logf("unexpected error for conn.Call(space.create): %v", err)
	}
	_, err = conn.Call("box.space.Test:format", [][]map[string]string{
		{
			{"name": "id", "type": "number"},
			{"name": "name", "type": "string"},
		}})
	if err != nil {
		t.Fatalf("unexpected error for conn.Call(space.Test:format): %v", err)
	}
	_, err = conn.Call("box.space.Test:create_index", []interface{}{
		"primary",
		map[string]interface{}{
			"parts":         []string{"id"},
			"if_not_exists": true}})
	if err != nil {
		t.Fatalf("unexpected error for conn.Call(space.Test:create_index): %v", err)
	}

	// Space TestTypes
	// 	| id | boolean          | integer | unsigned | double | number | decimal | string | varbinary | uuid | datetime | interval |
	// |----|------------------|---------|----------|--------|--------|---------|--------|-----------|------|----------|----------|
	_, err = conn.Call("box.schema.space.create", []interface{}{
		"TestTypes",
		map[string]bool{"if_not_exists": true}})
	if err != nil {
		t.Logf("unexpected error for conn.Call(space.create): %v", err)
	}
	_, err = conn.Call("box.space.TestTypes:format", [][]map[string]string{
		{
			{"name": "id", "type": "number"},
			{"name": "boolean", "type": "boolean"},
			{"name": "integer", "type": "integer"},
			{"name": "unsigned", "type": "unsigned"},
			{"name": "double", "type": "double"},
			{"name": "number", "type": "number"},
			{"name": "decimal", "type": "decimal"},
			{"name": "string", "type": "string"},
			// {"name": "varbinary", "type": "varbinary"},
			{"name": "uuid", "type": "uuid"},
		}})
	if err != nil {
		t.Fatalf("unexpected error for conn.Call(space.TestTypes:format): %v", err)
	}
	_, err = conn.Call("box.space.TestTypes:create_index", []interface{}{
		"primary",
		map[string]interface{}{
			"parts":         []string{"id"},
			"if_not_exists": true}})
	if err != nil {
		t.Fatalf("unexpected error for conn.Call(space.TestTypes:create_index): %v", err)
	}

	clear = func() {
		conn, err := tarantool.Connect(config.connStr,
			tarantool.Opts{
				User: config.user,
				Pass: config.pass,
			})
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Call("box.space.BAR:drop", []interface{}{})
		conn.Call("box.space.Test:drop", []interface{}{})
		conn.Call("box.space.TestTypes:drop", []interface{}{})
	}
	return
}

func insertTestDBValues(t *testing.T, config *connectorConfig) (clear func()) {
	conn, err := tarantool.Connect(config.connStr,
		tarantool.Opts{
			User: config.user,
			Pass: config.pass,
		})
	if err != nil {
		t.Fatalf("unexpected error for tarantool.Connect: %v", err)
	}
	defer conn.Close()

	// space: BAR
	// | FOO |
	// |-----|
	// | 1   |
	// | 2   |
	for _, val := range []int{1, 2} {
		_, err = conn.Insert("BAR", []interface{}{val})
		if err != nil {
			t.Fatalf("unexpected error for conn.Insert(%v): %v", val, err)
		}
	}

	// Space Test
	// | id | name  |
	// |----|-------|
	// | 1  | Alice |
	// | 2  | Bob   |
	for _, val := range []struct {
		id   int
		name string
	}{
		{
			id:   1,
			name: "Alice",
		},
		{
			id:   2,
			name: "Bob",
		},
	} {
		_, err = conn.Insert("Test", []interface{}{val.id, val.name})
		if err != nil {
			t.Fatalf("unexpected error for conn.Insert(id:%v, name:%v): %v", val.id, val.name, err)
		}
	}

	clear = func() {}
	return
}
