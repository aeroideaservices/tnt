package tnt

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/tarantool/go-tarantool"
)

type tx struct {
	conn   *conn
	stream *tarantool.Stream
	closed bool
}

func (tx *tx) Commit() (err error) {
	if tx.conn == nil || tx.conn.closed {
		return driver.ErrBadConn
	}
	if tx.closed {
		return errors.New("transaction already closed")
	}

	r, err := tx.stream.Do(tarantool.NewCommitRequest()).Get()
	if err == nil && r.Error != "" {
		err = errors.New(r.Error)
	}

	tx.closed = true
	tx.conn.inTx = false
	tx.conn.tx = nil
	tx.conn = nil
	return
}

func (tx *tx) Rollback() (err error) {
	if tx.conn == nil || tx.conn.closed {
		return driver.ErrBadConn
	}
	if tx.closed {
		return errors.New("transaction already closed")
	}

	r, err := tx.stream.Do(tarantool.NewRollbackRequest()).Get()
	if err == nil && r.Error != "" {
		err = errors.New(r.Error)
	}

	tx.closed = true
	tx.conn.inTx = false
	tx.conn.tx = nil
	tx.conn = nil
	return
}

func (tx *tx) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return NewStmt(tx.conn, query, tx.stream).ExecContext(ctx, args)
}

func (tx *tx) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return NewStmt(tx.conn, query, tx.stream).QueryContext(ctx, args)
}
