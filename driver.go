package tnt

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"sync"

	"github.com/tarantool/go-tarantool"
)

var _ driver.DriverContext = &Driver{}

func init() {
	sql.Register("tnt", &Driver{connectors: make(map[string]*connector)})
}

type Driver struct {
	mu         sync.Mutex
	connectors map[string]*connector
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	c, err := newConnector(d, name)
	if err != nil {
		return nil, err
	}
	return openDriverConn(context.Background(), c)
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return newConnector(d, name)
}

type connector struct {
	driver          *Driver
	dsn             string
	connectorConfig connectorConfig

	tarantoolConnectionOpts tarantool.Opts
	initConnection          sync.Once
	conn                    *tarantool.Connection
	connErr                 error
}

type connectorConfig struct {
	connStr string
	user    string
	pass    string
}

func extractConnectorConfig(dsn string) (connectorConfig, error) {
	c := connectorConfig{}
	u, err := url.Parse(dsn)
	if err != nil {
		return c, err
	}
	if u.Host == "" {
		return c, errors.New("connection host is empty")
	}
	c.connStr = u.Host
	if u.User != nil {
		c.user = u.User.Username()
		c.pass, _ = u.User.Password()
	} else {
		c.user = "guest"
	}
	return c, nil
}

func newConnector(d *Driver, dsn string) (*connector, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, ok := d.connectors[dsn]; ok {
		return c, nil
	}

	connectorConfig, err := extractConnectorConfig(dsn)
	if err != nil {
		return nil, err
	}
	connOpts := tarantool.Opts{
		User:        connectorConfig.user,
		Pass:        connectorConfig.pass,
		Concurrency: 1 << 8,
	}
	c := &connector{
		driver:                  d,
		dsn:                     dsn,
		connectorConfig:         connectorConfig,
		tarantoolConnectionOpts: connOpts,
	}
	d.connectors[dsn] = c
	return c, nil
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return openDriverConn(ctx, c)
}

func openDriverConn(ctx context.Context, c *connector) (driver.Conn, error) {
	c.initConnection.Do(func() {
		c.conn, c.connErr = tarantool.Connect(c.connectorConfig.connStr, c.tarantoolConnectionOpts)
	})
	if c.connErr != nil {
		return nil, c.connErr
	}
	return &conn{
		connector: c,
		tConn:     c.conn,
	}, nil
}

func (c *connector) Driver() driver.Driver {
	return c.driver
}

type conn struct {
	connector *connector
	closed    bool
	tConn     *tarantool.Connection
	tx        *tx
	inTx      bool
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &stmt{conn: c, rawQuery: query}, nil
}

func (c *conn) Close() error {
	c.connector.driver.mu.Lock()
	delete(c.connector.driver.connectors, c.connector.dsn)
	c.connector.driver.mu.Unlock()

	return c.tConn.Close()
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.inTx {
		return nil, errors.New("already in transaction")
	}

	stream, err := c.tConn.NewStream()
	if err != nil {
		return nil, fmt.Errorf("can't create stream: %w", err)
	}

	_, err = stream.Do(tarantool.NewBeginRequest().TxnIsolation(tarantool.BestEffortLevel)).Get()
	if err != nil {
		return nil, err
	}
	c.inTx = true
	c.tx = &tx{conn: c, stream: stream}
	return c.tx, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.inTx {
		return c.tx.ExecContext(ctx, query, args)
	}
	return NewStmt(c, query, nil).ExecContext(ctx, args)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.inTx {
		return c.tx.QueryContext(ctx, query, args)
	}
	return NewStmt(c, query, nil).QueryContext(ctx, args)
}

func (c *conn) Ping(ctx context.Context) error {
	if c.closed {
		return driver.ErrBadConn
	}
	_, err := c.tConn.Ping()
	return err
}

func (c *conn) CheckNamedValue(value *driver.NamedValue) error {
	return checkNamedValue(value)
}
