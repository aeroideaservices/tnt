package tnt

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/tarantool/go-tarantool"
)

var _ driver.DriverContext = &Driver{}

// Инициализация драйвера
// (для sql.Open("tnt", dsn))
func init() {
	sql.Register("tnt", &Driver{connectors: make(map[string]*connector)})
}

// Имплементация интерфейса
// https://pkg.go.dev/database/sql/driver@go1.20.1#Driver
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

// Имплементация интерфейса https://pkg.go.dev/database/sql/driver@go1.20.1#Connector
type connector struct {
	driver          *Driver
	dsn             string
	connectorConfig connectorConfig

	tarantoolConnectionOpts tarantool.Opts
	initConnection          sync.Once
	conn                    *tarantool.Connection
	counter                 int32
	connErr                 error
}

type connectorConfig struct {
	connStr string
	user    string
	pass    string
}

// Парсим конфиг из dsn подобного этому tarantool://admin:password@localhost:3301
// часть "tarantool://" добавлена для совместимости с url.Parse
// что бы не парсить вручную и не заморачиваться с регулярками
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

// создаем новый коннектор, мютексы добавлены по примеру драйвера, на основе которого писался этот
// задел на будущее для репликаций, шардирования и т.п.
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
		User: connectorConfig.user,
		Pass: connectorConfig.pass,
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

// Открываем "внутреннее(database/sql)" соединение, также инициализируем tarantool соединение
//
// Атомарное добавление 1 нужно для многопоточности, что бы tarantool соединение закрывалось
// только если закрывается последнее соединение в последней горутине,
// эта штука позволила решить проблему, при при которой драйвер переставал работать
// при количестве горутин > 4
func openDriverConn(ctx context.Context, c *connector) (driver.Conn, error) {
	c.initConnection.Do(func() {
		c.conn, c.connErr = tarantool.Connect(c.connectorConfig.connStr, c.tarantoolConnectionOpts)
	})
	if c.connErr != nil {
		return nil, c.connErr
	}
	atomic.AddInt32(&c.counter, 1)
	return &conn{
		connector: c,
		tConn:     c.conn,
	}, nil
}

func (c *connector) Driver() driver.Driver {
	return c.driver
}

// Имплементация интерфейса https://pkg.go.dev/database/sql/driver@go1.20.1#Conn
// и других дополнительных, позволяющих выполнять запросы
type conn struct {
	connector *connector
	closed    bool
	tConn     *tarantool.Connection
	tx        *tx
	inTx      bool
}

// Использование prepare statement'ов

// todo: сейчас это просто на вид подготовленные выражения, по факту же
// все выполняется без подготовки и хотелось бы это дело перевести на настоящие
// prepared из go-tarantool
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &stmt{conn: c, rawQuery: query}, nil
}

// Закрытиве "внтуреннего" соединения
func (c *conn) Close() error {

	if count := atomic.AddInt32(&c.connector.counter, -1); count > 0 {
		return nil
	}

	c.connector.driver.mu.Lock()
	delete(c.connector.driver.connectors, c.connector.dsn)
	c.connector.driver.mu.Unlock()

	return c.tConn.Close()
}

// Начало транцакции
// По новому стандарту следует использовать контекстные версии, обычне сделаны для совмстимости
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.inTx {
		return nil, errors.New("already in transaction")
	}

	// открываем "поток" - тарантуловский аналог обычных транзакций
	// https://www.tarantool.io/en/doc/latest/concepts/atomic/txn_mode_mvcc/#streams-and-interactive-transactions
	// для работы необходимо на сервере установить переменную memtx_use_mvcc_engine=true
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

// Выполнение DML запроса, тут я принял решение сделать это все через stmt, в силу того, что это позволяет
// удобнее работать с транзакциями
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.inTx {
		return c.tx.ExecContext(ctx, query, args)
	}
	return NewStmt(c, query, nil).ExecContext(ctx, args)
}

// Выполнение запросов, возвращающих строки
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

// Проверка на допустимый тип аргументов (передающихся через ?)
func (c *conn) CheckNamedValue(value *driver.NamedValue) error {
	return checkNamedValue(value)
}
