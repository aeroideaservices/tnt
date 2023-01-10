package tnt

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"unicode"
	"unicode/utf8"

	"github.com/aeroideaservices/tnt/time"
	"github.com/google/uuid"
	"github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/datetime"
	_ "github.com/tarantool/go-tarantool/uuid"
	"golang.org/x/exp/slices"
)

type stmt struct {
	conn        *conn
	stream      *tarantool.Stream
	numArgs     int
	rawQuery    string // не модифицированный sql запрос
	query       string // sql запрос с кастами
	pa          sync.Once
	argsBuilded bool
	args        []arg
}

func NewStmt(conn *conn, rawQuery string, stream *tarantool.Stream) *stmt {
	return &stmt{
		conn:     conn,
		rawQuery: rawQuery,
		query:    rawQuery,
		stream:   stream,
	}
}

func (s *stmt) resetQuery() {
	s.query = s.rawQuery
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	s.parseArgs()
	return s.numArgs
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("use ExecContext instead")
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.resetQuery()

	err := s.buildArgs(args)
	if err != nil {
		return nil, fmt.Errorf("build args error: %w", err)
	}
	err = s.modifyQuery(args)
	if err != nil {
		return nil, fmt.Errorf("modify query error: %w", err)
	}
	tArgs, err := makeArgs(args)
	if err != nil {
		return nil, err
	}
	var r *tarantool.Response
	if s.stream != nil {
		req := tarantool.NewExecuteRequest(s.query).Args(tArgs)
		r, err = s.stream.Do(req).Get()
	} else {
		r, err = s.conn.tConn.Execute(s.query, tArgs)
	}
	if err != nil {
		return nil, err
	}
	if r.Error != "" {
		return nil, fmt.Errorf("tarantool error: %w", errors.New(r.Error))
	}
	return &result{rowsAffected: int64(r.SQLInfo.AffectedCount)}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("use QueryContext instead")
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.resetQuery()
	err := s.buildArgs(args)
	if err != nil {
		return nil, fmt.Errorf("build args error: %w", err)
	}
	err = s.modifyQuery(args)
	if err != nil {
		return nil, fmt.Errorf("modify query error: %w", err)
	}
	tArgs, err := makeArgs(args)
	if err != nil {
		return nil, err
	}
	var r *tarantool.Response
	if s.stream != nil {
		req := tarantool.NewExecuteRequest(s.query).Args(tArgs)
		r, err = s.stream.Do(req).Get()
	} else {
		r, err = s.conn.tConn.Execute(s.query, tArgs)
	}
	if err != nil {
		return nil, err
	}
	if r.Error != "" {
		return nil, fmt.Errorf("tarantool error: %w", errors.New(r.Error))
	}
	return &rows{
		data:      r.Data,
		cMetaData: r.MetaData,
	}, nil
}

func (s *stmt) CheckNamedValue(value *driver.NamedValue) error {
	return checkNamedValue(value)
}

const (
	TypeUnnamed = iota
	TypeNamed
)

type arg struct {
	pos      int
	_type    int
	name     string
	castable bool
	castType string
}

func (s *stmt) parseArgs() {
	s.pa.Do(func() {
		q := strings.Clone(s.rawQuery)
		s.args = make([]arg, 0)
		var isName bool
		var name strings.Builder
		for i := 0; len(q) > 0; i++ {
			r, size := utf8.DecodeRuneInString(q)
			if isName {
				if unicode.IsLetter(r) || unicode.IsDigit(r) {
					name.WriteRune(r)
					q = q[size:]
					continue
				}
				s.args[len(s.args)-1].name = name.String()
				name.Reset()
				isName = !isName
			}
			switch {
			case r == '?':
				s.args = append(s.args, arg{
					pos:   i,
					_type: TypeUnnamed,
				})
			case r == ':':
				isName = true
				s.args = append(s.args, arg{
					pos:   i,
					_type: TypeNamed,
				})
			}
			q = q[size:]
		}
		s.numArgs = len(s.args)
	})
}

func (s *stmt) buildArgs(sqlArgs []driver.NamedValue) error {
	if s.argsBuilded {
		return nil
	}
	args := slices.Clone(sqlArgs)
	if s.args == nil {
		s.parseArgs()
	}
	if len(s.args) != len(args) {
		return fmt.Errorf("not enough parameters for query want %d have %d", len(s.args), len(args))
	}
	slices.SortFunc(args, func(a, b driver.NamedValue) bool {
		return a.Ordinal < b.Ordinal
	})
	for i := 0; i < len(s.args); i++ {
		var idx int
		switch s.args[i]._type {
		case TypeNamed:
			idx = slices.IndexFunc(args, func(v driver.NamedValue) bool {
				return v.Name == s.args[i].name
			})
			if idx == -1 {
				return fmt.Errorf("no parameter with name %s", s.args[i].name)
			}
		case TypeUnnamed:
			idx = slices.IndexFunc(args, func(v driver.NamedValue) bool {
				return v.Name == ""
			})
			if idx == -1 {
				return fmt.Errorf("not enough unnamed parameters")
			}
		}
		switch v := args[idx].Value; reflect.TypeOf(v) {
		case reflect.TypeOf(uuid.UUID{}), reflect.TypeOf(&uuid.UUID{}):
			s.args[i].castable = true
			s.args[i].castType = "UUID"
		case reflect.TypeOf(time.Time{}), reflect.TypeOf(&time.Time{}):
			s.args[i].castable = true
			s.args[i].castType = "DATETIME"
		case reflect.TypeOf(datetime.Datetime{}), reflect.TypeOf(&datetime.Datetime{}):
			s.args[i].castable = true
			s.args[i].castType = "DATETIME"
		default:
			s.args[i].castable = false
		}
		args = append(args[:idx], args[idx+1:]...)
	}
	s.argsBuilded = true
	return nil
}

func (s *stmt) modifyQuery(args []driver.NamedValue) error {
	if !s.argsBuilded {
		err := s.buildArgs(args)
		if err != nil {
			return err
		}
	}
	for i := 0; i < s.NumInput(); i++ {
		if s.args[i].castable {
			var newQuery strings.Builder
			var newArgStr string
			newQuery.WriteString(s.query[:s.args[i].pos])
			switch s.args[i]._type {
			case TypeUnnamed:
				newArgStr = fmt.Sprintf("CAST(? AS %s)", s.args[i].castType)
			case TypeNamed:
				newArgStr = fmt.Sprintf("CAST(:%s AS %s)", s.args[i].name, s.args[i].castType)
			}
			fmt.Fprint(&newQuery, newArgStr)
			newQuery.WriteString(s.query[s.args[i].pos+1+len(s.args[i].name):])
			for j := i + 1; j < s.NumInput(); j++ {
				s.args[j].pos += len(newArgStr) - 1 - len(s.args[i].name)
			}
			s.query = newQuery.String()
		}
	}
	return nil
}

func makeArgs(args []driver.NamedValue) ([]interface{}, error) {
	tArgs := make([]interface{}, len(args))
	for _, v := range args {
		val := covertValueForCustomTypes(v.Value)
		var a interface{}
		if v.Name != "" {
			a = tarantool.KeyValueBind{Key: v.Name, Value: val}
		} else {
			a = val
		}
		tArgs[v.Ordinal-1] = a
	}
	return tArgs, nil
}

func covertValueForCustomTypes(value driver.Value) driver.Value {
	switch reflect.TypeOf(value) {
	case reflect.TypeOf(time.Time{}):
		t := value.(time.Time)
		v := interface{}(t.ToTime().Format(time.RFC3339Nano))
		return driver.Value(v)
	case reflect.TypeOf(datetime.Datetime{}):
		t := value.(datetime.Datetime)
		v := interface{}(t.ToTime().Format(time.RFC3339Nano))
		return driver.Value(v)
	}
	return value
}

type result struct {
	rowsAffected int64
	lastInsertId int64
}

func (r *result) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r *result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func checkNamedValue(value *driver.NamedValue) error {
	if value == nil {
		return nil
	}
	switch t := value.Value.(type) {
	default:
		// Default is to fail, unless it is one of the following supported types.
		return fmt.Errorf("unsupported value type: %v", t)
	case nil:
	case sql.NullInt64:
	case sql.NullTime:
	case sql.NullString:
	case sql.NullFloat64:
	case sql.NullBool:
	case sql.NullInt32:
	case string:
	case []string:
	case *string:
	case []*string:
	case []byte:
	case [][]byte:
	case int:
	case *int:
	case []int:
	case uint:
	case *uint:
	case []uint:
	case int64:
	case []int64:
	case *int64:
	case []*int64:
	case bool:
	case []bool:
	case *bool:
	case []*bool:
	case float64:
	case []float64:
	case *float64:
	case []*float64:
	case time.Time:
	case []time.Time:
	case *time.Time:
	case []*time.Time:
	case uuid.UUID:
	case *uuid.UUID:
	case datetime.Datetime:
	case *datetime.Datetime:
	}
	return nil
}
