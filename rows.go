package tnt

import (
	"database/sql/driver"
	"errors"
	"io"
	"reflect"

	"github.com/aeroideaservices/tnt/time"
	"github.com/google/uuid"

	"github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/datetime"

	"github.com/tarantool/go-tarantool/decimal"
	_ "github.com/tarantool/go-tarantool/uuid"
)

// Получаемые кортежи, иплементация интерфейса https://pkg.go.dev/database/sql/driver@go1.20.1#Rows
type rows struct {
	data      []interface{} // данные, приходящие из тарантула через библиотеку go-tarantool
	cMetaData []tarantool.ColumnMetaData
	isClosed  bool
}

func (r *rows) Close() error {
	r.data = nil
	r.isClosed = true
	return nil
}

func (r *rows) Columns() []string {
	c := make([]string, len(r.cMetaData))
	for i, m := range r.cMetaData {
		c[i] = m.FieldName
	}
	return c
}

// проход по кортежам
func (r *rows) Next(dest []driver.Value) error {
	if r.isClosed {
		return errors.New("Next called after Close")
	}
	if len(r.data) > 0 {
		// забираем 1 кортеж из набора
		row, ok := r.data[0].([]interface{})
		if !ok {
			return errors.New("bad type assertion, want []interface{}")
		}
		r.data = r.data[1:]

		// сканим в destination
		for i := 0; i < len(row); i++ {
			switch v := reflect.ValueOf(row[i]); v.Kind() {
			// базовые типы
			case reflect.String:
				dest[i] = v.String()
			case reflect.Bool:
				dest[i] = v.Bool()
			case reflect.Float32, reflect.Float64:
				dest[i] = v.Float()
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				dest[i] = v.Int()
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				dest[i] = v.Uint()
			default:
				// сложные типы
				switch reflect.TypeOf(row[i]) {
				case reflect.TypeOf(uuid.New()):
					val, ok := row[i].(uuid.UUID)
					if !ok {
						return errors.New("wrong uuid type assertion")
					}
					// прикодим к строке, иначе не будет нормально работать
					// https://github.com/google/uuid/blob/master/sql.go
					// (тут нам повезло, что в google/uuid UUID имплементирует интерфейс сканера как раз для таких случаев)
					dest[i] = val.String()
				case reflect.TypeOf(datetime.Datetime{}):

					// todo
					val, ok := row[i].(datetime.Datetime)
					if !ok {
						return errors.New("wrong datetime type assertion")
					}
					dest[i] = val
				case reflect.TypeOf(time.Time{}):
					// кастомный тип-обртка для datetime тарантула, имплементирующий интерфейс сканера
					val, ok := row[i].(time.Time)
					if !ok {
						return errors.New("wrong tnt.Time type assertion")
					}
					dest[i] = val.ToTime().Format(time.RFC3339Nano)
				case reflect.TypeOf(decimal.Decimal{}):
					// тип для поддержки float тарантула
					val, ok := row[i].(decimal.Decimal)
					if !ok {
						return errors.New("wrong dacimal type assertion")
					}
					dest[i] = val
				}
			}

		}
	} else {
		return io.EOF
	}

	return nil
}
