package time

import (
	"fmt"
	"time"

	_ "time/tzdata"

	"github.com/tarantool/go-tarantool/datetime"
)

const (
	RFC3339Nano = time.RFC3339Nano
)

var (
	location = "Europe/Moscow"
)

type Time struct {
	datetime.Datetime
}

func (t Time) String() string {
	return t.ToTime().Format(RFC3339Nano)
}

func (t *Time) Scan(src interface{}) error {
	switch src := src.(type) {
	case nil:
		return nil
	case string:
		if src == "" {
			return nil
		}
		tt, err := Parse(RFC3339Nano, src)
		if err != nil {
			return fmt.Errorf("Scan: %v", err)
		}
		*t = tt
	case datetime.Datetime:
		*t = Time{src}
	default:
		return fmt.Errorf("Scan: unable to scan type %T into tnt.Time", src)
	}

	return nil
}

func Parse(layout, value string) (t Time, err error) {
	tt, err := time.Parse(layout, value)
	if err != nil {
		return
	}
	dt, err := datetime.NewDatetime(tt)
	if err != nil {
		return
	}
	return Time{*dt}, nil
}

func Now() (t Time, err error) {
	l, err := time.LoadLocation(location)
	if err != nil {
		return
	}
	dt, err := datetime.NewDatetime(time.Now().In(l))
	if err != nil {
		return
	}
	return Time{*dt}, nil
}
