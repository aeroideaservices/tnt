package tnt

import (
	"database/sql/driver"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParseArgs(t *testing.T) {
	tests := []struct {
		input       string
		wantNumArgs int
	}{
		{
			input:       `SELECT * FROM "test"`,
			wantNumArgs: 0,
		},
		{
			input:       `SELECT * FROM "test" WHERE "id"=?`,
			wantNumArgs: 1,
		},
		{
			input:       `SELECT * FROM "test" WHERE "id"=?, "name"=?, "age"=?`,
			wantNumArgs: 3,
		},
		{
			input:       `SELECT * FROM "test" WHERE "id"=:id`,
			wantNumArgs: 1,
		},
		{
			input:       `SELECT * FROM "test" WHERE "id"=:id, "name"=:name, "age"=:age`,
			wantNumArgs: 3,
		},
		{
			input:       `SELECT * FROM "test" WHERE "id"=:id, "name"=?, "age"=:age`,
			wantNumArgs: 3,
		},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			s := stmt{
				rawQuery: tc.input,
			}
			got := s.NumInput()
			if got != tc.wantNumArgs {
				t.Errorf("num input failed for query [%q]: got %v, want: %v", tc.input, got, tc.wantNumArgs)
			}
		})
	}
}

func TestBuildArgs(t *testing.T) {
	tests := []struct {
		input     string
		sqlArgs   []driver.NamedValue
		wantArgs  []arg
		wantError bool
	}{
		{
			input:     `SELECT * FROM "test"`,
			sqlArgs:   make([]driver.NamedValue, 0),
			wantArgs:  make([]arg, 0),
			wantError: false,
		},
		{
			input: `SELECT * FROM "test" WHERE "id"=?`,
			sqlArgs: []driver.NamedValue{
				{
					Name:    "",
					Ordinal: 1,
					Value:   3,
				},
			},
			wantArgs: []arg{
				{
					pos:      32,
					_type:    TypeUnnamed,
					name:     "",
					castable: false,
					castType: "",
				},
			},
			wantError: false,
		},
		{
			input: `SELECT * FROM "test" WHERE "id"=:id`,
			sqlArgs: []driver.NamedValue{
				{
					Name:    "id",
					Ordinal: 1,
					Value:   3,
				},
			},
			wantArgs: []arg{
				{
					pos:      32,
					_type:    TypeNamed,
					name:     "id",
					castable: false,
					castType: "",
				},
			},
			wantError: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			s := stmt{
				rawQuery: tc.input,
			}
			err := s.buildArgs(tc.sqlArgs)
			if err != nil {
				if tc.wantError {
					return
				}
				t.Error(err)
			} else {
				if tc.wantError {
					t.Error("did not encounter expected error")
				}
				if !cmp.Equal(s.args, tc.wantArgs, cmp.AllowUnexported(arg{})) {
					t.Errorf("args mismatch for %q\ngot: %v\nwant %v", tc.input, s.args, tc.wantArgs)
				}
			}
		})
	}
}
