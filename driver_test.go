package tnt

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/tarantool/go-tarantool"
)

func TestExtractDnsParts(t *testing.T) {
	tests := []struct {
		input               string
		wantConnectorConfig connectorConfig
		wantTarantoolOpts   tarantool.Opts
		wantErr             bool
	}{
		{
			input: "tarantool://golang:pass@192.168.1.71:3301",
			wantConnectorConfig: connectorConfig{
				connStr: "192.168.1.71:3301",
				user:    "golang",
				pass:    "pass",
			},
			wantTarantoolOpts: tarantool.Opts{
				User: "golang",
				Pass: "pass",
			},
		},
		{
			input:   "golang:pass@192.168.1.71:3301",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			config, err := extractConnectorConfig(tc.input)
			if err != nil {
				if tc.wantErr {
					return
				}
				t.Errorf("extract failed for %q: %v", tc.input, err)
			} else {
				if tc.wantErr {
					t.Error("did not encounter expected error")
				}
				if !cmp.Equal(config, tc.wantConnectorConfig, cmp.AllowUnexported(connectorConfig{})) {
					t.Errorf("connector config mismatch for %q\ngot: %v\nwant %v", tc.input, config, tc.wantConnectorConfig)
				}
				conn, err := newConnector(&Driver{connectors: make(map[string]*connector)}, tc.input)
				if err != nil {
					t.Errorf("failed to get connector for %q: %v", tc.input, err)
				}
				if !cmp.Equal(conn.tarantoolConnectionOpts, tc.wantTarantoolOpts, cmpopts.IgnoreUnexported(tarantool.Opts{})) {
					t.Errorf("connector Tarantools client opts mismatch for %q\n Got: %v\nWant: %v", tc.input, conn.tarantoolConnectionOpts, tc.wantTarantoolOpts)
				}
			}
		})
	}
}
