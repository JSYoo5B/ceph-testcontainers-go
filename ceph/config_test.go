package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestConfigurationRejectsMalformedDuplicateDump(t *testing.T) {
	for _, raw := range []string{"null", `[{"section":"osd","name":"debug_osd","value":"2"},{"section":"osd","name":"debug_osd","value":"3"}]`, `[{"section":"osd","mask":{},"name":"debug_osd"}]`} {
		c := &Container{Container: &configDumpFixture{raw: raw}, settings: options{startupTimeout: time.Second}}
		if _, err := readConfiguration(t.Context(), c.conn().ceph); err == nil {
			t.Fatal("invalid configuration dump accepted")
		}
	}
}

// configDumpFixture answers every command with raw as the output of config
// dump. The override tests of the configuration database live in package
// override.
type configDumpFixture struct {
	testcontainers.Container
	raw string
}

func (ctr *configDumpFixture) Exec(_ context.Context, _ []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(len(ctr.raw)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(ctr.raw)
	return 0, &stream, nil
}
