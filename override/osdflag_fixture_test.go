package override

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// osdFlagFixture is a fake control container that answers the OSD dump and
// the OSD flag commands of the Ceph CLI.
type osdFlagFixture struct {
	testcontainers.Container
	flags []string
	calls [][]string
	// raw replaces the output of osd dump while it is not empty.
	raw string
	// failAfter names the verb ("set" or "unset") whose native reply is lost
	// after the command took effect.
	failAfter string
	// breakDumpAfterSet makes osd dump undecodable once a flag was set.
	breakDumpAfterSet bool
}

func newOSDFlagFixture(t *testing.T, flags ...string) (*ceph.Container, *osdFlagFixture) {
	t.Helper()
	native := &osdFlagFixture{flags: flags}
	return servicehook.TestCluster(native, servicehook.TestConfig{StartupTimeout: time.Second}), native
}

// mutations counts the commands that changed a flag.
func (f *osdFlagFixture) mutations() int {
	count := 0
	for _, call := range f.calls {
		if len(call) == 3 && call[0] == "osd" && (call[1] == "set" || call[1] == "unset") {
			count++
		}
	}
	return count
}

func (f *osdFlagFixture) Exec(ctx context.Context, argv []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	args := slices.Clone(argv[3:])
	f.calls = append(f.calls, args)
	output, code := "", 0
	switch {
	case slices.Equal(args, []string{"osd", "dump", "--format", "json"}):
		raw, _ := json.Marshal(map[string]any{"flags": strings.Join(f.flags, ","), "osds": []any{}})
		output = string(raw)
		if f.raw != "" {
			output = f.raw
		}
	case len(args) == 3 && args[0] == "osd" && (args[1] == "set" || args[1] == "unset"):
		f.flags = slices.DeleteFunc(f.flags, func(flag string) bool { return flag == args[2] })
		if args[1] == "set" {
			f.flags = append(f.flags, args[2])
		}
		if f.breakDumpAfterSet && args[1] == "set" {
			f.raw = "not json"
		}
		if f.failAfter == args[1] {
			code, output = 1, "uncertain native reply"
		}
	default:
		return 0, nil, fmt.Errorf("unexpected command %v", args)
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(output)
	return code, &stream, nil
}
