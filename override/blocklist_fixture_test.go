package override

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"io"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const blocklistTestAddress = "192.0.2.7:0/1234"

// blocklistFixture is a fake control container that answers the blocklist
// commands of the Ceph CLI.
type blocklistFixture struct {
	testcontainers.Container
	entries                                             map[string]time.Time
	ranges                                              []map[string]string
	commands, additions, removals                       int
	loseAdd, loseRemove, failReadAfterAdd, failNextRead bool
	cancelAfterAdd                                      context.CancelFunc
}

func newBlocklistFixture(t *testing.T) (*ceph.Container, *blocklistFixture) {
	t.Helper()
	native := &blocklistFixture{entries: make(map[string]time.Time), ranges: []map[string]string{{"range": "198.51.100.0:0/24", "until": "2026-10-03T08:00:00.123456+0000"}}}
	return testhelper.TestCluster(native, testhelper.TestConfig{StartupTimeout: time.Second}), native
}

func (f *blocklistFixture) Exec(ctx context.Context, argv []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	f.commands++
	args := argv[3:]
	output, code := "", 0
	switch {
	case slices.Equal(args, []string{"osd", "blocklist", "ls", "--format", "json"}):
		if f.failNextRead {
			f.failNextRead = false
			return 0, nil, fmt.Errorf("lost readback")
		}
		entries := make([]map[string]string, 0, len(f.entries))
		for address, until := range f.entries {
			entries = append(entries, map[string]string{"addr": address, "until": until.Format("2006-01-02T15:04:05.999999-0700")})
		}
		a, _ := json.Marshal(entries)
		b, _ := json.Marshal(f.ranges)
		output = string(a) + string(b)
	case len(args) == 5 && slices.Equal(args[:3], []string{"osd", "blocklist", "add"}):
		seconds, _ := strconv.ParseFloat(args[4], 64)
		f.entries[args[3]] = time.Now().UTC().Add(time.Duration(seconds * float64(time.Second))).Truncate(time.Microsecond)
		f.additions++
		f.failNextRead = f.failReadAfterAdd
		if f.cancelAfterAdd != nil {
			f.cancelAfterAdd()
		}
		if f.loseAdd {
			code = 1
		}
	case len(args) == 4 && slices.Equal(args[:3], []string{"osd", "blocklist", "rm"}):
		delete(f.entries, args[3])
		f.removals++
		if f.loseRemove {
			code = 1
		}
	default:
		return 0, nil, fmt.Errorf("unexpected blocklist command %v", args)
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	return code, bytes.NewReader(append(header[:], []byte(output)...)), nil
}
