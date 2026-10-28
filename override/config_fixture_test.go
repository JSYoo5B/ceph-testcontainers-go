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
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// configCluster builds a cluster whose control container is ctr, with the
// bootstrap messenger mode mode.
func configCluster(ctr testcontainers.Container, mode ceph.MessengerMode) *ceph.Container {
	return testhelper.TestCluster(ctr, testhelper.TestConfig{StartupTimeout: time.Second, MessengerMode: mode})
}

// heldConfig returns the configuration handles that the cluster's registry
// holds. A nil value is a reservation that no handle settled.
func heldConfig(c *ceph.Container) map[string]any {
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	return testhelper.TestHeld(c, overridehook.Config)
}

// configFixture is a fake control container that answers config dump, set and
// rm like the monitors' configuration database.
type configFixture struct {
	testcontainers.Container
	entries                    []ceph.ConfigEntry
	calls                      [][]string
	normalized, failAfter, raw string
	dumps, failDumpAt          int
}

func (ctr *configFixture) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	args = slices.Clone(args[3:])
	ctr.calls = append(ctr.calls, args)
	output := ""
	code := 0
	if slices.Equal(args, []string{"config", "dump", "--format", "json"}) {
		ctr.dumps++
		if ctr.dumps == ctr.failDumpAt {
			return 1, strings.NewReader("PRIVATE-ERROR"), nil
		}
		data := ctr.entries
		if data == nil {
			data = []ceph.ConfigEntry{}
		}
		raw, _ := json.Marshal(data)
		output = string(raw)
		if ctr.raw != "" {
			output = ctr.raw
		}
	} else if len(args) >= 4 && args[0] == "config" && (args[1] == "set" || args[1] == "rm") {
		section, mask, _ := strings.Cut(args[2], "/")
		ctr.entries = slices.DeleteFunc(ctr.entries, func(entry ceph.ConfigEntry) bool {
			return entry.Section == section && entry.Mask == mask && entry.Name == args[3]
		})
		if args[1] == "set" {
			value := args[4]
			if ctr.normalized != "" {
				value = ctr.normalized
			}
			ctr.entries = append(ctr.entries, ceph.ConfigEntry{Section: section, Mask: mask, Name: args[3], Value: value})
		}
		if args[1] == ctr.failAfter {
			code = 1
			output = "PRIVATE-ERROR injected uncertain reply"
		}
	} else {
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
