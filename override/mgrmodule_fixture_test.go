package override

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/overridehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// mgrModuleCluster is a cluster whose control container is the fake native.
func mgrModuleCluster(ctr testcontainers.Container) *ceph.Container {
	return servicehook.TestCluster(ctr, servicehook.TestConfig{StartupTimeout: time.Second})
}

// setStartupTimeout changes the bound of one fixture operation.
func setStartupTimeout(c *ceph.Container, d time.Duration) {
	servicehook.TestSetStartupTimeout(c, d)
}

// heldMGR returns the MGR module handles that the cluster's registry holds.
func heldMGR(c *ceph.Container) map[string]any {
	servicehook.Lock(c)
	defer servicehook.Unlock(c)
	return overridehook.TestHeld(c, overridehook.MGRModule)
}

type mgrModuleFixture struct {
	testcontainers.Container
	enabled, always, forceDisabled                                                 map[string]bool
	canRun                                                                         map[string]bool
	calls                                                                          [][]string
	dumpResponses                                                                  []string
	failAfter, rawListing, rawDump                                                 string
	dumps, failDumpAt, failDumpFrom, mutations, probes, filesystems, canceledReads int
	mirrored                                                                       bool
	cancelOnSet                                                                    context.CancelFunc
	cancelOnDump                                                                   context.CancelFunc
}

func newMGRModuleFixture() *mgrModuleFixture {
	return &mgrModuleFixture{enabled: map[string]bool{"mirroring": false, "rbd_support": true, "volumes": true}, always: map[string]bool{"rbd_support": true, "volumes": true}, forceDisabled: map[string]bool{}, canRun: map[string]bool{"mirroring": true, "rbd_support": true, "volumes": true}}
}

func (ctr *mgrModuleFixture) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		ctr.canceledReads++
		return 0, nil, err
	}
	args = slices.Clone(args[3:])
	ctr.calls = append(ctr.calls, args)
	var result any
	code := 0
	if slices.Equal(args, []string{"mgr", "module", "ls", "--format", "json"}) {
		enabled, always, force := []string{}, []string{}, []string{}
		disabled := []map[string]any{}
		for name, on := range ctr.enabled {
			if ctr.always[name] {
				always = append(always, name)
			} else if on {
				enabled = append(enabled, name)
			} else {
				disabled = append(disabled, map[string]any{"name": name})
			}
			if ctr.forceDisabled[name] {
				force = append(force, name)
			}
		}
		result = map[string]any{"enabled_modules": enabled, "always_on_modules": always, "force_disabled_modules": force, "disabled_modules": disabled}
		if ctr.rawListing != "" {
			result = json.RawMessage(ctr.rawListing)
		}
	} else if slices.Equal(args, []string{"mgr", "dump", "--format", "json"}) {
		ctr.dumps++
		if ctr.dumps == ctr.failDumpAt || (ctr.failDumpFrom > 0 && ctr.dumps >= ctr.failDumpFrom) {
			return 0, nil, errors.New("injected module metadata readback error")
		}
		enabled := []string{}
		metadata := []map[string]any{}
		for name, on := range ctr.enabled {
			if on && !ctr.always[name] {
				enabled = append(enabled, name)
			}
		}
		for name, runnable := range ctr.canRun {
			diagnostic := ""
			if !runnable {
				diagnostic = "missing native dependency"
			}
			metadata = append(metadata, map[string]any{"name": name, "can_run": runnable, "error_string": diagnostic})
		}
		result = map[string]any{"available": true, "modules": enabled, "available_modules": metadata}
		if len(ctr.dumpResponses) != 0 {
			result = json.RawMessage(ctr.dumpResponses[0])
			ctr.dumpResponses = ctr.dumpResponses[1:]
		} else if ctr.rawDump != "" {
			result = json.RawMessage(ctr.rawDump)
		}
		if ctr.cancelOnDump != nil {
			cancel := ctr.cancelOnDump
			ctr.cancelOnDump = nil
			cancel()
		}
	} else if len(args) == 4 && slices.Equal(args[:2], []string{"mgr", "module"}) && slices.Contains([]string{"enable", "disable"}, args[2]) {
		ctr.mutations++
		ctr.enabled[args[3]] = args[2] == "enable"
		if ctr.cancelOnSet != nil {
			ctr.cancelOnSet()
			ctr.cancelOnSet = nil
			return 0, nil, context.Canceled
		}
		if args[2] == ctr.failAfter {
			code = 1
		}
		result = map[string]any{}
	} else if slices.Equal(args, []string{"fs", "dump", "--format", "json"}) {
		filesystems := []map[string]any{}
		for i := 0; i < ctr.filesystems; i++ {
			filesystems = append(filesystems, map[string]any{"id": i + 1})
		}
		if ctr.mirrored {
			filesystems = append(filesystems, map[string]any{"mirror_info": map[string]any{"peers": map[string]any{}}})
		}
		result = map[string]any{"filesystems": filesystems}
	} else if slices.Equal(args, []string{"rbd", "task", "list", "--format", "json"}) || slices.Equal(args, []string{"fs", "volume", "ls", "--format", "json"}) {
		ctr.probes++
		result = []any{}
	} else if slices.Equal(args, []string{"rbd", "mirror", "snapshot", "schedule", "list", "--format", "json"}) {
		ctr.probes++
		result = map[string]any{}
	} else {
		return 0, nil, fmt.Errorf("unexpected MGR fixture command: %v", args)
	}
	output, err := json.Marshal(result)
	if err != nil {
		return 0, nil, err
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(len(output)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.Write(output)
	return code, &stream, nil
}
