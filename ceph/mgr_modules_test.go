package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestMGRModulesReportMembershipAndNativeDependencies(t *testing.T) {
	ctr := newMGRModuleFixture()
	ctr.canRun["mirroring"] = false
	modules, err := readMGRModules(t.Context(), mgrModuleCluster(ctr).conn().ceph)
	if err != nil {
		t.Fatal(err)
	}
	rbd, ok := findMGRModule(modules, "rbd_support")
	if !ok || !rbd.Enabled || !rbd.AlwaysOn || !rbd.Available || !rbd.CanRun {
		t.Fatalf("always-on membership/dependencies lost: %+v", rbd)
	}
	mirror, ok := findMGRModule(modules, "mirroring")
	if !ok || mirror.Enabled || mirror.AlwaysOn || !mirror.Available || mirror.CanRun || mirror.ErrorString == "" {
		t.Fatalf("cannot-run metadata lost: %+v", mirror)
	}
	if modules[0].Name != "mirroring" {
		t.Fatal("module listing is not sorted")
	}
}

func TestMGRModulesRefuseMalformedAndInconsistentNativeState(t *testing.T) {
	for _, raw := range []string{"null", `{}`, `{"enabled_modules":[],"always_on_modules":[],"disabled_modules":[{"name":"bad name"}]}`, `{"enabled_modules":["mirroring"],"always_on_modules":["mirroring"],"disabled_modules":[]}`} {
		ctr := newMGRModuleFixture()
		ctr.rawListing = raw
		if _, err := readMGRModules(t.Context(), mgrModuleCluster(ctr).conn().ceph); err == nil {
			t.Fatal("invalid listing accepted", raw)
		}
	}
	for _, raw := range []string{"null", `{"available":true,"modules":[],"available_modules":[{"name":"mirroring"}]}`, `{"available":true,"modules":[],"available_modules":[{"name":"mirroring","can_run":true},{"name":"mirroring","can_run":true}]}`, `{"available":true,"modules":["other"],"available_modules":[]}`} {
		ctr := newMGRModuleFixture()
		ctr.rawDump = raw
		if _, err := readMGRModules(t.Context(), mgrModuleCluster(ctr).conn().ceph); err == nil {
			t.Fatal("invalid dump accepted", raw)
		}
	}
}

func TestWaitMGRModuleReadyProbesCommandsAndStopsOnClosedOrCanceled(t *testing.T) {
	ctr := newMGRModuleFixture()
	c := mgrModuleCluster(ctr)
	if err := c.WaitMGRModuleReady(t.Context(), "rbd_support"); err != nil || ctr.probes != 2 {
		t.Fatal("readiness skipped actual commands", err)
	}
	if err := c.WaitMGRModuleReady(t.Context(), "volumes"); err != nil || ctr.probes != 3 {
		t.Fatal("volumes readiness skipped command", err)
	}
	if err := c.WaitMGRModuleReady(t.Context(), "mirroring"); err == nil {
		t.Fatal("unprobed module claimed ready")
	}
	c.closed = true
	before := len(ctr.calls)
	if err := c.WaitMGRModuleReady(t.Context(), "rbd_support"); err == nil || len(ctr.calls) != before {
		t.Fatal("terminated cluster was polled")
	}
	c.closed = false
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := c.WaitMGRModuleReady(ctx, "rbd_support"); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled readiness succeeded", err)
	}
}

func mgrModuleCluster(ctr testcontainers.Container) *Container {
	return &Container{Container: ctr, settings: options{startupTimeout: time.Second}}
}

type mgrModuleFixture struct {
	testcontainers.Container
	enabled, always, forceDisabled map[string]bool
	canRun                         map[string]bool
	calls                          [][]string
	rawListing, rawDump            string
	probes                         int
}

func newMGRModuleFixture() *mgrModuleFixture {
	return &mgrModuleFixture{enabled: map[string]bool{"mirroring": false, "rbd_support": true, "volumes": true}, always: map[string]bool{"rbd_support": true, "volumes": true}, forceDisabled: map[string]bool{}, canRun: map[string]bool{"mirroring": true, "rbd_support": true, "volumes": true}}
}

// Exec answers the read-only commands of the module observation and of
// WaitMGRModuleReady. The module override tests live in package override.
func (ctr *mgrModuleFixture) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	args = slices.Clone(args[3:])
	ctr.calls = append(ctr.calls, args)
	var result any
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
		if ctr.rawDump != "" {
			result = json.RawMessage(ctr.rawDump)
		}
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
	return 0, &stream, nil
}
