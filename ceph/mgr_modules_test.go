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
	modules, err := mgrModuleCluster(ctr).MGRModules(t.Context())
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

func TestTemporaryMGRModuleRestoresSharedCopiesAndOriginalMembership(t *testing.T) {
	for _, prior := range []bool{false, true} {
		ctr := newMGRModuleFixture()
		ctr.enabled["mirroring"] = prior
		c := mgrModuleCluster(ctr)
		change, err := c.TemporaryMGRModule(t.Context(), "mirroring", !prior)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.TemporaryMGRModule(t.Context(), "mirroring", prior); err == nil {
			t.Fatal("overlap accepted")
		}
		copy := *change
		if err := copy.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		before := len(ctr.calls)
		if err := change.Restore(t.Context()); err != nil || len(ctr.calls) != before {
			t.Fatal("shared restoration is not idempotent")
		}
		if ctr.enabled["mirroring"] != prior || len(c.moduleOverrides) != 0 || !ctr.enabled["rbd_support"] {
			t.Fatal("original or unrelated module state lost")
		}
	}
}

func TestTemporaryMGRModuleRejectsMissingCannotRunAndAlwaysOn(t *testing.T) {
	for _, name := range []string{"--force", "bad name", "mirroring;exit", "unknown", "missing", "broken"} {
		ctr := newMGRModuleFixture()
		ctr.enabled["missing"], ctr.enabled["broken"] = false, false
		ctr.canRun["broken"] = false
		c := mgrModuleCluster(ctr)
		if change, err := c.TemporaryMGRModule(t.Context(), name, true); err == nil || change != nil || len(c.moduleOverrides) != 0 || ctr.mutations != 0 {
			t.Fatal("invalid/unavailable module reached mutation", name)
		}
	}
	ctr := newMGRModuleFixture()
	c := mgrModuleCluster(ctr)
	if _, err := c.TemporaryMGRModule(t.Context(), "rbd_support", false); err == nil || ctr.mutations != 0 {
		t.Fatal("always-on module disabled")
	}
	change, err := c.TemporaryMGRModule(t.Context(), "rbd_support", true)
	if err != nil || ctr.mutations != 0 {
		t.Fatal("always-on no-op rejected", err)
	}
	ctr.forceDisabled["rbd_support"] = true
	ctr.enabled["rbd_support"] = false
	if err := change.Restore(t.Context()); err == nil {
		t.Fatal("outside force-disable overwritten")
	}
	delete(ctr.forceDisabled, "rbd_support")
	ctr.enabled["rbd_support"] = true
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctr.forceDisabled["rbd_support"] = true
	ctr.enabled["rbd_support"] = false
	if _, err := c.TemporaryMGRModule(t.Context(), "rbd_support", true); err == nil {
		t.Fatal("unrestorable always-on enable accepted")
	}
}

func TestTemporaryMGRModuleProtectsFilesystemsAndNativeMirroringOnRestore(t *testing.T) {
	ctr := newMGRModuleFixture()
	c := mgrModuleCluster(ctr)
	change, err := c.TemporaryMGRModule(t.Context(), "mirroring", true)
	if err != nil {
		t.Fatal(err)
	}
	ctr.mirrored = true
	before := ctr.mutations
	if err := change.Restore(t.Context()); err == nil || ctr.mutations != before || !ctr.enabled["mirroring"] || len(c.moduleOverrides) != 1 {
		t.Fatal("new mirror policy did not prevent module restoration")
	}
	ctr.mirrored = false
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A non-always-on volumes variant still protects owned and native filesystems.
	delete(ctr.always, "volumes")
	c.filesystems = map[string]*CephFSContainer{"owned": {}}
	if _, err := c.TemporaryMGRModule(t.Context(), "volumes", false); err == nil {
		t.Fatal("owned filesystem's volumes module disabled")
	}
	c.filesystems = nil
	ctr.filesystems = 1
	if _, err := c.TemporaryMGRModule(t.Context(), "volumes", false); err == nil {
		t.Fatal("native filesystem's volumes module disabled")
	}
}

func TestTemporaryMGRModuleNoopRefusesOutsideMembershipChange(t *testing.T) {
	ctr := newMGRModuleFixture()
	c := mgrModuleCluster(ctr)
	change, err := c.TemporaryMGRModule(t.Context(), "mirroring", false)
	if err != nil {
		t.Fatal(err)
	}
	ctr.enabled["mirroring"] = true
	if err := change.Restore(t.Context()); err == nil || ctr.mutations != 0 || !ctr.enabled["mirroring"] {
		t.Fatal("outside edit was overwritten by no-op restore")
	}
}

func TestTemporaryMGRModuleLostRepliesAndReadbackReconcile(t *testing.T) {
	ctr := newMGRModuleFixture()
	ctr.failAfter = "enable"
	c := mgrModuleCluster(ctr)
	change, err := c.TemporaryMGRModule(t.Context(), "mirroring", true)
	if err == nil || change == nil || !ctr.enabled["mirroring"] || len(c.moduleOverrides) != 1 {
		t.Fatal("uncertain apply lost handle")
	}
	ctr.failAfter = "disable"
	if err := change.Restore(t.Context()); err == nil || ctr.enabled["mirroring"] {
		t.Fatal("uncertain restoration reply hidden")
	}
	ctr.failAfter = ""
	before := ctr.mutations
	if err := change.Restore(t.Context()); err != nil || ctr.mutations != before || len(c.moduleOverrides) != 0 {
		t.Fatal("native completed restore wasn't reconciled")
	}
	ctr.failDumpAt = ctr.dumps + 2
	change, err = c.TemporaryMGRModule(t.Context(), "mirroring", true)
	if err != nil || change == nil {
		t.Fatal("transient apply readback did not converge", err)
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctr.failDumpFrom = ctr.dumps + 2
	change, err = c.TemporaryMGRModule(t.Context(), "mirroring", true)
	if err == nil || change == nil || len(c.moduleOverrides) != 1 {
		t.Fatal("lost persistent apply readback lost handle")
	}
	ctr.failDumpFrom = 0
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestTemporaryMGRModuleRestorationRequiresRunnableMetadata(t *testing.T) {
	ctr := newMGRModuleFixture()
	ctr.enabled["mirroring"] = true
	c := mgrModuleCluster(ctr)
	change, err := c.TemporaryMGRModule(t.Context(), "mirroring", false)
	if err != nil {
		t.Fatal(err)
	}
	ctr.canRun["mirroring"] = false
	before := ctr.mutations
	if err := change.Restore(t.Context()); err == nil || ctr.mutations != before {
		t.Fatal("cannot-run restoration forced enable")
	}
	ctr.canRun["mirroring"] = true
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestMGRModulesRefuseMalformedAndInconsistentNativeState(t *testing.T) {
	for _, raw := range []string{"null", `{}`, `{"enabled_modules":[],"always_on_modules":[],"disabled_modules":[{"name":"bad name"}]}`, `{"enabled_modules":["mirroring"],"always_on_modules":["mirroring"],"disabled_modules":[]}`} {
		ctr := newMGRModuleFixture()
		ctr.rawListing = raw
		if _, err := mgrModuleCluster(ctr).MGRModules(t.Context()); err == nil {
			t.Fatal("invalid listing accepted", raw)
		}
	}
	for _, raw := range []string{"null", `{"available":true,"modules":[],"available_modules":[{"name":"mirroring"}]}`, `{"available":true,"modules":[],"available_modules":[{"name":"mirroring","can_run":true},{"name":"mirroring","can_run":true}]}`, `{"available":true,"modules":["other"],"available_modules":[]}`} {
		ctr := newMGRModuleFixture()
		ctr.rawDump = raw
		if _, err := mgrModuleCluster(ctr).MGRModules(t.Context()); err == nil {
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

func TestTemporaryMGRModuleCancellationKeepsAppliedHandleWithFreshReadback(t *testing.T) {
	ctr := newMGRModuleFixture()
	ctx, cancel := context.WithCancel(t.Context())
	ctr.cancelOnSet = cancel
	c := mgrModuleCluster(ctr)
	change, err := c.TemporaryMGRModule(ctx, "mirroring", true)
	if !errors.Is(err, context.Canceled) || change == nil || !ctr.enabled["mirroring"] {
		t.Fatal("canceled apply lost native state/handle", err)
	}
	if ctr.canceledReads != 0 {
		t.Fatal("apply readback reused canceled context")
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func mgrModuleCluster(ctr testcontainers.Container) *Container {
	return &Container{Container: ctr, settings: options{startupTimeout: time.Second}}
}

type mgrModuleFixture struct {
	testcontainers.Container
	enabled, always, forceDisabled                                                 map[string]bool
	canRun                                                                         map[string]bool
	calls                                                                          [][]string
	failAfter, rawListing, rawDump                                                 string
	dumps, failDumpAt, failDumpFrom, mutations, probes, filesystems, canceledReads int
	mirrored                                                                       bool
	cancelOnSet                                                                    context.CancelFunc
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
		if ctr.rawDump != "" {
			result = json.RawMessage(ctr.rawDump)
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
