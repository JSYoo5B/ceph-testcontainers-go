package ceph

import (
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	retainedClusterFSID = "6f0c66a8-3f2d-4f55-9b47-0a6d8e6a1c41"
	retainedKey         = "AQDretainedsyntheticosdkey00000000000000=="
	retainedStoreDir    = osdVolumeHelperPath + "/ceph-3"
)

// retainedControl adds the cluster FSID and the MON key of osd.3 to the
// lifecycle control.
type retainedControl struct {
	*osdLifecycleControl
	fsid, key string
	keyReads  int
}

func (control *retainedControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if len(args) >= 4 && args[0] == "ceph" {
		switch strings.Join(args[3:], " ") {
		case "fsid":
			return 0, noInitialOSDStream(control.fsid + "\n"), nil
		case "auth get-key osd.3":
			control.keyReads++
			return 0, noInitialOSDStream(control.key), nil
		}
	}
	return control.osdLifecycleControl.Exec(ctx, args, opts...)
}

// retainedHelper is the volume helper over a store of files. It records
// every command, so that a test can prove that nothing was written.
type retainedHelper struct {
	osdMemoryContainerFake
	files map[string]string
	links map[string]bool
	block string
}

func (h *retainedHelper) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	h.commands = append(h.commands, slices.Clone(args))
	exit := func(ok bool) (int, io.Reader, error) {
		if ok {
			return 0, noInitialOSDStream(""), nil
		}
		return 1, noInitialOSDStream(""), nil
	}
	switch {
	case len(args) == 3 && args[0] == "test" && args[1] == "-d":
		return exit(slices.ContainsFunc(slices.Collect(maps.Keys(h.files)), func(path string) bool { return strings.HasPrefix(path, args[2]+"/") }))
	case len(args) == 3 && args[0] == "test" && args[1] == "-f":
		_, ok := h.files[args[2]]
		return exit(ok)
	case len(args) == 4 && slices.Equal(args[:3], []string{"test", "!", "-L"}):
		return exit(!h.links[args[3]])
	case len(args) == 3 && args[0] == "cat" && args[1] == "--":
		content, ok := h.files[args[2]]
		if !ok {
			return 1, noInitialOSDStream(""), nil
		}
		return 0, noInitialOSDStream(content), nil
	case len(args) == 5 && args[0] == "stat" && args[4] == retainedStoreDir+"/block":
		return 0, noInitialOSDStream(h.block + "\n"), nil
	}
	return 0, nil, errors.New("unexpected retained-store command " + strings.Join(args, " "))
}

// retainedCandidate is a reopened OSD container: its start brings osd.3 up.
type retainedCandidate struct {
	osdLifecycleDaemon
}

func (c *retainedCandidate) GetContainerID() string { return "osd-reopened-3" }

type retainedFixture struct {
	cluster *Container
	control *retainedControl
	daemon  *osdLifecycleDaemon
	helper  *retainedHelper
	runs    int
	runErr  error
	request testcontainers.GenericContainerRequest
}

func newRetainedFixture(t *testing.T) *retainedFixture {
	t.Helper()
	cluster, lifecycle, daemon := newOSDLifecycleFixture()
	control := &retainedControl{osdLifecycleControl: lifecycle, fsid: retainedClusterFSID, key: retainedKey}
	cluster.Container = control
	cluster.settings.startupTimeout = 5 * time.Second
	cluster.settings.osdVolumeStorage = true
	storage, _, _ := osdVolumeFixture()
	helper := &retainedHelper{osdMemoryContainerFake: osdMemoryContainerFake{running: true, startedAt: "synthetic-start"}, links: map[string]bool{},
		block: "4242:1073741824", files: map[string]string{
			retainedStoreDir + "/ready": "", retainedStoreDir + "/whoami": "3\n", retainedStoreDir + "/fsid": osdLifecycleUUID + "\n",
			retainedStoreDir + "/ceph_fsid": retainedClusterFSID + "\n", retainedStoreDir + "/keyring": "[osd.3]\n\tkey = " + retainedKey + "\n",
			retainedStoreDir + "/block": "",
		}}
	storage.helper, storage.helperStarted = helper, helper.startedAt
	cluster.osdVolume = storage
	f := &retainedFixture{cluster: cluster, control: control, daemon: daemon, helper: helper}
	cluster.osdRunner = func(_ context.Context, _ string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		f.runs++
		f.request = testcontainers.GenericContainerRequest{}
		for _, opt := range opts {
			if err := opt.Customize(&f.request); err != nil {
				return nil, err
			}
		}
		state := lifecycle.native[3]
		state.Up = f.runErr == nil
		lifecycle.native[3] = state
		return &retainedCandidate{osdLifecycleDaemon{control: lifecycle}}, f.runErr
	}
	return f
}

// helperWrites lists every helper command that is not a read.
func (f *retainedFixture) helperWrites() [][]string {
	var writes [][]string
	for _, args := range f.helper.commands {
		if args[0] != "test" && args[0] != "cat" && args[0] != "stat" {
			writes = append(writes, args)
		}
	}
	return writes
}

func TestDetachedOSDReopensInANewContainerWithTheSameIdentity(t *testing.T) {
	f := newRetainedFixture(t)
	old := f.cluster.osds[3]
	detached, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if detached.ID() != 3 || detached.UUID() != osdLifecycleUUID || detached.ClusterFSID() != retainedClusterFSID || detached.Placement() != old.placement {
		t.Fatalf("detached identity %+v", detached)
	}
	if f.cluster.osds[3] != nil || f.cluster.detachedOSDs[3] != detached || f.daemon.stops != 1 || f.daemon.terminations != 1 || !f.cluster.nativeFenced["osd-lifecycle-3"] {
		t.Fatalf("detach state osds=%v detached=%v stops=%d terminations=%d", f.cluster.osds, f.cluster.detachedOSDs, f.daemon.stops, f.daemon.terminations)
	}
	// Detach keeps the registration: no reweight, out, drain or purge.
	if len(f.control.actions) != 0 || f.control.native[3].UUID != osdLifecycleUUID || f.control.native[3].Up {
		t.Fatalf("detach mutated the registration: %v %+v", f.control.actions, f.control.native[3])
	}
	if names := osdIDs(f.cluster.osdsLocked()); !slices.Equal(names, []int{4}) {
		t.Fatalf("OSDs after detach %v", names)
	}
	reopened, err := detached.Reopen(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if reopened == old || reopened.ID != 3 || reopened.nativeUUID != osdLifecycleUUID || reopened.GetContainerID() != "osd-reopened-3" || old.GetContainerID() != "osd-lifecycle-3" {
		t.Fatalf("reopened handle %+v old %+v", reopened, old)
	}
	if f.cluster.osds[3] != reopened || f.cluster.detachedOSDs[3] != nil || f.runs != 1 || f.control.keyReads != 1 || len(f.helperWrites()) != 0 {
		t.Fatalf("reopen state runs=%d keys=%d writes=%v", f.runs, f.control.keyReads, f.helperWrites())
	}
	env := f.request.Env
	if env["CEPH_OSD_ID"] != "3" || env["CEPH_OSD_UUID"] != osdLifecycleUUID || !slices.ContainsFunc(f.request.Files, func(file testcontainers.ContainerFile) bool {
		return file.ContainerFilePath == "/etc/ceph/osd.keyring"
	}) || len(f.request.Mounts) != 1 || f.request.Mounts[0].Target != "/var/lib/ceph/osd" {
		t.Fatalf("reopen request env=%v mounts=%v", env, f.request.Mounts)
	}
	if _, err := detached.Reopen(t.Context()); err == nil || !strings.Contains(err.Error(), "already reopened") || f.runs != 1 {
		t.Fatalf("second reopen: %v runs=%d", err, f.runs)
	}
}

func osdIDs(osds []*OSDContainer) []int {
	var ids []int
	for _, osd := range osds {
		ids = append(ids, osd.ID)
	}
	return ids
}

// Every reopen precondition is checked before any write, and a refusal
// leaves the OSD detached for another attempt.
func TestReopenRefusesChangedStoreOrRegistrationBeforeAnyWrite(t *testing.T) {
	for _, scenario := range []string{"keyring", "osd uuid", "whoami", "cluster fsid", "block", "symlink", "ready", "registration missing", "registration foreign", "registration up", "mon key", "cluster fsid served", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			f := newRetainedFixture(t)
			detached, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			files, native := f.helper.files, f.control.native
			saved := maps.Clone(files)
			savedState, savedKey, savedFSID, savedBlock := native[3], f.control.key, f.control.fsid, f.helper.block
			ctx := t.Context()
			switch scenario {
			case "keyring":
				files[retainedStoreDir+"/keyring"] = "[osd.3]\n\tkey = AQDother==\n"
			case "osd uuid":
				files[retainedStoreDir+"/fsid"] = osdLifecycleForeignUUID
			case "whoami":
				files[retainedStoreDir+"/whoami"] = "9"
			case "cluster fsid":
				files[retainedStoreDir+"/ceph_fsid"] = osdLifecycleForeignUUID
			case "block":
				f.helper.block = "4243:1073741824"
			case "symlink":
				f.helper.links[retainedStoreDir+"/block"] = true
			case "ready":
				delete(files, retainedStoreDir+"/ready")
			case "registration missing":
				delete(native, 3)
			case "registration foreign":
				state := native[3]
				state.UUID = osdLifecycleForeignUUID
				native[3] = state
			case "registration up":
				state := native[3]
				state.Up = true
				native[3] = state
			case "mon key":
				f.control.key = "AQDrotated=="
			case "cluster fsid served":
				f.control.fsid = osdLifecycleForeignUUID
			case "canceled":
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			if osd, err := detached.Reopen(ctx); err == nil || osd != nil {
				t.Fatalf("reopen admitted a changed %s: %v", scenario, osd)
			}
			if f.runs != 0 || f.cluster.osds[3] != nil || f.cluster.detachedOSDs[3] != detached || detached.reopened || len(f.helperWrites()) != 0 {
				t.Fatalf("refusal changed state: runs=%d writes=%v", f.runs, f.helperWrites())
			}
			f.helper.files, f.helper.links, f.helper.block = saved, map[string]bool{}, savedBlock
			f.control.native[3], f.control.key, f.control.fsid = savedState, savedKey, savedFSID
			if _, err := detached.Reopen(t.Context()); err != nil {
				t.Fatal("reopen after the refusal was resolved:", err)
			}
		})
	}
}

func TestDetachRefusesBeforeStoppingTheOSD(t *testing.T) {
	for _, scenario := range []string{"without volume storage", "unowned", "foreign registration", "network delay", "container pause", "removal pending", "no container"} {
		t.Run(scenario, func(t *testing.T) {
			f := newRetainedFixture(t)
			id := 3
			switch scenario {
			case "without volume storage":
				f.cluster.settings.osdVolumeStorage = false
			case "unowned":
				id = 7
			case "foreign registration":
				state := f.control.native[3]
				state.UUID = osdLifecycleForeignUUID
				f.control.native[3] = state
			case "network delay":
				f.cluster.faults.entries = map[faultID]*faultEntry{{kind: faultNetworkDelay, key: "public/osd-lifecycle-3"}: {handle: struct{}{}}}
			case "container pause":
				f.cluster.faults.entries = map[faultID]*faultEntry{{kind: faultContainerPause, key: "osd-lifecycle-3"}: {handle: struct{}{}}}
			case "removal pending":
				f.cluster.osds[3].purgeIssued = true
			case "no container":
				f.cluster.osds[3].Container = nil
			}
			detached, err := osdHandle(t, t.Context(), f.cluster, id).Detach(t.Context())
			if err == nil || detached != nil {
				t.Fatal("detach admitted", scenario)
			}
			if scenario == "without volume storage" && !errors.Is(err, ErrInvalidRequest) {
				t.Fatal("unsupported storage is not an invalid request:", err)
			}
			if f.daemon.stops != 0 || f.daemon.terminations != 0 || f.cluster.nativeFenced["osd-lifecycle-3"] || f.cluster.osds[3] == nil || f.cluster.osds[3].detaching {
				t.Fatalf("refusal stopped, fenced or released osd.3: %v", err)
			}
		})
	}
	// A completed delay lease no longer targets the container.
	f := newRetainedFixture(t)
	f.cluster.faults.entries = map[faultID]*faultEntry{{kind: faultNetworkDelay, key: "public/osd-lifecycle-3"}: {handle: struct{}{},
		complete: func(context.Context) (bool, error) { return true, nil }}}
	if _, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context()); err != nil {
		t.Fatal("completed delay refused detach:", err)
	}
}

// A detach that fails after the fence keeps the OSD owned and fenced;
// RemoveOSD refuses it, and DetachOSD resumes where it stopped.
func TestPartialDetachStaysOwnedAndResumes(t *testing.T) {
	t.Run("removal failed", func(t *testing.T) {
		f := newRetainedFixture(t)
		f.daemon.terminateErr = errors.New("synthetic removal failure")
		if _, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context()); err == nil {
			t.Fatal("failed removal released the store")
		}
		osd := f.cluster.osds[3]
		if osd == nil || !osd.detaching || osd.detachRemoved || f.cluster.detachedOSDs[3] != nil || !f.cluster.nativeFenced["osd-lifecycle-3"] {
			t.Fatalf("partial detach state %+v", osd)
		}
		if err := osdHandle(t, t.Context(), f.cluster, 3).Remove(t.Context()); err == nil || !strings.Contains(err.Error(), "being detached") || len(f.control.actions) != 0 {
			t.Fatal("RemoveOSD purged a detaching OSD:", err)
		}
		f.daemon.terminateErr = nil
		if _, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context()); err != nil {
			t.Fatal(err)
		}
		if f.daemon.terminations != 2 {
			t.Fatalf("terminations %d", f.daemon.terminations)
		}
	})
	t.Run("store unreadable after removal", func(t *testing.T) {
		f := newRetainedFixture(t)
		f.helper.files[retainedStoreDir+"/whoami"] = "9"
		if _, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context()); err == nil || !strings.Contains(err.Error(), "holds osd.9") {
			t.Fatal("foreign store detached:", err)
		}
		if osd := f.cluster.osds[3]; osd == nil || !osd.detachRemoved || f.cluster.detachedOSDs[3] != nil {
			t.Fatal("removed container's OSD was not kept for retry")
		}
		f.helper.files[retainedStoreDir+"/whoami"] = "3"
		if _, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context()); err != nil {
			t.Fatal(err)
		}
		if f.daemon.stops != 1 || f.daemon.terminations != 1 {
			t.Fatalf("resumed detach stopped or removed again: %d %d", f.daemon.stops, f.daemon.terminations)
		}
	})
}

func TestDetachedOSDHandleBelongsToOneCluster(t *testing.T) {
	f := newRetainedFixture(t)
	detached, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context()); err == nil || !strings.Contains(err.Error(), "already detached") {
		t.Fatal("second detach:", err)
	}
	if err := osdHandle(t, t.Context(), f.cluster, 3).Remove(t.Context()); !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "reopen it") {
		t.Fatal("RemoveOSD of a detached OSD:", err)
	}
	// A detached OSD acts on the cluster that detached it, never on another
	// cluster, even one that is asked to adopt its store.
	other := newRetainedFixture(t)
	foreign := *detached
	foreign.cluster = other.cluster
	if _, err := foreign.Reopen(t.Context()); !errors.Is(err, ErrInvalidRequest) || other.runs != 0 || f.runs != 0 {
		t.Fatal("another cluster reopened the store:", err)
	}
	if _, err := (*DetachedOSD)(nil).Reopen(t.Context()); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal("nil detached OSD:", err)
	}
	if _, err := (&DetachedOSD{cluster: f.cluster, id: 3}).Reopen(t.Context()); err == nil || f.runs != 0 {
		t.Fatal("a forged detached OSD was reopened:", err)
	}
}

// Once the new container is requested the detached OSD is consumed: a
// failed start returns the new handle, which the cluster owns.
func TestFailedReopenKeepsTheCandidateOwned(t *testing.T) {
	f := newRetainedFixture(t)
	detached, err := osdHandle(t, t.Context(), f.cluster, 3).Detach(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	f.runErr = errors.New("synthetic readiness failure")
	osd, err := detached.Reopen(t.Context())
	if err == nil || osd == nil || osd.GetContainerID() != "osd-reopened-3" || f.cluster.osds[3] != osd || !detached.reopened || f.cluster.detachedOSDs[3] != nil {
		t.Fatalf("failed reopen: %v %+v", err, osd)
	}
	if _, err := detached.Reopen(t.Context()); err == nil || f.runs != 1 {
		t.Fatal("a consumed detached OSD was replayed:", err)
	}
}
