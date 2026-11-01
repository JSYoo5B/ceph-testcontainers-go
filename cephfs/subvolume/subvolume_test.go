package subvolume

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// poolFixtureContainer is the canned CLI fake the cephfs tests share.
type poolFixtureContainer = clustertest.CannedCommandContainer

// subvolumeFixture returns the shared synthetic filesystem "fixture" with its
// volumes module, group and subvolume output.
func subvolumeFixture() (*fstest.Filesystem, *poolFixtureContainer) {
	fs := fstest.NewFilesystem()
	return fs, fs.Control
}

func assertAdmissionGate(t *testing.T, fs *fstest.Filesystem, gate string, operation func(context.Context) error) {
	t.Helper()
	fstest.AssertAdmissionGate(t, fs.Core.Cluster(), fs.SetupMu, gate, operation)
}

func assertAdmissionReleased(t *testing.T, fs *fstest.Filesystem) {
	t.Helper()
	fstest.AssertAdmissionReleased(t, fs.Core.Cluster(), fs.SetupMu)
}

func clientOwner(client *ceph.ClientConfig) *ceph.Container {
	owner, _, _, _ := testhelper.TestClientState(client)
	return owner
}

func clientCreated(client *ceph.ClientConfig) bool {
	_, created, _, _ := testhelper.TestClientState(client)
	return created
}

func clientReady(client *ceph.ClientConfig) bool {
	_, _, ready, _ := testhelper.TestClientState(client)
	return ready
}

func clientRevoked(client *ceph.ClientConfig) bool { return servicehook.ClientRevoked(client) }

func TestCephFSSubvolumeValidationPreventsCommands(t *testing.T) {
	for _, config := range []Config{
		{}, {Name: "_nogroup"}, {Name: ".."}, {Name: "--option"}, {Name: "bad/path"},
		{Name: "name", GroupName: "_nogroup"}, {Name: "name", SizeBytes: -1},
		{Name: "name", DataPool: "--option"}, {Name: strings.Repeat("a", 129)},
	} {
		fs, ctr := subvolumeFixture()
		if subvolume, err := create(t.Context(), fs.Core, config); err == nil || subvolume != nil || len(ctr.Calls) != 0 {
			t.Fatalf("invalid config executed commands: config=%+v handle=%+v error=%v calls=%v", config, subvolume, err, ctr.Calls)
		}
	}
	fs, ctr := subvolumeFixture()
	if _, err := createGroup(t.Context(), fs.Core, GroupConfig{Name: "group", SizeBytes: -1}); err == nil || len(ctr.Calls) != 0 {
		t.Fatal("negative group quota reached native CLI")
	}
	testhelper.TestSetClosed(fs.Core.Cluster(), true)
	if _, err := groupNames(t.Context(), fs.Core); err == nil || len(ctr.Calls) != 0 {
		t.Fatal("terminated cluster accepted subvolume operation")
	}
}

// The validation reads no filesystem configuration: a data pool attached after
// creation, which no configured name lists, passes it, and the native
// membership check after the setup gate decides. It runs no command.
func TestCephFSSubvolumeValidationReadsNoConfiguredPoolList(t *testing.T) {
	fs, ctr := subvolumeFixture()
	if err := validateConfig(fs.Core, "volume", "group", "extra", 1<<20); err != nil {
		t.Fatal("dynamic pool rejected by a static whitelist", err)
	}
	if err := validateConfig(nil, "volume", "group", "", 0); err == nil {
		t.Fatal("an unavailable filesystem passed validation")
	}
	if len(ctr.Calls) != 0 {
		t.Fatal("validation executed native commands")
	}
}

func TestCephFSSubvolumeDuplicatePreflightPreservesResources(t *testing.T) {
	for _, group := range []bool{false, true} {
		fs, ctr := subvolumeFixture()
		if group {
			ctr.Output["fs subvolumegroup ls fixture --format json"] = `[{"name":"group"}]`
			if handle, err := createGroup(t.Context(), fs.Core, GroupConfig{Name: "group", SizeBytes: 1}); err == nil || handle != nil {
				t.Fatal("existing group accepted for native idempotent modification")
			}
		} else {
			ctr.Output["fs subvolume ls fixture --group_name group --format json"] = `[{"name":"volume"}]`
			if handle, err := create(t.Context(), fs.Core, Config{Name: "volume", GroupName: "group", SizeBytes: 1}); err == nil || handle != nil {
				t.Fatal("existing subvolume accepted for native idempotent modification")
			}
		}
		for _, call := range ctr.Calls {
			if slices.Contains(call, "create") || slices.Contains(call, "resize") || slices.Contains(call, "rm") {
				t.Fatalf("duplicate preflight mutated existing resource: %v", call)
			}
		}
	}
}

func TestCephFSSubvolumeCreationOwnershipAndPrivateIdentity(t *testing.T) {
	fs, ctr := subvolumeFixture()
	group, err := createGroup(t.Context(), fs.Core, GroupConfig{Name: "group", SizeBytes: 65536, DataPool: "additional"})
	if err != nil || group.Path != "/volumes/group" || !group.identity.Ready {
		t.Fatalf("group creation failed: %+v error=%v", group, err)
	}
	subvolume, err := create(t.Context(), fs.Core, Config{Name: "volume", GroupName: "group", SizeBytes: 32768, DataPool: "additional", NamespaceIsolated: true})
	if err != nil || !subvolume.identity.Ready || subvolume.Path != "/volumes/group/volume/unique-id" {
		t.Fatalf("subvolume creation failed: %+v error=%v", subvolume, err)
	}
	want := []string{"fs", "subvolume", "create", "fixture", "volume", "--group_name", "group", "--size", "32768", "--pool_layout", "additional", "--namespace-isolated"}
	if !slices.ContainsFunc(ctr.Calls, func(call []string) bool { return slices.Equal(call, want) }) {
		t.Fatalf("native creation did not preserve requested options: %v", ctr.Calls)
	}
	subvolume.Name, subvolume.GroupName, subvolume.Path, subvolume.FilesystemName = "foreign", "foreign", "/foreign", "foreign"
	if err := subvolume.Resize(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	want = []string{"fs", "subvolume", "resize", "fixture", "volume", "inf", "--group_name", "group", "--no_shrink"}
	if !slices.Equal(ctr.Calls[len(ctr.Calls)-1], want) {
		t.Fatalf("public descriptor edit redirected mutation: %v", ctr.Calls)
	}
	before := len(ctr.Calls)
	if err := subvolume.Resize(t.Context(), -1); err == nil || len(ctr.Calls) != before {
		t.Fatal("negative resize reached Ceph")
	}
}

func TestCephFSSubvolumeReplacedResourcesRefuseMutation(t *testing.T) {
	for _, changed := range []string{"path", "birth-time", "filesystem-id"} {
		fs, ctr := subvolumeFixture()
		subvolume, err := create(t.Context(), fs.Core, Config{Name: "volume", GroupName: "group", SizeBytes: 32768})
		if err != nil {
			t.Fatal(err)
		}
		switch changed {
		case "path":
			ctr.Output["fs subvolume info fixture volume --group_name group --format json"] = strings.ReplaceAll(ctr.Output["fs subvolume info fixture volume --group_name group --format json"], "unique-id", "replacement-id")
		case "birth-time":
			ctr.Output["fs subvolume info fixture volume --group_name group --format json"] = strings.ReplaceAll(ctr.Output["fs subvolume info fixture volume --group_name group --format json"], "00:00:01.123456", "00:00:02.123456")
		case "filesystem-id":
			ctr.Output["fs dump --format json"] = strings.ReplaceAll(ctr.Output["fs dump --format json"], `"id":41`, `"id":42`)
		}
		ctr.Calls = nil
		if err := subvolume.Remove(t.Context()); err == nil {
			t.Fatalf("changed %s accepted for removal", changed)
		}
		for _, call := range ctr.Calls {
			if slices.Contains(call, "rm") {
				t.Fatal("replacement was removed")
			}
		}
	}
	fs, ctr := subvolumeFixture()
	group, err := createGroup(t.Context(), fs.Core, GroupConfig{Name: "group", SizeBytes: 65536})
	if err != nil {
		t.Fatal(err)
	}
	ctr.Output["fs subvolumegroup info fixture group --format json"] = strings.ReplaceAll(ctr.Output["fs subvolumegroup info fixture group --format json"], "00:00:00.123456", "00:00:09.123456")
	if err := group.Resize(t.Context(), 1); err == nil {
		t.Fatal("replacement group accepted for resize")
	}
}

func TestCephFSSubvolumePartialCreationAndSafeRetryableRemoval(t *testing.T) {
	fs, ctr := subvolumeFixture()
	ctr.Fail = "fs subvolume create fixture volume --group_name group --size 32768"
	subvolume, err := create(t.Context(), fs.Core, Config{Name: "volume", GroupName: "group", SizeBytes: 32768})
	if err == nil || subvolume == nil || subvolume.identity.Ready {
		t.Fatal("failed native create lost partial handle or marked it ready")
	}
	before := len(ctr.Calls)
	if err := subvolume.Remove(t.Context()); err == nil || len(ctr.Calls) != before {
		t.Fatal("unconfirmed resource was mutated")
	}
	ctr.Fail = ""
	subvolume, err = create(t.Context(), fs.Core, Config{Name: "volume", GroupName: "group", SizeBytes: 32768})
	if err != nil {
		t.Fatal(err)
	}
	ctr.Fail = "fs subvolume rm fixture volume --group_name group"
	if err := subvolume.Remove(t.Context()); err == nil || subvolume.identity.Removed {
		t.Fatal("failed native removal lost retry handle")
	}
	ctr.Fail = ""
	ctr.Output["fs subvolume ls fixture --group_name group --format json"] = `[{"name":"volume"}]`
	if err := subvolume.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	before = len(ctr.Calls)
	if err := subvolume.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, call := range ctr.Calls[before:] {
		if slices.Contains(call, "rm") {
			t.Fatal("confirmed removal executed twice")
		}
	}
	for _, call := range ctr.Calls {
		if slices.Contains(call, "--force") || slices.Contains(call, "--retain-snapshots") {
			t.Fatal("safe removal widened native semantics")
		}
	}
}

func TestCephFSSubvolumeCopiedHandlesShareRemovalAndUnknownSuccessConverges(t *testing.T) {
	fs, ctr := subvolumeFixture()
	subvolume, err := create(t.Context(), fs.Core, Config{Name: "volume", GroupName: "group", SizeBytes: 32768})
	if err != nil {
		t.Fatal(err)
	}
	copyBefore := *subvolume
	ctr.Fail = "fs subvolume rm fixture volume --group_name group"
	if err := subvolume.Remove(t.Context()); err == nil {
		t.Fatal("injected unknown removal result was ignored")
	}
	// Native deletion happened, but the command response was lost. Its empty
	// authoritative listing lets a copied handle reconcile without force.
	ctr.Fail = ""
	ctr.Output["fs subvolume ls fixture --group_name group --format json"] = `[]`
	ctr.Calls = nil
	if err := (&copyBefore).Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !subvolume.identity.Removed || !copyBefore.identity.Removed {
		t.Fatal("copied descriptor does not share lifecycle")
	}
	for _, call := range ctr.Calls {
		if slices.Contains(call, "rm") {
			t.Fatal("unknown successful deletion was repeated")
		}
	}
	if err := subvolume.Resize(t.Context(), 65536); err == nil {
		t.Fatal("removed original handle accepted quota mutation")
	}
	group, err := createGroup(t.Context(), fs.Core, GroupConfig{Name: "group", SizeBytes: 65536})
	if err != nil {
		t.Fatal(err)
	}
	copyGroup := *group
	if err := group.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := (&copyGroup).Resize(t.Context(), 1); err == nil {
		t.Fatal("copied removed group accepted mutation")
	}
}

func TestCephFSSnapshotRetainedAndReplacedFSRefuseRemoval(t *testing.T) {
	fs, ctr := subvolumeFixture()
	subvolume, err := create(t.Context(), fs.Core, Config{Name: "volume", GroupName: "group", SizeBytes: 32768})
	if err != nil {
		t.Fatal(err)
	}
	ctr.Output["fs subvolume info fixture volume --group_name group --format json"] = `{"state":"snapshot-retained","type":"subvolume","features":["snapshot-retention"]}`
	ctr.Calls = nil
	if err := subvolume.Remove(t.Context()); err == nil {
		t.Fatal("retained snapshots accepted as active owned subvolume")
	}
	for _, call := range ctr.Calls {
		if slices.Contains(call, "rm") {
			t.Fatal("snapshot-retained subvolume mutated")
		}
	}
	subvolume.identity.removalAttempted = true
	ctr.Output["fs dump --format json"] = strings.ReplaceAll(ctr.Output["fs dump --format json"], `"id":41`, `"id":42`)
	if err := subvolume.Remove(t.Context()); err == nil || subvolume.identity.Removed {
		t.Fatal("replacement FS listing falsely confirmed old removal")
	}
}

func TestCephFSSubvolumeEnablesVolumesAndRejectsUnavailableFilesystem(t *testing.T) {
	fs, ctr := subvolumeFixture()
	ctr.Output["mgr module ls --format json"] = `{"enabled_modules":[],"always_on_modules":["status"]}`
	if _, err := groupNames(t.Context(), fs.Core); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(ctr.Calls, func(call []string) bool { return slices.Equal(call, []string{"mgr", "module", "enable", "volumes"}) }) {
		t.Fatal("missing volumes module was never enabled")
	}
	ctr.Calls = nil
	ctr.Fail = "mgr module enable volumes"
	if _, err := groupNames(t.Context(), fs.Core); err == nil {
		t.Fatal("MGR module failure was ignored")
	}
	for _, call := range ctr.Calls {
		if slices.Contains(call, "subvolumegroup") {
			t.Fatal("volume operations ran without MGR module readiness")
		}
	}
	testhelper.TestSetClosed(fs.Core.Cluster(), true)
	_, err := groupNames(t.Context(), fs.Core)
	if err == nil {
		t.Fatal("closed filesystem accepted")
	}
}
