package ceph

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

func snapshotFixture(t *testing.T) (*CephFSContainer, *poolFixtureContainer, *CephFSSubvolume, *CephFSSubvolumeSnapshot) {
	t.Helper()
	fs, ctr := subvolumeFixture()
	ctr.output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = `{"created_at":"2026-10-03 00:01:00.123456","data_pool":"additional","has_pending_clones":"no"}`
	ctr.output["fs subvolume snapshot getpath fixture volume checkpoint --group_name group"] = "/volumes/group/volume/.snap/checkpoint/unique-id\n"
	volume, err := fs.CreateSubvolume(t.Context(), CephFSSubvolumeConfig{Name: "volume", GroupName: "group", SizeBytes: 32768})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := fs.CreateSubvolumeSnapshot(t.Context(), volume, "checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	ctr.calls = nil
	return fs, ctr, volume, snapshot
}

func TestCephFSSnapshotOwnershipAndPendingGuard(t *testing.T) {
	fs, ctr, volume, snapshot := snapshotFixture(t)
	if snapshot.Path != "/volumes/group/volume/.snap/checkpoint/unique-id" {
		t.Fatal("snapshot has no native frozen path")
	}
	ctr.output["fs subvolume snapshot ls fixture volume --group_name group --format json"] = `[{"name":"checkpoint"}]`
	if duplicate, err := fs.CreateSubvolumeSnapshot(t.Context(), volume, "checkpoint"); err == nil || duplicate != nil {
		t.Fatal("duplicate native snapshot adopted")
	}
	if err := fs.RemoveSubvolumeSnapshot(t.Context(), &CephFSSubvolumeSnapshot{Name: "checkpoint"}); err == nil {
		t.Fatal("external descriptor granted snapshot removal")
	}
	ctr.output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = `{"created_at":"2026-10-03 00:01:00.123456","data_pool":"additional","has_pending_clones":"yes","pending_clones":[{"name":"copy","target_group":"restores"}]}`
	ctr.calls = nil
	if err := fs.RemoveSubvolumeSnapshot(t.Context(), snapshot); err == nil {
		t.Fatal("pending clone source accepted removal")
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "rm") {
			t.Fatal("pending source reached native removal")
		}
	}
	snapshot.Name, snapshot.SubvolumeName, snapshot.GroupName, snapshot.Path, snapshot.FilesystemName = "foreign", "foreign", "foreign", "/foreign", "foreign"
	ctr.output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = `{"created_at":"2026-10-03 00:01:00.123456","data_pool":"additional","has_pending_clones":"no"}`
	copyBefore := *snapshot
	if err := fs.RemoveSubvolumeSnapshot(t.Context(), snapshot); err != nil {
		t.Fatal(err)
	}
	want := []string{"fs", "subvolume", "snapshot", "rm", "fixture", "volume", "checkpoint", "--group_name", "group"}
	if !slices.Equal(ctr.calls[len(ctr.calls)-1], want) {
		t.Fatal("exported descriptor redirected native mutation")
	}
	ctr.calls = nil
	if err := fs.RemoveSubvolumeSnapshot(t.Context(), &copyBefore); err != nil {
		t.Fatal(err)
	}
	if len(ctr.calls) != 3 || !snapshot.identity.removed {
		t.Fatalf("copied descriptor repeated removal or lost shared state: %v", ctr.calls)
	}
	if _, err := fs.CloneSubvolumeSnapshot(t.Context(), &copyBefore, CephFSCloneConfig{Name: "copy"}); err == nil {
		t.Fatal("removed copied snapshot accepted clone")
	}
}

func TestCephFSSnapshotReplacementAndUnknownRemoval(t *testing.T) {
	for _, change := range []string{"snapshot-birth", "snapshot-path", "subvolume-path", "filesystem"} {
		fs, ctr, _, snapshot := snapshotFixture(t)
		switch change {
		case "snapshot-birth":
			ctr.output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = strings.ReplaceAll(ctr.output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"], "00:01:00", "00:01:01")
		case "snapshot-path":
			ctr.output["fs subvolume snapshot getpath fixture volume checkpoint --group_name group"] = "/volumes/group/volume/.snap/checkpoint/replaced-id"
		case "subvolume-path":
			ctr.output["fs subvolume info fixture volume --group_name group --format json"] = strings.ReplaceAll(ctr.output["fs subvolume info fixture volume --group_name group --format json"], "unique-id", "replaced-id")
		case "filesystem":
			ctr.output["fs dump --format json"] = strings.ReplaceAll(ctr.output["fs dump --format json"], `"id":41`, `"id":42`)
		}
		if err := fs.RemoveSubvolumeSnapshot(t.Context(), snapshot); err == nil {
			t.Fatalf("replacement %s accepted", change)
		}
		for _, call := range ctr.calls {
			if slices.Contains(call, "rm") {
				t.Fatalf("replacement %s removed", change)
			}
		}
	}
	fs, ctr, _, snapshot := snapshotFixture(t)
	ctr.fail = "fs subvolume snapshot rm fixture volume checkpoint --group_name group"
	if err := fs.RemoveSubvolumeSnapshot(t.Context(), snapshot); err == nil || snapshot.identity.removed {
		t.Fatal("failed removal lost retry state")
	}
	copyBefore := *snapshot
	ctr.fail = ""
	ctr.output["fs subvolume snapshot ls fixture volume --group_name group --format json"] = `[]`
	ctr.calls = nil
	if err := fs.RemoveSubvolumeSnapshot(t.Context(), &copyBefore); err != nil {
		t.Fatal(err)
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "rm") {
			t.Fatal("lost successful response caused repeated deletion")
		}
	}
	if !snapshot.identity.removed {
		t.Fatal("snapshot copies do not share reconciled deletion")
	}
}

func TestCephFSSnapshotPartialCreateAndCloneDuplicate(t *testing.T) {
	fs, ctr := subvolumeFixture()
	volume, err := fs.CreateSubvolume(t.Context(), CephFSSubvolumeConfig{Name: "volume", GroupName: "group", SizeBytes: 32768})
	if err != nil {
		t.Fatal(err)
	}
	ctr.fail = "fs subvolume snapshot create fixture volume checkpoint --group_name group"
	snapshot, err := fs.CreateSubvolumeSnapshot(t.Context(), volume, "checkpoint")
	if err == nil || snapshot == nil || snapshot.identity.ready {
		t.Fatal("failed snapshot create lost attempted descriptor or marked ready")
	}
	ctr.calls = nil
	if err := fs.RemoveSubvolumeSnapshot(t.Context(), snapshot); err == nil || len(ctr.calls) != 0 {
		t.Fatal("partial snapshot granted removal")
	}
	fs, ctr, _, snapshot = snapshotFixture(t)
	ctr.output["fs subvolume ls fixture --group_name restores --format json"] = `[{"name":"copy"}]`
	clone, err := fs.CloneSubvolumeSnapshot(t.Context(), snapshot, CephFSCloneConfig{Name: "copy", GroupName: "restores"})
	if err == nil || clone != nil {
		t.Fatal("existing clone target accepted")
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "clone") {
			t.Fatal("duplicate target reached native clone")
		}
	}
}

func TestCephFSCloneSubmissionStatusAndCompletionOwnership(t *testing.T) {
	fs, ctr, _, snapshot := snapshotFixture(t)
	clone, err := fs.CloneSubvolumeSnapshot(t.Context(), snapshot, CephFSCloneConfig{Name: "copy", GroupName: "restores", DataPool: "data"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"fs", "subvolume", "snapshot", "clone", "fixture", "volume", "checkpoint", "copy", "--group_name", "group", "--target_group_name", "restores", "--pool_layout", "data"}
	if !slices.Equal(ctr.calls[len(ctr.calls)-1], want) {
		t.Fatalf("clone layout or source/target group arguments changed: %v", ctr.calls)
	}
	ctr.output["fs clone status fixture copy --group_name restores --format json"] = `{"status":{"state":"pending","source":{"volume":"fixture","subvolume":"volume","group":"group","snapshot":"checkpoint"}}}`
	status, err := fs.SubvolumeCloneStatus(t.Context(), clone)
	if err != nil || status.State != "pending" || status.SourceGroup != "group" {
		t.Fatalf("pending source not verified: %+v error=%v", status, err)
	}
	// A wait timeout leaves the request and source snapshot available.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if adopted, err := fs.WaitForSubvolumeClone(ctx, clone); err == nil || adopted != nil {
		t.Fatal("pending clone was adopted or wait ignored deadline")
	}
	clone.Name, clone.GroupName, clone.FilesystemName = "foreign", "foreign", "foreign"
	ctr.output["fs clone status fixture copy --group_name restores --format json"] = `{"status":{"state":"complete"}}`
	ctr.output["fs subvolume info fixture copy --group_name restores --format json"] = `{"path":"/volumes/restores/copy/clone-id","bytes_quota":32768,"bytes_used":8192,"data_pool":"data","pool_namespace":"","created_at":"2026-10-03 00:02:00.123456","state":"complete","type":"clone"}`
	adopted, err := fs.WaitForSubvolumeClone(t.Context(), clone)
	if err != nil || adopted.Path != "/volumes/restores/copy/clone-id" || adopted.Name != "copy" || adopted.GroupName != "restores" {
		t.Fatalf("completed clone not adopted safely: %+v error=%v", adopted, err)
	}
	copyClone := *clone
	second, err := fs.WaitForSubvolumeClone(t.Context(), &copyClone)
	if err != nil || second.identity != adopted.identity || second == adopted {
		t.Fatal("clone copies do not share adopted lifecycle or descriptors alias")
	}
	if err := fs.ResizeSubvolume(t.Context(), adopted, 0); err != nil {
		t.Fatal(err)
	}
	ctr.output["fs subvolume info fixture copy --group_name restores --format json"] = strings.ReplaceAll(ctr.output["fs subvolume info fixture copy --group_name restores --format json"], "clone-id", "replacement-id")
	if _, err := fs.WaitForSubvolumeClone(t.Context(), &copyClone); err == nil {
		t.Fatal("replaced completed target was adopted")
	}
}

func TestCephFSClonePartialSubmissionFailureAndSourceReplacement(t *testing.T) {
	fs, ctr, _, snapshot := snapshotFixture(t)
	ctr.fail = "fs subvolume snapshot clone fixture volume checkpoint copy --group_name group"
	clone, err := fs.CloneSubvolumeSnapshot(t.Context(), snapshot, CephFSCloneConfig{Name: "copy"})
	if err == nil || clone == nil || clone.identity.submitted {
		t.Fatal("uncertain clone submission adopted as confirmed")
	}
	ctr.calls = nil
	if _, err := fs.SubvolumeCloneStatus(t.Context(), clone); err == nil || len(ctr.calls) != 0 {
		t.Fatal("unconfirmed clone request granted adoption")
	}
	ctr.fail = ""
	clone, err = fs.CloneSubvolumeSnapshot(t.Context(), snapshot, CephFSCloneConfig{Name: "copy"})
	if err != nil {
		t.Fatal(err)
	}
	ctr.output["fs clone status fixture copy --format json"] = `{"status":{"state":"failed","source":{"volume":"fixture","subvolume":"volume","group":"group","snapshot":"checkpoint"},"failure":{"errno":"122","error_msg":"Disk quota exceeded"}}}`
	start := time.Now()
	if _, err := fs.WaitForSubvolumeClone(t.Context(), clone); err == nil || !strings.Contains(err.Error(), "122") || time.Since(start) > time.Second {
		t.Fatal("failed clone did not stop wait promptly with reason")
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "--force") || slices.Contains(call, "rm") {
			t.Fatal("failed clone auto-deleted partial data")
		}
	}
	ctr.output["fs clone status fixture copy --format json"] = `{"status":{"state":"pending","source":{"volume":"fixture","subvolume":"foreign","snapshot":"checkpoint"}}}`
	if _, err := fs.SubvolumeCloneStatus(t.Context(), clone); err == nil {
		t.Fatal("foreign native source accepted")
	}
	ctr.output["fs dump --format json"] = strings.ReplaceAll(ctr.output["fs dump --format json"], `"id":41`, `"id":42`)
	if _, err := fs.SubvolumeCloneStatus(t.Context(), clone); err == nil {
		t.Fatal("replaced filesystem accepted clone status")
	}
}

func TestCephFSSnapshotAndCloneDecodeRejectInvalidState(t *testing.T) {
	for _, data := range []string{`null`, `{}`, `{"created_at":"t","data_pool":"data","has_pending_clones":"true"}`, `{"created_at":"t","data_pool":"data","has_pending_clones":"yes"}`, `{"created_at":"t","data_pool":"data","has_pending_clones":"no","pending_clones":[{"name":"copy"}]}`, `{"created_at":"t","data_pool":"data","has_pending_clones":"yes","pending_clones":[{"name":"copy"},{"name":"copy"}]}`} {
		if _, err := decodeCephFSSnapshotInfo([]byte(data)); err == nil {
			t.Fatalf("invalid snapshot info accepted: %s", data)
		}
	}
	for _, data := range []string{`null`, `{}`, `{"status":{"state":"unknown"}}`, `{"status":{"state":"pending"}}`, `{"status":{"state":"failed","source":{"volume":"f","subvolume":"s","snapshot":"p"},"failure":{"errno":"bad"}}}`} {
		if _, err := decodeCephFSCloneStatus([]byte(data)); err == nil {
			t.Fatalf("invalid clone status accepted: %s", data)
		}
	}
	for _, field := range []string{"error_msg", "errstr"} {
		data := `{"status":{"state":"failed","source":{"volume":"f","subvolume":"s","snapshot":"p"},"failure":{"errno":"122","` + field + `":"Disk quota exceeded"}}}`
		status, err := decodeCephFSCloneStatus([]byte(data))
		if err != nil || status.FailureErrno != 122 || status.FailureMessage != "Disk quota exceeded" {
			t.Fatalf("native failure variant rejected: %+v %v", status, err)
		}
	}
}
