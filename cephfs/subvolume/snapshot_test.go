package subvolume

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
)

func snapshotFixture(t *testing.T) (*fstest.Filesystem, *poolFixtureContainer, *Subvolume, *Snapshot) {
	t.Helper()
	fs, ctr := subvolumeFixture()
	ctr.Output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = `{"created_at":"2026-10-03 00:01:00.123456","data_pool":"additional","has_pending_clones":"no"}`
	ctr.Output["fs subvolume snapshot getpath fixture volume checkpoint --group_name group"] = "/volumes/group/volume/.snap/checkpoint/unique-id\n"
	for _, group := range []string{"_nogroup", "restores"} {
		ctr.Output["fixture /volumes/"+group+"/copy"] = `{"version":2,"type":"clone","state":"pending","path":"/volumes/` + group + `/copy/10000000-0000-4000-8000-000000000001","inode":71,"birth_time":"2026-10-03T00:02:00.123456","source":{"volume":"fixture","subvolume":"volume","group":"group","snapshot":"checkpoint"}}`
	}
	volume, err := create(t.Context(), fs.Core, Config{Name: "volume", GroupName: "group", SizeBytes: 32768})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := volume.CreateSnapshot(t.Context(), "checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	ctr.Calls = nil
	return fs, ctr, volume, snapshot
}

func TestCephFSSnapshotOwnershipAndPendingGuard(t *testing.T) {
	_, ctr, volume, snapshot := snapshotFixture(t)
	if snapshot.Path != "/volumes/group/volume/.snap/checkpoint/unique-id" {
		t.Fatal("snapshot has no native frozen path")
	}
	ctr.Output["fs subvolume snapshot ls fixture volume --group_name group --format json"] = `[{"name":"checkpoint"}]`
	if duplicate, err := volume.CreateSnapshot(t.Context(), "checkpoint"); err == nil || duplicate != nil {
		t.Fatal("duplicate native snapshot adopted")
	}
	if err := (&Snapshot{Name: "checkpoint"}).Remove(t.Context()); err == nil {
		t.Fatal("external descriptor granted snapshot removal")
	}
	ctr.Output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = `{"created_at":"2026-10-03 00:01:00.123456","data_pool":"additional","has_pending_clones":"yes","pending_clones":[{"name":"copy","target_group":"restores"}]}`
	ctr.Calls = nil
	if err := snapshot.Remove(t.Context()); err == nil {
		t.Fatal("pending clone source accepted removal")
	}
	for _, call := range ctr.Calls {
		if slices.Contains(call, "rm") {
			t.Fatal("pending source reached native removal")
		}
	}
	snapshot.Name, snapshot.SubvolumeName, snapshot.GroupName, snapshot.Path, snapshot.FilesystemName = "foreign", "foreign", "foreign", "/foreign", "foreign"
	ctr.Output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = `{"created_at":"2026-10-03 00:01:00.123456","data_pool":"additional","has_pending_clones":"no"}`
	copyBefore := *snapshot
	if err := snapshot.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []string{"fs", "subvolume", "snapshot", "rm", "fixture", "volume", "checkpoint", "--group_name", "group"}
	if !slices.Equal(ctr.Calls[len(ctr.Calls)-1], want) {
		t.Fatal("exported descriptor redirected native mutation")
	}
	ctr.Calls = nil
	if err := (&copyBefore).Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(ctr.Calls, func(call []string) bool {
		return len(call) > 3 && slices.Equal(call[:4], []string{"fs", "subvolume", "snapshot", "rm"})
	}) || !snapshot.identity.removed {
		t.Fatalf("copied descriptor repeated removal or lost shared state: %v", ctr.Calls)
	}
	if _, err := (&copyBefore).Clone(t.Context(), CloneConfig{Name: "copy"}); err == nil {
		t.Fatal("removed copied snapshot accepted clone")
	}
}

func TestCephFSSnapshotReplacementAndUnknownRemoval(t *testing.T) {
	for _, change := range []string{"snapshot-birth", "snapshot-path", "subvolume-path", "filesystem"} {
		_, ctr, _, snapshot := snapshotFixture(t)
		switch change {
		case "snapshot-birth":
			ctr.Output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = strings.ReplaceAll(ctr.Output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"], "00:01:00", "00:01:01")
		case "snapshot-path":
			ctr.Output["fs subvolume snapshot getpath fixture volume checkpoint --group_name group"] = "/volumes/group/volume/.snap/checkpoint/replaced-id"
		case "subvolume-path":
			ctr.Output["fs subvolume info fixture volume --group_name group --format json"] = strings.ReplaceAll(ctr.Output["fs subvolume info fixture volume --group_name group --format json"], "unique-id", "replaced-id")
		case "filesystem":
			ctr.Output["fs dump --format json"] = strings.ReplaceAll(ctr.Output["fs dump --format json"], `"id":41`, `"id":42`)
		}
		if err := snapshot.Remove(t.Context()); err == nil {
			t.Fatalf("replacement %s accepted", change)
		}
		for _, call := range ctr.Calls {
			if slices.Contains(call, "rm") {
				t.Fatalf("replacement %s removed", change)
			}
		}
	}
	_, ctr, _, snapshot := snapshotFixture(t)
	ctr.Fail = "fs subvolume snapshot rm fixture volume checkpoint --group_name group"
	if err := snapshot.Remove(t.Context()); err == nil || snapshot.identity.removed {
		t.Fatal("failed removal lost retry state")
	}
	copyBefore := *snapshot
	ctr.Fail = ""
	ctr.Output["fs subvolume snapshot ls fixture volume --group_name group --format json"] = `[]`
	ctr.Calls = nil
	if err := (&copyBefore).Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, call := range ctr.Calls {
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
	volume, err := create(t.Context(), fs.Core, Config{Name: "volume", GroupName: "group", SizeBytes: 32768})
	if err != nil {
		t.Fatal(err)
	}
	ctr.Fail = "fs subvolume snapshot create fixture volume checkpoint --group_name group"
	snapshot, err := volume.CreateSnapshot(t.Context(), "checkpoint")
	if err == nil || snapshot == nil || snapshot.identity.ready {
		t.Fatal("failed snapshot create lost attempted descriptor or marked ready")
	}
	ctr.Calls = nil
	if err := snapshot.Remove(t.Context()); err == nil || len(ctr.Calls) != 0 {
		t.Fatal("partial snapshot granted removal")
	}
	_, ctr, _, snapshot = snapshotFixture(t)
	ctr.Output["fs subvolume ls fixture --group_name restores --format json"] = `[{"name":"copy"}]`
	clone, err := snapshot.Clone(t.Context(), CloneConfig{Name: "copy", GroupName: "restores"})
	if err == nil || clone != nil {
		t.Fatal("existing clone target accepted")
	}
	for _, call := range ctr.Calls {
		if slices.Contains(call, "clone") {
			t.Fatal("duplicate target reached native clone")
		}
	}
}

func TestCephFSCloneSubmissionStatusAndCompletionOwnership(t *testing.T) {
	_, ctr, _, snapshot := snapshotFixture(t)
	clone, err := snapshot.Clone(t.Context(), CloneConfig{Name: "copy", GroupName: "restores", DataPool: "data"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"fs", "subvolume", "snapshot", "clone", "fixture", "volume", "checkpoint", "copy", "--group_name", "group", "--target_group_name", "restores", "--pool_layout", "data"}
	if !slices.ContainsFunc(ctr.Calls, func(call []string) bool { return slices.Equal(call, want) }) {
		t.Fatalf("clone layout or source/target group arguments changed: %v", ctr.Calls)
	}
	ctr.Output["fs clone status fixture copy --group_name restores --format json"] = `{"status":{"state":"pending","source":{"volume":"fixture","subvolume":"volume","group":"group","snapshot":"checkpoint"}}}`
	status, err := clone.Status(t.Context())
	if err != nil || status.State != "pending" || status.SourceGroup != "group" {
		t.Fatalf("pending source not verified: %+v error=%v", status, err)
	}
	// A wait timeout leaves the request and source snapshot available.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if adopted, err := clone.Wait(ctx); err == nil || adopted != nil {
		t.Fatal("pending clone was adopted or wait ignored deadline")
	}
	clone.Name, clone.GroupName, clone.FilesystemName = "foreign", "foreign", "foreign"
	ctr.Output["fs clone status fixture copy --group_name restores --format json"] = `{"status":{"state":"complete"}}`
	ctr.Output["fs subvolume info fixture copy --group_name restores --format json"] = `{"path":"/volumes/restores/copy/10000000-0000-4000-8000-000000000001","bytes_quota":32768,"bytes_used":8192,"data_pool":"data","pool_namespace":"","created_at":"2026-10-03 00:02:00.123456","state":"complete","type":"clone"}`
	adopted, err := clone.Wait(t.Context())
	if err != nil || adopted.Path != "/volumes/restores/copy/10000000-0000-4000-8000-000000000001" || adopted.Name != "copy" || adopted.GroupName != "restores" {
		t.Fatalf("completed clone not adopted safely: %+v error=%v", adopted, err)
	}
	copyClone := *clone
	second, err := (&copyClone).Wait(t.Context())
	if err != nil || second.identity != adopted.identity || second == adopted {
		t.Fatal("clone copies do not share adopted lifecycle or descriptors alias")
	}
	if err := adopted.Resize(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	ctr.Output["fs subvolume info fixture copy --group_name restores --format json"] = strings.ReplaceAll(ctr.Output["fs subvolume info fixture copy --group_name restores --format json"], "10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002")
	if _, err := (&copyClone).Wait(t.Context()); err == nil {
		t.Fatal("replaced completed target was adopted")
	}
}

func TestCephFSClonePartialSubmissionFailureAndSourceReplacement(t *testing.T) {
	_, ctr, _, snapshot := snapshotFixture(t)
	ctr.Fail = "fs subvolume snapshot clone fixture volume checkpoint copy --group_name group"
	clone, err := snapshot.Clone(t.Context(), CloneConfig{Name: "copy"})
	if err == nil || clone == nil || clone.identity.submitted {
		t.Fatal("uncertain clone submission adopted as confirmed")
	}
	ctr.Calls = nil
	if _, err := clone.Status(t.Context()); err == nil || len(ctr.Calls) != 0 {
		t.Fatal("unconfirmed clone request granted adoption")
	}
	ctr.Fail = ""
	clone, err = snapshot.Clone(t.Context(), CloneConfig{Name: "copy"})
	if err != nil {
		t.Fatal(err)
	}
	ctr.Output["fs clone status fixture copy --format json"] = `{"status":{"state":"failed","source":{"volume":"fixture","subvolume":"volume","group":"group","snapshot":"checkpoint"},"failure":{"errno":"122","error_msg":"Disk quota exceeded"}}}`
	start := time.Now()
	if _, err := clone.Wait(t.Context()); err == nil || !strings.Contains(err.Error(), "122") || time.Since(start) > time.Second {
		t.Fatal("failed clone did not stop wait promptly with reason")
	}
	for _, call := range ctr.Calls {
		if slices.Contains(call, "--force") || slices.Contains(call, "rm") {
			t.Fatal("failed clone auto-deleted partial data")
		}
	}
	ctr.Output["fs clone status fixture copy --format json"] = `{"status":{"state":"pending","source":{"volume":"fixture","subvolume":"foreign","snapshot":"checkpoint"}}}`
	if _, err := clone.Status(t.Context()); err == nil {
		t.Fatal("foreign native source accepted")
	}
	ctr.Output["fs dump --format json"] = strings.ReplaceAll(ctr.Output["fs dump --format json"], `"id":41`, `"id":42`)
	if _, err := clone.Status(t.Context()); err == nil {
		t.Fatal("replaced filesystem accepted clone status")
	}
}

func TestCephFSSnapshotAndCloneDecodeRejectInvalidState(t *testing.T) {
	for _, data := range []string{`null`, `{}`, `{"created_at":"t","data_pool":"data","has_pending_clones":"true"}`, `{"created_at":"t","data_pool":"data","has_pending_clones":"yes"}`, `{"created_at":"t","data_pool":"data","has_pending_clones":"no","pending_clones":[{"name":"copy"}]}`, `{"created_at":"t","data_pool":"data","has_pending_clones":"yes","pending_clones":[{"name":"copy"},{"name":"copy"}]}`} {
		if _, err := decodeSnapshotInfo([]byte(data)); err == nil {
			t.Fatalf("invalid snapshot info accepted: %s", data)
		}
	}
	for _, data := range []string{`null`, `{}`, `{"status":{"state":"unknown"}}`, `{"status":{"state":"pending"}}`, `{"status":{"state":"failed","source":{"volume":"f","subvolume":"s","snapshot":"p"},"failure":{"errno":"bad"}}}`} {
		if _, err := decodeCloneStatus([]byte(data)); err == nil {
			t.Fatalf("invalid clone status accepted: %s", data)
		}
	}
	for _, field := range []string{"error_msg", "errstr"} {
		data := `{"status":{"state":"failed","source":{"volume":"f","subvolume":"s","snapshot":"p"},"failure":{"errno":"122","` + field + `":"Disk quota exceeded"}}}`
		status, err := decodeCloneStatus([]byte(data))
		if err != nil || status.FailureErrno != 122 || status.FailureMessage != "Disk quota exceeded" {
			t.Fatalf("native failure variant rejected: %+v %v", status, err)
		}
	}
}
