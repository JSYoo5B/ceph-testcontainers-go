package ceph

import (
	"context"
	"io"
	"slices"
	"strings"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type cloneLifecycleFixtureContainer struct {
	*poolFixtureContainer
	cancelApplies, removeApplies bool
}

func (ctr *cloneLifecycleFixtureContainer) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	key := strings.Join(args[3:], " ")
	if key == "fs clone cancel fixture copy --group_name restores" && ctr.cancelApplies {
		ctr.output["fs clone status fixture copy --group_name restores --format json"] = cloneLifecycleStatus("canceled")
		ctr.output["fixture /volumes/restores/copy"] = strings.ReplaceAll(ctr.output["fixture /volumes/restores/copy"], `"state":"pending"`, `"state":"canceled"`)
		ctr.output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = `{"created_at":"2026-10-03 00:01:00.123456","data_pool":"additional","has_pending_clones":"no"}`
	}
	if key == "fs subvolume rm fixture copy --group_name restores --force" && ctr.removeApplies {
		ctr.output["fs subvolume ls fixture --group_name restores --format json"] = "[]"
	}
	return ctr.poolFixtureContainer.Exec(ctx, args, opts...)
}

func cloneLifecycleStatus(state string) string {
	return `{"status":{"state":"` + state + `","source":{"volume":"fixture","subvolume":"volume","group":"group","snapshot":"checkpoint"}}}`
}

func cloneLifecycleFixture(t *testing.T) (*CephFSContainer, *cloneLifecycleFixtureContainer, *CephFSSubvolumeClone) {
	t.Helper()
	fs, base, _, snapshot := snapshotFixture(t)
	clone, err := fs.CloneSubvolumeSnapshot(t.Context(), snapshot, CephFSCloneConfig{Name: "copy", GroupName: "restores"})
	if err != nil {
		t.Fatal(err)
	}
	base.output["fs subvolume ls fixture --group_name restores --format json"] = `[{"name":"copy"}]`
	base.output["fs clone status fixture copy --group_name restores --format json"] = cloneLifecycleStatus("pending")
	base.output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = `{"created_at":"2026-10-03 00:01:00.123456","data_pool":"additional","has_pending_clones":"yes","pending_clones":[{"name":"copy","target_group":"restores"}]}`
	ctr := &cloneLifecycleFixtureContainer{poolFixtureContainer: base, cancelApplies: true, removeApplies: true}
	fs.cluster.Container = ctr
	ctr.calls = nil
	return fs, ctr, clone
}

func TestCephFSCloneCancelAndExplicitPartialCleanupShareIdentity(t *testing.T) {
	fs, ctr, clone := cloneLifecycleFixture(t)
	copy := *clone
	copy.Name, copy.GroupName, copy.FilesystemName = "other", "other", "other"
	if err := fs.RemovePartialSubvolumeClone(t.Context(), &copy); err == nil {
		t.Fatal("active clone removed")
	}
	if err := fs.CancelSubvolumeClone(t.Context(), &copy); err != nil {
		t.Fatal(err)
	}
	status, err := fs.SubvolumeCloneStatus(t.Context(), clone)
	if err != nil || status.State != "canceled" {
		t.Fatalf("canceled status=%+v error=%v", status, err)
	}
	before := len(ctr.calls)
	if err := fs.CancelSubvolumeClone(t.Context(), clone); err != nil {
		t.Fatal("repeated cancellation not reconciled", err)
	}
	for _, call := range ctr.calls[before:] {
		if slices.Contains(call, "cancel") {
			t.Fatal("canceled clone canceled twice")
		}
	}
	if err := fs.RemovePartialSubvolumeClone(t.Context(), &copy); err != nil {
		t.Fatal(err)
	}
	before = len(ctr.calls)
	if err := fs.RemovePartialSubvolumeClone(t.Context(), clone); err != nil {
		t.Fatal("copied removal state not shared", err)
	}
	for _, call := range ctr.calls[before:] {
		if slices.Contains(call, "--force") {
			t.Fatal("partial target removal repeated")
		}
	}
	if _, err := fs.SubvolumeCloneStatus(t.Context(), clone); err == nil {
		t.Fatal("removed partial target still adopted")
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "rm") && !slices.Equal(call, []string{"fs", "subvolume", "rm", "fixture", "copy", "--group_name", "restores", "--force"}) {
			t.Fatal("cleanup touched original snapshot/data", call)
		}
	}
}

func TestCephFSCloneLostCancelAndCleanupRepliesConverge(t *testing.T) {
	fs, ctr, clone := cloneLifecycleFixture(t)
	ctr.fail = "fs clone cancel fixture copy --group_name restores"
	if err := fs.CancelSubvolumeClone(t.Context(), clone); err == nil {
		t.Fatal("lost cancel reply silently accepted")
	}
	ctr.fail = ""
	if err := fs.CancelSubvolumeClone(t.Context(), clone); err != nil {
		t.Fatal("cancel unknown success did not reconcile", err)
	}
	ctr.fail = "fs subvolume rm fixture copy --group_name restores --force"
	if err := fs.RemovePartialSubvolumeClone(t.Context(), clone); err == nil || !clone.identity.removalAttempted {
		t.Fatal("lost cleanup reply not retained")
	}
	ctr.fail = ""
	copy := *clone
	if err := fs.RemovePartialSubvolumeClone(t.Context(), &copy); err != nil || !clone.identity.removed {
		t.Fatal("unknown successful cleanup not reconciled", err)
	}
}

func TestCephFSCloneReplacedTargetAndSourceRefuseMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*cloneLifecycleFixtureContainer, *CephFSSubvolumeClone)
	}{
		{"UUID", func(c *cloneLifecycleFixtureContainer, _ *CephFSSubvolumeClone) {
			c.output["fixture /volumes/restores/copy"] = strings.ReplaceAll(c.output["fixture /volumes/restores/copy"], "10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002")
		}},
		{"inode", func(c *cloneLifecycleFixtureContainer, _ *CephFSSubvolumeClone) {
			c.output["fixture /volumes/restores/copy"] = strings.ReplaceAll(c.output["fixture /volumes/restores/copy"], `"inode":71`, `"inode":72`)
		}},
		{"birth time", func(c *cloneLifecycleFixtureContainer, _ *CephFSSubvolumeClone) {
			c.output["fixture /volumes/restores/copy"] = strings.ReplaceAll(c.output["fixture /volumes/restores/copy"], "00:02:00.123456", "00:03:00.123456")
		}},
		{"source snapshot", func(c *cloneLifecycleFixtureContainer, _ *CephFSSubvolumeClone) {
			c.output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = strings.ReplaceAll(c.output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"], "00:01:00.123456", "00:03:00.123456")
		}},
		{"uncaptured incarnation", func(_ *cloneLifecycleFixtureContainer, c *CephFSSubvolumeClone) { c.identity.incarnation = nil }},
		{"unknown metadata version", func(c *cloneLifecycleFixtureContainer, _ *CephFSSubvolumeClone) {
			c.output["fixture /volumes/restores/copy"] = strings.ReplaceAll(c.output["fixture /volumes/restores/copy"], `"version":2`, `"version":3`)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs, c, clone := cloneLifecycleFixture(t)
			test.edit(c, clone)
			if err := fs.CancelSubvolumeClone(t.Context(), clone); err == nil {
				t.Fatal("unsafe cancellation accepted")
			}
			c.output["fs clone status fixture copy --group_name restores --format json"] = cloneLifecycleStatus("failed")
			if err := fs.RemovePartialSubvolumeClone(t.Context(), clone); err == nil {
				t.Fatal("unsafe partial cleanup accepted")
			}
			for _, call := range c.calls {
				if slices.Contains(call, "cancel") || slices.Contains(call, "--force") {
					t.Fatal("guard sent mutation", call)
				}
			}
		})
	}
}

func TestCephFSClonePartialCleanupRejectsProtectionAndCompletedTarget(t *testing.T) {
	for _, state := range []string{"pending", "in-progress", "complete", "failed", "canceled"} {
		t.Run(state, func(t *testing.T) {
			fs, c, clone := cloneLifecycleFixture(t)
			c.output["fs clone status fixture copy --group_name restores --format json"] = cloneLifecycleStatus(state)
			if err := fs.RemovePartialSubvolumeClone(t.Context(), clone); err == nil {
				t.Fatal("running/completed or source-protected clone removed")
			}
			for _, call := range c.calls {
				if slices.Contains(call, "--force") {
					t.Fatal("guard used force")
				}
			}
		})
	}
	fs, c, clone := cloneLifecycleFixture(t)
	c.output["fs clone status fixture copy --group_name restores --format json"] = cloneLifecycleStatus("failed")
	c.output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = `{"created_at":"2026-10-03 00:01:00.123456","data_pool":"additional","has_pending_clones":"no"}`
	if err := fs.RemovePartialSubvolumeClone(t.Context(), clone); err != nil {
		t.Fatal("owned failed clone explicit cleanup refused", err)
	}
}

func TestCephFSCloneCleanupLostReplyCannotRemoveRecreatedTarget(t *testing.T) {
	fs, c, clone := cloneLifecycleFixture(t)
	if err := fs.CancelSubvolumeClone(t.Context(), clone); err != nil {
		t.Fatal(err)
	}
	c.fail = "fs subvolume rm fixture copy --group_name restores --force"
	if err := fs.RemovePartialSubvolumeClone(t.Context(), clone); err == nil {
		t.Fatal("lost cleanup reply not reported")
	}
	c.fail = ""
	c.calls = nil
	c.output["fs subvolume ls fixture --group_name restores --format json"] = `[{"name":"copy"}]`
	c.output["fixture /volumes/restores/copy"] = strings.ReplaceAll(c.output["fixture /volumes/restores/copy"], "10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002")
	if err := fs.RemovePartialSubvolumeClone(t.Context(), clone); err == nil {
		t.Fatal("recreated target removed after lost reply")
	}
	for _, call := range c.calls {
		if slices.Contains(call, "--force") {
			t.Fatal("recreated clone force-removed")
		}
	}
}

func TestCephFSCloneSubmissionIdentityFailureNeverAdoptsLaterTarget(t *testing.T) {
	fs, ctr, _, snapshot := snapshotFixture(t)
	key := "fixture /volumes/restores/copy"
	metadata := ctr.output[key]
	ctr.output[key] = strings.ReplaceAll(metadata, `"version":2`, `"version":3`)
	clone, err := fs.CloneSubvolumeSnapshot(t.Context(), snapshot, CephFSCloneConfig{Name: "copy", GroupName: "restores"})
	if err == nil || clone == nil || !clone.identity.submitted || clone.identity.incarnation != nil {
		t.Fatal("unconfirmed submission metadata was accepted", clone, err)
	}
	// Even valid metadata on a subsequent read is not proof of the incarnation
	// created by the original submission. It could be an external replacement.
	ctr.output[key] = metadata
	ctr.output["fs subvolume ls fixture --group_name restores --format json"] = `[{"name":"copy"}]`
	ctr.output["fs clone status fixture copy --group_name restores --format json"] = `{"status":{"state":"complete"}}`
	ctr.calls = nil
	if _, err := fs.SubvolumeCloneStatus(t.Context(), clone); err == nil {
		t.Fatal("later status adopted an uncaptured target")
	}
	if _, err := fs.WaitForSubvolumeClone(t.Context(), clone); err == nil {
		t.Fatal("later completion adopted an uncaptured target")
	}
	if err := fs.CancelSubvolumeClone(t.Context(), clone); err == nil {
		t.Fatal("later cancellation adopted an uncaptured target")
	}
	if err := fs.RemovePartialSubvolumeClone(t.Context(), clone); err == nil {
		t.Fatal("later cleanup adopted an uncaptured target")
	}
	if clone.identity.incarnation != nil {
		t.Fatal("incarnation was captured after submission")
	}
	for _, call := range ctr.calls {
		if slices.Contains(call, "cancel") || slices.Contains(call, "--force") || slices.Contains(call, "status") {
			t.Fatal("uncaptured target was queried or mutated through an owned handle", call)
		}
	}
}

func TestCephFSCloneLifecycleCanceledContextAndClosedClusterRefuseMutation(t *testing.T) {
	fs, ctr, clone := cloneLifecycleFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := fs.CancelSubvolumeClone(ctx, clone); err == nil {
		t.Fatal("cancellation ignored canceled context")
	}
	if err := fs.RemovePartialSubvolumeClone(ctx, clone); err == nil {
		t.Fatal("partial cleanup ignored canceled context")
	}
	fs.cluster.closed = true
	if err := fs.CancelSubvolumeClone(t.Context(), clone); err == nil {
		t.Fatal("closed cluster allowed cancellation")
	}
	if err := fs.RemovePartialSubvolumeClone(t.Context(), clone); err == nil {
		t.Fatal("closed cluster allowed partial cleanup")
	}
	if len(ctr.calls) != 0 {
		t.Fatal("unavailable cluster reached native commands", ctr.calls)
	}
}
