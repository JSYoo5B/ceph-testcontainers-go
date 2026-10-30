package cephfs

import (
	"context"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
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
		ctr.Output["fs clone status fixture copy --group_name restores --format json"] = cloneLifecycleStatus("canceled")
		ctr.Output["fixture /volumes/restores/copy"] = strings.ReplaceAll(ctr.Output["fixture /volumes/restores/copy"], `"state":"pending"`, `"state":"canceled"`)
		ctr.Output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = `{"created_at":"2026-10-03 00:01:00.123456","data_pool":"additional","has_pending_clones":"no"}`
	}
	if key == "fs subvolume rm fixture copy --group_name restores --force" && ctr.removeApplies {
		ctr.Output["fs subvolume ls fixture --group_name restores --format json"] = "[]"
	}
	return ctr.poolFixtureContainer.Exec(ctx, args, opts...)
}

func cloneLifecycleStatus(state string) string {
	return `{"status":{"state":"` + state + `","source":{"volume":"fixture","subvolume":"volume","group":"group","snapshot":"checkpoint"}}}`
}

func cloneLifecycleFixture(t *testing.T) (*Filesystem, *cloneLifecycleFixtureContainer, *SubvolumeClone) {
	t.Helper()
	fs, base, _, snapshot := snapshotFixture(t)
	clone, err := snapshot.Clone(t.Context(), CloneConfig{Name: "copy", GroupName: "restores"})
	if err != nil {
		t.Fatal(err)
	}
	base.Output["fs subvolume ls fixture --group_name restores --format json"] = `[{"name":"copy"}]`
	base.Output["fs clone status fixture copy --group_name restores --format json"] = cloneLifecycleStatus("pending")
	base.Output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = `{"created_at":"2026-10-03 00:01:00.123456","data_pool":"additional","has_pending_clones":"yes","pending_clones":[{"name":"copy","target_group":"restores"}]}`
	ctr := &cloneLifecycleFixtureContainer{poolFixtureContainer: base, cancelApplies: true, removeApplies: true}
	fs.cluster.Container = ctr
	ctr.Calls = nil
	return fs, ctr, clone
}

func TestCephFSCloneCancelAndExplicitPartialCleanupShareIdentity(t *testing.T) {
	_, ctr, clone := cloneLifecycleFixture(t)
	copy := *clone
	copy.Name, copy.GroupName, copy.FilesystemName = "other", "other", "other"
	if err := (&copy).RemovePartial(t.Context()); err == nil {
		t.Fatal("active clone removed")
	}
	if err := (&copy).Cancel(t.Context()); err != nil {
		t.Fatal(err)
	}
	status, err := clone.Status(t.Context())
	if err != nil || status.State != "canceled" {
		t.Fatalf("canceled status=%+v error=%v", status, err)
	}
	before := len(ctr.Calls)
	if err := clone.Cancel(t.Context()); err != nil {
		t.Fatal("repeated cancellation not reconciled", err)
	}
	for _, call := range ctr.Calls[before:] {
		if slices.Contains(call, "cancel") {
			t.Fatal("canceled clone canceled twice")
		}
	}
	if err := (&copy).RemovePartial(t.Context()); err != nil {
		t.Fatal(err)
	}
	before = len(ctr.Calls)
	if err := clone.RemovePartial(t.Context()); err != nil {
		t.Fatal("copied removal state not shared", err)
	}
	for _, call := range ctr.Calls[before:] {
		if slices.Contains(call, "--force") {
			t.Fatal("partial target removal repeated")
		}
	}
	if _, err := clone.Status(t.Context()); err == nil {
		t.Fatal("removed partial target still adopted")
	}
	for _, call := range ctr.Calls {
		if slices.Contains(call, "rm") && !slices.Equal(call, []string{"fs", "subvolume", "rm", "fixture", "copy", "--group_name", "restores", "--force"}) {
			t.Fatal("cleanup touched original snapshot/data", call)
		}
	}
}

func TestCephFSCloneLostCancelAndCleanupRepliesConverge(t *testing.T) {
	_, ctr, clone := cloneLifecycleFixture(t)
	ctr.Fail = "fs clone cancel fixture copy --group_name restores"
	if err := clone.Cancel(t.Context()); err == nil {
		t.Fatal("lost cancel reply silently accepted")
	}
	ctr.Fail = ""
	if err := clone.Cancel(t.Context()); err != nil {
		t.Fatal("cancel unknown success did not reconcile", err)
	}
	ctr.Fail = "fs subvolume rm fixture copy --group_name restores --force"
	if err := clone.RemovePartial(t.Context()); err == nil || !clone.identity.removalAttempted {
		t.Fatal("lost cleanup reply not retained")
	}
	ctr.Fail = ""
	copy := *clone
	if err := (&copy).RemovePartial(t.Context()); err != nil || !clone.identity.removed {
		t.Fatal("unknown successful cleanup not reconciled", err)
	}
}

func TestCephFSCloneReplacedTargetAndSourceRefuseMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*cloneLifecycleFixtureContainer, *SubvolumeClone)
	}{
		{"UUID", func(c *cloneLifecycleFixtureContainer, _ *SubvolumeClone) {
			c.Output["fixture /volumes/restores/copy"] = strings.ReplaceAll(c.Output["fixture /volumes/restores/copy"], "10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002")
		}},
		{"inode", func(c *cloneLifecycleFixtureContainer, _ *SubvolumeClone) {
			c.Output["fixture /volumes/restores/copy"] = strings.ReplaceAll(c.Output["fixture /volumes/restores/copy"], `"inode":71`, `"inode":72`)
		}},
		{"birth time", func(c *cloneLifecycleFixtureContainer, _ *SubvolumeClone) {
			c.Output["fixture /volumes/restores/copy"] = strings.ReplaceAll(c.Output["fixture /volumes/restores/copy"], "00:02:00.123456", "00:03:00.123456")
		}},
		{"source snapshot", func(c *cloneLifecycleFixtureContainer, _ *SubvolumeClone) {
			c.Output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = strings.ReplaceAll(c.Output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"], "00:01:00.123456", "00:03:00.123456")
		}},
		{"uncaptured incarnation", func(_ *cloneLifecycleFixtureContainer, c *SubvolumeClone) { c.identity.incarnation = nil }},
		{"unknown metadata version", func(c *cloneLifecycleFixtureContainer, _ *SubvolumeClone) {
			c.Output["fixture /volumes/restores/copy"] = strings.ReplaceAll(c.Output["fixture /volumes/restores/copy"], `"version":2`, `"version":3`)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, c, clone := cloneLifecycleFixture(t)
			test.edit(c, clone)
			if err := clone.Cancel(t.Context()); err == nil {
				t.Fatal("unsafe cancellation accepted")
			}
			c.Output["fs clone status fixture copy --group_name restores --format json"] = cloneLifecycleStatus("failed")
			if err := clone.RemovePartial(t.Context()); err == nil {
				t.Fatal("unsafe partial cleanup accepted")
			}
			for _, call := range c.Calls {
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
			_, c, clone := cloneLifecycleFixture(t)
			c.Output["fs clone status fixture copy --group_name restores --format json"] = cloneLifecycleStatus(state)
			if err := clone.RemovePartial(t.Context()); err == nil {
				t.Fatal("running/completed or source-protected clone removed")
			}
			for _, call := range c.Calls {
				if slices.Contains(call, "--force") {
					t.Fatal("guard used force")
				}
			}
		})
	}
	_, c, clone := cloneLifecycleFixture(t)
	c.Output["fs clone status fixture copy --group_name restores --format json"] = cloneLifecycleStatus("failed")
	c.Output["fs subvolume snapshot info fixture volume checkpoint --group_name group --format json"] = `{"created_at":"2026-10-03 00:01:00.123456","data_pool":"additional","has_pending_clones":"no"}`
	if err := clone.RemovePartial(t.Context()); err != nil {
		t.Fatal("owned failed clone explicit cleanup refused", err)
	}
}

func TestCephFSCloneCleanupLostReplyCannotRemoveRecreatedTarget(t *testing.T) {
	_, c, clone := cloneLifecycleFixture(t)
	if err := clone.Cancel(t.Context()); err != nil {
		t.Fatal(err)
	}
	c.Fail = "fs subvolume rm fixture copy --group_name restores --force"
	if err := clone.RemovePartial(t.Context()); err == nil {
		t.Fatal("lost cleanup reply not reported")
	}
	c.Fail = ""
	c.Calls = nil
	c.Output["fs subvolume ls fixture --group_name restores --format json"] = `[{"name":"copy"}]`
	c.Output["fixture /volumes/restores/copy"] = strings.ReplaceAll(c.Output["fixture /volumes/restores/copy"], "10000000-0000-4000-8000-000000000001", "10000000-0000-4000-8000-000000000002")
	if err := clone.RemovePartial(t.Context()); err == nil {
		t.Fatal("recreated target removed after lost reply")
	}
	for _, call := range c.Calls {
		if slices.Contains(call, "--force") {
			t.Fatal("recreated clone force-removed")
		}
	}
}

func TestCephFSCloneSubmissionIdentityFailureNeverAdoptsLaterTarget(t *testing.T) {
	_, ctr, _, snapshot := snapshotFixture(t)
	key := "fixture /volumes/restores/copy"
	metadata := ctr.Output[key]
	ctr.Output[key] = strings.ReplaceAll(metadata, `"version":2`, `"version":3`)
	clone, err := snapshot.Clone(t.Context(), CloneConfig{Name: "copy", GroupName: "restores"})
	if err == nil || clone == nil || !clone.identity.submitted || clone.identity.incarnation != nil {
		t.Fatal("unconfirmed submission metadata was accepted", clone, err)
	}
	// Even valid metadata on a subsequent read is not proof of the incarnation
	// created by the original submission. It could be an external replacement.
	ctr.Output[key] = metadata
	ctr.Output["fs subvolume ls fixture --group_name restores --format json"] = `[{"name":"copy"}]`
	ctr.Output["fs clone status fixture copy --group_name restores --format json"] = `{"status":{"state":"complete"}}`
	ctr.Calls = nil
	if _, err := clone.Status(t.Context()); err == nil {
		t.Fatal("later status adopted an uncaptured target")
	}
	if _, err := clone.Wait(t.Context()); err == nil {
		t.Fatal("later completion adopted an uncaptured target")
	}
	if err := clone.Cancel(t.Context()); err == nil {
		t.Fatal("later cancellation adopted an uncaptured target")
	}
	if err := clone.RemovePartial(t.Context()); err == nil {
		t.Fatal("later cleanup adopted an uncaptured target")
	}
	if clone.identity.incarnation != nil {
		t.Fatal("incarnation was captured after submission")
	}
	for _, call := range ctr.Calls {
		if slices.Contains(call, "cancel") || slices.Contains(call, "--force") || slices.Contains(call, "status") {
			t.Fatal("uncaptured target was queried or mutated through an owned handle", call)
		}
	}
}

func TestCephFSCloneLifecycleCanceledContextAndClosedClusterRefuseMutation(t *testing.T) {
	fs, ctr, clone := cloneLifecycleFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := clone.Cancel(ctx); err == nil {
		t.Fatal("cancellation ignored canceled context")
	}
	if err := clone.RemovePartial(ctx); err == nil {
		t.Fatal("partial cleanup ignored canceled context")
	}
	testhelper.TestSetClosed(fs.cluster, true)
	if err := clone.Cancel(t.Context()); err == nil {
		t.Fatal("closed cluster allowed cancellation")
	}
	if err := clone.RemovePartial(t.Context()); err == nil {
		t.Fatal("closed cluster allowed partial cleanup")
	}
	if len(ctr.Calls) != 0 {
		t.Fatal("unavailable cluster reached native commands", ctr.Calls)
	}
}
