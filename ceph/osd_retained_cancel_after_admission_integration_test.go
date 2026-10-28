//go:build integration && ci_optional && ci_batch_retained_cancel_after_admission

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package ceph

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// TestRetainedOSDReopenCanceledAfterCreation cancels the caller's context of a
// real ReopenOSD only after the ordinary runner returned a ready container.
// Same-package placement uses the existing private OSD runner test seam.
func TestRetainedOSDReopenCanceledAfterCreation(t *testing.T) {
	ctx, cancelTest := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancelTest()
	image, clientImage := os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE"), os.Getenv("CEPH_TEST_CLIENT_IMAGE")
	if image == "" || clientImage == "" {
		t.Fatal("explicit essential and client images are required")
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	c, err := Run(ctx, image, WithImages(ImageSet{Essential: image, Client: clientImage}), WithNoInitialOSDs(), WithOSDVolumeStorage())
	terminated := false
	if c != nil {
		t.Cleanup(func() {
			if terminated {
				return
			}
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if err := c.Terminate(cleanup); err != nil {
				t.Error("module cleanup after failed qualification", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	original, err := c.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldID := original.GetContainerID()
	volume := c.stores.volume.name
	pool, err := c.CreatePool(ctx, PoolConfig{Name: "cancel-after-admission", Application: "rados", Replicas: 1, MinSize: 1, PGNum: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	client, err := c.clientContainerContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	retainedCancelBytes(t, ctx, client, pool.Name, "seed")
	// The cluster is new, so noout is unset; the flag is set and cleared
	// directly because a test of package ceph cannot import package override.
	if err := c.setOSDFlag(ctx, OSDFlagNoOut, true); err != nil {
		t.Fatal(err)
	}
	detached, err := c.DetachOSD(ctx, original.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := docker.Client.ContainerInspect(ctx, oldID, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("old container remains", err)
	}
	before := retainedCancelClock(t, ctx, docker.Client)
	reopenCtx, cancelReopen := context.WithCancel(ctx)
	defer cancelReopen()
	fault := &retainedCancelFault{next: runContainer, cancel: cancelReopen}
	previous := c.stores.runner
	c.stores.runner = fault.run
	t.Cleanup(func() { c.stores.runner = previous })
	candidate, reopenErr := c.ReopenOSD(reopenCtx, detached)
	if reopenErr == nil || reopenCtx.Err() == nil || ctx.Err() != nil {
		t.Fatalf("reopen did not observe the post-creation cancellation: err=%v caller=%v test=%v", reopenErr, reopenCtx.Err(), ctx.Err())
	}
	if candidate == nil || candidate.Container == nil || c.osdHandles.get(original.ID) != candidate || !detached.reopened || c.osdHandles.detachedOf(original.ID) != nil {
		t.Fatalf("canceled reopen lost consumed ownership: candidate=%v err=%v", candidate != nil, reopenErr)
	}
	if fault.calls != 1 || fault.canceled != 1 || fault.observed == nil || fault.observed != candidate.Container {
		t.Fatal("actual successful creation was not the owned candidate container")
	}
	firstID := candidate.GetContainerID()
	if firstID == oldID {
		t.Fatal("runtime reused the removed instance")
	}
	created := retainedCancelCreated(t, ctx, docker.Client, c.owner, before, retainedCancelClock(t, ctx, docker.Client))
	if len(created) != 1 || created[0] != firstID {
		t.Fatal("native creation inventory differs", created)
	}
	if again, err := c.ReopenOSD(ctx, detached); err == nil || again != nil || !strings.Contains(err.Error(), "already reopened") {
		t.Fatal("consumed detached handle admitted retry", err)
	}
	if fault.calls != 1 {
		t.Fatal("canceled creation was replayed")
	}
	// The cancellation ended the wait, not the daemon: the owned candidate is
	// settled through the ordinary lifecycle with a fresh context.
	raw, err := docker.Client.ContainerInspect(ctx, firstID, mobycl.ContainerInspectOptions{})
	if err != nil || raw.Container.State == nil || !raw.Container.State.Running {
		t.Fatal("canceled candidate container is not running", err)
	}
	// The candidate must finish booting before it is detached. A detach right
	// after the cancellation stops a daemon that is still starting: it ignored
	// the stop signal for the 10 s grace period in the first native attempt and
	// its down state was then never observed. Only a booted daemon is
	// expected to leave the OSDMap as down at a graceful stop.
	if err := c.waitOSDsUp(ctx, []int{original.ID}); err != nil {
		t.Fatal("canceled candidate never reached up/in", err)
	}
	second, err := c.DetachOSD(ctx, original.ID)
	if err != nil {
		t.Fatal("detach of the canceled candidate", err)
	}
	if _, err := docker.Client.ContainerInspect(ctx, firstID, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("canceled candidate container remains after detach", err)
	}
	c.stores.runner = previous
	replacement, err := c.ReopenOSD(ctx, second)
	if err != nil || replacement == nil || replacement.Container == nil {
		t.Fatal("reopen after the canceled candidate", err)
	}
	finalID := replacement.GetContainerID()
	if finalID == oldID || finalID == firstID {
		t.Fatal("final instance reuses a removed container")
	}
	if err := c.waitOSDsUp(ctx, []int{original.ID}); err != nil {
		t.Fatal(err)
	}
	state, err := c.removalOSDState(ctx, original.ID)
	if err != nil || state.UUID != detached.UUID() || !state.Up || !state.In {
		t.Fatal("native reopened registration differs", state, err)
	}
	fsid, err := c.clusterFSID(ctx)
	if err != nil || fsid != detached.ClusterFSID() || second.UUID() != detached.UUID() {
		t.Fatal("native cluster identity changed", err)
	}
	if err := c.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	retainedCancelBytes(t, ctx, client, pool.Name, "readback")
	if err := c.setOSDFlag(ctx, OSDFlagNoOut, false); err != nil {
		t.Fatal(err)
	}
	if err := c.Terminate(ctx); err != nil {
		t.Fatal("module cleanup", err)
	}
	terminated = true
	for _, id := range []string{firstID, finalID} {
		if _, err := docker.Client.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Fatal("container remains after cleanup", id, err)
		}
	}
	if _, err := docker.Client.VolumeInspect(ctx, volume, mobycl.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("owned retained volume remains", err)
	}
	if fault.calls != 1 {
		t.Fatal("cleanup replayed the canceled creation")
	}
	t.Logf("OSD_RETAINED_CANCEL_AFTER_CREATION osd=%d old=%s canceled=%s final=%s caller_error_is_canceled=%t runner_dispatches=1 create_events=1 candidate=owned_with_container detached=consumed reopen_retry=refused fresh_context_detach=confirmed second_reopen=ready registration=up_in same_uuid_fsid=true single_replica=true bytes=1048583 volume_cleanup=confirmed",
		original.ID, oldID, firstID, finalID, errors.Is(reopenErr, context.Canceled))
}

func retainedCancelBytes(t *testing.T, ctx context.Context, client testcontainers.Container, pool, phase string) {
	t.Helper()
	data, err := command(ctx, client, "python3", "-c", `import rados,sys
pool,phase=sys.argv[1:]
payload=bytes((i*13+3)%251 for i in range(1048583))
with rados.Rados(conffile='/etc/ceph/ceph.conf',name='client.admin',conf={'client_mount_timeout':'10','rados_mon_op_timeout':'10','rados_osd_op_timeout':'30'}) as c:
    with c.open_ioctx(pool) as io:
        if phase=='seed':io.write_full('known-payload',payload)
        size,_=io.stat('known-payload')
        assert size==len(payload) and io.read('known-payload',size)==payload
print('verified')`, pool, phase)
	if err != nil || string(data) != "verified\n" {
		t.Fatal("independent known-byte oracle", err, string(data))
	}
	t.Logf("OSD_RETAINED_CANCEL_BYTES phase=%s bytes=1048583 verified=true", phase)
}

func retainedCancelClock(t *testing.T, ctx context.Context, docker *mobycl.Client) string {
	t.Helper()
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return info.Info.SystemTime
}

func retainedCancelCreated(t *testing.T, ctx context.Context, docker *mobycl.Client, owner, since, until string) []string {
	t.Helper()
	stream := docker.Events(ctx, mobycl.EventsListOptions{Since: since, Until: until, Filters: make(mobycl.Filters).Add("type", "container").Add("event", "create").Add("label", fmt.Sprintf("%s=%s", clusterOwnerLabel, owner))})
	var ids []string
	for {
		select {
		case event := <-stream.Messages:
			ids = append(ids, event.Actor.ID)
		case err := <-stream.Err:
			if !errors.Is(err, io.EOF) {
				t.Fatal("native creation events", err)
			}
			return ids
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
