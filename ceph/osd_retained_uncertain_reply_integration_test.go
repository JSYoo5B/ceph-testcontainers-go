//go:build integration && ci_optional && ci_batch_retained_uncertain_reply

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

// TestRetainedOSDUncertainReopenReply withholds a completed runner's reply,
// rather than simulating network loss or replacing Docker with a fake engine.
// Same-package placement uses the existing private OSD runner test seam.
func TestRetainedOSDUncertainReopenReply(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	image, clientImage := os.Getenv("CEPH_TEST_ESSENTIAL_IMAGE"), os.Getenv("CEPH_TEST_CLIENT_IMAGE")
	if image == "" || clientImage == "" {
		t.Fatal("explicit essential and client images are required")
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	c, err := Run(ctx, RunOptions{}.WithImageSet(ImageSet{Essential: image, Client: clientImage}).WithCustomizers(WithNoInitialOSDs(), WithOSDVolumeStorage()))
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
	pool, err := c.CreatePool(ctx, PoolConfig{Name: "uncertain-reopen", Application: "rados", Replicas: 1, MinSize: 1, PGNum: 1})
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
	retainedUncertainBytes(t, ctx, client, pool.Name, "seed")
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
	if !c.nativeCmds.fenced[oldID] {
		t.Fatal("detached instance lost its fence")
	}
	before := retainedUncertainClock(t, ctx, docker.Client)
	injected := errors.New("injected client-side completed-runner reply loss")
	fault := &retainedReplyFault{next: runContainer, lost: injected}
	previous := c.stores.runner
	c.stores.runner = fault.run
	t.Cleanup(func() { c.stores.runner = previous })
	candidate, reopenErr := c.ReopenOSD(ctx, detached)
	if !errors.Is(reopenErr, injected) || candidate == nil || candidate.Container != nil || c.osdHandles.get(original.ID) != candidate || !detached.reopened || c.osdHandles.detachedOf(original.ID) != nil {
		t.Fatalf("uncertain result lost consumed ownership: candidate=%v err=%v", candidate != nil, reopenErr)
	}
	if fault.calls != 1 || fault.observed == nil {
		t.Fatal("actual successful creation was not observed")
	}
	hiddenID := fault.observed.GetContainerID()
	if hiddenID == oldID {
		t.Fatal("runtime reused the removed instance")
	}
	raw, err := docker.Client.ContainerInspect(ctx, hiddenID, mobycl.ContainerInspectOptions{})
	if err != nil || raw.Container.State == nil || !raw.Container.State.Running || raw.Container.Config == nil || raw.Container.Config.Labels[clusterOwnerLabel] != c.startup.ownerID() || raw.Container.Image != image {
		t.Fatal("withheld native instance identity/ownership differs", err)
	}
	mounted := false
	for _, m := range raw.Container.Mounts {
		if m.Name == volume && m.Destination == "/var/lib/ceph/osd" {
			mounted = true
		}
	}
	if !mounted {
		t.Fatal("withheld instance does not mount retained volume")
	}
	created := retainedUncertainCreated(t, ctx, docker.Client, c.startup.ownerID(), before, retainedUncertainClock(t, ctx, docker.Client))
	if len(created) != 1 || created[0] != hiddenID {
		t.Fatal("native creation inventory differs", created)
	}
	for range 2 {
		if again, err := c.ReopenOSD(ctx, detached); err == nil || again != nil || !strings.Contains(err.Error(), "already reopened") {
			t.Fatal("consumed detached handle admitted retry", err)
		}
		if again, err := c.DetachOSD(ctx, original.ID); err == nil || again != nil || !strings.Contains(err.Error(), "no container") {
			t.Fatal("nil-container candidate admitted detach", err)
		}
	}
	if fault.calls != 1 {
		t.Fatal("uncertain creation was replayed")
	}
	if err := c.RemoveOSD(ctx, original.ID); err == nil || !strings.Contains(err.Error(), "last OSD") {
		t.Fatal("last uncertain OSD was purged", err)
	}
	// Observe native registration and data through healthy control/client paths.
	// Never invoke a method on candidate's nil embedded Container or claim the
	// unknown replacement CID is fenced against network-attached callers.
	if err := c.waitOSDsUp(ctx, []int{original.ID}); err != nil {
		t.Fatal(err)
	}
	state, err := c.removalOSDState(ctx, original.ID)
	if err != nil || state.UUID != detached.UUID() || !state.Up || !state.In {
		t.Fatal("native reopened registration differs", state, err)
	}
	fsid, err := c.clusterFSID(ctx)
	if err != nil || fsid != detached.ClusterFSID() {
		t.Fatal("native cluster FSID changed", err)
	}
	if err := c.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	retainedUncertainBytes(t, ctx, client, pool.Name, "readback")
	if err := c.setOSDFlag(ctx, OSDFlagNoOut, false); err != nil {
		t.Fatal(err)
	}
	// The module knows no container handle for this candidate. Only its normal
	// cluster-owner label reconciliation can find the actual running instance.
	// Do not call fault.observed.Terminate on this success path.
	if err := c.Terminate(ctx); err != nil {
		t.Fatal("module reconciliation cleanup", err)
	}
	terminated = true
	if _, err := docker.Client.ContainerInspect(ctx, hiddenID, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("module owner-label scan left withheld instance", err)
	}
	if _, err := docker.Client.VolumeInspect(ctx, volume, mobycl.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("owned retained volume remains", err)
	}
	if fault.calls != 1 {
		t.Fatal("cleanup replayed uncertain creation")
	}
	t.Logf("OSD_RETAINED_UNCERTAIN_REPLY osd=%d old=%s observed=%s source=injected_client_reply_after_real_ready runner_dispatches=1 create_events=1 candidate=owned_without_container detached=consumed reopen_retry=refused detach_retry=refused old_fence=retained registration=up_in same_uuid_fsid=true single_replica=true bytes=1048583 owner_label_cleanup=confirmed volume_cleanup=confirmed", original.ID, oldID, hiddenID)
}

func retainedUncertainBytes(t *testing.T, ctx context.Context, client testcontainers.Container, pool, phase string) {
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
	t.Logf("OSD_RETAINED_UNCERTAIN_BYTES phase=%s bytes=1048583 verified=true", phase)
}
func retainedUncertainClock(t *testing.T, ctx context.Context, docker *mobycl.Client) string {
	t.Helper()
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return info.Info.SystemTime
}
func retainedUncertainCreated(t *testing.T, ctx context.Context, docker *mobycl.Client, owner, since, until string) []string {
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
