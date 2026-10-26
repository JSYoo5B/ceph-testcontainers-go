//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_osd_retained_concurrency))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// TestOSDRetainedConcurrentReopen checks native attachment and saved handles,
// independently of the fake runner's admission/uncertain-reply controls.
func TestOSDRetainedConcurrentReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithNoInitialOSDs(), ceph.WithOSDVolumeStorage())
	cluster, err := ceph.Run(ctx, image, opts...)
	terminated := false
	if cluster != nil {
		t.Cleanup(func() {
			if terminated {
				return
			}
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	original, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldRaw := original.Container
	oldID := oldRaw.GetContainerID()
	oldInspect := osdMemoryInspect(t, ctx, docker.Client, oldID)
	owner := oldInspect.Config.Labels["org.testcontainers.ceph.cluster"]
	if owner == "" {
		t.Fatal("missing cluster owner label")
	}
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "retained-concurrent", Application: "rados", Replicas: 1, MinSize: 1, PGNum: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	osdVolumeData(t, ctx, cluster, pool.Name, "seed")
	volume, _ := osdVolumeBacking(t, ctx, docker.Client, cluster)
	quorum, err := cluster.QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	registration := osdMemoryIdentities(t, ctx, cluster)
	noout, err := cluster.OverrideOSDFlag(ctx, "noout", true)
	if err != nil {
		t.Fatal(err)
	}
	detached, err := cluster.DetachOSD(ctx, original.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detached.UUID() != registration[original.ID] || detached.ClusterFSID() != quorum.MonMap.FSID {
		t.Fatal("detached identity differs")
	}
	if _, err := docker.Client.ContainerInspect(ctx, oldID, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("old instance remains", err)
	}

	before, err := docker.Client.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		osd *ceph.OSDContainer
		err error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	admitted := make(chan struct{})
	var once sync.Once
	for range 2 {
		go func() {
			<-start
			observed := &retainedReopenContext{Context: ctx, acquired: func() { once.Do(func() { close(admitted) }) }}
			osd, err := cluster.ReopenOSD(observed, detached)
			results <- result{osd, err}
		}()
	}
	close(start)
	select {
	case <-admitted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Deadline is read when ReopenOSD creates its bounded operation context,
	// after topology acquisition. The waiter records Done only before that
	// boundary: LockMutex reached its contended select, rather than sleeping
	// for an assumed startup interval.
	waiterBase, stopWaiter := context.WithCancel(ctx)
	defer stopWaiter()
	queued := false
	waiter := &retainedReopenContext{Context: waiterBase, waiting: func() { queued = true; stopWaiter() }}
	refused, waiterErr := cluster.ReopenOSD(waiter, detached)
	if !queued || refused != nil || !errors.Is(waiterErr, context.Canceled) {
		t.Fatalf("cancelled topology waiter queued=%v candidate=%v err=%v", queued, refused != nil, waiterErr)
	}
	var winner *ceph.OSDContainer
	refusedCount := 0
	for range 2 {
		r := <-results
		switch {
		case r.err == nil && r.osd != nil && winner == nil:
			winner = r.osd
		case r.err != nil && r.osd == nil && strings.Contains(r.err.Error(), "already reopened"):
			refusedCount++
		default:
			t.Fatalf("unexpected concurrent reopen candidate=%v err=%v", r.osd != nil, r.err)
		}
	}
	if winner == nil || refusedCount != 1 {
		t.Fatal("concurrent results differ")
	}
	newID := winner.GetContainerID()
	after, err := docker.Client.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		t.Fatal(err)
	}
	events := docker.Client.Events(ctx, mobycl.EventsListOptions{Since: before.Info.SystemTime, Until: after.Info.SystemTime, Filters: make(mobycl.Filters).Add("type", "container").Add("event", "create").Add("label", "org.testcontainers.ceph.cluster="+owner)})
	var created []string
eventLoop:
	for {
		select {
		case event := <-events.Messages:
			created = append(created, event.Actor.ID)
		case err := <-events.Err:
			if !errors.Is(err, io.EOF) {
				t.Fatal("read creation events", err)
			}
			break eventLoop
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if !slices.Equal(created, []string{newID}) {
		t.Fatalf("creation events %v, winner %s", created, newID)
	}
	if winner == original || newID == oldID || original.GetContainerID() != oldID || oldRaw.GetContainerID() != oldID {
		t.Fatal("instance handles were rebound")
	}
	owned := clusteraccess.OSDs(t, cluster)
	if len(owned) != 1 || owned[0] != winner {
		t.Fatal("owned replacement differs")
	}
	// Count every Docker container mounting this run's volume at the OSD data
	// path, including stopped candidates. The helper has a different target.
	list, err := docker.Client.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	var attached []string
	for _, ctr := range list.Items {
		for _, m := range ctr.Mounts {
			if m.Name == volume && m.Destination == "/var/lib/ceph/osd" {
				attached = append(attached, ctr.ID)
			}
		}
	}
	if !slices.Equal(attached, []string{newID}) {
		t.Fatalf("retained volume candidates %v, winner %s", attached, newID)
	}
	if err := original.Start(ctx); err == nil {
		t.Fatal("old descriptor restarted")
	}
	if err := oldRaw.Start(ctx); err == nil {
		t.Fatal("old raw handle restarted")
	}
	probe := ceph.NativeCommand{Argv: []string{"test", "-f", "/var/lib/ceph/osd/ceph-" + strconv.Itoa(original.ID) + "/ready"}, Timeout: 30 * time.Second, MaxStdout: 1024, MaxStderr: 1024, Label: "retained instance probe"}
	if _, err := cluster.RunNativeCommand(ctx, original, probe); err == nil {
		t.Fatal("old native target admitted")
	}
	if _, err := cluster.RunNativeCommand(ctx, oldRaw, probe); err == nil {
		t.Fatal("old raw native target admitted")
	}
	fresh, err := cluster.RunNativeCommand(ctx, winner, probe)
	if err != nil || fresh.ExitCode != 0 {
		t.Fatal("fresh native target failed", fresh.ExitCode, err)
	}
	if current := osdMemoryIdentities(t, ctx, cluster); len(current) != 1 || current[original.ID] != registration[original.ID] {
		t.Fatal("registration changed", current)
	}
	if current, _ := osdVolumeBacking(t, ctx, docker.Client, cluster); current != volume {
		t.Fatal("replacement volume changed")
	}
	currentQuorum, err := cluster.QuorumStatus(ctx)
	if err != nil || currentQuorum.MonMap.FSID != quorum.MonMap.FSID {
		t.Fatal("reopened FSID differs", err)
	}
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	osdVolumeData(t, ctx, cluster, pool.Name, "concurrent-reopened")
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	terminated = true
	if _, err := docker.Client.ContainerInspect(ctx, newID, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("replacement remains", err)
	}
	if _, err := docker.Client.VolumeInspect(ctx, volume, mobycl.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("owned volume remains", err)
	}
	t.Logf("OSD_RETAINED_CONCURRENT osd=%d uuid=%s old=%s replacement=%s winners=1 consumed_refusals=1 cancelled_waiter=queued creation_events=1 volume_candidates=1 saved_descriptor=refused saved_raw=refused saved_native=refused fresh_native=passed same_fsid=true single_replica=true bytes=1048583 cleanup=confirmed", original.ID, detached.UUID(), oldID, newID)
}
