//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/finalize"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestClusterLifecycle(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	started := time.Now()
	image, opts := harness.IntegrationImages(t)
	opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(2)))
	cluster, err := ceph.Run(ctx, image, opts...)
	// One finalization archives a failure before the client and the
	// cluster are removed; go test -artifacts keeps the archive.
	var finalization *finalize.Finalization
	if cluster != nil {
		var registerErr error
		finalization, registerErr = finalize.TerminateOnCleanup(t, cluster, finalize.Config{Archive: true, Scenario: "cluster-lifecycle"})
		if registerErr != nil {
			t.Fatal(registerErr)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("bootstrap: %s", time.Since(started).Round(time.Millisecond))
	version, err := cluster.Ceph(ctx, "--version")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("image=%s version=%s", image, strings.TrimSpace(string(version)))
	harness.CephCommand(t, ctx, cluster, "osd", "pool", "create", "tc-poc", "8")
	harness.CephCommand(t, ctx, cluster, "osd", "pool", "set", "tc-poc", "pg_autoscale_mode", "off")
	harness.CephCommand(t, ctx, cluster, "osd", "pool", "application", "enable", "tc-poc", "rados")
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	harness.LogStatus(t, ctx, cluster, "initial")
	quorum, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
	if err != nil || quorum.MonMap.FSID == "" {
		t.Fatal("original health oracle FSID unavailable", err)
	}
	harness.IntegrationHealthDetails(t, ctx, cluster, quorum.MonMap.FSID, "initial-pool-clean")
	originalPGs := integrationPoolPGs(t, ctx, cluster, quorum.MonMap.FSID, "tc-poc", "initial-pool-clean")

	// A separate container proves MON discovery and direct OSD connectivity.
	client, err := testcontainers.Run(ctx, harness.NativeClientImage(t, image), cluster.WithClient(),
		ceph.WithIdleEntrypoint(),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})),
	)
	if client != nil {
		finalization.Cleanup("client", func(ctx context.Context) error { return client.Terminate(ctx) })
	}
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("testcontainers-ceph-roundtrip\n"), 256)
	if err := client.CopyToContainer(ctx, payload, "/tmp/payload", 0o600); err != nil {
		t.Fatal(err)
	}
	for i := range 16 {
		harness.ExecCommand(t, ctx, client, "rados", "-p", "tc-poc", "put", fmt.Sprintf("object-%02d", i), "/tmp/payload")
	}
	harness.VerifyObjects(t, ctx, client, payload)
	t.Log("independent client: wrote and read 16 objects")

	// Failure injection keeps OSD identity/data; graceful removal deletes it.
	original := clusteraccess.OSDs(t, cluster)[0]
	stopTimeout := 5 * time.Second
	if err := original.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	harness.ExecCommand(t, ctx, client, "rados", "-p", "tc-poc", "get", "object-00", "/tmp/during-outage")
	if err := original.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("osd.%d stop/start: read succeeded with one replica unavailable", original.ID)

	added, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	harness.LogStatus(t, ctx, cluster, fmt.Sprintf("added osd.%d", added.ID))
	addedPGs := integrationPoolPGs(t, ctx, cluster, quorum.MonMap.FSID, "tc-poc", "after-osd-add")
	if addedPGs.PoolBefore.ID != originalPGs.PoolBefore.ID {
		t.Fatal("OSD addition replaced original pool")
	}
	if err := original.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	harness.VerifyObjects(t, ctx, client, payload)
	harness.LogStatus(t, ctx, cluster, fmt.Sprintf("removed original osd.%d; all objects intact", original.ID))
	removedPGs := integrationPoolPGs(t, ctx, cluster, quorum.MonMap.FSID, "tc-poc", "after-original-osd-remove")
	if removedPGs.PoolBefore.ID != originalPGs.PoolBefore.ID {
		t.Fatal("OSD removal replaced original pool")
	}

	readded, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	if err := readded.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	harness.VerifyObjects(t, ctx, client, payload)
	t.Logf("second add/remove cycle: osd.%d; total %s", readded.ID, time.Since(started).Round(time.Millisecond))
	harness.IntegrationHealthDetails(t, ctx, cluster, quorum.MonMap.FSID, "after-osd-lifecycle")
	finalPGs := integrationPoolPGs(t, ctx, cluster, quorum.MonMap.FSID, "tc-poc", "after-osd-lifecycle")
	if finalPGs.PoolBefore.ID != originalPGs.PoolBefore.ID {
		t.Fatal("OSD lifecycle replaced original pool")
	}
}

func TestBootstrapFailureCleanup(t *testing.T) {
	image, opts := harness.IntegrationImages(t)
	opts = append(opts,
		ceph.WithStartupTimeout(15*time.Second),
		testcontainers.WithEntrypoint("/bin/sh", "-c", "exit 23"),
	)
	cluster, err := ceph.Run(t.Context(), image, opts...)
	if cluster != nil {
		testcontainers.CleanupContainer(t, cluster)
	}
	if err == nil || cluster == nil {
		t.Fatalf("expected inspectable partial cluster, got cluster=%v error=%v", cluster, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatalf("repeated cleanup: %v", err)
	}
}

// An image that cannot be pulled fails Run with the engine's error before a
// MON container exists; the partial cluster still cleans up its networks.
func TestRunMissingImage(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	const missing = "ceph-testcontainers-go.invalid/missing:0"
	cluster, err := ceph.Run(ctx, missing)
	if cluster != nil {
		testcontainers.CleanupContainer(t, cluster)
	}
	if err == nil || !strings.Contains(err.Error(), "run ceph monitor") {
		t.Fatalf("Run with %s: %v", missing, err)
	}
	if cluster == nil {
		t.Fatal("Run returned no partial cluster to clean up")
	}
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatalf("repeated cleanup: %v", err)
	}
	t.Logf("Run with %s failed without a MON container: %v", missing, err)
}
