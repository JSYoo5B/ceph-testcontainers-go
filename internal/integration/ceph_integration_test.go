//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestClusterLifecycle(t *testing.T) {
	parallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	started := time.Now()
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithOSDCount(2))
	cluster, err := ceph.Run(ctx, image, opts...)
	if cluster != nil {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if t.Failed() {
				for _, ctr := range append([]testcontainers.Container{cluster.Container}, osdContainers(cluster)...) {
					if ctr == nil {
						continue
					}
					logs, err := ctr.Logs(cleanupCtx)
					if err != nil {
						continue
					}
					data, _ := io.ReadAll(logs)
					logs.Close()
					if len(data) > 16000 {
						data = data[len(data)-16000:]
					}
					t.Logf("%s logs:\n%s", ctr.GetContainerID(), data)
				}
			}
			if err := cluster.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate cluster: %v", err)
			}
		})
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
	cephCommand(t, ctx, cluster, "osd", "pool", "create", "tc-poc", "8")
	cephCommand(t, ctx, cluster, "osd", "pool", "set", "tc-poc", "pg_autoscale_mode", "off")
	cephCommand(t, ctx, cluster, "osd", "pool", "application", "enable", "tc-poc", "rados")
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	logStatus(t, ctx, cluster, "initial")
	quorum, err := cluster.QuorumStatus(ctx)
	if err != nil || quorum.MonMap.FSID == "" {
		t.Fatal("original health oracle FSID unavailable", err)
	}
	integrationHealthDetails(t, ctx, cluster, quorum.MonMap.FSID, "initial-pool-clean")
	originalPGs := integrationPoolPGs(t, ctx, cluster, quorum.MonMap.FSID, "tc-poc", "initial-pool-clean")

	// A separate container proves MON discovery and direct OSD connectivity.
	client, err := testcontainers.Run(ctx, image, cluster.WithClient(),
		ceph.WithIdleEntrypoint(),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})),
	)
	if client != nil {
		testcontainers.CleanupContainer(t, client)
	}
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("testcontainers-ceph-roundtrip\n"), 256)
	if err := client.CopyToContainer(ctx, payload, "/tmp/payload", 0o600); err != nil {
		t.Fatal(err)
	}
	for i := range 16 {
		execCommand(t, ctx, client, "rados", "-p", "tc-poc", "put", fmt.Sprintf("object-%02d", i), "/tmp/payload")
	}
	verifyObjects(t, ctx, client, payload)
	t.Log("independent client: wrote and read 16 objects")

	// Failure injection keeps OSD identity/data; graceful removal deletes it.
	original := cluster.OSDs()[0]
	stopTimeout := 5 * time.Second
	if err := original.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, client, "rados", "-p", "tc-poc", "get", "object-00", "/tmp/during-outage")
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
	logStatus(t, ctx, cluster, fmt.Sprintf("added osd.%d", added.ID))
	addedPGs := integrationPoolPGs(t, ctx, cluster, quorum.MonMap.FSID, "tc-poc", "after-osd-add")
	if addedPGs.PoolBefore.ID != originalPGs.PoolBefore.ID {
		t.Fatal("OSD addition replaced original pool")
	}
	if err := cluster.RemoveOSD(ctx, original.ID); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	verifyObjects(t, ctx, client, payload)
	logStatus(t, ctx, cluster, fmt.Sprintf("removed original osd.%d; all objects intact", original.ID))
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
	if err := cluster.RemoveOSD(ctx, readded.ID); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	verifyObjects(t, ctx, client, payload)
	t.Logf("second add/remove cycle: osd.%d; total %s", readded.ID, time.Since(started).Round(time.Millisecond))
	integrationHealthDetails(t, ctx, cluster, quorum.MonMap.FSID, "after-osd-lifecycle")
	finalPGs := integrationPoolPGs(t, ctx, cluster, quorum.MonMap.FSID, "tc-poc", "after-osd-lifecycle")
	if finalPGs.PoolBefore.ID != originalPGs.PoolBefore.ID {
		t.Fatal("OSD lifecycle replaced original pool")
	}
}

func TestBootstrapFailureCleanup(t *testing.T) {
	image, opts := integrationImages(t)
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
