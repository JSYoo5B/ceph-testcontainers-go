//go:build native_regression && (all || (integration && multicluster && features)) && (!ci || (ci_optional && (!ci_batch || ci_batch_rbd_journal_timing)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,rbd-aux

package rbd_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
)

// TestRBDJournalTimingProfile verifies the public startup condition independently
// of the central-config qualification probe. A conflicting central value stays
// unchanged throughout the owned daemon's one-second polling lifetime.
func TestRBDJournalTimingProfile(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	const pool = "journal-profile"
	harness.RBDMultiClusterPool(t, ctx, source, sourceClient, pool)
	harness.RBDMultiClusterPool(t, ctx, destination, destinationClient, pool)
	prior, err := override.SetConfig(ctx, destination, ceph.ConfigSetting{Section: "client", Name: "rbd_mirror_journal_poll_age", Value: "9"})
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := prior.Restore(cleanup); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	link, err := rbdmirror.Run(ctx, harness.NativeMirrorImage(t, "rbd-aux", harness.StaticFacts(source).Images.RBDAux), rbdmirror.WithConfig(rbdmirror.Config{Source: source, Destination: destination, Pool: pool, Mode: rbdmirror.ModeJournal, Timing: rbdmirror.TimingProfile{JournalPolling: true}}))
	if link != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
			defer stop()
			if err := link.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := link.WaitReceiverReady(ctx); err != nil {
		t.Fatal(err)
	}
	daemon := link.Daemons()[0]
	rbdJournalTimingEffective(t, ctx, daemon.Container, "/tmp/rbd-mirror.asok", 1)
	sourceStatus, err := harness.ObserveCluster(source).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	destinationStatus, err := harness.ObserveCluster(destination).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const name = "payload"
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "create", pool+"/"+name, "--size", "1M", "--image-feature", "layering,exclusive-lock,journaling")
	payload := harness.RBDMultiClusterPayload(1<<20, 43)
	rbdFanoutWrite(t, ctx, sourceClient, sourceStatus.FSID, pool, name, 0, payload)
	if err := link.EnableImage(ctx, name); err != nil {
		t.Fatal(err)
	}
	rbdFanoutWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, name, payload)
	if _, err := link.WaitReplayReady(ctx, name); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	for update := range 3 {
		payload = harness.RBDMultiClusterPayload(1<<20, 99+update)
		rbdFanoutWrite(t, ctx, sourceClient, sourceStatus.FSID, pool, name, 0, payload)
		rbdFanoutWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, name, payload)
	}
	t.Logf("RBD_JOURNAL_PUBLIC_PROFILE incremental=%s updates=3", time.Since(started))
	if err := link.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	rbdFanoutRead(t, ctx, destinationClient, destinationStatus.FSID, pool, name, payload)
	out, err := destination.Ceph(ctx, "config", "get", "client", "rbd_mirror_journal_poll_age")
	value, parseErr := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil || parseErr != nil || value != 9 {
		t.Fatalf("profile changed preexisting central entry: value=%s err=%v", out, err)
	}
	t.Log("RBD_JOURNAL_PUBLIC_PROFILE terminated owned replay; central entry unchanged; destination bytes preserved")
}
