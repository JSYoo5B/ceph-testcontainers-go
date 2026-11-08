//go:build native_regression && (all || (integration && multicluster && features)) && (!ci || (ci_optional && (!ci_batch || ci_batch_rbd_journal_timing)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,rbd-aux

package rbd_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
)

// TestRBDJournalPollingTiming records descriptive timings for fresh journal replay instances because
// start_live_replay captures rbd_mirror_journal_poll_age. A daemon config
// readback is not evidence that an already enrolled image changed its poll age.
func TestRBDJournalPollingTiming(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	const pool = "journal-timing"
	harness.RBDMultiClusterPool(t, ctx, source, sourceClient, pool)
	harness.RBDMultiClusterPool(t, ctx, destination, destinationClient, pool)
	link, err := rbdmirror.Run(ctx, harness.NativeMirrorImage(t, "rbd-aux", harness.StaticFacts(source).Images.RBDAux), rbdmirror.WithConfig(rbdmirror.Config{Source: source, Destination: destination, Pool: pool, Mode: rbdmirror.ModeJournal}))
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
	daemons := link.Daemons()
	if len(daemons) != 1 {
		t.Fatalf("daemons=%d", len(daemons))
	}
	daemon := daemons[0]
	const socket = "/tmp/rbd-mirror.asok" // The owned daemon socket used by Daemon.Status.
	var sourceFSID, destinationFSID string
	for c, id := range map[*ceph.Container]*string{source: &sourceFSID, destination: &destinationFSID} {
		status, err := harness.ObserveCluster(c).Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		*id = status.FSID
	}
	version := harness.ExecOutput(t, ctx, daemon.Container, "rbd-mirror", "--version")
	t.Logf("RBD_JOURNAL_TIMING_VERSION %s", strings.TrimSpace(version))
	for trial, seconds := range []int{5, 1, 5, 1, 5, 1} {
		t.Run(fmt.Sprintf("trial-%d-poll-%d", trial, seconds), func(t *testing.T) {
			started := time.Now()
			var phase time.Duration
			change, err := override.SetConfig(ctx, destination, ceph.ConfigSetting{Section: daemon.ClientName, Name: "rbd_mirror_journal_poll_age", Value: strconv.Itoa(seconds)})
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
				defer stop()
				if err := change.Restore(cleanup); err != nil {
					t.Error(err)
				}
				rbdJournalTimingEffective(t, cleanup, daemon.Container, socket, 5)
				t.Logf("RBD_JOURNAL_TIMING_TRIAL poll=%d incremental=%s scenario=%s failed=%t", seconds, phase, time.Since(started), t.Failed())
			}()
			if err != nil {
				t.Fatal(err)
			}
			rbdJournalTimingEffective(t, ctx, daemon.Container, socket, seconds)
			name := fmt.Sprintf("fresh-%d", trial)
			image := pool + "/" + name
			harness.ExecCommand(t, ctx, sourceClient, "rbd", "create", image, "--size", "1M", "--image-feature", "layering,exclusive-lock,journaling")
			data := harness.RBDMultiClusterPayload(1<<20, 43)
			rbdFanoutWrite(t, ctx, sourceClient, sourceFSID, pool, name, 0, data)
			enrolled := time.Now()
			if err := link.EnableImage(ctx, name); err != nil {
				t.Fatal(err)
			}
			rbdFanoutWaitBytes(t, ctx, destinationClient, destinationFSID, pool, name, data)
			if _, err := link.WaitReplayReady(ctx, name); err != nil {
				t.Fatal(err)
			}
			t.Logf("RBD_JOURNAL_TIMING_ENROLL poll=%d elapsed=%s", seconds, time.Since(enrolled))
			for update := range 3 {
				data = harness.RBDMultiClusterPayload(1<<20, 71+update)
				write := time.Now()
				rbdFanoutWrite(t, ctx, sourceClient, sourceFSID, pool, name, 0, data)
				rbdFanoutWaitBytes(t, ctx, destinationClient, destinationFSID, pool, name, data)
				elapsed := time.Since(write)
				phase += elapsed
				t.Logf("RBD_JOURNAL_TIMING_UPDATE poll=%d update=%d elapsed=%s", seconds, update, elapsed)
			}
			// Disable this image before restoring the daemon setting. No later trial
			// reuses its captured poll interval; restoration is checked on the daemon,
			// not claimed to retime an already-live JournalPlayer.
			if _, err := link.SourceRBD(ctx, "mirror", "image", "disable", image); err != nil {
				t.Fatal(err)
			}
			rbdFanoutRead(t, ctx, sourceClient, sourceFSID, pool, name, data)
		})
	}
}

func rbdJournalTimingEffective(t *testing.T, parent context.Context, daemon testcontainers.Container, socket string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	for {
		output := harness.ExecOutput(t, ctx, daemon, "ceph", "--admin-daemon", socket, "config", "get", "rbd_mirror_journal_poll_age")
		var value map[string]string
		if err := json.Unmarshal([]byte(output), &value); err != nil {
			t.Fatal(err)
		}
		number, err := strconv.ParseFloat(value["rbd_mirror_journal_poll_age"], 64)
		if err == nil && number == float64(want) {
			t.Logf("RBD_JOURNAL_TIMING_EFFECTIVE poll=%g", number)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("mirror did not consume poll=%d: %s", want, output)
		case <-time.After(200 * time.Millisecond):
		}
	}
}
