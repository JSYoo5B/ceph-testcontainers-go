//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// This deliberately underprovisioned fixture proves that observations need not
// be healthy, active, complete, or current. Pool preparation uses the raw CLI:
// CreatePool correctly requires sufficient owned placement domains.
func TestPoolPGReportedBoundaries(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithNoInitialOSDs())
	cluster, err := ceph.Run(ctx, image, opts...)
	if cluster != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 2*time.Minute)
			defer done()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	quorum, err := cluster.QuorumStatus(ctx)
	if err != nil || quorum.MonMap.FSID == "" || len(cluster.OSDs()) != 0 {
		t.Fatal("cold PG original fixture identity unavailable", err)
	}
	fsid := quorum.MonMap.FSID
	const replicated, erasure, profile = "tc-pg-cold-replicated", "tc-pg-cold-erasure", "tc-pg-cold-profile"
	for _, args := range [][]string{
		{"osd", "pool", "create", replicated, "8", "8", "--autoscale-mode=off"},
		{"osd", "pool", "set", replicated, "size", "2"},
		{"osd", "pool", "set", replicated, "min_size", "1"},
		{"osd", "pool", "application", "enable", replicated, "rados"},
		{"osd", "erasure-code-profile", "set", profile, "plugin=jerasure", "technique=reed_sol_van", "k=2", "m=1", "crush-failure-domain=osd"},
		{"osd", "pool", "create", erasure, "8", "8", "erasure", profile, "--autoscale-mode=off"},
		{"osd", "pool", "set", erasure, "min_size", "2"},
		{"osd", "pool", "application", "enable", erasure, "rados"},
	} {
		cephCommand(t, ctx, cluster, args...)
	}
	identities := map[string]int64{}
	for _, pool := range []string{replicated, erasure} {
		cold := integrationPoolPGs(t, ctx, cluster, fsid, pool, "cold-zero-osds")
		identities[pool] = cold.PoolBefore.ID
		if len(cold.PGs) != 8 {
			t.Fatal("cold fixture did not report its eight PG rows")
		}
		for _, p := range cold.PGs {
			if p.State != "unknown" || len(p.Up) != 0 || len(p.Acting) != 0 || p.UpPrimary != -1 || p.ActingPrimary != -1 || p.ReportedEpoch != 0 || p.ReportedSequence != 0 || p.MappingEpoch != 0 || p.LastEpochClean != 0 || p.StatsInvalid || p.Stats != (ceph.PGStats{}) {
				t.Fatal("cold report lost valid unknown/empty/zero native fields")
			}
		}
	}
	first, err := cluster.AddOSD(ctx)
	if err != nil || first == nil || first.Container == nil {
		t.Fatal("first PG fixture OSD unavailable", err)
	}
	deadline, done := context.WithTimeout(ctx, 90*time.Second)
	defer done()
	for {
		observed := integrationPoolPGs(t, deadline, cluster, fsid, erasure, "one-osd-erasure")
		if observed.PoolBefore.ID != identities[erasure] {
			t.Fatal("EC pool identity changed")
		}
		incomplete := len(observed.PGs) == 8
		for _, p := range observed.PGs {
			incomplete = incomplete && slices.Contains(strings.Split(p.State, "+"), "incomplete") &&
				slices.Equal(p.Up, []int{first.ID, 2147483647, 2147483647}) && slices.Equal(p.Acting, p.Up) &&
				p.UpPrimary == first.ID && p.ActingPrimary == first.ID
		}
		if incomplete {
			break
		}
		select {
		case <-deadline.Done():
			t.Fatal("EC incomplete/NONE report unavailable", deadline.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
	client, err := testcontainers.Run(ctx, image, cluster.WithClient(), testcontainers.WithEntrypoint("sleep"),
		testcontainers.WithCmd("infinity"), testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})))
	if client != nil {
		testcontainers.CleanupContainer(t, client)
	}
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("PG report visibility is asynchronous\n"), 128)[:4096]
	if err := client.CopyToContainer(ctx, payload, "/tmp/pg-payload", 0o600); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, client, "rados", "-p", replicated, "put", "retained", "/tmp/pg-payload")
	execCommand(t, ctx, client, "rados", "-p", replicated, "get", "retained", "/tmp/pg-result")
	r, err := client.CopyFileFromContainer(ctx, "/tmp/pg-result")
	if err != nil {
		t.Fatal(err)
	}
	actual, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(actual, payload) {
		t.Fatal("replicated PG fixture retained bytes changed", err)
	}
	t.Logf("POOL_PGS_BYTES phase=replicated-readback fsid=%s pool_id=%d bytes=%d sha256=%x", fsid, identities[replicated], len(actual), sha256.Sum256(actual))
	observed := integrationPoolPGs(t, ctx, cluster, fsid, replicated, "after-replicated-io")
	if observed.PoolBefore.ID != identities[replicated] {
		t.Fatal("replicated pool identity changed")
	}
	for {
		active := len(observed.PGs) == 8
		for _, p := range observed.PGs {
			active = active && slices.Contains(strings.Split(p.State, "+"), "active") &&
				slices.Contains(strings.Split(p.State, "+"), "undersized")
		}
		if active {
			break
		}
		select {
		case <-deadline.Done():
			t.Fatal("one-OSD replicated active/undersized report unavailable", deadline.Err())
		case <-time.After(200 * time.Millisecond):
		}
		observed = integrationPoolPGs(t, deadline, cluster, fsid, replicated, "await-replicated-active")
		if observed.PoolBefore.ID != identities[replicated] {
			t.Fatal("replicated pool identity changed")
		}
	}
	// A successfully read object need not yet appear in reported counters. No
	// zero-counter or positive-counter timing assumption is made here.
	t.Logf("POOL_PGS_BOUNDARIES fsid=%s replicated_id=%d erasure_id=%d osd_id=%d complete=true", fsid, identities[replicated], identities[erasure], first.ID)
}
