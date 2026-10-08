//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Compare complete public fields with independently decoded CLI observations.
// The surrounding reads must agree before a comparison is made. This is a test
// oracle at a quiet observation, not an atomicity guarantee from PoolPGs.
func integrationPoolPGs(t *testing.T, ctx context.Context, cluster *ceph.Container, fsid, pool, phase string) ceph.PoolPGSnapshot {
	t.Helper()
	for {
		before := integrationNativePoolPGs(t, ctx, cluster, fsid, pool)
		actual, err := cluster.PoolPGs(ctx, pool)
		if err != nil {
			t.Fatal("PoolPGs failed", phase, err)
		}
		after := integrationNativePoolPGs(t, ctx, cluster, fsid, pool)
		if reflect.DeepEqual(before, after) {
			if !reflect.DeepEqual(actual, after) {
				t.Fatal("PoolPGs differs from stable native JSON", phase)
			}
			data, err := json.Marshal(actual)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("POOL_PGS phase=%s fsid=%s pool=%s pool_id=%d pg_ready=%t rows=%d native_match=true snapshot_sha256=%x snapshot=%s", phase, fsid, pool, actual.PoolBefore.ID, actual.PGReady, len(actual.PGs), sha256.Sum256(data), data)
			return actual
		}
		select {
		case <-ctx.Done():
			t.Fatal("native PG report did not stabilize for comparison", phase, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func integrationNativePoolPGs(t *testing.T, ctx context.Context, cluster *ceph.Container, fsid, name string) ceph.PoolPGSnapshot {
	t.Helper()
	checkFSID := func() {
		data, err := cluster.Ceph(ctx, "fsid")
		if err != nil || strings.TrimSpace(string(data)) != fsid {
			t.Fatal("native PG oracle lost original FSID", err)
		}
	}
	readPool := func() (ceph.PoolState, uint32) {
		data, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
		if err != nil {
			t.Fatal("native PG oracle OSDMap failed", err)
		}
		var native struct {
			FSID  string  `json:"fsid"`
			Epoch *uint32 `json:"epoch"`
			Pools *[]struct {
				ID                 *int64  `json:"pool"`
				Name               string  `json:"pool_name"`
				Type               *int    `json:"type"`
				Size               *int    `json:"size"`
				MinSize            *int    `json:"min_size"`
				PGNum              *int    `json:"pg_num"`
				CRUSHRule          *int    `json:"crush_rule"`
				AutoscaleMode      *string `json:"pg_autoscale_mode"`
				ErasureCodeProfile *string `json:"erasure_code_profile"`
				Flags              *string `json:"flags_names"`
				QuotaBytes         *uint64 `json:"quota_max_bytes"`
				QuotaObjects       *uint64 `json:"quota_max_objects"`
			} `json:"pools"`
		}
		if err := json.Unmarshal(data, &native); err != nil || native.FSID != fsid || native.Epoch == nil || native.Pools == nil {
			t.Fatal("native PG oracle OSDMap JSON unavailable", err)
		}
		seenNames, seenIDs := map[string]bool{}, map[int64]bool{}
		var result ceph.PoolState
		for _, p := range *native.Pools {
			if p.ID == nil || p.Name == "" || seenNames[p.Name] || seenIDs[*p.ID] {
				t.Fatal("native PG oracle pool identity is ambiguous")
			}
			seenNames[p.Name], seenIDs[*p.ID] = true, true
			if p.Name != name {
				continue
			}
			if p.Type == nil || p.Size == nil || p.MinSize == nil || p.PGNum == nil || p.CRUSHRule == nil || p.AutoscaleMode == nil || p.ErasureCodeProfile == nil || p.Flags == nil || p.QuotaBytes == nil || p.QuotaObjects == nil {
				t.Fatal("native PG oracle pool policy is incomplete")
			}
			kind := "replicated"
			if *p.Type == 3 {
				kind = "erasure"
			} else if *p.Type != 1 {
				t.Fatal("native PG oracle pool type unavailable")
			}
			result = ceph.PoolState{ID: *p.ID, Name: p.Name, Type: kind, Size: *p.Size, MinSize: *p.MinSize,
				PGNum: *p.PGNum, CRUSHRule: *p.CRUSHRule, AutoscaleMode: *p.AutoscaleMode, ErasureCodeProfile: *p.ErasureCodeProfile,
				Flags: *p.Flags, Quota: ceph.PoolQuota{MaxBytes: *p.QuotaBytes, MaxObjects: *p.QuotaObjects}}
		}
		if result.Name != name {
			t.Fatal("native PG oracle selected pool absent")
		}
		return result, *native.Epoch
	}
	checkFSID()
	before, epochBefore := readPool()
	data, err := cluster.Ceph(ctx, "pg", "ls-by-pool", name, "--format", "json")
	if err != nil {
		t.Fatal("native PG oracle report failed", err)
	}
	var native struct {
		Ready *bool `json:"pg_ready"`
		Rows  []struct {
			ID             string  `json:"pgid"`
			State          string  `json:"state"`
			Up             *[]int  `json:"up"`
			Acting         *[]int  `json:"acting"`
			UpPrimary      *int    `json:"up_primary"`
			ActingPrimary  *int    `json:"acting_primary"`
			ReportedEpoch  *uint32 `json:"reported_epoch"`
			ReportedSeq    *uint64 `json:"reported_seq"`
			MappingEpoch   *uint32 `json:"mapping_epoch"`
			LastEpochClean *uint32 `json:"last_epoch_clean"`
			StatsInvalid   *bool   `json:"stats_invalid"`
			Sum            *struct {
				Objects          *int64 `json:"num_objects"`
				Bytes            *int64 `json:"num_bytes"`
				Copies           *int64 `json:"num_object_copies"`
				ObjectsDegraded  *int64 `json:"num_objects_degraded"`
				ObjectsMisplaced *int64 `json:"num_objects_misplaced"`
				ObjectsUnfound   *int64 `json:"num_objects_unfound"`
			} `json:"stat_sum"`
		} `json:"pg_stats"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native.Ready == nil {
		t.Fatal("native PG oracle report JSON unavailable", err)
	}
	// Native PGMap omits pg_stats entirely when the filtered set is empty.
	// An explicit null is malformed; absence is a successful empty report.
	if native.Rows == nil {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil || fields["pg_stats"] != nil {
			t.Fatal("native PG oracle report array is invalid", err)
		}
	}
	rows := make([]ceph.PGState, 0, len(native.Rows))
	for _, p := range native.Rows {
		if p.ID == "" || p.State == "" || p.Up == nil || p.Acting == nil || p.UpPrimary == nil || p.ActingPrimary == nil || p.ReportedEpoch == nil || p.ReportedSeq == nil || p.MappingEpoch == nil || p.LastEpochClean == nil || p.StatsInvalid == nil || p.Sum == nil {
			t.Fatal("native PG oracle row incomplete")
		}
		s := p.Sum
		if s.Objects == nil || s.Bytes == nil || s.Copies == nil || s.ObjectsDegraded == nil || s.ObjectsMisplaced == nil || s.ObjectsUnfound == nil {
			t.Fatal("native PG oracle counters incomplete")
		}
		rows = append(rows, ceph.PGState{PGID: p.ID, State: p.State, Up: *p.Up, Acting: *p.Acting,
			UpPrimary: *p.UpPrimary, ActingPrimary: *p.ActingPrimary, ReportedEpoch: *p.ReportedEpoch, ReportedSequence: *p.ReportedSeq,
			MappingEpoch: *p.MappingEpoch, LastEpochClean: *p.LastEpochClean, StatsInvalid: *p.StatsInvalid,
			Stats: ceph.PGStats{Objects: *s.Objects, Bytes: *s.Bytes, ObjectCopies: *s.Copies,
				ObjectsDegraded: *s.ObjectsDegraded, ObjectsMisplaced: *s.ObjectsMisplaced, ObjectsUnfound: *s.ObjectsUnfound}})
	}
	after, epochAfter := readPool()
	checkFSID()
	if before.ID != after.ID || before.Name != after.Name {
		t.Fatal("native PG oracle selected pool replaced")
	}
	return ceph.PoolPGSnapshot{FSID: fsid, PoolBefore: before, PoolAfter: after, OSDMapEpochBefore: epochBefore,
		OSDMapEpochAfter: epochAfter, PGReady: *native.Ready, PGs: rows}
}

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
