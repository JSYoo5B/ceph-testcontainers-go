//go:build all || integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
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
				PGNumTarget        int     `json:"pg_num_target"`
				PGNumPending       int     `json:"pg_num_pending"`
				PGPlacementNum     int     `json:"pg_placement_num"`
				PGPlacementTarget  int     `json:"pg_placement_num_target"`
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
				PGNum: *p.PGNum, PGNumTarget: p.PGNumTarget, PGNumPending: p.PGNumPending, PGPlacementNum: p.PGPlacementNum,
				PGPlacementNumTarget: p.PGPlacementTarget, CRUSHRule: *p.CRUSHRule, AutoscaleMode: *p.AutoscaleMode, ErasureCodeProfile: *p.ErasureCodeProfile,
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
