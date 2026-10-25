//go:build all || integration

package integration_test

import (
	"slices"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func secondFaultPoolConfig(erasure bool) ceph.PoolConfig {
	p := ceph.PoolConfig{Name: "tc-paths", PGNum: 1, Replicas: 3, MinSize: 2, Application: "rados"}
	if erasure {
		p.Replicas = 0
		p.FailureDomain = "osd"
		p.ErasureCode = &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true}
	}
	return p
}

// Replicated vectors omit unavailable members; EC vectors retain shard indices
// with NONE in the missing slot. The surviving two must be the original shards,
// with the original primary, rather than any two IDs in a shifted vector.
func secondFaultTwoSurvivors(pg, original ceph.PGState, missing int, erasure bool) bool {
	if !erasure {
		return len(pg.Acting) == 2
	}
	if len(original.Acting) != 3 || len(pg.Acting) != 3 || pg.ActingPrimary != original.ActingPrimary || pg.ReportedEpoch <= original.ReportedEpoch {
		return false
	}
	slot := slices.Index(original.Acting, missing)
	if slot < 0 || missing == original.ActingPrimary {
		return false
	}
	for i, id := range original.Acting {
		want := id
		if i == slot {
			want = ecFragmentNone
		}
		if pg.Acting[i] != want {
			return false
		}
	}
	return true
}

func ecReconstructionTarget(base backfillBaseline, original ceph.PGState, missing, target int) bool {
	if !secondFaultTwoSurvivors(ceph.PGState{Acting: base.Acting, ActingPrimary: original.ActingPrimary, ReportedEpoch: uint32(base.Epoch)}, original, missing, true) || len(base.Up) != 3 || target < 0 || slices.Contains(original.Acting, target) {
		return false
	}
	for i, id := range original.Acting {
		if id == missing {
			id = target
		}
		if base.Up[i] != id {
			return false
		}
	}
	return base.Target == target
}
