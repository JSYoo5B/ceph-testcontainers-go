//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"slices"
	"testing"
)

func TestECBelowKPlacement(t *testing.T) {
	p, err := planECBelowK([]int{9, 4, 7}, 4)
	if err != nil || !slices.Equal(p.Losses, []int{0, 2}) {
		t.Fatalf("plan %+v %v", p, err)
	}
	pool := ceph.PoolState{ID: 2, Name: "fixture", Size: 3, MinSize: 2}
	base := ceph.PoolPGSnapshot{FSID: "fixture", PoolBefore: pool, PoolAfter: pool, PGs: []ceph.PGState{{PGID: "2.0"}}}
	makeNow := func() ceph.PoolPGSnapshot {
		return ceph.PoolPGSnapshot{FSID: base.FSID, PoolBefore: pool, PoolAfter: pool, OSDMapEpochBefore: 20, OSDMapEpochAfter: 20, PGs: []ceph.PGState{{PGID: "2.0", Acting: []int{ecFragmentNone, 4, ecFragmentNone}, ActingPrimary: 4, ReportedEpoch: 20, State: "down"}}}
	}
	if err := checkECBelowKPlacement(p, base, makeNow(), p.Losses, 10); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*ceph.PoolPGSnapshot){"compacted": func(s *ceph.PoolPGSnapshot) { s.PGs[0].Acting = []int{4} }, "primary changed": func(s *ceph.PoolPGSnapshot) { s.PGs[0].ActingPrimary = 7 }, "silently lowered min_size": func(s *ceph.PoolPGSnapshot) { s.PoolAfter.MinSize = 1 }, "old report": func(s *ceph.PoolPGSnapshot) { s.PGs[0].ReportedEpoch = 10 }, "unbracketed": func(s *ceph.PoolPGSnapshot) { s.OSDMapEpochAfter++ }, "peered K-holder policy state": func(s *ceph.PoolPGSnapshot) { s.PGs[0].State = "undersized+degraded+peered" },
		"active below K": func(s *ceph.PoolPGSnapshot) { s.PGs[0].State = "active+undersized+degraded" }} {
		s := makeNow()
		edit(&s)
		if checkECBelowKPlacement(p, base, s, p.Losses, 10) == nil {
			t.Error("accepted", name)
		}
	}
	if checkECBelowKPlacement(p, base, makeNow(), []int{1}, 10) == nil {
		t.Error("admitted primary loss")
	}
	one := makeNow()
	one.PGs[0].Acting = []int{ecFragmentNone, 4, 7}
	one.PGs[0].State = "active+undersized+degraded"
	if err := checkECBelowKPlacement(p, base, one, p.Losses[:1], 10); err != nil {
		t.Fatal(err)
	}
	one.PGs[0].State = "undersized+degraded+peered"
	if checkECBelowKPlacement(p, base, one, p.Losses[:1], 10) == nil {
		t.Error("admitted failed exact-K control")
	}
}
func TestECBelowKUnknown(t *testing.T) {
	seed := ecFourTwoIO{PID: 40, Writer: 10, Reader: 11}
	makeNow := func() ecFourTwoIO {
		return ecFourTwoIO{Seq: 2, Phase: "blocked", PID: 40, Writer: 10, Reader: 11, Read: &ecFourTwoAttempt{Outcome: "unknown", Errno: 110}, Write: &ecFourTwoAttempt{Outcome: "unknown", Errno: 110}}
	}
	if err := checkECBelowKUnknown(seed, makeNow()); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*ecFourTwoIO){"replacement client": func(s *ecFourTwoIO) { s.Writer++ }, "wrong command": func(s *ecFourTwoIO) { s.Seq = 1 }, "read ack": func(s *ecFourTwoIO) { s.Read.Outcome = "acknowledged" }, "write refused": func(s *ecFourTwoIO) { s.Write.Errno = 13 }, "missing write": func(s *ecFourTwoIO) { s.Write = nil }} {
		s := makeNow()
		edit(&s)
		if checkECBelowKUnknown(seed, s) == nil {
			t.Error("accepted", name)
		}
	}
}
