//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestECFourTwoPlan(t *testing.T) {
	p, err := planECFourTwo([]int{7, 4, 9, 2, 8, 3}, 9, 5)
	if err != nil {
		t.Fatal(err)
	}
	lost, err := p.losses(7)
	if err != nil || !slices.Equal(lost, []int{2, 1}) {
		t.Fatalf("loss plan %v: %v", lost, err)
	}
	want, err := p.actingWithout(lost)
	if err != nil || !slices.Equal(want, []int{7, ecFragmentNone, ecFragmentNone, 2, 8, 3}) {
		t.Fatalf("shard slots %v: %v", want, err)
	}
	for _, acting := range [][]int{{0, 1, 2, 3, 4}, {0, 1, 2, 3, 4, 4}, {0, 1, 2, 3, 4, ecFragmentNone}, {0, 1, 2, 3, 4, -1}} {
		if _, err := planECFourTwo(acting, 0, 5); err == nil {
			t.Fatalf("accepted %v", acting)
		}
	}
	if _, err := p.losses(9); err == nil {
		t.Fatal("admitted unchanged primary")
	}
	if _, err := p.actingWithout([]int{2, 2}); err == nil {
		t.Fatal("admitted duplicate fragment")
	}
}

func TestECFourTwoPlacement(t *testing.T) {
	for _, minSize := range []int{4, 5} {
		p, _ := planECFourTwo([]int{0, 1, 2, 3, 4, 5}, 0, minSize)
		pool := ceph.PoolState{ID: 2, Name: "fixture", Size: 6, MinSize: minSize}
		base := ceph.PoolPGSnapshot{FSID: "fixture", PoolBefore: pool, PoolAfter: pool, PGs: []ceph.PGState{{PGID: "2.0", ReportedEpoch: 10}}}
		makeNow := func() ceph.PoolPGSnapshot {
			state := "active+undersized+degraded"
			if minSize == 5 {
				state = "undersized+degraded+peered"
			}
			return ceph.PoolPGSnapshot{FSID: base.FSID, PoolBefore: pool, PoolAfter: pool, OSDMapEpochBefore: 20, OSDMapEpochAfter: 20, PGs: []ceph.PGState{{PGID: "2.0", ReportedEpoch: 20, State: state, Acting: []int{ecFragmentNone, 1, ecFragmentNone, 3, 4, 5}, ActingPrimary: 1}}}
		}
		if err := checkECFourTwoPlacement(p, base, makeNow(), []int{0, 2}, 15, 1); err != nil {
			t.Fatal(err)
		}
		for name, edit := range map[string]func(*ceph.PoolPGSnapshot){
			"compacted slots":        func(s *ceph.PoolPGSnapshot) { s.PGs[0].Acting = []int{1, 3, 4, 5} },
			"shifted slots":          func(s *ceph.PoolPGSnapshot) { s.PGs[0].Acting = []int{ecFragmentNone, ecFragmentNone, 1, 3, 4, 5} },
			"second loss is primary": func(s *ceph.PoolPGSnapshot) { s.PGs[0].ActingPrimary = 3 },
			"changed policy":         func(s *ceph.PoolPGSnapshot) { s.PoolAfter.MinSize = 3 },
			"wrong PG":               func(s *ceph.PoolPGSnapshot) { s.PGs[0].PGID = "8.0" },
			"old report":             func(s *ceph.PoolPGSnapshot) { s.PGs[0].ReportedEpoch = 15 },
			"unbracketed epoch":      func(s *ceph.PoolPGSnapshot) { s.OSDMapEpochAfter++ },
			"wrong admission":        func(s *ceph.PoolPGSnapshot) { s.PGs[0].State = "active+clean" },
		} {
			s := makeNow()
			edit(&s)
			if err := checkECFourTwoPlacement(p, base, s, []int{0, 2}, 15, 1); err == nil {
				t.Errorf("min_size %d accepted %s", minSize, name)
			}
		}
	}
}

func TestECFourTwoAcknowledgement(t *testing.T) {
	base := ecFourTwoIO{Seq: 0, Phase: "seed", PID: 40, Writer: 10, Reader: 11, Snap: 2, HeadVersion: 1, HeadSHA256: ecFourTwoPayloadHash(32799, 0)}
	good := ecFourTwoIO{Seq: 1, Phase: "ack", PID: 40, Writer: 10, Reader: 11, Snap: 2, Value: 1, Size: 32799, HeadVersion: 2, HeadSHA256: ecFourTwoPayloadHash(32799, 1), SnapSHA256: base.HeadSHA256, SnapVersion: 1}
	if err := checkECFourTwoAck(base, base, good, 32799, 1, 1); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*ecFourTwoIO){
		"replacement actor":  func(r *ecFourTwoIO) { r.PID++ },
		"replacement writer": func(r *ecFourTwoIO) { r.Writer++ },
		"stale version":      func(r *ecFourTwoIO) { r.HeadVersion = 1 },
		"unrelated bytes":    func(r *ecFourTwoIO) { r.HeadSHA256 = base.HeadSHA256 },
		"lost snapshot":      func(r *ecFourTwoIO) { r.SnapSHA256 = "" },
		"different snapshot": func(r *ecFourTwoIO) { r.Snap++ },
		"wrong phase":        func(r *ecFourTwoIO) { r.Phase = "blocked" },
	} {
		r := good
		edit(&r)
		if err := checkECFourTwoAck(base, base, r, 32799, 1, 1); err == nil {
			t.Errorf("accepted %s", name)
		}
	}
}

func TestECFourTwoShutdownAndReadback(t *testing.T) {
	seed := ecFourTwoIO{PID: 40, Writer: 10, Reader: 11, Snap: 2, HeadVersion: 1, HeadSHA256: ecFourTwoPayloadHash(32799, 0)}
	shutdown := ecFourTwoShutdown{PID: 40, Writer: 10, Reader: 11, Shutdown: true, Commands: 2}
	if err := checkECFourTwoShutdown(seed, shutdown); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []ecFourTwoShutdown{{PID: 40, Writer: 10, Reader: 11, Commands: 2}, {PID: 40, Writer: 10, Reader: 11, Shutdown: true, Commands: 1}, {PID: 41, Writer: 10, Reader: 11, Shutdown: true, Commands: 2}} {
		if err := checkECFourTwoShutdown(seed, bad); err == nil {
			t.Fatalf("accepted shutdown %+v", bad)
		}
	}
	last := ecFourTwoIO{HeadVersion: 3, HeadSHA256: ecFourTwoPayloadHash(32799, 1)}
	absent, present := false, true
	makeRead := func() ecFourTwoReadback {
		return ecFourTwoReadback{Reader: 12, Snap: 2, Size: 32799, HeadSHA256: last.HeadSHA256, HeadVersion: 3, SnapSHA256: seed.HeadSHA256, SnapVersion: 1, Unknown: &ecFourTwoObject{Present: &absent}}
	}
	if err := checkECFourTwoReadback(seed, last, makeRead(), 32799, true); err != nil {
		t.Fatal(err)
	}
	whole := makeRead()
	whole.Unknown = &ecFourTwoObject{Present: &present, Size: 32799, SHA256: ecFourTwoPayloadHash(32799, 2), Version: 4}
	if err := checkECFourTwoReadback(seed, last, whole, 32799, true); err != nil {
		t.Fatal(err)
	}
	if err := checkECFourTwoReadback(seed, last, whole, 32799, false); err == nil {
		t.Fatal("accepted an unsubmitted object")
	}
	for name, edit := range map[string]func(*ecFourTwoReadback){
		"old reader":                    func(r *ecFourTwoReadback) { r.Reader = 11 },
		"old head":                      func(r *ecFourTwoReadback) { r.HeadSHA256 = seed.HeadSHA256 },
		"wrong head version":            func(r *ecFourTwoReadback) { r.HeadVersion = 2 },
		"wrong snapshot":                func(r *ecFourTwoReadback) { r.Snap = 3 },
		"missing uncertain observation": func(r *ecFourTwoReadback) { r.Unknown = nil },
		"missing presence":              func(r *ecFourTwoReadback) { r.Unknown = &ecFourTwoObject{} },
		"torn uncertain write": func(r *ecFourTwoReadback) {
			r.Unknown = &ecFourTwoObject{Present: &present, Size: 32798, SHA256: ecFourTwoPayloadHash(32799, 2), Version: 4}
		},
	} {
		r := makeRead()
		edit(&r)
		if err := checkECFourTwoReadback(seed, last, r, 32799, true); err == nil {
			t.Errorf("accepted %s", name)
		}
	}
}
