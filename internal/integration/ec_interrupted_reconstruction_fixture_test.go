//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestECReconstructionPolicyAndSurvivors(t *testing.T) {
	repl, ec := secondFaultPoolConfig(false), secondFaultPoolConfig(true)
	if repl.Replicas != 3 || repl.MinSize != 2 || repl.ErasureCode != nil || ec.ErasureCode == nil || ec.ErasureCode.K != 2 || ec.ErasureCode.M != 1 || !ec.ErasureCode.AllowOverwrites || ec.MinSize != 2 || ec.FailureDomain != "osd" {
		t.Fatal(repl, ec)
	}
	original := ceph.PGState{Acting: []int{7, 2, 9}, ActingPrimary: 2, ReportedEpoch: 10}
	good := ceph.PGState{Acting: []int{7, 2, ecFragmentNone}, ActingPrimary: 2, ReportedEpoch: 12}
	if !secondFaultTwoSurvivors(good, original, 9, true) {
		t.Fatal("valid indexed survivors refused")
	}
	for name, alter := range map[string]func(*ceph.PGState){
		"shifted":     func(p *ceph.PGState) { p.Acting = []int{ecFragmentNone, 2, 7} },
		"wrong shard": func(p *ceph.PGState) { p.Acting[0] = 8 },
		"duplicate":   func(p *ceph.PGState) { p.Acting[0] = 2 },
		"short":       func(p *ceph.PGState) { p.Acting = []int{7, 2} },
		"unchanged":   func(p *ceph.PGState) { p.Acting = slices.Clone(original.Acting) },
		"primary":     func(p *ceph.PGState) { p.ActingPrimary = 7 },
		"stale":       func(p *ceph.PGState) { p.ReportedEpoch = 10 },
	} {
		p := good
		p.Acting = slices.Clone(good.Acting)
		alter(&p)
		if secondFaultTwoSurvivors(p, original, 9, true) {
			t.Error("accepted", name)
		}
	}
	if secondFaultTwoSurvivors(good, original, 2, true) || secondFaultTwoSurvivors(good, original, 99, true) {
		t.Fatal("wrong selected slot accepted")
	}
	if !secondFaultTwoSurvivors(ceph.PGState{Acting: []int{7, 2}}, original, 9, false) {
		t.Fatal("replicated omission changed")
	}
}

func TestECReconstructionProgressUsesShardPeers(t *testing.T) {
	sample := newProgressSample()
	sample.acting = []int{1, 2, ecFragmentNone}
	sample.targets = []string{"3(2)"}
	decode := func(s progressSample) pgProgress {
		t.Helper()
		raw := strings.ReplaceAll(string(s.json(t)), `"peer":"3"`, `"peer":"3(2)"`)
		p, err := parsePGProgress([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	base, err := backfillHeld(decode(sample), 3)
	if err != nil {
		t.Fatal(err)
	}
	original := ceph.PGState{Acting: []int{1, 2, 4}, ActingPrimary: 1, ReportedEpoch: 20}
	if !ecReconstructionTarget(base, original, 4, 3) {
		t.Fatal("indexed EC replacement refused")
	}
	shifted := base
	shifted.Up = []int{3, 2, 1}
	if ecReconstructionTarget(shifted, original, 4, 3) {
		t.Fatal("shifted replacement admitted")
	}
	if v, _ := backfillProgress(base, decode(sample)); v != progressWait {
		t.Fatal("held cursor is progress", v)
	}
	sample.cursor = "1:0a000000::::seed-1:head"
	if v, why := backfillProgress(base, decode(sample)); v != progressReached {
		t.Fatal(v, why)
	}
	sample.cursor = "MAX"
	if v, _ := backfillProgress(base, decode(sample)); v != progressTerminal {
		t.Fatal("finished cursor accepted", v)
	}
	sample.cursor = "1:0a000000::::seed-1:head"
	sample.interval++
	if v, _ := backfillProgress(base, decode(sample)); v != progressInvalid {
		t.Fatal("changed interval accepted", v)
	}
	sample.interval--
	sample.ahead = 0
	if v, _ := backfillProgress(base, decode(sample)); v != progressWait {
		t.Fatal("no unfinished work accepted", v)
	}
}
