//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strings"
	"testing"
)

// These checks run the attachment judge on synthetic receipts only. No OSD,
// keeper, tmpfs, Ceph daemon or Docker is involved, so they show the judge's
// decisions and nothing about native behavior.

func goodAttachRun() *attachRun {
	fill := syntheticScratchFill("write", "tmpfs", 28, 65536, 0, 100)
	allocated := int64(8192)
	fill.File.Allocated = &allocated
	yes, zero, version := true, 0, 1
	sha, seedLen := "seed", int64(10)
	return &attachRun{
		Link: storeLinkSame, Backing: blockBackingSparse, Policy: [2]string{"n=0.85 b=0.9 f=0.95", "n=0.85 b=0.9 f=0.95"},
		Fill: fill, SeedSHA: "seed", WriterJoined: true,
		Release: &scratchRelease{Version: &version, Identified: &yes, Unlinked: &yes, PathAbsent: &yes, Errno: &zero,
			VFSBefore: scratchVFSOf(4096, 0, 100), VFSAfter: scratchVFSOf(4096, 10, 100), SeedSHA256: &sha, SeedLen: &seedLen},
		KeeperStarted: [2]string{"t1", "t1"}, KeeperRunning: true, Daemon: daemonUp,
		Ledger: []attachMutation{
			{Key: "a", Outcome: mutAcked, SHA256: "da"},
			{Key: "b", Outcome: mutUnknown, SHA256: "db"},
			{Key: "c", Outcome: mutFailed, SHA256: "dc"},
		},
		Recovered: map[string]string{"a": "da"},
	}
}

func TestAttachmentJudgeOutcomes(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*attachRun)
		want   string
	}{
		{"recovered", func(*attachRun) {}, attachRecovered},
		{"below-length backing still qualifies", func(r *attachRun) { r.Backing = blockBackingBelowLength }, attachRecovered},
		{"different filesystem", func(r *attachRun) { r.Link = storeLinkDifferent }, attachNotAttached},
		{"block elsewhere", func(r *attachRun) { r.Backing = blockBackingElsewhere }, attachNotAttached},
		{"block not a file", func(r *attachRun) { r.Backing = blockBackingNotFile }, attachNotAttached},
		{"backing may already cover its length", func(r *attachRun) { r.Backing = blockBackingAtLeastLen }, attachAllocUnproved},
		{"budget reached", func(r *attachRun) { r.Fill = unreachedFill(t) }, attachUnreached},
		{"inode limit", func(r *attachRun) { r.Fill = syntheticScratchFill("create", "tmpfs", 28, 0, 5, 0) }, attachNotByteCeiling},
		{"other errno", func(r *attachRun) { r.Fill = syntheticScratchFill("write", "tmpfs", 5, 4096, 0, 100) }, attachNotByteCeiling},
		{"unmatched ENOSPC", func(r *attachRun) { r.Fill = syntheticScratchFill("write", "tmpfs", 28, 4096, 10, 100) }, attachNotByteCeiling},
		{"filler owned no allocation and nothing was reclaimed", func(r *attachRun) {
			zero := int64(0)
			r.Fill.File.Allocated = &zero
			r.Release.VFSAfter = scratchVFSOf(4096, 0, 100)
		}, attachNoReclaim},
		{"daemon exited", func(r *attachRun) { r.Daemon = daemonAborted; r.Recovered = nil }, attachDaemonAborted},
		{"daemon state unknown", func(r *attachRun) { r.Daemon = daemonUnknown; r.Recovered = nil }, attachDaemonIndefinite},
		{"unknown mutation read back is not promoted", func(r *attachRun) { r.Recovered["b"] = "db" }, attachRecovered},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := goodAttachRun()
			c.mutate(r)
			got, err := judgeScratchAttachment(r)
			if err != nil || got != c.want {
				t.Fatalf("got %q, %v; want %q", got, err, c.want)
			}
		})
	}
}

func unreachedFill(t *testing.T) *scratchFillReceipt {
	t.Helper()
	f := syntheticScratchFill("write", "tmpfs", 0, 0, 100, 100)
	outcome := "budget"
	f.Outcome = &outcome
	f.Ops = nil
	budget := int64(0)
	f.Budget = &budget
	size := int64(0)
	f.File.Size = &size
	return f
}

func TestAttachmentJudgeRefusesIncompleteOrContradictoryEvidence(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*attachRun)
		want   string
	}{
		{"link verdict missing", func(r *attachRun) { r.Link = "" }, "missing"},
		{"backing verdict missing", func(r *attachRun) { r.Backing = "" }, "missing"},
		{"backing verdict unknown", func(r *attachRun) { r.Backing = "other" }, "not recognized"},
		{"policy not recorded before", func(r *attachRun) { r.Policy[0] = "" }, "policy"},
		{"policy not recorded after", func(r *attachRun) { r.Policy[1] = "" }, "policy"},
		{"policy changed", func(r *attachRun) { r.Policy[1] = "n=0.5 b=0.9 f=0.95" }, "policy changed"},
		{"fill receipt missing", func(r *attachRun) { r.Fill = nil }, "fill receipt"},
		{"writer descriptor not closed", func(r *attachRun) { no := false; r.Fill.Closed = &no }, "closure"},
		{"writer not joined", func(r *attachRun) { r.WriterJoined = false }, "writer"},
		{"release missing", func(r *attachRun) { r.Release = nil }, "release"},
		{"seed digest missing", func(r *attachRun) { r.SeedSHA = "" }, "seed"},
		{"seed digest differs", func(r *attachRun) { r.SeedSHA = "other" }, "seed digest"},
		{"capacity not released", func(r *attachRun) { r.Release.VFSAfter = scratchVFSOf(4096, 0, 100) }, "released"},
		{"keeper start not recorded", func(r *attachRun) { r.KeeperStarted[0] = "" }, "keeper"},
		{"keeper not running", func(r *attachRun) { r.KeeperRunning = false }, "keeper"},
		{"keeper restarted", func(r *attachRun) { r.KeeperStarted[1] = "t2" }, "keeper restarted"},
		{"daemon state missing", func(r *attachRun) { r.Daemon = "" }, "daemon state"},
		{"readback missing", func(r *attachRun) { r.Recovered = nil }, "missing or different"},
		{"readback null map entry differs", func(r *attachRun) { r.Recovered["a"] = "other" }, "missing or different"},
		{"no acknowledged mutation", func(r *attachRun) { r.Ledger = r.Ledger[1:] }, "no acknowledged"},
		{"acknowledged without digest", func(r *attachRun) { r.Ledger[0].SHA256 = "" }, "lacks"},
		{"unknown mutation outcome", func(r *attachRun) { r.Ledger[1].Outcome = "maybe" }, "mutation outcome"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := goodAttachRun()
			c.mutate(r)
			got, err := judgeScratchAttachment(r)
			if err == nil || got != "" || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %q, %v; want an error containing %q", got, err, c.want)
			}
		})
	}
	if _, err := judgeScratchAttachment(nil); err == nil {
		t.Fatal("nil run accepted")
	}
}

// A keeper restart must refuse the run even when the daemon is up and every
// acknowledged mutation reads back: the restart empties the volume, so the
// recovery would not belong to the release.
func TestAttachmentJudgeKeeperRestartCannotManufactureRecovery(t *testing.T) {
	r := goodAttachRun()
	r.KeeperStarted[1] = "t2"
	if got, err := judgeScratchAttachment(r); err == nil || got == attachRecovered {
		t.Fatalf("restarted keeper judged %q, %v", got, err)
	}
	r.Daemon, r.Recovered = daemonAborted, nil
	if got, err := judgeScratchAttachment(r); err == nil || got != "" {
		t.Fatalf("restarted keeper with a gone daemon judged %q, %v", got, err)
	}
}
