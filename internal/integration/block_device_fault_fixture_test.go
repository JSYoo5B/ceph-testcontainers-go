//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"strconv"
	"strings"
	"testing"
)

func blockView() blockAttachView {
	rw, path := int64(0o2+0o40000), int64(0o10000000)
	return blockAttachView{PIDs: []int{41}, OPath: 0o10000000, ODirect: 0o40000, Link: blockDeviceContainerPath, FSID: "u-1",
		FDs: []blockFD{{PID: 41, FD: 12, RDev: "253:4", Flags: &rw}, {PID: 41, FD: 13, RDev: "253:4", Flags: &path}}}
}

func TestJudgeBlockAttachmentNeedsAReadWriteDescriptorOfTheStoreDevice(t *testing.T) {
	if v, d := judgeBlockAttachment(blockView(), "253:4", "u-1"); v != blockAttached {
		t.Fatalf("attached view: %s %s", v, d)
	}
	readOnly, path := int64(0), int64(0o10000000|2)
	for name, mutate := range map[string]func(*blockAttachView){
		"no daemon":         func(v *blockAttachView) { v.PIDs = nil },
		"two daemons":       func(v *blockAttachView) { v.PIDs = []int{41, 42} },
		"block is a file":   func(v *blockAttachView) { v.Link = ""; v.LinkErr = "not a link" },
		"another store":     func(v *blockAttachView) { v.FSID = "u-2" },
		"O_PATH unknown":    func(v *blockAttachView) { v.OPath = 0 },
		"only O_PATH":       func(v *blockAttachView) { v.FDs = v.FDs[1:] },
		"read-write O_PATH": func(v *blockAttachView) { v.FDs[0].Flags = &path },
		"read-only":         func(v *blockAttachView) { v.FDs[0].Flags = &readOnly },
		"flags unreadable":  func(v *blockAttachView) { v.FDs[0].Flags = nil },
		"other device":      func(v *blockAttachView) { v.FDs[0].RDev = "253:5" },
		"other process":     func(v *blockAttachView) { v.FDs[0].PID = 99 },
	} {
		v := blockView()
		v.FDs = append([]blockFD(nil), v.FDs...)
		mutate(&v)
		if got, d := judgeBlockAttachment(v, "253:4", "u-1"); got != blockNotAttached {
			t.Errorf("%s: %s %s", name, got, d)
		}
	}
}

func TestJudgeBlockExposureRequiresMovedCountersAndAStillControl(t *testing.T) {
	at := func(writes, sectors int64) blockKeeperReply {
		return blockKeeperReply{Writes: writes, WriteSectors: sectors}
	}
	cases := []struct {
		name                     string
		before, after, idleAfter blockKeeperReply
		want                     string
	}{
		{"exposed", at(10, 80), at(30, 400), at(0, 0), blockExposed},
		{"no writes", at(10, 80), at(10, 80), at(0, 0), blockNotExposed},
		{"count without sectors", at(10, 80), at(11, 80), at(0, 0), blockNotExposed},
		{"control moved", at(10, 80), at(30, 400), at(1, 8), blockControlRan},
		{"control read", at(10, 80), at(30, 400), blockKeeperReply{Reads: 1}, blockControlRan},
	}
	for _, c := range cases {
		if got := judgeBlockExposure(c.before, c.after, at(0, 0), c.idleAfter); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

func blockTimed(seconds ...float64) blockTimedLedger {
	l := blockTimedLedger{Seconds: seconds}
	for i := range seconds {
		l.Ledger = append(l.Ledger, attachMutation{Key: "k-" + strconv.Itoa(i), Outcome: mutAcked})
	}
	return l
}

func TestJudgeBlockDelayComparesMediansWithTheConfiguredDelay(t *testing.T) {
	base := blockTimed(0.01, 0.012, 0.011)
	lost := blockTimed(0.3, 0.31, 0.29)
	lost.Ledger[1].Outcome = mutUnknown
	restoredFailed := blockTimed(0.001, 0.001, 0.001)
	for i := range restoredFailed.Ledger {
		restoredFailed.Ledger[i].Outcome = mutFailed
	}
	restoredUnknown := blockTimed(0.01, 0.01, 30)
	restoredUnknown.Ledger[2].Outcome = mutUnknown
	cases := []struct {
		name              string
		delayed, restored blockTimedLedger
		want              string
	}{
		{"delayed", blockTimed(0.21, 0.42, 0.22), blockTimed(0.012, 0.01, 0.02), blockDelayed},
		{"not slowed", blockTimed(0.05, 0.06, 0.07), blockTimed(0.01, 0.01, 0.01), blockDelayWeak},
		{"still slow", blockTimed(0.3, 0.3, 0.3), blockTimed(0.2, 0.25, 0.2), blockDelayLingered},
		{"lost write", lost, blockTimed(0.01, 0.01, 0.01), blockDelayLost},
		{"restored writes failed fast", blockTimed(0.25, 0.26, 0.25), restoredFailed, blockRestoreLost},
		{"restored write unknown", blockTimed(0.25, 0.26, 0.25), restoredUnknown, blockRestoreLost},
		{"no restored writes", blockTimed(0.25, 0.26, 0.25), blockTimedLedger{}, blockRestoreLost},
		{"no delayed writes", blockTimedLedger{}, blockTimed(0.01, 0.01, 0.01), blockDelayLost},
	}
	for _, c := range cases {
		if got := judgeBlockDelay(base, c.delayed, c.restored, 200); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

func TestJudgeBlockErrorNeedsTheDaemonOrTheClientToShowTheError(t *testing.T) {
	unknown := blockTimed(15, 15)
	for i := range unknown.Ledger {
		unknown.Ledger[i].Outcome = mutUnknown
	}
	eio := blockTimed(0.1)
	eio.Ledger[0].Outcome, eio.Errnos = mutFailed, map[string]int{"k-0": 5}
	ioLog := []string{"2026-10-31T00:00:00 bdev(0x1 /var/lib/ceph/osd/ceph-0/block) _aio_thread got r=-5 ((5) Input/output error)"}
	cases := []struct {
		name string
		p    blockErrorPhase
		want string
	}{
		{"aborted", blockErrorPhase{Writes: unknown, Running: false, Log: ioLog}, blockOSDAborted},
		{"exit without marker", blockErrorPhase{Writes: unknown, Running: false, Log: []string{"Caught signal (Aborted)"}}, blockNoEvidence},
		{"client EIO", blockErrorPhase{Writes: eio, Running: true}, blockClientEIO},
		{"held writes", blockErrorPhase{Writes: unknown, Running: true, Log: ioLog}, blockNoEvidence},
		{"acknowledged", blockErrorPhase{Writes: blockTimed(0.1), Running: false, Log: ioLog}, blockAckedInside},
	}
	for _, c := range cases {
		if got := judgeBlockError(c.p); got != c.want {
			t.Errorf("%s: %s, want %s", c.name, got, c.want)
		}
	}
}

func TestParseBlockTimedLedgerKeepsOneDurationPerWrite(t *testing.T) {
	out := `{"errnos": {"f-1": 110}, "seconds": [0.01, 15.0]}
[{"key": "f-0", "sha256": "` + strings.Repeat("a", 64) + `", "outcome": "acked"}, {"key": "f-1", "sha256": "` + strings.Repeat("b", 64) + `", "outcome": "unknown"}]`
	l, err := parseBlockTimedLedger(out)
	if err != nil || len(l.Ledger) != 2 || l.Errnos["f-1"] != 110 || l.Seconds[1] != 15 {
		t.Fatalf("%+v %v", l, err)
	}
	short := strings.Replace(out, "[0.01, 15.0]", "[0.01]", 1)
	if _, err := parseBlockTimedLedger(short); err == nil {
		t.Fatal("a missing duration was accepted")
	}
	if _, err := parseBlockKeeperReply([]byte(`{"action": "stat"}`), "create"); err == nil {
		t.Fatal("a reply for another action was accepted")
	}
}
