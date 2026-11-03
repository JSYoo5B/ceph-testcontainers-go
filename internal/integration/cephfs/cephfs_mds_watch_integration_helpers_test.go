//go:build all || integration

package cephfs_test

import (
	"context"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// awaitMDSWatchEvent reads events until match accepts one, and fails on a
// closed watch, a skipped sequence number or after limit. Gaps are counted,
// not fatal: a read can fail while the MONs process the failure.
func awaitMDSWatchEvent(t *testing.T, w *cephfs.MDSWatch, last *uint64, gaps *int, limit time.Duration, what string, match func(cephfs.MDSWatchEvent) bool) cephfs.MDSWatchEvent {
	t.Helper()
	timeout := time.After(limit)
	for {
		select {
		case event, ok := <-w.Events():
			if !ok {
				t.Fatalf("%s: watch ended: %v", what, w.Stop())
			}
			if event.Sequence != *last+1 {
				t.Fatalf("%s: event %d follows %d", what, event.Sequence, *last)
			}
			*last = event.Sequence
			if event.Kind == ceph.WatchGap {
				*gaps++
				t.Logf("%s: gap at event %d after %v: %v", what, event.Sequence, event.Finished.Sub(event.Started), event.Err)
				continue
			}
			if match(event) {
				return event
			}
			t.Logf("%s: event %d %s FSMap epoch %d MDSMap epoch %d: max_mds=%d active=%+v standby=%+v replay=%+v",
				what, event.Sequence, event.Kind, event.Epoch, event.MDSMapEpoch, event.Status.MaxMDS, event.Status.Active, event.Status.Standby, event.Status.StandbyReplay)
		case <-timeout:
			t.Fatalf("%s: no matching event within %v", what, limit)
		}
	}
}

// mdsWatchRankHolder returns the active rank 0 of an event.
func mdsWatchRankHolder(event cephfs.MDSWatchEvent) (cephfs.MDSStatus, bool) {
	for _, active := range event.Status.Active {
		if active.Rank == 0 && active.State == "up:active" {
			return active, true
		}
	}
	return cephfs.MDSStatus{}, false
}

// testCephFSMDSWatch observes a rank takeover and the restored standby
// through a watch of the filesystem. The cluster has one owned active MDS and
// one owned standby, and a client seeds files through libcephfs. After the
// baseline the active MDS is SIGKILLed without ceph mds fail, so the MONs
// replace it after mds_beacon_grace. The takeover event, rank 0 active under
// the standby's name and GID, advances the test: a fresh libcephfs session,
// which can only be served by the new MDS, must read every byte written
// before the fault and write new data. The killed daemon is then started in
// its own container, and the event in which it registers again as an owned
// standby under a new GID, with rank 0 still on the former standby, is
// followed by WaitReady for the requested capacity and a third session that
// verifies the writes made after the takeover.
//
// An event proves only that one fs dump of the MONs reported that topology
// at that FSMap epoch, with ownership matched by daemon name against the
// fixture's roster; the client sessions prove the data path.
func testCephFSMDSWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	m := startCephFSStandbyFailover(t, ctx)
	harness.ExecCommand(t, ctx, m.Client, "python3", "-c", cephFSClientScript, m.FS.FilesystemName, "seed")
	// The cost of one read: the same roster snapshot and fs dump the watch makes.
	const samples = 10
	began := time.Now()
	for range samples {
		if _, err := m.FS.MDSStatus(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("query cost: %d MDSStatus reads took %v (%v each)", samples, time.Since(began), time.Since(began)/samples)

	const interval = time.Second
	watchStarted := time.Now()
	w, err := m.FS.WatchMDSs(ctx, ceph.WatchConfig{Interval: interval})
	if err != nil {
		t.Fatal(err)
	}
	// Registered after the cluster cleanup, so it runs before it.
	t.Cleanup(func() { _ = w.Stop() })
	var last uint64
	gaps := 0
	baseline := awaitMDSWatchEvent(t, w, &last, &gaps, time.Second, "baseline", func(e cephfs.MDSWatchEvent) bool { return e.Kind == ceph.WatchBaseline })
	old, ok := mdsWatchRankHolder(baseline)
	if !ok || old != m.MDS || len(baseline.Status.Active) != 1 || len(baseline.Status.Standby) != 1 || baseline.Status.Standby[0] != m.standby ||
		baseline.Status.MaxMDS != 1 || baseline.Status.FilesystemName != m.FS.FilesystemName {
		t.Fatalf("baseline = %+v, want mds.%s active and mds.%s standby", baseline, m.MDS.Name, m.standby.Name)
	}
	t.Logf("baseline: FSMap epoch %d MDSMap epoch %d, rank 0 mds.%s/%d, standby mds.%s/%d, read %v",
		baseline.Epoch, baseline.MDSMapEpoch, old.Name, old.GID, m.standby.Name, m.standby.GID, baseline.Finished.Sub(baseline.Started))

	var daemon *cephfs.MDS
	for _, member := range m.FS.MDSs() {
		if member.ID == old.Name {
			daemon = member
		}
	}
	if daemon == nil {
		t.Fatalf("active mds.%s has no owned daemon: %+v", old.Name, m.FS.MDSs())
	}
	cid := daemon.GetContainerID()
	kill := time.Duration(0)
	if err := daemon.Stop(ctx, &kill); err != nil {
		t.Fatal("kill MDS:", err)
	}
	killed := time.Now()
	took := awaitMDSWatchEvent(t, w, &last, &gaps, 3*time.Minute, "takeover", func(e cephfs.MDSWatchEvent) bool {
		holder, ok := mdsWatchRankHolder(e)
		return ok && holder.Name != old.Name
	})
	holder, _ := mdsWatchRankHolder(took)
	if holder.Name != m.standby.Name || holder.GID != m.standby.GID || !holder.Owned || took.Epoch <= baseline.Epoch || took.Status.FilesystemID != baseline.Status.FilesystemID {
		t.Fatalf("takeover event = %+v, want rank 0 on standby mds.%s/%d", took, m.standby.Name, m.standby.GID)
	}
	t.Logf("takeover: event %d %s FSMap epoch %d, observed %v after the kill returned", took.Sequence, took.Kind, took.Epoch, took.Finished.Sub(killed))

	// The takeover advances the test: a new session reaches the new MDS.
	harness.ExecCommand(t, ctx, m.Client, "python3", "-c", cephFSClientScript, m.FS.FilesystemName, "after-topology")

	if err := daemon.Start(ctx); err != nil {
		t.Fatal("restart MDS:", err)
	}
	restarted := time.Now()
	restored := awaitMDSWatchEvent(t, w, &last, &gaps, 3*time.Minute, "restored standby", func(e cephfs.MDSWatchEvent) bool {
		for _, standby := range e.Status.Standby {
			if standby.Name == old.Name {
				return true
			}
		}
		return false
	})
	if current, ok := mdsWatchRankHolder(restored); !ok || current != holder || len(restored.Status.Standby) != 1 ||
		!restored.Status.Standby[0].Owned || restored.Status.Standby[0].GID == old.GID || restored.Epoch <= took.Epoch || daemon.GetContainerID() != cid {
		t.Fatalf("restored event = %+v, want rank 0 still on mds.%s/%d and mds.%s back as a standby under a new GID in container %s", restored, holder.Name, holder.GID, old.Name, cid)
	}
	t.Logf("restored: event %d %s FSMap epoch %d, mds.%s/%d standby %v after the start returned",
		restored.Sequence, restored.Kind, restored.Epoch, old.Name, restored.Status.Standby[0].GID, restored.Finished.Sub(restarted))
	if err := m.FS.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	harness.ExecCommand(t, ctx, m.Client, "python3", "-c", cephFSClientScript, m.FS.FilesystemName, "final")

	if err := w.Stop(); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	for event := range w.Events() {
		if event.Sequence != last+1 {
			t.Fatalf("event %d follows %d", event.Sequence, last)
		}
		last = event.Sequence
	}
	elapsed := time.Since(watchStarted)
	t.Logf("CEPHFS_MDS_WATCH %s: baseline -> mds.%s SIGKILLed -> mds.%s/%d active on rank 0 -> fresh session read and wrote -> mds.%s restarted -> standby again under GID %d -> WaitReady -> third session verified; %d events, %d gaps, watch ran %v at a %v interval (about %d reads)",
		m.Version, old.Name, holder.Name, holder.GID, old.Name, restored.Status.Standby[0].GID, last, gaps, elapsed.Round(time.Second), interval, int(elapsed/interval)+1)
}
