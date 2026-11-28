package mirror

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// directoryBoundaryDaemon changes the supplied native state at the replay Exec
// boundary, inside DirectoryStatus's lock. The change models an independent
// native session transition; it does not call a fixture lifecycle operation.
type directoryBoundaryDaemon struct {
	*cephFSObserverDraftDaemon
	beforeReplay func()
}

func (d *directoryBoundaryDaemon) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if clustertest.Supervised(args) {
		return clustertest.Supervise(ctx, args, opts, d.Exec)
	}
	if slices.Contains(args, "peer") && d.beforeReplay != nil {
		d.beforeReplay()
	}
	return d.cephFSObserverDraftDaemon.Exec(ctx, args, opts...)
}

// directoryBoundaryControl changes only the independently read watcher list;
// the MGR mapping remains the original fixture response.
type directoryBoundaryControl struct {
	*cephFSObserverDraftControl
	watchers string
}

func (c *directoryBoundaryControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if clustertest.Supervised(args) {
		return clustertest.Supervise(ctx, args, opts, c.Exec)
	}
	if slices.Contains(args, "listwatchers") && c.watchers != "" {
		return 0, cephFSObserverDraftStream(c.watchers), nil
	}
	return c.cephFSObserverDraftControl.Exec(ctx, args, opts...)
}

func TestDirectoryStatusConfirmsSelectedSessionAfterReplay(t *testing.T) {
	for _, change := range []string{"unchanged", "session", "watcher", "peer", "stopped", "failed"} {
		t.Run(change, func(t *testing.T) {
			mirror, source, _, a, _ := newCephFSObserverDraft()
			control := &directoryBoundaryControl{cephFSObserverDraftControl: source}
			mirror.pair.source.Container = control
			boundary := &directoryBoundaryDaemon{cephFSObserverDraftDaemon: a}
			mirror.roster.daemons[0].Container = boundary
			boundary.beforeReplay = func() {
				switch change {
				case "session":
					a.session = strings.Replace(a.session, "172.20.0.8:0/1234", "172.20.0.8:0/9876", 1)
				case "watcher":
					control.watchers = "watcher=172.20.0.8:0/1234 client.9999 cookie=1\n"
				case "peer":
					a.session = strings.Replace(a.session, "destination-site", "another-site", 1)
				case "stopped":
					a.running = false
				case "failed":
					a.session = `{"state":"failed"}`
				}
			}
			status, err := mirror.DirectoryStatus(t.Context(), "/owned")
			if change == "unchanged" {
				if err != nil || !status.Ready || status.LastSyncedSnapshot == nil || status.LastSyncedSnapshot.ID != 120 {
					t.Fatalf("stable observation lost replay evidence: %+v %v", status, err)
				}
				return
			}
			if err == nil || errors.Is(err, errCephFSObservationGuard) || status.Ready || status.LastSyncedSnapshot != nil || status.CurrentSnapshot != nil || status.SnapshotsSynced != 0 || status.State != "" {
				t.Fatalf("mixed observation published replay evidence or could not retry: %+v %v", status, err)
			}
			if source.mapping != `{"state":"mapped","instance_id":"4262"}` {
				t.Fatal("the control did not retain the original MGR mapping")
			}
		})
	}
}

func TestDirectoryWaitsRetryMidReadSessionTransition(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		name := "ready"
		if checkpoint {
			name = "checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			mirror, source, _, a, b := newCephFSObserverDraft()
			b.running = false
			boundary := &directoryBoundaryDaemon{cephFSObserverDraftDaemon: a}
			mirror.roster.daemons[0].Container = boundary
			reads := 0
			boundary.beforeReplay = func() {
				reads++
				if reads == 1 {
					a.session = strings.Replace(a.session, "172.20.0.8:0/1234", "172.20.0.9:0/5678", 1)
					a.replay = strings.Replace(a.replay, `"snaps_synced":1`, `"snaps_synced":0`, 1)
				}
			}
			mappingReads := 0
			source.hook = func(_ *cephFSObserverDraftControl, args []string) {
				if slices.Contains(args, "dirmap") {
					mappingReads++
					if mappingReads == 3 {
						source.mapping = `{"state":"mapped","instance_id":"4266"}`
					}
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			var status DirectoryStatus
			var err error
			if checkpoint {
				status, err = mirror.WaitSnapshotSynced(ctx, "/owned", Snapshot{ID: 120, Name: "checkpoint"})
			} else {
				status, err = mirror.WaitDirectoryReady(ctx, "/owned")
			}
			if err != nil || !status.Ready || status.InstanceID != "4266" || status.SnapshotsSynced != 0 || reads < 2 {
				t.Fatalf("wait did not retry mixed evidence and accept the new session: reads=%d status=%+v err=%v", reads, status, err)
			}
		})
	}
}

func TestDirectoryWatchRejectsMixedSessionBaseline(t *testing.T) {
	f := newDirectoryWatchFixture()
	boundary := &directoryBoundaryDaemon{cephFSObserverDraftDaemon: f.a, beforeReplay: func() {
		f.a.session = strings.Replace(f.a.session, "172.20.0.8:0/1234", "172.20.0.8:0/9876", 1)
	}}
	f.mirror.roster.daemons[0].Container = boundary
	w, err := f.mirror.WatchDirectory(t.Context(), ceph.WatchConfig{}, "/owned")
	if w != nil {
		_ = w.Stop()
	}
	if err == nil || w != nil || errors.Is(err, errCephFSObservationGuard) {
		t.Fatalf("watch accepted a mixed-session baseline: watch=%v err=%v", w, err)
	}
}

func TestDirectoryWatchReportsMidReadSessionChangeAsGap(t *testing.T) {
	f := newDirectoryWatchFixture()
	replays := 0
	boundary := &directoryBoundaryDaemon{cephFSObserverDraftDaemon: f.a, beforeReplay: func() {
		replays++
		if replays == 2 {
			f.a.session = strings.Replace(f.a.session, "172.20.0.8:0/1234", "172.20.0.8:0/9876", 1)
		}
	}}
	f.mirror.roster.daemons[0].Container = boundary
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	w, err := f.mirror.WatchDirectory(ctx, ceph.WatchConfig{Interval: 100 * time.Millisecond, QueryTimeout: time.Second, Buffer: 8}, "/owned")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Stop() }()
	for _, kind := range []ceph.WatchEventKind{ceph.WatchBaseline, ceph.WatchGap} {
		select {
		case event := <-w.Events():
			if event.Kind != kind {
				t.Fatalf("event=%+v, want %s", event, kind)
			}
			if kind == ceph.WatchGap && (event.Err == nil || event.Status.Ready || event.Status.LastSyncedSnapshot != nil || event.Status.CurrentSnapshot != nil || event.Status.SnapshotsSynced != 0) {
				t.Fatalf("mixed session became a successful watch checkpoint: %+v", event)
			}
		case <-ctx.Done():
			t.Fatal("watch did not report the mid-read session transition")
		}
	}
}
