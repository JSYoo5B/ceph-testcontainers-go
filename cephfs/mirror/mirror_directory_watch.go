package mirror

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// directoryWatchDefaultInterval is the default WatchConfig.Interval of
// WatchDirectory: each read is one DirectoryStatus observation, about a dozen
// CLI, rados and admin-socket commands across both clusters and every owned
// daemon.
const directoryWatchDefaultInterval = 5 * time.Second

const directoryWatchOperation = "watch CephFS mirror directory"

// DirectoryWatchEvent is one event of a DirectoryWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time Ceph changed state; Ceph reports no transition time.
//
// Status is the DirectoryStatus of that read. On a WatchBaseline, WatchChanged
// or WatchResumed event Err is nil and Status is what DirectoryStatus returned
// without an error or with its partial error, which the watch keeps as a
// successful read: a non-empty DaemonProblems names the owned members that
// could not be observed, and when the directory's assignment points at one of
// them DaemonName is empty and Ready false. A WatchChanged event differs from
// the previous successful read in a compared field: every field except the
// free text of FailureReason, which is delivered but not compared, so the
// exact source snapshot ID and name, the counters and each DaemonProblems
// entry all count. On a WatchGap event Err is the read error and Status holds
// only what that read established before it failed, with Ready false; it is
// never compared and proves nothing about the directory. Each event has its
// own Status, snapshots and map, which the consumer may keep or change. Two
// events separated by a gap, or simply by the polling interval, do not
// establish the transitions in between.
type DirectoryWatchEvent struct {
	Sequence          uint64
	Kind              ceph.WatchEventKind
	Started, Finished time.Time
	Status            DirectoryStatus
	Err               error
}

// DirectoryWatch is a running watch of one owned mirrored directory. One
// goroutine observes the directory and sends events; Events closes after that
// goroutine has stopped using the fixture. Stop ends the watch and waits for
// it.
type DirectoryWatch struct {
	stream *watch.Stream[DirectoryWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when WatchDirectory returns. The channel closes when the watch ends;
// Stop returns why.
//
//purpose:observation
func (w *DirectoryWatch) Events() <-chan DirectoryWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it, the context's error when the WatchDirectory
// context ended, a ceph.ErrWatchOverflow error, or the read error that ended
// it (a terminated mirror or cluster, a replaced identity, or a native report
// that the observation refuses). Buffered events stay readable after Stop.
// Stop is safe to call more than once and from any goroutine; every call
// returns the same cause. Call it before the mirror's and the clusters'
// cleanup: cancelling the context alone does not wait.
//
//purpose:observation,lifetime
func (w *DirectoryWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return servicehook.Invalid(fmt.Errorf("%s: the watch is not started by its Watch call", directoryWatchOperation))
	}
	return w.stream.Stop()
}

// WatchDirectory polls one current owned directory policy, as DirectoryStatus
// observes it, and reports its changes: MGR assignment, the live owner and
// its native filesystem watcher, idle/syncing/failed replay state and the
// exact source snapshots it reports. The default WatchConfig.Interval is 5
// seconds.
//
// Before it returns, WatchDirectory makes one read under ctx and
// QueryTimeout. A failed first read returns its error and starts nothing; an
// invalid path, a directory this mirror does not own and an invalid cfg are
// refused. That read is the baseline: the first event, already buffered, and
// the fixed identities of the watch: the source and destination filesystem
// IDs and metadata pools, the peer UUID and this registration of the
// directory policy. Receive the baseline before triggering the change to
// observe.
//
// Each later read takes each cluster's topology lock briefly to check that it
// still runs, then the mirror's lock for one DirectoryStatus observation, all under
// QueryTimeout, and releases them before
// the event is sent and during the wait for the next read, so the consumer
// may call any fixture operation while the watch runs; a read that waits for
// a long fixture operation is a gap. A successful read whose compared state
// equals the previous successful read produces no event. A failed read
// produces a WatchGap and the watch continues; the next successful read is
// WatchResumed. A new owner after daemon loss, a new native
// watcher instance after a restart and reset counters are changes, not the
// end of the watch. These end the watch instead: ctx ending, Stop, a
// terminated mirror or source or destination cluster, the directory policy
// removed from the mirror or registered again (including by RebalanceDirectories),
// the peer removed or replaced, a changed filesystem or metadata pool
// identity, a native report that DirectoryStatus refuses as malformed or
// contradictory, and a full buffer (ceph.ErrWatchOverflow).
//
// The watch never rebalances, restarts or changes policy, and it does not
// replace the tracked removal receipts: DirectoryRemoval and PeerRemoval stay
// the proof that the daemons released a directory or a peer. Ready and the
// reported snapshots are point-in-time native reports of this directory, not
// whole-fixture health, destination snapshot retention or byte equality;
// verify the destination data separately. The source snapshot ID is the
// source's own number, never comparable with a destination snapshot ID, and
// snapshot names carry no order. The stream reports differences between
// samples, not a complete history of native transitions.
//
//purpose:observation
func (mirror *Container) WatchDirectory(ctx context.Context, cfg ceph.WatchConfig, directory string) (*DirectoryWatch, error) {
	if mirror == nil {
		return nil, servicehook.Invalid(fmt.Errorf("%s: CephFS mirror fixture is unavailable", directoryWatchOperation))
	}
	normalized, err := normalizeMirrorDirectory(directory)
	if err != nil {
		return nil, servicehook.Invalid(fmt.Errorf("%s: %v", directoryWatchOperation, err))
	}
	settings, err := watch.Validate(cfg.Interval, cfg.QueryTimeout, cfg.Buffer, directoryWatchDefaultInterval)
	if err != nil {
		return nil, servicehook.Invalid(fmt.Errorf("%s: %v", directoryWatchOperation, err))
	}
	return startDirectoryWatch(ctx, settings, normalized, mirror.directoryWatchRead(normalized))
}

// directoryWatchRead returns the read of one watch of a normalized directory:
// the cluster check, then one observation whose admission pins the
// identities on the first call and checks them on every later one.
func (mirror *Container) directoryWatchRead(directory string) func(context.Context) (DirectoryStatus, error) {
	pin := &directoryWatchPin{mirror: mirror, directory: directory}
	return func(ctx context.Context) (DirectoryStatus, error) {
		if err := pin.running(ctx); err != nil {
			return DirectoryStatus{}, err
		}
		return mirror.directoryStatus(ctx, directory, pin.admit)
	}
}

// directoryWatchPin holds the identities that the baseline fixed. admit runs
// under Container.mu, and only on the producer, one read at a time.
type directoryWatchPin struct {
	mirror                              *Container
	directory                           string
	pinned                              bool
	directoryGeneration, peerGeneration uint64
	peerID                              string
}

// running refuses a read once the source or the destination cluster is
// terminated. It takes each cluster's topology lock only to read that state,
// before Container.mu, so a long cluster operation never holds up the mirror's
// own operations behind a waiting read; a lock wait that ctx ends is a gap.
// Both clusters are fixed when the mirror is constructed.
func (p *directoryWatchPin) running(ctx context.Context) error {
	m := p.mirror
	for _, site := range []struct {
		name    string
		cluster *ceph.Container
	}{{"source", m.source}, {"destination", m.destination}} {
		if site.cluster == nil {
			continue
		}
		if err := servicehook.LockTopology(ctx, site.cluster); err != nil {
			return err
		}
		closed := servicehook.Closed(site.cluster)
		servicehook.Unlock(site.cluster)
		if closed {
			return directoryWatchEnded{servicehook.Invalid(fmt.Errorf("the %s cluster has been terminated", site.name))}
		}
	}
	return nil
}

// admit refuses a read of a terminated mirror or of a replaced policy. Every
// refusal ends the watch.
func (p *directoryWatchPin) admit(context.Context) error {
	m := p.mirror
	if m.closed {
		return directoryWatchEnded{servicehook.Invalid(errors.New("the CephFS mirror has been terminated"))}
	}
	if !m.directories.owns(p.directory) {
		return directoryWatchEnded{cephFSObserveGuard("CephFS mirror does not own the watched directory policy")}
	}
	directoryGeneration, peerGeneration := m.directories.generation(p.directory), m.ops.peerGeneration
	if !p.pinned {
		p.pinned, p.directoryGeneration, p.peerGeneration, p.peerID = true, directoryGeneration, peerGeneration, m.peerID
		return nil
	}
	switch {
	case directoryGeneration != p.directoryGeneration:
		return directoryWatchEnded{cephFSObserveGuard("CephFS watched directory policy was registered again")}
	case peerGeneration != p.peerGeneration || m.peerID != p.peerID:
		return directoryWatchEnded{cephFSObserveGuard("CephFS watched peer was removed or replaced")}
	}
	return nil
}

func startDirectoryWatch(ctx context.Context, settings watch.Settings, directory string, read func(context.Context) (DirectoryStatus, error)) (*DirectoryWatch, error) {
	stream, err := watch.Start(ctx, watch.Spec[DirectoryStatus, DirectoryWatchEvent]{
		Name:     directoryWatchOperation + " " + directory,
		Settings: settings,
		Read: func(ctx context.Context) (DirectoryStatus, error) {
			status, err := read(ctx)
			if _, partial := err.(*cephFSObservationPartialError); err == nil || partial {
				// Only other owned members are missing: the directory
				// itself was observed, and DaemonProblems keeps them.
				return status, nil
			}
			status.Ready = false
			var ended directoryWatchEnded
			if errors.As(err, &ended) {
				err = ended.err
			} else if !errors.Is(err, errCephFSObservationGuard) {
				return status, err
			}
			return status, watch.Fatal(fmt.Errorf("%s %s: %w", directoryWatchOperation, directory, err))
		},
		Equal: directoryStatusEqual,
		Event: func(m watch.Meta, s DirectoryStatus, err error) DirectoryWatchEvent {
			e := DirectoryWatchEvent{Status: cloneDirectoryStatus(s), Err: err}
			e.Sequence, e.Started, e.Finished = m.Sequence, m.Started, m.Finished
			e.Kind = ceph.WatchEventKind(m.Kind)
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &DirectoryWatch{stream: stream}, nil
}

// directoryWatchEnded is a refusal of the admission check, which a later
// read cannot undo.
type directoryWatchEnded struct{ err error }

func (e directoryWatchEnded) Error() string { return e.err.Error() }
func (e directoryWatchEnded) Unwrap() error { return e.err }

// directoryStatusEqual compares the semantic fields of two successful reads;
// the free-text FailureReason is not one.
func directoryStatusEqual(a, b DirectoryStatus) bool {
	type compared struct {
		sourceFilesystem, destinationFilesystem     string
		sourceFilesystemID, destinationFilesystemID int
		directory, peerID, mappingState, instanceID string
		daemonName, state                           string
		synced, deleted, renamed                    uint64
		ready                                       bool
	}
	fields := func(s DirectoryStatus) compared {
		return compared{s.SourceFilesystem, s.DestinationFilesystem, s.SourceFilesystemID, s.DestinationFilesystemID,
			s.Directory, s.PeerID, s.MappingState, s.InstanceID, s.DaemonName, s.State,
			s.SnapshotsSynced, s.SnapshotsDeleted, s.SnapsRenamed, s.Ready}
	}
	return fields(a) == fields(b) && snapshotEqual(a.CurrentSnapshot, b.CurrentSnapshot) &&
		snapshotEqual(a.LastSyncedSnapshot, b.LastSyncedSnapshot) && maps.Equal(a.DaemonProblems, b.DaemonProblems)
}

func snapshotEqual(a, b *Snapshot) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// cloneDirectoryStatus copies s with its own snapshots and map.
func cloneDirectoryStatus(s DirectoryStatus) DirectoryStatus {
	if s.CurrentSnapshot != nil {
		current := *s.CurrentSnapshot
		s.CurrentSnapshot = &current
	}
	if s.LastSyncedSnapshot != nil {
		last := *s.LastSyncedSnapshot
		s.LastSyncedSnapshot = &last
	}
	s.DaemonProblems = maps.Clone(s.DaemonProblems)
	return s
}
