//go:build all || integration

package cephfs_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfssession "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/session"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// sessionWatchLog runs a session.Watch and keeps every event it sends, so
// that a burst of gaps during a takeover cannot overflow the watch's buffer.
type sessionWatchLog struct {
	watch  *cephfssession.Watcher
	mu     sync.Mutex
	events []cephfssession.WatchEvent
	notify chan struct{}
	closed chan struct{}
	cause  error
}

// startSessionWatchLog starts the watch of clients on fs and returns once its
// baseline is logged. The watch stops in t's cleanup at the latest.
func startSessionWatchLog(t *testing.T, ctx context.Context, fs *cephfs.Filesystem, interval time.Duration, clients ...uint64) *sessionWatchLog {
	t.Helper()
	w, err := cephfssession.Watch(ctx, fs, ceph.WatchConfig{Interval: interval, QueryTimeout: 15 * time.Second, Buffer: 64}, clients...)
	if err != nil {
		t.Fatal("watch sessions:", err)
	}
	l := &sessionWatchLog{watch: w, notify: make(chan struct{}, 1), closed: make(chan struct{})}
	go func() {
		defer close(l.closed)
		for event := range w.Events() {
			l.mu.Lock()
			l.events = append(l.events, event)
			l.mu.Unlock()
			select {
			case l.notify <- struct{}{}:
			default:
			}
		}
	}()
	t.Cleanup(func() {
		// Stop is idempotent: after a checked stop this repeats its cause.
		// A failed test already reports why the watch may have ended.
		if err := l.stop(t); err != nil && !t.Failed() {
			t.Errorf("stop session watch: %v", err)
		}
	})
	if base := l.await(t, ctx, time.Minute, func(e cephfssession.WatchEvent) bool { return e.Kind == ceph.WatchBaseline }); base.Sequence != 1 {
		t.Fatalf("session watch baseline = %+v", base)
	}
	return l
}

// await returns the first logged event that match accepts, waiting up to
// timeout for it.
func (l *sessionWatchLog) await(t *testing.T, ctx context.Context, timeout time.Duration, match func(cephfssession.WatchEvent) bool) cephfssession.WatchEvent {
	t.Helper()
	var found cephfssession.WatchEvent
	l.awaitLog(t, ctx, timeout, func(events []cephfssession.WatchEvent) bool {
		for _, event := range events {
			if match(event) {
				found = event
				return true
			}
		}
		return false
	})
	return found
}

// awaitLog waits up to timeout until done accepts the whole log so far and
// returns that log.
func (l *sessionWatchLog) awaitLog(t *testing.T, ctx context.Context, timeout time.Duration, done func([]cephfssession.WatchEvent) bool) []cephfssession.WatchEvent {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if events := l.snapshot(); done(events) {
			return events
		}
		select {
		case <-l.notify:
		case <-l.closed:
			if events := l.snapshot(); done(events) {
				return events
			}
			t.Fatalf("the session watch ended (%v) without the awaited event: %+v", l.stop(t), l.snapshot())
		case <-deadline.C:
			t.Fatalf("no awaited session watch event within %s: %+v", timeout, l.snapshot())
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func (l *sessionWatchLog) snapshot() []cephfssession.WatchEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events)
}

// stop ends the watch, waits for the log and returns the watch's cause.
func (l *sessionWatchLog) stop(t *testing.T) error {
	t.Helper()
	l.cause = l.watch.Stop()
	<-l.closed
	return l.cause
}

// checkNoRemoval requires consecutive sequence numbers, gaps without
// sessions and no removal of a client in ids, and counts the gaps.
func (l *sessionWatchLog) checkNoRemoval(t *testing.T, ids ...uint64) (gaps int) {
	t.Helper()
	for i, event := range l.snapshot() {
		if event.Sequence != uint64(i+1) {
			t.Fatalf("session watch event %d has sequence %d", i+1, event.Sequence)
		}
		if event.Kind == ceph.WatchGap {
			gaps++
			if event.Sessions != nil || event.Removed != nil {
				t.Fatalf("a session watch gap reported sessions: %+v", event)
			}
		}
		for _, removed := range event.Removed {
			if slices.Contains(ids, removed.ID) {
				t.Fatalf("session watch removed surviving client.%d: %+v", removed.ID, event)
			}
		}
	}
	return gaps
}

func sessionIDs(sessions []cephfssession.Session) []uint64 {
	var ids []uint64
	for _, s := range sessions {
		ids = append(ids, s.ID)
	}
	return ids
}

// sessionWatchKey is the identity the session watch gives a session.
type sessionWatchKey struct {
	rank    int
	id      uint64
	address string
}

func sessionWatchKeyOf(s cephfssession.Session) sessionWatchKey {
	return sessionWatchKey{rank: s.Rank, id: s.ID, address: s.Address}
}

// rankOneSessionWatch watches two clients of a filesystem with two active
// ranks while rank 1 is taken over: the retained client, and a departing
// client in another container that is evicted after the takeover. Each has
// one session on each rank, opened by its mount on rank 0 and by its file in
// the directory pinned to rank 1.
type rankOneSessionWatch struct {
	log                 *sessionWatchLog
	departingActor      *cephFSActorRun
	retained, departing uint64
	// sessions are the four sessions of the baseline.
	sessions map[sessionWatchKey]cephfssession.Session
	// evicted and finished are when the departing client was evicted and
	// when the retained client was told to close; a client's sessions may
	// change or go only after that.
	evicted, finished time.Time
	takeover          cephfssession.WatchEvent
}

// startRankOneSessionWatch starts the departing client, which creates a file
// in the pinned directory, waits until both ranks hold its open session, and
// starts the watch of both clients. retained are the retained client's open
// sessions on rank 0 and rank 1.
func startRankOneSessionWatch(t *testing.T, ctx context.Context, cluster *ceph.Container, fs *cephfs.Filesystem, pinned string, retained ...cephfssession.Session) *rankOneSessionWatch {
	t.Helper()
	w := &rankOneSessionWatch{retained: retained[0].ID, sessions: map[sessionWatchKey]cephfssession.Session{}}
	w.departingActor = startCephFSActor(t, ctx, cluster, harness.ToolkitContainer(t, cluster), cephFSRetainedActor, fs.FilesystemName, "/tmp/departing-actor",
		pinned+"/departing", pinned+"/departing-renamed", hex.EncodeToString([]byte("departing")), hex.EncodeToString([]byte("-tail")), hex.EncodeToString([]byte("x")), "-")
	opened := w.departingActor.Receipt(t, ctx, 1)
	if opened.Client == 0 || opened.Client == w.retained {
		t.Fatalf("departing client receipt = %+v, retained client.%d", opened, w.retained)
	}
	w.departing = opened.Client
	for _, s := range append(slices.Clone(retained), cephFSRetainedSession(t, ctx, fs, w.departing, 0), cephFSRetainedSession(t, ctx, fs, w.departing, 1)) {
		w.sessions[sessionWatchKeyOf(s)] = s
	}
	if len(w.sessions) != 4 {
		t.Fatalf("want one session per client per rank: %+v", w.sessions)
	}
	w.log = startSessionWatchLog(t, ctx, fs, 500*time.Millisecond, w.retained, w.departing)
	base := w.log.snapshot()[0]
	if base.MaxMDS != 2 || len(base.After) != 2 || !w.listsAll(base.Sessions) {
		t.Fatalf("session watch baseline = %+v, want both ranks and the sessions %+v", base, w.sessions)
	}
	return w
}

// listsAll reports whether sessions are exactly the four baseline sessions,
// each open.
func (w *rankOneSessionWatch) listsAll(sessions []cephfssession.Session) bool {
	return listsOpenSessions(w.sessions, sessions)
}

// listsOpenSessions reports whether sessions list each identity of want once,
// open, and nothing else. A repeated row fails even when the row count
// matches, since it stands in for an identity that is missing.
func listsOpenSessions(want map[sessionWatchKey]cephfssession.Session, sessions []cephfssession.Session) bool {
	seen := make(map[sessionWatchKey]bool, len(sessions))
	for _, s := range sessions {
		key := sessionWatchKeyOf(s)
		if _, ok := want[key]; !ok || seen[key] || s.State != "open" {
			return false
		}
		seen[key] = true
	}
	return len(seen) == len(want)
}

// takenOver checks the first coherent sample with rank 0 held by zero and
// rank 1 by one, the daemon that took it over: rank 0 kept its daemon, so it
// is compared and nothing changed on it, while rank 1 is rebaselined; every
// session is listed, on both ranks, with the address it had.
func (w *rankOneSessionWatch) takenOver(t *testing.T, ctx context.Context, zero, one cephfs.MDSStatus) {
	t.Helper()
	roster := []cephfssession.RankHolder{{Rank: 0, Name: zero.Name, GID: zero.GID}, {Rank: 1, Name: one.Name, GID: one.GID}}
	w.takeover = w.log.await(t, ctx, time.Minute, func(e cephfssession.WatchEvent) bool {
		return e.Kind != ceph.WatchGap && slices.Equal(e.After, roster)
	})
	if !slices.Equal(w.takeover.Rebaselined, []int{1}) || w.takeover.Added != nil || w.takeover.Removed != nil || w.takeover.Changed != nil || !w.listsAll(w.takeover.Sessions) {
		t.Fatalf("the first session sample after the rank 1 takeover = %+v, want rank 1 rebaselined and the sessions %+v", w.takeover, w.sessions)
	}
}

// evict evicts the departing client through the daemon of rank 0. Rank 0
// closes its session and blocklists its address; rank 1 drops its own
// session of the client when its OSDMap carries that entry. The watch must
// remove both sessions, each from a sample of the daemon that listed it, and
// nothing else; the blocklist, read on its own, names the removed address.
func (w *rankOneSessionWatch) evict(t *testing.T, ctx context.Context, cluster *ceph.Container, zero cephfs.MDSStatus) {
	t.Helper()
	w.evicted = time.Now()
	harness.MustCeph(t, ctx, cluster, "tell", "mds."+zero.Name, "client", "evict", "id="+strconv.FormatUint(w.departing, 10))
	removals := w.awaitRemoval(t, ctx, w.departing)
	entries, err := harness.ObserveCluster(cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range w.sessions {
		blocklisted := slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == s.Address })
		if blocklisted != (s.ID == w.departing) {
			t.Fatalf("client.%d %s blocklisted=%t after the eviction of client.%d: %+v", s.ID, s.Address, blocklisted, w.departing, entries)
		}
	}
	// The evicted client's own calls fail: it lost its sessions.
	w.departingActor.Open(t, ctx, "act")
	if acted := w.departingActor.Receipt(t, ctx, 2); acted.Errno == 0 {
		t.Fatalf("the evicted client still acts: %+v", acted)
	} else {
		t.Logf("CEPHFS_SESSION_WATCH evicted client.%d: removal events %v, %s after the eviction; its next calls failed with errno %d (%s)",
			w.departing, removals, time.Since(w.evicted).Round(time.Second), acted.Errno, acted.Error)
	}
	w.departingActor.Open(t, ctx, "finish")
	w.departingActor.Receipt(t, ctx, 3)
}

// finishing records that the retained client is told to close.
func (w *rankOneSessionWatch) finishing() { w.finished = time.Now() }

// unmounted requires the watch to remove both sessions of the retained
// client after it closed and unmounted, while no blocklist entry names it,
// and then stops the watch and checks the whole log.
func (w *rankOneSessionWatch) unmounted(t *testing.T, ctx context.Context, cluster *ceph.Container) {
	t.Helper()
	removals := w.awaitRemoval(t, ctx, w.retained)
	entries, err := harness.ObserveCluster(cluster).BlocklistEntries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	address := w.retainedAddress()
	if slices.ContainsFunc(entries, func(entry ceph.BlocklistEntry) bool { return entry.Address == address }) {
		t.Fatalf("the unmounted client.%d %s is blocklisted: %+v", w.retained, address, entries)
	}
	if err := w.log.stop(t); err != nil {
		t.Fatal("session watch:", err)
	}
	w.check(t)
	t.Logf("CEPHFS_SESSION_WATCH unmounted client.%d: removal events %v, %s after its close; not blocklisted", w.retained, removals, time.Since(w.finished).Round(time.Second))
}

func (w *rankOneSessionWatch) retainedAddress() string {
	for key := range w.sessions {
		if key.id == w.retained {
			return key.address
		}
	}
	return ""
}

// awaitRemoval waits until the watch has removed both sessions of client and
// returns the sequence numbers of the events that removed them.
func (w *rankOneSessionWatch) awaitRemoval(t *testing.T, ctx context.Context, client uint64) []uint64 {
	t.Helper()
	var sequences []uint64
	w.log.awaitLog(t, ctx, 2*time.Minute, func(events []cephfssession.WatchEvent) bool {
		sequences = nil
		removed := map[int]bool{}
		for _, event := range events {
			for _, s := range event.Removed {
				if s.ID == client {
					removed[s.Rank] = true
					sequences = append(sequences, event.Sequence)
				}
			}
		}
		return removed[0] && removed[1]
	})
	return sequences
}

// check requires, over the whole log, consecutive sequence numbers, gaps
// without sessions, one rebaseline (rank 1 at the takeover), no addition,
// and changes and removals only of the four baseline sessions, each removed
// once and only after its client was evicted or closed. It logs the counts
// and the read times.
func (w *rankOneSessionWatch) check(t *testing.T) {
	t.Helper()
	events := w.log.snapshot()
	gaps := w.log.checkNoRemoval(t)
	since := map[uint64]time.Time{w.departing: w.evicted, w.retained: w.finished}
	removed := map[sessionWatchKey]bool{}
	var coherent, gapped []time.Duration
	for _, event := range events {
		if event.Kind == ceph.WatchGap {
			gapped = append(gapped, event.Finished.Sub(event.Started))
		} else {
			coherent = append(coherent, event.Finished.Sub(event.Started))
		}
		if event.Rebaselined != nil && event.Sequence != w.takeover.Sequence {
			t.Fatalf("session watch event %d rebaselined %v: only the takeover of rank 1 changed a holder", event.Sequence, event.Rebaselined)
		}
		if event.Added != nil {
			t.Fatalf("session watch event %d added %+v: neither client opened a session", event.Sequence, event.Added)
		}
		changed := slices.Clone(event.Removed)
		for _, change := range event.Changed {
			changed = append(changed, change.Session)
		}
		for _, s := range changed {
			if _, ok := w.sessions[sessionWatchKeyOf(s)]; !ok || event.Started.Before(since[s.ID]) {
				t.Fatalf("session watch event %d changed or removed %+v before its client was evicted or closed: %+v", event.Sequence, s, event)
			}
		}
		for _, s := range event.Removed {
			if removed[sessionWatchKeyOf(s)] {
				t.Fatalf("session watch event %d removed %+v again", event.Sequence, s)
			}
			removed[sessionWatchKeyOf(s)] = true
		}
	}
	t.Logf("CEPHFS_SESSION_WATCH rank1 takeover: %d events, %d gaps, rank 1 rebaselined at event %d; coherent reads %s, gap reads %s",
		len(events), gaps, w.takeover.Sequence, readTimes(coherent), readTimes(gapped))
}

// readTimes renders the count, minimum, median and maximum of durations.
func readTimes(durations []time.Duration) string {
	if len(durations) == 0 {
		return "none"
	}
	slices.Sort(durations)
	return fmt.Sprintf("n=%d min=%s median=%s max=%s", len(durations), durations[0].Round(time.Millisecond), durations[len(durations)/2].Round(time.Millisecond), durations[len(durations)-1].Round(time.Millisecond))
}
