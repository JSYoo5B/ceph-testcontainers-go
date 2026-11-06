package session

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// watchDefaultInterval is the default WatchConfig.Interval of Watch: each
// read is two FSMap reads around one session listing per active rank, and
// session states move on the scale of session_timeout, at least 30 seconds.
const watchDefaultInterval = 2 * time.Second

// watchOperation names the watch in its errors.
const watchOperation = "watch CephFS sessions"

// RankHolder is the MDS daemon that held one active rank when a sample read
// the FSMap: the rank, the daemon's name and its GID. A daemon that restarts
// or takes over a rank has a new GID, so an unchanged GID means the same
// daemon instance kept the rank.
type RankHolder struct {
	Rank int
	Name string
	GID  uint64
}

// Change is a session whose native state differs from the previous coherent
// sample: Session is the current row and PreviousState the state it had.
type Change struct {
	Session       Session
	PreviousState string
}

// WatchEvent is one event of a Watcher.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time a session changed.
//
// Before and After are the active ranks, sorted by rank, that the FSMap
// reads before and after the session listings reported; MaxMDS is the
// max_mds of both. A successful event comes from a coherent sample: Before
// and After are equal, every rank below MaxMDS is active and at least one rank
// is. On a WatchGap they show what was read, nil for a read that failed, and
// Err says why the sample was not used: a failed query, or a roster that
// changed or did not cover the ranks.
//
// Sessions are the selected sessions of a coherent sample, sorted by rank,
// ID and address, one row per session per rank; empty but not nil when no
// selected session is listed, and nil on a gap. A session is
// identified by its rank, client ID and address (with its nonce): the same
// client ID on another rank, or with another address after a reconnect, is
// another session. Rank, ID, Address and State are compared; Caps and the
// client metadata travel with the event but cause none by themselves.
//
// Added, Removed and Changed compare this sample with the previous coherent
// one, across any gaps in between, and only for a rank that the same daemon
// (GID) held in both. A session in Removed was listed by that daemon before
// and is not listed by it now: it closed, was evicted or killed, or
// reconnected under a new address (then it is also in Added). Removal does
// not establish why, nor blocklisting, flush completion or data survival;
// read those independently. Rebaselined lists, sorted, every rank whose
// holder differs between the two samples, including a rank that became or
// stopped being active: its sessions are in Sessions but in none of Added,
// Removed or Changed, because the sessions a new daemon lists, after a
// takeover for example, are its own reconnected view, not a continuation of
// the previous daemon's list. The baseline has only Sessions. Each event has
// its own slices, which the consumer may keep or change.
type WatchEvent struct {
	Sequence          uint64
	Kind              ceph.WatchEventKind
	Started, Finished time.Time
	MaxMDS            int
	Before, After     []RankHolder
	Sessions          []Session
	Added, Removed    []Session
	Changed           []Change
	Rebaselined       []int
	Err               error
}

// Watcher is a running watch of the client sessions of one filesystem. One
// goroutine reads the sessions and sends events; Events closes after that
// goroutine has stopped using the filesystem. Stop ends the watch and waits
// for it.
type Watcher struct {
	stream *watch.Stream[WatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when Watch returns. The channel closes when the watch ends; Stop returns
// why.
//
//purpose:observation
func (w *Watcher) Events() <-chan WatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it, the context's error when the Watch context
// ended, a ceph.ErrWatchOverflow error, or the ceph.ErrInvalidRequest read
// error that ended it (a terminated cluster, or a filesystem that was
// removed, is no longer the cluster's owned one or was recreated under the
// same name). Buffered events stay readable after Stop. Stop is safe to call
// more than once and from any goroutine; every call returns the same cause.
// Call it before the fixture's cleanup: cancelling the context alone does not
// wait.
//
//purpose:observation,lifetime
func (w *Watcher) Stop() error {
	if w == nil || !w.stream.Started() {
		return servicehook.Invalid(errors.New(watchOperation + ": the watch is not started by Watch"))
	}
	return w.stream.Stop()
}

// Watch polls the client sessions of fs, as List reads them, and reports
// sessions that appear, change native state (open, stale, killing, closing
// and the others the MDS reports) or disappear. clients, when given, selects
// the sessions whose Session.ID is one of them; without it every session is
// selected, including the fixture's own short-lived libcephfs probes. The
// default WatchConfig.Interval is 2 seconds.
//
// Before it returns, Watch makes one read under ctx and QueryTimeout. A
// failed or incoherent first read returns its error and starts nothing; a
// filesystem that is not the cluster's active owned one, an invalid
// configuration and a zero client ID are ceph.ErrInvalidRequest. That read is
// the baseline: the first event, already buffered. Receive it before
// triggering the change to observe.
//
// Each read takes the filesystem's admission, as List does, through its
// QueryTimeout context, reads the active ranks, lists the sessions of each
// active rank in turn and reads the active ranks again, and releases the
// admission before the event is sent and during the wait for the next read.
// The ranks are listed one after another, so a sample is not an atomic
// cross-rank snapshot: each rank's list is one native reply, taken at its own
// moment. The two rank reads bracket the listings: a sample whose roster of
// rank holders (rank and GID) or max_mds changed between them, in which a rank
// below max_mds is not active, or in which no rank is active, is a WatchGap,
// as is a failed query, and the watch continues; the next coherent read is
// WatchResumed. A gap never removes a session. An MDS failover therefore
// shows as gaps while the rank is not active and then as a resumed sample
// that rebaselines the rank. A coherent read whose selected sessions and
// roster equal the previous one produces no event.
//
// These end the watch with ceph.ErrInvalidRequest instead: a terminated
// cluster, a filesystem that was removed or is no longer the registered one,
// and an FSMap that no longer lists the filesystem or lists it under another
// FSMap ID (recreated under the same name). ctx ending, Stop and a full
// buffer (ceph.ErrWatchOverflow) end it too.
//
// The roster is read from the FSMap; the session listings are addressed by
// rank. Unchanged GIDs before and after mean that the daemon that answered
// each rank held it throughout, since a daemon instance never returns to a
// rank under the same GID, but the two reads are not one atomic snapshot.
// Polling misses a session that opens and closes between reads, and
// intermediate states that last shorter than the interval. Native tests
// cover one and two active ranks; more ranks follow the same rules but are
// exercised by unit tests only. The watch never evicts, blocklists or
// otherwise changes a session.
//
//purpose:observation
func Watch(ctx context.Context, fs *cephfs.Filesystem, cfg ceph.WatchConfig, clients ...uint64) (*Watcher, error) {
	if fs == nil {
		return nil, servicehook.Invalid(errors.New(watchOperation + ": the filesystem is nil"))
	}
	settings, err := watch.Validate(cfg.Interval, cfg.QueryTimeout, cfg.Buffer, watchDefaultInterval)
	if err != nil {
		return nil, servicehook.Invalid(fmt.Errorf(watchOperation+": %v", err))
	}
	if slices.Contains(clients, 0) {
		return nil, servicehook.Invalid(errors.New(watchOperation + ": client ID 0 selects no session"))
	}
	core := coreOf(fs)
	return startWatch(ctx, settings, fmt.Sprintf("%s of %q", watchOperation, core.Name()), newSampler(core, clients).read)
}

// sessionKey is the identity of one session on one rank.
type sessionKey struct {
	rank    int
	id      uint64
	address string
}

func keyOf(s Session) sessionKey { return sessionKey{rank: s.Rank, id: s.ID, address: s.Address} }

// sample is one read. A coherent sample has the diff against the previous
// coherent one; a gap carries the rosters that were read.
type sample struct {
	maxMDS         int
	before, after  []RankHolder
	sessions       []Session
	added, removed []Session
	changed        []Change
	rebaselined    []int
}

// sampler is the producer's read state: the selection and the last coherent
// sample, against which the next one is compared. Only the producer uses it.
type sampler struct {
	core    *fsbase.Core
	clients map[uint64]bool
	last    *coherent
}

// coherent is the comparison state of one coherent sample.
type coherent struct {
	holders  map[int]uint64
	sessions map[sessionKey]Session
}

func newSampler(core *fsbase.Core, clients []uint64) *sampler {
	s := &sampler{core: core}
	if len(clients) > 0 {
		s.clients = make(map[uint64]bool, len(clients))
		for _, id := range clients {
			s.clients[id] = true
		}
	}
	return s
}

// watchEnded is a read refusal that a later read cannot undo.
type watchEnded struct{ err error }

func (e watchEnded) Error() string { return e.err.Error() }
func (e watchEnded) Unwrap() error { return e.err }

// read is one admitted, bracketed sample.
func (s *sampler) read(ctx context.Context) (sample, error) {
	ctx, fsID, done, err := begin(ctx, s.core)
	if err != nil {
		if errors.Is(err, ceph.ErrInvalidRequest) {
			return sample{}, watchEnded{err}
		}
		return sample{}, err
	}
	defer done()
	var out sample
	var beforeMax, afterMax int
	beforeMax, out.before, err = s.roster(ctx, fsID)
	if err != nil {
		return out, err
	}
	var listed []Session
	for _, holder := range out.before {
		ranked, err := rankSessions(ctx, s.core, holder.Rank)
		if err != nil {
			return out, err
		}
		listed = append(listed, ranked...)
	}
	afterMax, out.after, err = s.roster(ctx, fsID)
	if err != nil {
		return out, err
	}
	if err := coverage(beforeMax, afterMax, out.before, out.after); err != nil {
		return out, err
	}
	out.maxMDS = beforeMax
	current := &coherent{holders: make(map[int]uint64, len(out.after)), sessions: make(map[sessionKey]Session)}
	for _, holder := range out.after {
		current.holders[holder.Rank] = holder.GID
	}
	for _, session := range listed {
		if s.clients == nil || s.clients[session.ID] {
			current.sessions[keyOf(session)] = session
		}
	}
	out.sessions = sortedSessions(slices.Collect(maps.Values(current.sessions)))
	if s.last != nil {
		out.diff(s.last, current)
	}
	s.last = current
	return out, nil
}

// roster reads the active ranks with their holders, sorted by rank. A
// filesystem whose FSMap ID is not fsID ends the watch; so does a failed read
// that a native pool read explains as a removed or replaced filesystem.
func (s *sampler) roster(ctx context.Context, fsID int64) (int, []RankHolder, error) {
	ranks, err := s.core.ActiveRanks(ctx)
	if err != nil {
		if _, identity := s.core.ReadNativePools(ctx); fsbase.IdentityChanged(identity) {
			return 0, nil, watchEnded{identity}
		}
		return 0, nil, err
	}
	if ranks.FilesystemID != fsID {
		return 0, nil, watchEnded{fmt.Errorf("the FSMap reports FSMap ID %d instead of %d: the filesystem was recreated under the same name", ranks.FilesystemID, fsID)}
	}
	holders := make([]RankHolder, 0, len(ranks.Active))
	for _, active := range ranks.Active {
		holders = append(holders, RankHolder{Rank: active.Rank, Name: active.Name, GID: active.GID})
	}
	slices.SortFunc(holders, func(a, b RankHolder) int { return cmp.Compare(a.Rank, b.Rank) })
	return ranks.MaxMDS, holders, nil
}

// coverage refuses a sample whose bracket is not one stable roster that
// covers every rank below max_mds.
func coverage(beforeMax, afterMax int, before, after []RankHolder) error {
	switch {
	case beforeMax != afterMax:
		return fmt.Errorf("incoherent session sample: max_mds changed from %d to %d during the sample", beforeMax, afterMax)
	case !slices.Equal(before, after):
		return fmt.Errorf("incoherent session sample: the active ranks changed during the sample from %v to %v", before, after)
	case len(after) == 0:
		return errors.New("incomplete session sample: no rank is active")
	}
	for rank := range beforeMax {
		if !slices.ContainsFunc(after, func(h RankHolder) bool { return h.Rank == rank }) {
			return fmt.Errorf("incomplete session sample: rank %d of max_mds %d is not active", rank, beforeMax)
		}
	}
	return nil
}

// diff compares current with the previous coherent sample, rank by rank, for
// the ranks the same daemon held in both, and rebaselines the others.
func (out *sample) diff(previous, current *coherent) {
	same := func(rank int) bool {
		gid, ok := previous.holders[rank]
		now, held := current.holders[rank]
		return ok && held && gid == now
	}
	ranks := map[int]bool{}
	for rank := range previous.holders {
		ranks[rank] = true
	}
	for rank := range current.holders {
		ranks[rank] = true
	}
	for rank := range ranks {
		if !same(rank) {
			out.rebaselined = append(out.rebaselined, rank)
		}
	}
	slices.Sort(out.rebaselined)
	for key, session := range current.sessions {
		if !same(key.rank) {
			continue
		}
		if before, ok := previous.sessions[key]; !ok {
			out.added = append(out.added, session)
		} else if before.State != session.State {
			out.changed = append(out.changed, Change{Session: session, PreviousState: before.State})
		}
	}
	for key, session := range previous.sessions {
		if _, ok := current.sessions[key]; !ok && same(key.rank) {
			out.removed = append(out.removed, session)
		}
	}
	out.added, out.removed = sortedSessions(out.added), sortedSessions(out.removed)
	slices.SortFunc(out.changed, func(a, b Change) int { return compareSessions(a.Session, b.Session) })
}

func compareSessions(a, b Session) int {
	return cmp.Or(cmp.Compare(a.Rank, b.Rank), cmp.Compare(a.ID, b.ID), cmp.Compare(a.Address, b.Address))
}

func sortedSessions(sessions []Session) []Session {
	slices.SortFunc(sessions, compareSessions)
	return sessions
}

// equalSamples compares the roster and the identity and state of the
// selected sessions of two coherent samples.
func equalSamples(a, b sample) bool {
	return a.maxMDS == b.maxMDS && slices.Equal(a.after, b.after) &&
		slices.EqualFunc(a.sessions, b.sessions, func(x, y Session) bool { return keyOf(x) == keyOf(y) && x.State == y.State })
}

func startWatch(ctx context.Context, settings watch.Settings, name string, read func(context.Context) (sample, error)) (*Watcher, error) {
	stream, err := watch.Start(ctx, watch.Spec[sample, WatchEvent]{
		Name:     name,
		Settings: settings,
		Read: func(ctx context.Context) (sample, error) {
			s, err := read(ctx)
			var ended watchEnded
			if errors.As(err, &ended) {
				return sample{}, watch.Fatal(servicehook.Invalid(fmt.Errorf("%s: %w", name, ended.err)))
			}
			return s, err
		},
		Equal: equalSamples,
		Event: func(m watch.Meta, s sample, err error) WatchEvent {
			e := WatchEvent{Sequence: m.Sequence, Kind: ceph.WatchEventKind(m.Kind), Started: m.Started, Finished: m.Finished,
				Before: slices.Clone(s.before), After: slices.Clone(s.after), Err: err}
			if err == nil {
				e.MaxMDS = s.maxMDS
				e.Sessions, e.Added, e.Removed = append([]Session{}, s.sessions...), slices.Clone(s.added), slices.Clone(s.removed)
				e.Changed, e.Rebaselined = slices.Clone(s.changed), slices.Clone(s.rebaselined)
			}
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &Watcher{stream: stream}, nil
}
