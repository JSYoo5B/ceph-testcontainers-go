package ceph

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// poolPGWatchDefaultInterval is the default WatchConfig.Interval of
// Pool.WatchPGs: each read is the five CLI calls of Pool.PGs, and the
// reported PG states refresh only as often as the OSDs send PG statistics to
// the manager, every few seconds.
const poolPGWatchDefaultInterval = 5 * time.Second

// PoolPGWatchEvent is one event of a PoolPGWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time Ceph changed state.
//
// Snapshot is the read as Pool.PGs returns it, with its PGs sorted by PGID
// (pool, then numeric seed) instead of in reported order. Its FSID and pool ID
// are those of the baseline, and its OSDMap epochs and per-PG reported epochs
// and sequences are the native values of that read. The watch compares only
// the pool policies of both brackets, PGReady, the set of PGIDs and, for each
// PG, State, Up, Acting (in order), UpPrimary and ActingPrimary. A read that
// differs only in epochs, reported sequences, StatsInvalid or Stats produces
// no event, so these fields are as fresh as the last event, not the last
// read. Each event has its own snapshot, which the consumer may keep or
// change. Two snapshots separated by a gap, or simply by the polling
// interval, do not establish the transitions in between. On a WatchGap,
// Snapshot is zero and Err is the failure.
type PoolPGWatchEvent struct {
	Sequence          uint64
	Kind              WatchEventKind
	Started, Finished time.Time
	Snapshot          PoolPGSnapshot
	Err               error
}

// PoolPGWatch is a running watch of one pool's reported PGs. One goroutine
// reads them and sends events; Events closes after that goroutine has
// stopped using the cluster. Stop ends the watch and waits for it.
type PoolPGWatch struct {
	stream *watch.Stream[PoolPGWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when WatchPGs returns. The channel closes when the watch ends; Stop
// returns why.
//
//purpose:observation
func (w *PoolPGWatch) Events() <-chan PoolPGWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it, the context's error when the WatchPGs
// context ended, an ErrWatchOverflow error, or the read error that ended it
// (a terminated cluster, a foreign native FSID, or a pool that the handle no
// longer identifies). Buffered events stay readable after Stop. Stop is safe
// to call more than once and from any goroutine; every call returns the same
// cause. Call it before the fixture's cleanup: cancelling the context alone
// does not wait.
//
//purpose:observation,lifetime
func (w *PoolPGWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return notStarted("watch pool PGs")
	}
	return w.stream.Stop()
}

// WatchPGs polls the PGs that Ceph reports for this pool and reports their
// changes. The default WatchConfig.Interval is 5 seconds.
//
// Before it returns, WatchPGs makes one read under ctx and QueryTimeout. A
// failed first read returns its error and starts nothing. That read is the
// baseline: the first event, already buffered, which fixes the watched
// identity, the original cluster FSID and the pool's native ID. PGs are not
// pinned: a split or merge of this pool changes the reported PG set, which
// is an event. Receive the baseline before triggering the change to observe.
//
// Each later read is one Pool.PGs observation, which takes the topology lock
// for its five CLI calls and releases it before the event is sent and during
// the wait for the next read, so the consumer may call any fixture operation
// while the watch runs. A successful read whose compared fields equal the
// previous successful read produces no event. A failed read, a malformed
// report included, produces a WatchGap and the watch continues; the next
// successful read is WatchResumed. These end the watch instead: ctx ending,
// Stop, a terminated cluster, a native FSID of another cluster, a pool that
// this handle removed, that no longer has this name, or whose name now
// belongs to another native pool ID, and a full buffer (ErrWatchOverflow).
// The watch never follows a recreated pool by name and makes no change.
//
// The reads of one sample are not atomic: the two OSDMap brackets can carry
// different epochs and policies, and the PG reports can lag either of them.
// A reported active+clean, up or acting state does not prove that a serving
// OSD applied a map, that a write is visible or that data is durable; check
// those with the SUT or the workload. The stream reports differences between
// samples, not a complete history of native transitions.
//
//purpose:observation
func (p *Pool) WatchPGs(ctx context.Context, cfg WatchConfig) (*PoolPGWatch, error) {
	c, err := p.cluster("Pool.WatchPGs")
	if err != nil {
		return nil, err
	}
	settings, err := watchSettings("watch pool PGs", cfg, poolPGWatchDefaultInterval)
	if err != nil {
		return nil, err
	}
	return startPoolPGWatch(ctx, settings, p.Name, func(ctx context.Context) (PoolPGSnapshot, error) {
		return p.observePGs(ctx, c)
	})
}

// poolPGRead is one Pool.PGs read with its observationEnd marks;
// Pool.observePGs in production.
type poolPGRead func(context.Context) (PoolPGSnapshot, error)

func startPoolPGWatch(ctx context.Context, settings watch.Settings, name string, read poolPGRead) (*PoolPGWatch, error) {
	operation := fmt.Sprintf("watch PGs of pool %q", name)
	pinned := false
	var fsid string
	var poolID int64
	stream, err := watch.Start(ctx, watch.Spec[PoolPGSnapshot, PoolPGWatchEvent]{
		Name:     operation,
		Settings: settings,
		Read: func(ctx context.Context) (PoolPGSnapshot, error) {
			snapshot, err := read(ctx)
			if cause := observationEndCause(err); cause != nil {
				return PoolPGSnapshot{}, watch.Fatal(fmt.Errorf("%s: %w", operation, cause))
			}
			if err != nil {
				return PoolPGSnapshot{}, err
			}
			if !pinned {
				pinned, fsid, poolID = true, snapshot.FSID, snapshot.PoolBefore.ID
			} else if snapshot.FSID != fsid || snapshot.PoolBefore.ID != poolID || snapshot.PoolAfter.ID != poolID {
				return PoolPGSnapshot{}, watch.Fatal(invalid(fmt.Errorf("%s: %w: the read identifies another cluster or pool than the baseline", operation, errStalePool)))
			}
			sortPoolPGs(snapshot.PGs)
			return snapshot, nil
		},
		Equal: equalPoolPGSamples,
		Event: func(m watch.Meta, s PoolPGSnapshot, err error) PoolPGWatchEvent {
			e := PoolPGWatchEvent{Err: err}
			e.Sequence, e.Kind, e.Started, e.Finished = watchMeta(m)
			if err == nil {
				e.Snapshot = clonePoolPGSnapshot(s)
			}
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &PoolPGWatch{stream: stream}, nil
}

// sortPoolPGs orders PGs by pool and numeric seed. The decoder accepted only
// canonical "<pool>.<hex>" PGIDs of one pool, so the seeds parse.
func sortPoolPGs(pgs []PGState) {
	seed := func(id PGID) uint64 {
		_, hex, _ := strings.Cut(string(id), ".")
		n, _ := strconv.ParseUint(hex, 16, 32)
		return n
	}
	slices.SortFunc(pgs, func(a, b PGState) int {
		return cmp.Or(cmp.Compare(seed(a.PGID), seed(b.PGID)), strings.Compare(string(a.PGID), string(b.PGID)))
	})
}

// equalPoolPGSamples compares the semantic fields of two sorted samples:
// policies, readiness, the PG set and each PG's state and placement.
func equalPoolPGSamples(a, b PoolPGSnapshot) bool {
	if a.PoolBefore != b.PoolBefore || a.PoolAfter != b.PoolAfter || a.PGReady != b.PGReady || len(a.PGs) != len(b.PGs) {
		return false
	}
	for i := range a.PGs {
		x, y := a.PGs[i], b.PGs[i]
		if x.PGID != y.PGID || x.State != y.State || x.UpPrimary != y.UpPrimary || x.ActingPrimary != y.ActingPrimary ||
			!slices.Equal(x.Up, y.Up) || !slices.Equal(x.Acting, y.Acting) {
			return false
		}
	}
	return true
}

func clonePoolPGSnapshot(s PoolPGSnapshot) PoolPGSnapshot {
	pgs := slices.Clone(s.PGs)
	for i := range pgs {
		pgs[i].Up, pgs[i].Acting = slices.Clone(pgs[i].Up), slices.Clone(pgs[i].Acting)
	}
	s.PGs = pgs
	return s
}
