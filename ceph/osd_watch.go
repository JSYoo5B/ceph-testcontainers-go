package ceph

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
	"github.com/google/uuid"
)

// osdWatchDefaultInterval is the default WatchConfig.Interval of WatchOSDs:
// each read is one osd dump.
const osdWatchDefaultInterval = 2 * time.Second

// OSDWatchEvent is one event of an OSDWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time Ceph changed state. Epoch is the native OSDMap epoch
// of that read; an epoch change that leaves States equal produces no event.
//
// States are the watched OSDs, sorted by ID, compared field by field (ID,
// UUID, Up, In, Weight). Each event has its own slice, which the consumer may
// keep or change. Two States separated by a gap, or simply by the polling
// interval, do not establish the transitions in between.
type OSDWatchEvent struct {
	Sequence          uint64
	Kind              WatchEventKind
	Started, Finished time.Time
	Epoch             uint64
	States            []OSDState
	Err               error
}

// OSDWatch is a running watch of owned OSD state. One goroutine reads the
// OSDMap and sends events; Events closes after that goroutine has stopped
// using the cluster. Stop ends the watch and waits for it.
type OSDWatch struct {
	stream *watch.Stream[OSDWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when WatchOSDs returns. The channel closes when the watch ends; Stop
// returns why.
//
//purpose:observation
func (w *OSDWatch) Events() <-chan OSDWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it, the context's error when the WatchOSDs
// context ended, an ErrWatchOverflow error, or the read error that ended it
// (a terminated cluster, or an identity change of a watched OSD). Buffered
// events stay readable after Stop. Stop is safe to call more than once and
// from any goroutine; every call returns the same cause. Call it before the
// fixture's cleanup: cancelling the context alone does not wait.
//
//purpose:observation,lifetime
func (w *OSDWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return notStarted("watch OSDs")
	}
	return w.stream.Stop()
}

// osdSampleRead is one serialized read of the OSDMap and the owned
// registrations; Container.ownedOSDSample in production.
type osdSampleRead func(context.Context) (osdPolicySnapshot, map[int]*osdRegistration, error)

// WatchOSDs polls the native OSDMap and reports changes of the owned OSDs.
// The default WatchConfig.Interval is 2 seconds.
//
// Before it returns, WatchOSDs makes one read under ctx and QueryTimeout. A
// failed first read returns its error and starts nothing; a cluster that owns
// no OSD is ErrInvalidRequest. That read is the baseline: the first event,
// already buffered, and the fixed set of watched OSDs, pinned by ID and native
// UUID. OSDs added later are not watched. Receive the baseline before
// triggering the change to observe.
//
// Each later read takes the topology lock for one osd dump, as OSDStates
// does, and releases it before the event is sent and during the wait for the
// next read, so the consumer may call any fixture operation while the watch
// runs. A successful read whose watched states equal the previous successful
// read produces no event. A failed read produces a WatchGap and the watch
// continues; the next successful read is WatchResumed. These end the watch
// instead: ctx ending, Stop, a terminated cluster, a watched OSD that is no
// longer owned (removed or detached) or whose native ID now has another UUID,
// and a full buffer (ErrWatchOverflow).
//
// Up and down are the reported OSDMap state, not proof that the daemon process
// runs or that clients can reach it. The stream reports differences between
// samples, not a complete history of native transitions.
//
//purpose:observation
func (o *Observer) WatchOSDs(ctx context.Context, cfg WatchConfig) (*OSDWatch, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	settings, err := watchSettings("watch OSDs", cfg, osdWatchDefaultInterval)
	if err != nil {
		return nil, err
	}
	return startOSDWatch(ctx, settings, c.ownedOSDSample)
}

// osdSample is one successful or failed read of the watched OSDs.
type osdSample struct {
	epoch  uint64
	states []OSDState
}

func startOSDWatch(ctx context.Context, settings watch.Settings, read osdSampleRead) (*OSDWatch, error) {
	var cohort map[int]uuid.UUID
	stream, err := watch.Start(ctx, watch.Spec[osdSample, OSDWatchEvent]{
		Name:     "watch OSDs",
		Settings: settings,
		Read: func(ctx context.Context) (osdSample, error) {
			dump, owned, err := read(ctx)
			if err != nil {
				if errors.Is(err, errClusterTerminated) {
					return osdSample{}, watch.Fatal(err)
				}
				return osdSample{}, err
			}
			if cohort == nil {
				return pinOSDCohort(dump, owned, &cohort)
			}
			states, err := resolveWatchedOSDs(dump, owned, cohort)
			if err != nil {
				return osdSample{}, watch.Fatal(err)
			}
			return osdSample{epoch: dump.epoch, states: states}, nil
		},
		Equal: func(a, b osdSample) bool { return slices.Equal(a.states, b.states) },
		Event: func(m watch.Meta, s osdSample, err error) OSDWatchEvent {
			e := OSDWatchEvent{Err: err}
			e.Sequence, e.Kind, e.Started, e.Finished = watchMeta(m)
			if err == nil {
				e.Epoch, e.States = s.epoch, slices.Clone(s.states)
			}
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &OSDWatch{stream: stream}, nil
}

// pinOSDCohort resolves the baseline read and fixes the watched OSDs.
func pinOSDCohort(dump osdPolicySnapshot, owned map[int]*osdRegistration, cohort *map[int]uuid.UUID) (osdSample, error) {
	if len(owned) == 0 {
		return osdSample{}, invalidRequest("watch OSDs: the cluster owns no OSD")
	}
	states, err := resolveOwnedOSDStates(dump, owned)
	if err != nil {
		return osdSample{}, err
	}
	pinned := make(map[int]uuid.UUID, len(states))
	for _, s := range states {
		pinned[s.ID] = uuid.MustParse(s.UUID) // resolveOwnedOSDState parsed it
	}
	*cohort = pinned
	return osdSample{epoch: dump.epoch, states: states}, nil
}

// resolveWatchedOSDs checks the watched OSDs of one successful read. Every
// refusal is an identity change that a later read cannot undo.
func resolveWatchedOSDs(dump osdPolicySnapshot, owned map[int]*osdRegistration, cohort map[int]uuid.UUID) ([]OSDState, error) {
	states := make([]OSDState, 0, len(cohort))
	for id, pinned := range cohort {
		state, err := resolveOwnedOSDState(dump, id, owned[id])
		if err != nil {
			return nil, fmt.Errorf("watch OSDs: watched osd.%d: %w", id, err)
		}
		if uuid.MustParse(state.UUID) != pinned {
			return nil, fmt.Errorf("watch OSDs: watched osd.%d now has native UUID %s instead of %s", id, state.UUID, pinned)
		}
		states = append(states, state)
	}
	slices.SortFunc(states, func(a, b OSDState) int { return a.ID - b.ID })
	return states, nil
}
