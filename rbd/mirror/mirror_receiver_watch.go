package mirror

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// receiverWatchDefaultInterval is the default WatchConfig.Interval of
// WatchReceivers: each read checks both clusters' identities and policies
// twice and reads every expected daemon's process state and rbd-mirror status
// twice, a dozen or more container executions, while native leader election
// moves on rbd-mirror's heartbeat of several seconds.
const receiverWatchDefaultInterval = 5 * time.Second

const receiverWatchOperation = "watch RBD receivers"

// ReceiverWatchEvent is one event of a ReceiverWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time a daemon changed state; rbd-mirror reports no
// election epoch, so an event has none.
//
// Status is the ReceiverStatus of that read, compared as a whole: scope and
// pinned identities, each expected daemon's process, native instance, pool
// state, namespace discovery and leader view, the agreed leader, the native
// members, ForeignInstances and DaemonProblems. Lists are sorted; Daemons and
// DaemonProblems are keyed by daemon name. On a WatchGap Status is the partial
// report of the failed read, with Ready false: evidence of what the read got
// before Err, not a successful observation. Each event has its own lists and
// maps, which the consumer may keep or change. Two statuses separated by a
// gap, or simply by the polling interval, do not establish the elections in
// between.
type ReceiverWatchEvent struct {
	Sequence          uint64
	Kind              ceph.WatchEventKind
	Started, Finished time.Time
	Status            ReceiverStatus
	Err               error
}

// ReceiverWatch is a running watch of an owned receiver cohort. One goroutine
// reads the cohort and sends events; Events closes after that goroutine has
// stopped using the fixture. Stop ends the watch and waits for it.
type ReceiverWatch struct {
	stream *watch.Stream[ReceiverWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when WatchReceivers returns. The channel closes when the watch ends; Stop
// returns why.
//
//purpose:observation
func (w *ReceiverWatch) Events() <-chan ReceiverWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it, the context's error when the WatchReceivers
// context ended, a ceph.ErrWatchOverflow error, or the read error that ended
// it (a terminated mirror or cluster, or a changed identity of the watched
// scope or cohort). Buffered events stay readable after Stop. Stop is safe to
// call more than once and from any goroutine; every call returns the same
// cause. Call it before the mirror's cleanup: cancelling the context alone
// does not wait.
//
//purpose:observation,lifetime
func (w *ReceiverWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return rbdWatchNotStarted(receiverWatchOperation)
	}
	return w.stream.Stop()
}

// WatchReceivers polls the receiving scope of this mirror and reports changes
// of an exact owned cohort's discovery, membership and leader agreement, the
// same observation as ReceiverStatus. The default WatchConfig.Interval is 5
// seconds.
//
// Before it returns, WatchReceivers makes one read under ctx and QueryTimeout.
// A failed first read returns its error and starts nothing, and a cohort
// without a daemon is ceph.ErrInvalidRequest. That read is the baseline: the
// first event, already buffered, and the fixed watch identity, as for
// WaitReceiverReady: the cluster, pool, policy and peer generation, the scope,
// and the expected daemons with their original handles and container IDs.
// With no names the cohort is every owned daemon at the baseline, stopped
// ones included. Receive the baseline before triggering the change to observe.
//
// Each later read takes the owner and member gates for one bounded
// observation, as ReceiverStatus does, and the clusters' topology locks only to
// check that they are not terminated; it holds none of them when the event is
// sent or during the wait for the next read, so the consumer may call any
// fixture operation while the watch runs. A stopped, restarted or undiscovered
// member, a new native instance of the same container, a moved leader,
// ForeignInstances and leader disagreement are changes of Status, not errors.
// A read whose Status equals the previous successful read produces no event.
// A failed read produces a WatchGap and the watch continues; the next
// successful read is WatchResumed. These end the watch instead: ctx ending,
// Stop, a terminated mirror or cluster (ceph.ErrInvalidRequest), a full buffer
// (ceph.ErrWatchOverflow), and every refusal that also ends WaitReceiverReady:
// a removed or added daemon of an all-owned cohort, a replaced container,
// handle or client, a changed cluster, pool, policy or peer (Rebootstrap
// included), and a malformed native report. Those errors carry no native
// output.
//
// Ready is discovery and election agreement only. No event proves that an
// image replays or that a write or checkpoint arrived, and the stream reports
// differences between samples, not a complete history of native elections.
//
//purpose:observation
func (m *Mirror) WatchReceivers(ctx context.Context, cfg ceph.WatchConfig, expectedDaemons ...string) (*ReceiverWatch, error) {
	if m == nil {
		return nil, rbdReceiverGuard("RBD receiver fixture is unavailable")
	}
	settings, err := rbdWatchSettings(receiverWatchOperation, cfg, receiverWatchDefaultInterval)
	if err != nil {
		return nil, err
	}
	names := slices.Clone(expectedDaemons)
	return startReceiverWatch(ctx, settings, m.checkWatchOpen, func(ctx context.Context, previous *rbdReceiverWitness) (ReceiverStatus, *rbdReceiverWitness, error) {
		return m.receiverStatusForScope(ctx, nil, names, previous)
	})
}

// WatchReceivers polls this bound namespace pair and reports changes of the
// original owner's exact owned cohort, the same observation as
// ReceiverStatus. It behaves as Mirror.WatchReceivers in every other respect,
// with the bound scope and its selected policies pinned as well; a
// Rebootstrap attempt or Terminate of the owner ends it. The default
// WatchConfig.Interval is 5 seconds.
//
//purpose:observation
func (v *Namespace) WatchReceivers(ctx context.Context, cfg ceph.WatchConfig, expectedDaemons ...string) (*ReceiverWatch, error) {
	if v == nil || v.owner == nil || v.scope.binding == nil {
		return nil, rbdReceiverGuard("RBD namespace binding is unavailable")
	}
	settings, err := rbdWatchSettings(receiverWatchOperation, cfg, receiverWatchDefaultInterval)
	if err != nil {
		return nil, err
	}
	names := slices.Clone(expectedDaemons)
	return startReceiverWatch(ctx, settings, v.owner.checkWatchOpen, func(ctx context.Context, previous *rbdReceiverWitness) (ReceiverStatus, *rbdReceiverWitness, error) {
		return v.owner.receiverStatusForScope(ctx, &v.scope, names, previous)
	})
}

// rbdReceiverSample is one scoped receiver observation that checks, from the
// second read on, the witness the first read captured.
type rbdReceiverSample func(context.Context, *rbdReceiverWitness) (ReceiverStatus, *rbdReceiverWitness, error)

// startReceiverWatch runs the one receiver watch lifecycle of both scopes.
// open is Mirror.checkWatchOpen of the owner in production.
func startReceiverWatch(ctx context.Context, settings watch.Settings, open func(context.Context, string) error, read rbdReceiverSample) (*ReceiverWatch, error) {
	var witness *rbdReceiverWitness
	stream, err := watch.Start(ctx, watch.Spec[ReceiverStatus, ReceiverWatchEvent]{
		Name:     receiverWatchOperation,
		Settings: settings,
		Read: func(ctx context.Context) (ReceiverStatus, error) {
			if err := open(ctx, receiverWatchOperation); err != nil {
				if errors.Is(err, errRBDWatchTerminated) {
					return ReceiverStatus{}, watch.Fatal(err)
				}
				return ReceiverStatus{}, err
			}
			first := witness == nil
			status, captured, err := read(ctx, witness)
			if first {
				witness = captured
			}
			if err != nil {
				status.Ready = false
				if rbdReceiverPermanent(err) {
					return status, watch.Fatal(fmt.Errorf("%s: %w", receiverWatchOperation, err))
				}
				return status, err
			}
			if first && len(status.ExpectedDaemons) == 0 {
				return status, servicehook.Invalid(fmt.Errorf("%s: the cohort has no daemon", receiverWatchOperation))
			}
			return status, nil
		},
		Equal: func(a, b ReceiverStatus) bool { return reflect.DeepEqual(a, b) },
		Event: func(m watch.Meta, s ReceiverStatus, err error) ReceiverWatchEvent {
			e := ReceiverWatchEvent{Status: cloneReceiverStatus(s), Err: err}
			e.Sequence, e.Kind, e.Started, e.Finished = rbdWatchMeta(m)
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &ReceiverWatch{stream: stream}, nil
}

// cloneReceiverStatus copies every list and map of s.
func cloneReceiverStatus(s ReceiverStatus) ReceiverStatus {
	s.ExpectedDaemons = slices.Clone(s.ExpectedDaemons)
	s.Daemons = maps.Clone(s.Daemons)
	s.Instances = slices.Clone(s.Instances)
	s.ForeignInstances = slices.Clone(s.ForeignInstances)
	s.DaemonProblems = maps.Clone(s.DaemonProblems)
	return s
}
