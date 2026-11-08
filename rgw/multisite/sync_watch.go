package multisite

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/multicluster"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// syncWatchDefaultInterval is the default WatchConfig.Interval of WatchSync
// and WatchBucketSync. One read runs several radosgw-admin commands and takes
// over a second on a two-zone fixture, while RGW data sync itself polls
// every 20 seconds by default.
const syncWatchDefaultInterval = 5 * time.Second

// bucketPolicyWatchDefaultInterval is the default WatchConfig.Interval of
// WatchBucketPolicy. A destination imports a bucket policy within seconds of
// the change, and one read also takes about a second, so a longer interval
// tends to see only the imported result.
const bucketPolicyWatchDefaultInterval = 2 * time.Second

const (
	syncWatchOperation         = "watch RGW zone sync"
	bucketSyncWatchOperation   = "watch RGW bucket sync"
	bucketPolicyWatchOperation = "watch RGW bucket policy import"
)

// errSyncPeriodChanged ends a watch whose observed committed period or realm
// epoch is no longer the baseline's.
var errSyncPeriodChanged = errors.New("the committed period changed; start a new watch")

// syncWatchSettings validates cfg for one Watch of this package.
func syncWatchSettings(operation string, cfg ceph.WatchConfig, defaultInterval time.Duration) (watch.Settings, error) {
	s, err := watch.Validate(cfg.Interval, cfg.QueryTimeout, cfg.Buffer, defaultInterval)
	if err != nil {
		return watch.Settings{}, multisiteInvalid(fmt.Errorf("%s: %v", operation, err))
	}
	return s, nil
}

func syncWatchNotStarted(operation string) error {
	return multisiteInvalid(fmt.Errorf("%s: the watch is not started by its Watch call", operation))
}

// SyncWatchEvent is one event of a SyncWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped. Started and Finished are the local clock around
// the read, not the time RGW changed state. Status is the SyncStatus of that
// read; on a WatchGap it is the partial report of the failed read, evidence
// of what the read got before Err. Ready is the condition WaitSyncReady waits
// for, for the source zones the watch selected: metadata caught up and every
// selected source's data caught up. It is false on a gap. Two statuses
// separated by a gap, or by the polling interval, do not establish the
// transitions in between.
type SyncWatchEvent struct {
	Sequence          uint64
	Kind              ceph.WatchEventKind
	Started, Finished time.Time
	Status            SyncStatus
	Ready             bool
	Err               error
}

// SyncWatch is a running watch of one zone's sync state. One goroutine reads
// the zone and sends events; Events closes after that goroutine has stopped
// using the fixture. Stop ends the watch and waits for it.
type SyncWatch struct {
	stream *watch.Stream[SyncWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when WatchSync returns. The channel closes when the watch ends; Stop
// returns why.
//
//purpose:observation
func (w *SyncWatch) Events() <-chan SyncWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it, the context's error when the WatchSync
// context ended, a ceph.ErrWatchOverflow error, or the read error that ended
// it. Buffered events stay readable after Stop. Stop is safe to call more than
// once and from any goroutine; every call returns the same cause. Call it
// before Terminate: cancelling the context alone does not wait.
//
//purpose:observation,lifetime
func (w *SyncWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return syncWatchNotStarted(syncWatchOperation)
	}
	return w.stream.Stop()
}

// WatchSync polls the sync state of one attached owned zone, the same
// observation as SyncStatus, and reports its changes. sourceZones selects the
// data sources the watch compares and Ready covers, as for WaitSyncReady: no
// sources means metadata only. The default WatchConfig.Interval is 5 seconds.
//
// Before it returns, WatchSync makes one read under ctx and QueryTimeout. A
// failed first read returns its error and starts nothing. That read is the
// baseline: the first event, already buffered, and the identities the watch
// keeps for its lifetime: the realm, zonegroup and zone IDs and the committed
// period ID and realm epoch. Receive the baseline before triggering the
// change to observe.
//
// Each read takes the fixture's topology lock for one bounded observation, as
// SyncStatus does, and holds it neither while the event is sent nor while the
// watch waits for the next read, so the test may change the topology, stop
// gateways or faults while the watch runs. A read compares the metadata
// status and, for each selected source, whether it is reported and its data
// status; the other sources and the zone's MetadataMaster flag are reported
// but not compared, so their changes alone produce no event. A failed read,
// including a zone that has yet to import the current period or a native
// sync-status header whose realm, zonegroup or zone differs from the fixture's
// recorded identity, produces a WatchGap. The next successful read is WatchResumed.
// These end the watch instead: ctx ending, Stop, Terminate
// (ceph.ErrInvalidRequest), a full buffer (ceph.ErrWatchOverflow), a zone or
// selected source that is no longer an attached owned zone of the
// zonegroup, and a committed
// period or realm epoch other than the baseline's, as any sync policy or
// topology commit makes, even while the zone has not imported it yet: watch
// the new period with a new WatchSync.
//
// Ready reports the native comparison at that read. It does not prove that a
// particular object arrived or that sync completes under continuous writes:
// verify the application's objects separately.
//
//purpose:observation
func (f *Fixture) WatchSync(ctx context.Context, cfg ceph.WatchConfig, zoneName string, sourceZones ...string) (*SyncWatch, error) {
	if f == nil {
		return nil, multisiteInvalid(errors.New("RGW multisite fixture is required"))
	}
	settings, err := syncWatchSettings(syncWatchOperation, cfg, syncWatchDefaultInterval)
	if err != nil {
		return nil, err
	}
	sources := slices.Clone(sourceZones)
	var pin *SyncStatus
	stream, err := watch.Start(ctx, watch.Spec[SyncStatus, SyncWatchEvent]{
		Name:     syncWatchOperation,
		Settings: settings,
		Read: func(ctx context.Context) (SyncStatus, error) {
			status, err := f.watchSyncRead(ctx, zoneName, sources)
			if pin == nil {
				if err == nil {
					pin = &status
				}
				return status, err
			}
			// The read fills the committed period before it checks that the
			// zone imported it, so a known new period ends the watch even
			// when a later step of the read failed.
			if status.PeriodID != "" && (status.PeriodID != pin.PeriodID || status.RealmEpoch != pin.RealmEpoch) {
				return status, watch.Fatal(fmt.Errorf("%s: %w", syncWatchOperation, errSyncPeriodChanged))
			}
			if err != nil {
				return status, err
			}
			if refusal := syncWatchPinned(*pin, status); refusal != nil {
				return status, watch.Fatal(fmt.Errorf("%s: %w", syncWatchOperation, refusal))
			}
			return status, nil
		},
		Equal: func(a, b SyncStatus) bool { return syncWatchEqual(a, b, sources) },
		Event: func(m watch.Meta, s SyncStatus, err error) SyncWatchEvent {
			s.DataSources = slices.Clone(s.DataSources)
			e := SyncWatchEvent{Status: s, Err: err, Ready: err == nil && syncStatusReady(s, sources)}
			e.Sequence, e.Kind, e.Started, e.Finished = m.Sequence, ceph.WatchEventKind(m.Kind), m.Started, m.Finished
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &SyncWatch{stream: stream}, nil
}

// watchSyncRead is one admitted SyncStatus read. A terminated fixture or a
// zone selection that no longer resolves is fatal; everything else that fails
// is a gap.
func (f *Fixture) watchSyncRead(ctx context.Context, zoneName string, sources []string) (SyncStatus, error) {
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return SyncStatus{}, err
	}
	defer f.topologyMu.Unlock()
	if f.closed {
		return SyncStatus{}, watch.Fatal(multisiteInvalid(fmt.Errorf("%s: RGW multisite is terminated", syncWatchOperation)))
	}
	if _, err := f.syncStatusZones(zoneName, sources); err != nil {
		return SyncStatus{}, watch.Fatal(fmt.Errorf("%s: %w", syncWatchOperation, err))
	}
	return f.syncStatus(ctx, zoneName)
}

// syncWatchPinned refuses a successful read whose identities or committed
// period differ from the baseline's.
func syncWatchPinned(pin, status SyncStatus) error {
	if status.RealmID != pin.RealmID || status.ZonegroupID != pin.ZonegroupID || status.Zone != pin.Zone || status.ZoneID != pin.ZoneID {
		return errors.New("the zone's realm, zonegroup or zone identity changed")
	}
	if status.PeriodID != pin.PeriodID || status.RealmEpoch != pin.RealmEpoch {
		return errSyncPeriodChanged
	}
	return nil
}

// syncWatchEqual compares the metadata status and the selected sources.
func syncWatchEqual(a, b SyncStatus, sources []string) bool {
	if a.Metadata != b.Metadata {
		return false
	}
	for _, source := range sources {
		x, xok := syncWatchSource(a, source)
		y, yok := syncWatchSource(b, source)
		if xok != yok || x != y {
			return false
		}
	}
	return true
}

func syncWatchSource(status SyncStatus, name string) (DataSyncStatus, bool) {
	i := slices.IndexFunc(status.DataSources, func(s DataSyncStatus) bool { return s.SourceZone == name })
	if i < 0 {
		return DataSyncStatus{}, false
	}
	return status.DataSources[i], true
}

// BucketSyncWatchEvent is one event of a BucketSyncWatch.
//
// Sequence, Kind, Started and Finished are as in SyncWatchEvent. Status is the
// BucketSyncStatus of that read, compared whole; on a WatchGap it is the
// partial report of the failed read, with CaughtUp false. Two statuses
// separated by a gap, or by the polling interval, do not establish the
// transitions in between.
type BucketSyncWatchEvent struct {
	Sequence          uint64
	Kind              ceph.WatchEventKind
	Started, Finished time.Time
	Status            BucketSyncStatus
	Err               error
}

// BucketSyncWatch is a running watch of one bucket checkpoint. It behaves as
// SyncWatch.
type BucketSyncWatch struct {
	stream *watch.Stream[BucketSyncWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when WatchBucketSync returns. The channel closes when the watch ends;
// Stop returns why.
//
//purpose:observation
func (w *BucketSyncWatch) Events() <-chan BucketSyncWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch, as SyncWatch.Stop does. Call it before Terminate.
//
//purpose:observation,lifetime
func (w *BucketSyncWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return syncWatchNotStarted(bucketSyncWatchOperation)
	}
	return w.stream.Stop()
}

// WatchBucketSync polls the bucket checkpoint of one pipe of this owned group
// between two owned zones, the same observation as BucketSyncStatus, and
// reports its changes. The default WatchConfig.Interval is 5 seconds.
//
// Before it returns, WatchBucketSync makes one read under ctx and
// QueryTimeout. A failed first read returns its error and starts nothing.
// That read resolves the checkpoint once, as WaitBucketSyncReady does, and
// the watch keeps it for its lifetime: the group's policy as the fixture
// confirmed it, the two zones, the source and destination bucket instances,
// and the committed period ID and realm epoch of the baseline. Receive the
// baseline before triggering the change to observe.
//
// Each read takes the fixture's topology lock for one bounded observation and
// holds it neither while the event is sent nor between reads. A read first
// checks both bucket instances, then reads the checkpoint as BucketSyncStatus
// does. A failed read, including a bucket that is deleted but not recreated
// and an endpoint that has not imported the current period, produces a
// WatchGap and the watch continues; the next successful read is
// WatchResumed. These end the watch instead: ctx ending, Stop, Terminate
// (ceph.ErrInvalidRequest), a full buffer (ceph.ErrWatchOverflow), the group
// removed or changed through the fixture or a pending change on it, the
// group's native policy changed outside the fixture, a bucket recreated
// under the same name, and a committed period or realm epoch other than the
// baseline's, even when that period no longer publishes the pipe or an
// endpoint has not imported it yet.
//
// CaughtUp reports the native comparison at that read. Native RGW combines
// comparisons from matching pipes, so it does not attest the selected pipe's
// filters, and it does not prove that a particular object arrived: verify
// the application's objects separately.
//
//purpose:observation
func (g *SyncGroup) WatchBucketSync(ctx context.Context, cfg ceph.WatchConfig, pipeID, sourceZone, destinationZone string) (*BucketSyncWatch, error) {
	f, err := g.fixture()
	if err != nil {
		return nil, err
	}
	settings, err := syncWatchSettings(bucketSyncWatchOperation, cfg, syncWatchDefaultInterval)
	if err != nil {
		return nil, err
	}
	pins := &bucketSyncWatchPins{group: g}
	stream, err := watch.Start(ctx, watch.Spec[BucketSyncStatus, BucketSyncWatchEvent]{
		Name:     bucketSyncWatchOperation,
		Settings: settings,
		Read: func(ctx context.Context) (BucketSyncStatus, error) {
			status, err := pins.read(ctx, f, pipeID, sourceZone, destinationZone)
			if err != nil {
				status.CaughtUp = false
			}
			return status, err
		},
		Equal: func(a, b BucketSyncStatus) bool { return a == b },
		Event: func(m watch.Meta, s BucketSyncStatus, err error) BucketSyncWatchEvent {
			e := BucketSyncWatchEvent{Status: s, Err: err}
			e.Sequence, e.Kind, e.Started, e.Finished = m.Sequence, ceph.WatchEventKind(m.Kind), m.Started, m.Finished
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &BucketSyncWatch{stream: stream}, nil
}

// bucketSyncWatchPins is what a bucket watch keeps from its baseline. Only the
// watch's goroutine uses it after the baseline.
type bucketSyncWatchPins struct {
	group      *SyncGroup
	checkpoint *rgwSyncBucketCheckpoint
	policy     map[string]any
	period     string
	epoch      uint64
}

func (p *bucketSyncWatchPins) read(ctx context.Context, f *Fixture, pipeID, sourceZone, destinationZone string) (BucketSyncStatus, error) {
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return BucketSyncStatus{}, err
	}
	defer f.topologyMu.Unlock()
	fatal := func(err error) error { return watch.Fatal(fmt.Errorf("%s: %w", bucketSyncWatchOperation, err)) }
	if p.checkpoint == nil {
		checkpoint, err := p.group.bucketCheckpoint(ctx, pipeID, sourceZone, destinationZone)
		if err != nil {
			return BucketSyncStatus{}, err
		}
		status, err := f.syncBucketStatus(ctx, checkpoint)
		if err != nil {
			return status, err
		}
		p.checkpoint, p.policy = checkpoint, syncJSONClone(p.group.state.group)
		p.period, p.epoch = status.PeriodID, status.RealmEpoch
		return status, nil
	}
	if err := f.validateSyncGroup(p.group, false); err != nil {
		return BucketSyncStatus{}, fatal(err)
	}
	if p.group.state.pending != nil || !reflect.DeepEqual(p.group.state.group, p.policy) {
		return BucketSyncStatus{}, fatal(errors.New("the owned sync group changed since the baseline"))
	}
	for _, side := range []struct {
		zone   *rgwZoneState
		bucket SyncBucketIdentity
	}{{p.checkpoint.source, p.checkpoint.sourceBucket}, {p.checkpoint.destination, p.checkpoint.destBucket}} {
		if err := p.group.checkBucketInstance(ctx, side.zone, side.bucket); err != nil {
			if errors.Is(err, errSyncBucketRecreated) {
				return BucketSyncStatus{}, fatal(err)
			}
			return BucketSyncStatus{}, err
		}
	}
	status, err := f.syncBucketStatus(ctx, p.checkpoint)
	if errors.Is(err, errSyncBucketRecreated) || errors.Is(err, errSyncPolicyChanged) {
		return status, fatal(err)
	}
	// The read fills the committed period before its later steps, which a
	// new period can make fail: a policy commit that drops the pipe fails
	// the publication check, and an endpoint may not have imported it yet.
	// A known new period ends the watch whether or not the rest succeeded.
	if status.PeriodID != "" && (status.PeriodID != p.period || status.RealmEpoch != p.epoch) {
		return status, fatal(errSyncPeriodChanged)
	}
	return status, err
}

// BucketPolicyWatchEvent is one event of a BucketPolicyWatch.
//
// Sequence, Kind, Started and Finished are as in SyncWatchEvent. Status is the
// BucketSyncPolicyStatus of that read; its stages PeriodImported,
// PolicyImported, BucketsImported and Imported are compared. On a WatchGap
// Status contains the partial read with Imported false, or the zero value
// if the query timed out acquiring the topology lock. A referenced bucket that
// the destination does not know yet is a successful read with BucketsImported
// false (and PolicyImported false while that bucket is the group's own), not a
// gap. Two statuses
// separated by a gap, or by the polling interval, do not establish the stages
// in between.
type BucketPolicyWatchEvent struct {
	Sequence          uint64
	Kind              ceph.WatchEventKind
	Started, Finished time.Time
	Status            BucketSyncPolicyStatus
	Err               error
}

// BucketPolicyWatch is a running watch of one bucket policy import. It
// behaves as SyncWatch, except that it ends by itself once it has sent an
// imported status.
type BucketPolicyWatch struct {
	stream *watch.Stream[BucketPolicyWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when WatchBucketPolicy returns. The channel closes when the watch ends;
// Stop returns why.
//
//purpose:observation
func (w *BucketPolicyWatch) Events() <-chan BucketPolicyWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch, as SyncWatch.Stop does: nil also when the watch ended after an
// imported status. Call it before Terminate.
//
//purpose:observation,lifetime
func (w *BucketPolicyWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return syncWatchNotStarted(bucketPolicyWatchOperation)
	}
	return w.stream.Stop()
}

// WatchBucketPolicy polls the import of this owned bucket group's policy
// into an explicit attached destination selected by its pipes, the same
// observation as WaitBucketPolicyReady, and reports its stages as they
// change: the captured committed period, the group's policy and every
// referenced bucket instance imported, and all of them together as Imported.
// The default WatchConfig.Interval is 2 seconds.
//
// Under ctx and QueryTimeout, WatchBucketPolicy first admits the request
// once, as WaitBucketPolicyReady does. An error reading the master during
// admission returns immediately. After admission, a failed first sample is
// retried once a second within QueryTimeout, releasing the topology lock
// between attempts. A fatal error or expiry before the first successful
// sample starts no watch. A destination that has not imported a referenced
// bucket yet is not a failed sample: the baseline reports that stage, however
// long the destination's metadata sync takes.
// The watch keeps the identities captured at admission: the master's committed period, the
// group's policy as the fixture confirmed it, the destination zone and the
// referenced source and destination bucket instances. Start the watch after
// the policy change, as you would WaitBucketPolicyReady; a baseline that is
// already imported is the only event.
//
// Each read takes the fixture's topology lock for one bounded observation and
// holds it neither while the event is sent nor between reads, so the test may
// stop gateways or fault the zone link while the watch runs. A failed read,
// including a gateway that cannot be reached and a bucket that is deleted but
// not recreated, produces a WatchGap and the watch continues; the next
// successful read is WatchResumed. The watch ends with a nil cause after it
// has sent the first imported status. These end it instead: ctx ending, Stop,
// Terminate (ceph.ErrInvalidRequest), a full buffer (ceph.ErrWatchOverflow),
// the group removed or changed through the fixture or a pending change on it,
// the destination no longer attached, the group's native policy changed on
// the master, a referenced bucket recreated under the same name, a destination
// period of another realm, and a committed period on the master other than
// the captured one, as any sync policy or topology commit makes: watch the
// new policy with a new WatchBucketPolicy. A destination that has not yet
// imported the captured period is the first stage, not an error.
//
// Imported is the prerequisite WaitBucketPolicyReady waits for. It does not
// attest data checkpoints, payloads or permissions: verify the application's
// objects and the bucket checkpoint separately.
//
//purpose:observation
func (g *SyncGroup) WatchBucketPolicy(ctx context.Context, cfg ceph.WatchConfig, destinationZone string) (*BucketPolicyWatch, error) {
	if _, err := g.fixture(); err != nil {
		return nil, err
	}
	settings, err := syncWatchSettings(bucketPolicyWatchOperation, cfg, bucketPolicyWatchDefaultInterval)
	if err != nil {
		return nil, err
	}
	var target *policyImportTarget
	stream, err := watch.Start(ctx, watch.Spec[BucketSyncPolicyStatus, BucketPolicyWatchEvent]{
		Name:     bucketPolicyWatchOperation,
		Settings: settings,
		Read: func(ctx context.Context) (BucketSyncPolicyStatus, error) {
			if target == nil {
				admitted, status, err := g.policyImportWatchBaseline(ctx, destinationZone)
				target = admitted
				return status, err
			}
			status, fatal, err := target.lockedSample(ctx)
			if fatal {
				return status, watch.Fatal(fmt.Errorf("%s: %w", bucketPolicyWatchOperation, err))
			}
			return status, err
		},
		Equal: bucketPolicyWatchEqual,
		Final: func(s BucketSyncPolicyStatus) bool { return s.Imported },
		Event: func(m watch.Meta, s BucketSyncPolicyStatus, err error) BucketPolicyWatchEvent {
			s.Buckets = slices.Clone(s.Buckets)
			e := BucketPolicyWatchEvent{Status: s, Err: err}
			e.Sequence, e.Kind, e.Started, e.Finished = m.Sequence, ceph.WatchEventKind(m.Kind), m.Started, m.Finished
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &BucketPolicyWatch{stream: stream}, nil
}

// policyImportWatchBaseline admits the watch and makes its first read. A
// failed first read (a gateway or CLI that does not answer yet) is retried
// once a second until ctx ends, releasing the topology lock in between.
// Admission errors and fatal reads are not retried. A bucket that the
// destination has not imported is a successful read of that stage.
func (g *SyncGroup) policyImportWatchBaseline(ctx context.Context, destinationZone string) (*policyImportTarget, BucketSyncPolicyStatus, error) {
	f := g.owner
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return nil, BucketSyncPolicyStatus{}, err
	}
	target, status, err := g.admitPolicyImport(ctx, destinationZone)
	f.topologyMu.Unlock()
	if err != nil {
		return nil, status, err
	}
	for {
		status, fatal, err := target.lockedSample(ctx)
		if err == nil {
			return target, status, nil
		}
		if fatal || ctx.Err() != nil {
			return nil, status, err
		}
		select {
		case <-ctx.Done():
			return nil, status, fmt.Errorf("%s: no successful first read: %w; last read: %v", bucketPolicyWatchOperation, ctx.Err(), err)
		case <-time.After(time.Second):
		}
	}
}

// lockedSample is one sample under the topology lock.
func (t *policyImportTarget) lockedSample(ctx context.Context) (BucketSyncPolicyStatus, bool, error) {
	f := t.group.owner
	if err := multicluster.LockObservation(ctx, &f.topologyMu); err != nil {
		return BucketSyncPolicyStatus{}, false, err
	}
	defer f.topologyMu.Unlock()
	return t.sample(ctx)
}

// bucketPolicyWatchEqual compares the import stages of successful samples.
func bucketPolicyWatchEqual(a, b BucketSyncPolicyStatus) bool {
	return a.PeriodImported == b.PeriodImported && a.PolicyImported == b.PolicyImported && a.BucketsImported == b.BucketsImported && a.Imported == b.Imported
}
