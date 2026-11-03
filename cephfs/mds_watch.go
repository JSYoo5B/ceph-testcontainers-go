package cephfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// mdsWatchDefaultInterval is the default WatchConfig.Interval of WatchMDSs:
// each read is one roster snapshot and one fs dump, about the cost of an
// MDSStatus call, and MDS state moves with the daemon beacons, which arrive
// every 4 seconds (mds_beacon_interval).
const mdsWatchDefaultInterval = 2 * time.Second

// mdsWatchOperation names the watch in its errors.
const mdsWatchOperation = "watch MDSs"

// mdsWatchInvalid is a refusal of the watch: an invalid request that names
// the operation.
func mdsWatchInvalid(format string, args ...any) error {
	return servicehook.Invalid(fmt.Errorf(mdsWatchOperation+": "+format, args...))
}

// MDSWatchEvent is one event of an MDSWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time Ceph changed state. Epoch is the native FSMap epoch
// of that read and MDSMapEpoch the epoch of this filesystem's MDSMap in the
// same FSMap; an epoch change that leaves Status equal produces no event.
//
// Status is what MDSStatus reports for that one FSMap. FilesystemName and
// FilesystemID are the watched filesystem and never change. MaxMDS and the
// Active, Standby and StandbyReplay lists are compared field by field (name,
// rank, GID, state, ownership), each list sorted by rank and then name, so a
// takeover, a new GID after a restart or a standby that registers or leaves
// is a change. As in MDSStatus, a rank whose daemon is not up:active (in
// replay, reconnect or rejoin during a takeover) is in no list, so a takeover
// shows as the rank leaving Active and coming back under the new name and
// GID. Each event has its own slices, which the consumer may keep or
// change. A WatchGap carries a zero Status and zero epochs. Two Statuses
// separated by a gap, or simply by the polling interval, do not establish the
// transitions in between.
type MDSWatchEvent struct {
	Sequence           uint64
	Kind               ceph.WatchEventKind
	Started, Finished  time.Time
	Epoch, MDSMapEpoch uint64
	Status             FilesystemStatus
	Err                error
}

// MDSWatch is a running watch of one filesystem's MDS ranks and standbys. One
// goroutine reads the FSMap and sends events; Events closes after that
// goroutine has stopped using the cluster. Stop ends the watch and waits for
// it.
type MDSWatch struct {
	stream *watch.Stream[MDSWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when WatchMDSs returns. The channel closes when the watch ends; Stop
// returns why.
//
//purpose:observation
func (w *MDSWatch) Events() <-chan MDSWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it, the context's error when the WatchMDSs
// context ended, a ceph.ErrWatchOverflow error, or the read error that ended
// it (a terminated cluster, a removed filesystem or one recreated under the
// same name). Buffered events stay readable after Stop. Stop is safe to call
// more than once and from any goroutine; every call returns the same cause.
// Call it before the fixture's cleanup: cancelling the context alone does not
// wait.
//
//purpose:observation,lifetime
func (w *MDSWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return mdsWatchInvalid("the watch is not started by its Watch call")
	}
	return w.stream.Stop()
}

// mdsWatchRead is one serialized read of the owner state and the FSMap;
// mdsFleet.watchSample in production. data is nil when the owner state
// already ends the watch.
type mdsWatchRead func(context.Context) (owner mdsOwnerSample, data []byte, err error)

// WatchMDSs polls the native FSMap and reports changes of this filesystem's
// MDS ranks and standbys. The default WatchConfig.Interval is 2 seconds.
//
// Before it returns, WatchMDSs makes one read under ctx and QueryTimeout. A
// failed first read returns its error and starts nothing; a filesystem that
// is not registered to an open cluster, or has no native identity, is
// ceph.ErrInvalidRequest. That read is the baseline: the first event, already
// buffered. The watched filesystem is fixed by its name and the native FSMap
// ID and pool IDs recorded when it was created, not by its daemons: a
// takeover, a restarted daemon with a new GID, a ScaleMDS or an external
// daemon taking a rank are events. Receive the baseline before triggering
// the change to observe.
//
// Each later read takes the topology lock to snapshot the owned daemons, as
// MDSStatus does, releases it and then reads one fs dump; it takes no lock
// while the event is sent or during the wait for the next read, and never
// the setup gate, so the consumer may call any fixture operation while the
// watch runs, and reads continue while a ScaleMDS is in progress. A
// successful read whose Status equals the previous successful read produces
// no event. A failed read produces a WatchGap and the watch continues; the
// next successful read is WatchResumed. These end the watch instead: ctx
// ending, Stop, a terminated cluster, a filesystem that was removed or is
// absent from a decoded FSMap, one whose FSMap ID or original pools differ
// from those recorded at creation (recreated under the same name), and a
// full buffer (ceph.ErrWatchOverflow). Reaching the requested topology does
// not end it, so later degradation and recovery stay visible.
//
// Ranks and states are what the MONs' FSMap reported, not proof that a
// daemon process runs, that its container is in a given Docker state or
// that clients can reach it; ownership is matched by daemon name against the
// roster snapshot taken just before the FSMap. The stream reports
// differences between samples, not a complete history of native transitions.
// It does not wait for readiness: use WaitReady for the requested standby
// capacity.
//
//purpose:observation
func (fs *Filesystem) WatchMDSs(ctx context.Context, cfg ceph.WatchConfig) (*MDSWatch, error) {
	if fs == nil || fs.base().Cluster() == nil {
		return nil, mdsWatchInvalid("cephfs cluster is unavailable")
	}
	settings, err := watch.Validate(cfg.Interval, cfg.QueryTimeout, cfg.Buffer, mdsWatchDefaultInterval)
	if err != nil {
		return nil, mdsWatchInvalid("%v", err)
	}
	f := fs.fleet()
	return startMDSWatch(ctx, settings, f.core.Name(), f.watchSample)
}

// watchSample is one watch read: the owner sample and, when the cluster is
// open and still registers the filesystem, the FSMap read after the lock was
// released.
func (f mdsFleet) watchSample(ctx context.Context) (mdsOwnerSample, []byte, error) {
	owner, err := f.ownerSample(ctx)
	if err != nil || !owner.open || !owner.registered {
		return owner, nil, err
	}
	data, err := f.readFSMap(ctx)
	if err != nil {
		return owner, nil, fmt.Errorf("read cephfs MDS status: %w", err)
	}
	return owner, data, nil
}

// mdsSample is one successful read of the watched filesystem.
type mdsSample struct {
	epoch, mdsMapEpoch uint64
	status             FilesystemStatus
}

func startMDSWatch(ctx context.Context, settings watch.Settings, name string, read mdsWatchRead) (*MDSWatch, error) {
	stream, err := watch.Start(ctx, watch.Spec[mdsSample, MDSWatchEvent]{
		Name:     mdsWatchOperation,
		Settings: settings,
		Read: func(ctx context.Context) (mdsSample, error) {
			owner, data, err := read(ctx)
			if err != nil {
				return mdsSample{}, err
			}
			return resolveWatchedMDSs(name, owner, data)
		},
		Equal: func(a, b mdsSample) bool { return mdsStatusEqual(a.status, b.status) },
		Event: func(m watch.Meta, s mdsSample, err error) MDSWatchEvent {
			e := MDSWatchEvent{Sequence: m.Sequence, Kind: ceph.WatchEventKind(m.Kind), Started: m.Started, Finished: m.Finished, Err: err}
			if err == nil {
				e.Epoch, e.MDSMapEpoch, e.Status = s.epoch, s.mdsMapEpoch, cloneFilesystemStatus(s.status)
			}
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &MDSWatch{stream: stream}, nil
}

// resolveWatchedMDSs checks one read against the watched identity and
// decodes it. A refused owner state, an absent filesystem and an identity
// change are fatal: a later read cannot undo them. A decode failure is a gap.
func resolveWatchedMDSs(name string, owner mdsOwnerSample, data []byte) (mdsSample, error) {
	switch {
	case !owner.open:
		return mdsSample{}, watch.Fatal(mdsWatchInvalid("the cluster is terminated"))
	case !owner.registered:
		return mdsSample{}, watch.Fatal(mdsWatchInvalid("cephfs %q was removed or this handle no longer owns it", name))
	case owner.native == nil:
		return mdsSample{}, watch.Fatal(mdsWatchInvalid("cephfs %q has no confirmed creation identity", name))
	}
	native := owner.native
	topology, err := decodeCephFSMDSTopology(data, name, owner.daemons)
	if errors.Is(err, errFilesystemAbsent) {
		if err := checkWatchedFSMap(data, name); err != nil {
			return mdsSample{}, err
		}
		return mdsSample{}, watch.Fatal(mdsWatchInvalid("cephfs %q (FSMap ID %d) is absent from the FSMap: the filesystem was removed", name, native.ID))
	}
	if err != nil {
		return mdsSample{}, err
	}
	if topology.status.FilesystemID != native.ID || topology.metadataPool != native.MetadataPool || topology.defaultPool != native.DefaultPool {
		if err := checkWatchedFSMap(data, name); err != nil {
			return mdsSample{}, err
		}
		return mdsSample{}, watch.Fatal(mdsWatchInvalid("cephfs %q now has FSMap ID %d with metadata pool %d and default data pool %d instead of %d, %d and %d: recreated under the same name",
			name, topology.status.FilesystemID, topology.metadataPool, topology.defaultPool, native.ID, native.MetadataPool, native.DefaultPool))
	}
	return mdsSample{epoch: topology.epoch, mdsMapEpoch: topology.mdsMapEpoch, status: *topology.status}, nil
}

// watchedFSMapSchema is the presence view of the FSMap fields that the
// watch's permanent refusals rest on. A field that is absent or null is nil.
type watchedFSMapSchema struct {
	Filesystems *[]struct {
		ID     *int64 `json:"id"`
		MDSMap *struct {
			Name         *string  `json:"fs_name"`
			MetadataPool *int64   `json:"metadata_pool"`
			DataPools    *[]int64 `json:"data_pools"`
		} `json:"mdsmap"`
	} `json:"filesystems"`
}

// checkWatchedFSMap confirms that an FSMap which lacks the watched filesystem,
// or shows it with other identities, is a complete report before the watch
// ends on it: the filesystems list is present, every entry is named, and the
// named entry carries its ID and pools. An empty or truncated reply such as {}
// is a gap, because a later read can still show the filesystem unchanged.
func checkWatchedFSMap(data []byte, name string) error {
	var schema watchedFSMapSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("decode cephfs FSMap: %w", err)
	}
	if schema.Filesystems == nil {
		return errors.New("decode cephfs FSMap: the filesystems list is missing")
	}
	for i, filesystem := range *schema.Filesystems {
		m := filesystem.MDSMap
		if m == nil || m.Name == nil {
			return fmt.Errorf("decode cephfs FSMap: filesystem entry %d has no name", i)
		}
		if *m.Name == name && (filesystem.ID == nil || m.MetadataPool == nil || m.DataPools == nil) {
			return fmt.Errorf("decode cephfs FSMap: the entry of cephfs %q lacks its ID or pools", name)
		}
	}
	return nil
}

// mdsStatusEqual compares the semantic fields of two statuses of the same
// filesystem; the lists are already sorted.
func mdsStatusEqual(a, b FilesystemStatus) bool {
	return a.MaxMDS == b.MaxMDS && slices.Equal(a.Active, b.Active) && slices.Equal(a.Standby, b.Standby) && slices.Equal(a.StandbyReplay, b.StandbyReplay)
}

// cloneFilesystemStatus returns s with its own lists.
func cloneFilesystemStatus(s FilesystemStatus) FilesystemStatus {
	s.Active, s.Standby, s.StandbyReplay = slices.Clone(s.Active), slices.Clone(s.Standby), slices.Clone(s.StandbyReplay)
	return s
}
