package ceph

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// managerWatchDefaultInterval is the default WatchConfig.Interval of
// WatchManagers: each read is three commands, the FSID before and after one
// mgr dump.
const managerWatchDefaultInterval = 3 * time.Second

// ManagerWatchEvent is one event of a ManagerWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time Ceph changed state. Epoch is the native MGRMap epoch
// of that read.
//
// Status is the MGRMap's active manager, its availability and the registered
// standbys, as ManagerStatus reports them, with Standbys sorted by name and
// GID. An empty ActiveName with Available false is a successful observation
// of a map without an active manager, not a failed read. Services are the
// service URLs of the same MGRMap, keyed by module name: the URLs that this
// map's active manager reported, as ManagerServices returns them, not a
// separate query. An advertised URL does not prove that its listener accepts
// requests.
//
// Status and Services are compared field by field; the same name with another
// GID is a change, and an epoch change that leaves them equal produces no
// event. Each event has its own slice and map, which the consumer may keep or
// change. Two events separated by a gap, or simply by the polling interval, do
// not establish the transitions in between.
type ManagerWatchEvent struct {
	Sequence          uint64
	Kind              WatchEventKind
	Started, Finished time.Time
	Epoch             uint64
	Status            ManagerStatus
	Services          map[string]string
	Err               error
}

// ManagerWatch is a running watch of the active MGR and its standbys. One
// goroutine reads the MGRMap and sends events; Events closes after that
// goroutine has stopped using the cluster. Stop ends the watch and waits for
// it.
type ManagerWatch struct {
	stream *watch.Stream[ManagerWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when WatchManagers returns. The channel closes when the watch ends; Stop
// returns why.
//
//purpose:observation
func (w *ManagerWatch) Events() <-chan ManagerWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it, the context's error when the WatchManagers
// context ended, an ErrWatchOverflow error, or the read error that ended it
// (a terminated cluster, or another cluster behind the control container).
// Buffered events stay readable after Stop. Stop is safe to call more than
// once and from any goroutine; every call returns the same cause. Call it
// before the fixture's cleanup: cancelling the context alone does not wait.
//
//purpose:observation,lifetime
func (w *ManagerWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return notStarted("watch managers")
	}
	return w.stream.Stop()
}

// WatchManagers polls the native MGRMap and reports changes of the active
// manager, its availability, the standbys and the advertised service URLs.
// The default WatchConfig.Interval is 3 seconds.
//
// Before it returns, WatchManagers makes one read under ctx and QueryTimeout.
// A failed first read returns its error and starts nothing. That read is the
// baseline: the first event, already buffered. It pins the cluster, not a
// manager: each read runs ceph fsid, mgr dump and ceph fsid again through one
// control container, and both FSIDs must be the FSID of this fixture's
// original bootstrap configuration. A failover, a new GID, a standby that
// registers or leaves, including managers the fixture does not own, are
// changes the watch reports; a reported name does not make the fixture own
// that manager. Another FSID ends the watch. Receive the baseline before
// triggering the change to observe.
//
// Each read takes no topology lock, as ManagerStatus: it waits for the
// control container's handle no longer than QueryTimeout allows, so the
// consumer may call any fixture operation while the watch runs. A successful
// read whose compared fields equal the previous successful read produces no
// event. A failed read produces a WatchGap and the watch continues; the next
// successful read is WatchResumed. A gap is not a map without managers. These
// end the watch instead: ctx ending, Stop, a terminated cluster, another
// cluster's FSID, and a full buffer (ErrWatchOverflow).
//
// The watch is read-only: it never fails over a manager, enables a module or
// connects to a service. Available is the MGRMap's report, not proof that a
// module answers commands; use WaitMGRModuleReady or the module's own
// requests for that. The stream reports differences between samples, not a
// complete history of failovers.
//
//purpose:observation
func (o *Observer) WatchManagers(ctx context.Context, cfg WatchConfig) (*ManagerWatch, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	settings, err := watchSettings("watch managers", cfg, managerWatchDefaultInterval)
	if err != nil {
		return nil, err
	}
	return startManagerWatch(ctx, settings, func(ctx context.Context) (string, error) {
		return c.watchOriginalFSID(ctx, "watch managers")
	}, c.managerMapReport)
}

// managerSample is one successful read of the MGRMap.
type managerSample struct {
	epoch    uint64
	status   ManagerStatus
	services map[string]string
}

// managerMapReport is one mgr dump with the FSIDs read before and after it
// through the same control container.
type managerMapReport struct {
	before, after string
	sample        managerSample
}

// managerMapRead is one read of the MGRMap; Container.managerMapReport in
// production.
type managerMapRead func(context.Context) (managerMapReport, error)

func startManagerWatch(ctx context.Context, settings watch.Settings, original originalFSIDRead, read managerMapRead) (*ManagerWatch, error) {
	var fsid string
	stream, err := watch.Start(ctx, watch.Spec[managerSample, ManagerWatchEvent]{
		Name:     "watch managers",
		Settings: settings,
		Read: func(ctx context.Context) (managerSample, error) {
			pinned := fsid
			if pinned == "" {
				var err error
				if pinned, err = original(ctx); err != nil {
					return managerSample{}, err
				}
			}
			report, err := read(ctx)
			if err != nil {
				if errors.Is(err, errClusterTerminated) {
					return managerSample{}, watch.Fatal(err)
				}
				return managerSample{}, err
			}
			for _, native := range []string{report.before, report.after} {
				if err := checkWatchFSID("watch managers", "native", native, pinned); err != nil {
					return managerSample{}, err
				}
			}
			fsid = pinned
			return report.sample, nil
		},
		Equal: func(a, b managerSample) bool {
			return a.status.Available == b.status.Available && a.status.ActiveName == b.status.ActiveName &&
				a.status.ActiveGID == b.status.ActiveGID && slices.Equal(a.status.Standbys, b.status.Standbys) &&
				maps.Equal(a.services, b.services)
		},
		Event: func(m watch.Meta, s managerSample, err error) ManagerWatchEvent {
			e := ManagerWatchEvent{Err: err}
			e.Sequence, e.Kind, e.Started, e.Finished = watchMeta(m)
			if err == nil {
				e.Epoch, e.Status, e.Services = s.epoch, s.status, maps.Clone(s.services)
				e.Status.Standbys = slices.Clone(s.status.Standbys)
			}
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &ManagerWatch{stream: stream}, nil
}

// managerMapReport reads the MGRMap for one watch read. It takes no topology
// lock, as Observer.ManagerStatus, selects the control container under ctx and
// runs every command of the read there. A cluster whose Terminate has started
// is errClusterTerminated, also when it made a command fail.
func (c *Container) managerMapReport(ctx context.Context) (managerMapReport, error) {
	if c.native.calls.isClosed() {
		return managerMapReport{}, errClusterTerminated
	}
	report, err := c.queryManagerMapReport(ctx)
	if err != nil && c.native.calls.isClosed() {
		return managerMapReport{}, errClusterTerminated
	}
	return report, err
}

func (c *Container) queryManagerMapReport(ctx context.Context) (managerMapReport, error) {
	control, err := c.conn().controlContainer(ctx)
	if err != nil {
		return managerMapReport{}, fmt.Errorf("select Ceph CLI container: %w", err)
	}
	run := func(args ...string) ([]byte, error) {
		return command(ctx, control, append([]string{"ceph", "--connect-timeout", "5"}, args...)...)
	}
	before, err := watchNativeFSID(run)
	if err != nil {
		return managerMapReport{}, err
	}
	data, err := run("mgr", "dump", "--format", "json")
	if err != nil {
		return managerMapReport{}, fmt.Errorf("mgr dump: %w", err)
	}
	sample, err := decodeManagerMap(data)
	if err != nil {
		return managerMapReport{}, err
	}
	after, err := watchNativeFSID(run)
	if err != nil {
		return managerMapReport{}, err
	}
	return managerMapReport{before: before, after: after, sample: sample}, nil
}

func watchNativeFSID(run func(args ...string) ([]byte, error)) (string, error) {
	data, err := run("fsid")
	if err != nil {
		return "", fmt.Errorf("fsid: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

// decodeManagerMap decodes one mgr dump into the ManagerStatus fields, the
// epoch and the services of the same map. Unlike ManagerStatus it refuses a
// map with a missing field, so a malformed reply is a failed read rather than
// an empty map.
func decodeManagerMap(data []byte) (managerSample, error) {
	var shape struct {
		Epoch      *uint64 `json:"epoch"`
		ActiveName *string `json:"active_name"`
		ActiveGID  *uint64 `json:"active_gid"`
		Available  *bool   `json:"available"`
		Standbys   *[]struct {
			Name *string `json:"name"`
			GID  *uint64 `json:"gid"`
		} `json:"standbys"`
		Services json.RawMessage `json:"services"`
	}
	if err := json.Unmarshal(data, &shape); err != nil || shape.Epoch == nil || shape.ActiveName == nil || shape.ActiveGID == nil || shape.Available == nil || shape.Standbys == nil {
		return managerSample{}, errors.New("decode native MGRMap: missing epoch, active manager, availability or standbys")
	}
	if (*shape.ActiveName == "") != (*shape.ActiveGID == 0) {
		return managerSample{}, errors.New("decode native MGRMap: the active name and GID disagree")
	}
	for _, standby := range *shape.Standbys {
		if standby.Name == nil || *standby.Name == "" || standby.GID == nil || *standby.GID == 0 {
			return managerSample{}, errors.New("decode native MGRMap: a standby lacks its name or GID")
		}
	}
	var status ManagerStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return managerSample{}, fmt.Errorf("decode native MGRMap: %w", err)
	}
	services, err := decodeManagerServices(shape.Services)
	if err != nil {
		return managerSample{}, err
	}
	slices.SortFunc(status.Standbys, func(a, b struct {
		Name string `json:"name"`
		GID  uint64 `json:"gid"`
	}) int {
		return cmp.Or(strings.Compare(a.Name, b.Name), cmp.Compare(a.GID, b.GID))
	})
	return managerSample{epoch: *shape.Epoch, status: status, services: services}, nil
}
