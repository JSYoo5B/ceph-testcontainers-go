package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/watch"
)

// monitorWatchDefaultInterval is the default WatchConfig.Interval of
// WatchMonitors: each read is one quorum_status command, which reports the
// monmap with its FSID, so no second command is needed.
const monitorWatchDefaultInterval = time.Second

// MonitorWatchEvent is one event of a MonitorWatch.
//
// Sequence starts at 1 with the baseline and increases by one for each event,
// and no event is dropped, so consecutive numbers mean the consumer saw every
// event the watch produced. Started and Finished are the local clock around
// the read, not the time Ceph changed state.
//
// MonMapEpoch and ElectionEpoch are the native monmap and election epochs of
// that read, and Leader is the quorum leader it names. Members are the monmap
// members, sorted by name, each with the public address vector the monmap
// advertises in native order; Quorum are the names of the members in quorum,
// sorted. Membership and quorum participation are separate facts: a stopped
// MON stays a member and leaves Quorum. Members, Quorum and Leader are
// compared; an epoch change that leaves them equal produces no event. Each
// event has its own slices, which the consumer may keep or change. Two events
// separated by a gap, or simply by the polling interval, do not establish the
// transitions in between.
type MonitorWatchEvent struct {
	Sequence          uint64
	Kind              WatchEventKind
	Started, Finished time.Time
	MonMapEpoch       uint64
	ElectionEpoch     uint64
	Leader            string
	Members           []DaemonAddress
	Quorum            []string
	Err               error
}

// MonitorWatch is a running watch of the MON quorum and monmap membership.
// One goroutine reads the quorum status and sends events; Events closes after
// that goroutine has stopped using the cluster. Stop ends the watch and waits
// for it.
type MonitorWatch struct {
	stream *watch.Stream[MonitorWatchEvent]
}

// Events returns the receive-only event channel. The baseline is already in
// it when WatchMonitors returns. The channel closes when the watch ends; Stop
// returns why.
//
//purpose:observation
func (w *MonitorWatch) Events() <-chan MonitorWatchEvent {
	if w == nil {
		return nil
	}
	return w.stream.Events()
}

// Stop cancels the watch if it is still running, waits until its goroutine
// has returned and Events is closed, and returns the cause that ended the
// watch: nil when Stop ended it, the context's error when the WatchMonitors
// context ended, an ErrWatchOverflow error, or the read error that ended it
// (a terminated cluster, or a monmap of another cluster). Buffered events
// stay readable after Stop. Stop is safe to call more than once and from any
// goroutine; every call returns the same cause. Call it before the fixture's
// cleanup: cancelling the context alone does not wait.
//
//purpose:observation,lifetime
func (w *MonitorWatch) Stop() error {
	if w == nil || !w.stream.Started() {
		return notStarted("watch monitors")
	}
	return w.stream.Stop()
}

// WatchMonitors polls the native quorum status and reports changes of the
// MON quorum and the monmap membership. The default WatchConfig.Interval is 1
// second.
//
// Before it returns, WatchMonitors makes one read under ctx and QueryTimeout.
// A failed first read returns its error and starts nothing. That read is the
// baseline: the first event, already buffered. It pins the cluster, not a
// member: its monmap must carry the FSID of this fixture's original bootstrap
// configuration, and every later read must too. MONs that are added, removed,
// replaced or moved to other addresses, including members the fixture does
// not own, are changes the watch reports; a monmap with another FSID ends it.
// Receive the baseline before triggering the change to observe.
//
// Each read takes no topology lock, as QuorumStatus: it waits for the control
// container's handle no longer than QueryTimeout allows and runs quorum_status
// under the same 5-second process limit, so the consumer may call any fixture
// operation, including a long MON operation, while the watch runs. A
// successful read whose compared fields equal the previous successful read
// produces no event. A failed read produces a WatchGap and the watch
// continues; the next successful read is WatchResumed. Without a majority the
// query does not complete, so a gap, possibly a run of them, can be all a
// quorum loss shows: a gap is not an empty quorum or proof that a majority is
// lost. These end the watch instead: ctx ending, Stop, a terminated cluster, a
// monmap of another cluster, and a full buffer (ErrWatchOverflow).
//
// The watch is read-only. Quorum is the MONs' own report, not proof that a
// daemon process runs, and an advertised address does not prove that clients
// can reach it. The stream reports differences between samples, not a
// complete history of elections or membership changes.
//
//purpose:observation
func (o *Observer) WatchMonitors(ctx context.Context, cfg WatchConfig) (*MonitorWatch, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	settings, err := watchSettings("watch monitors", cfg, monitorWatchDefaultInterval)
	if err != nil {
		return nil, err
	}
	return startMonitorWatch(ctx, settings, func(ctx context.Context) (string, error) {
		return c.watchOriginalFSID(ctx, "watch monitors")
	}, c.monitorQuorumReport)
}

// monitorSample is one successful read of the quorum status.
type monitorSample struct {
	monMapEpoch, electionEpoch uint64
	leader                     string
	monmap                     []DaemonAddress
	quorum                     []string
}

// monitorQuorumReport is one quorum_status read with the FSID its monmap
// carries.
type monitorQuorumReport struct {
	fsid   string
	sample monitorSample
}

// monitorQuorumRead is one read of the quorum status;
// Container.monitorQuorumReport in production.
type monitorQuorumRead func(context.Context) (monitorQuorumReport, error)

// originalFSIDRead returns the original bootstrap FSID that a cluster watch
// pins; Container.watchOriginalFSID in production.
type originalFSIDRead func(context.Context) (string, error)

func startMonitorWatch(ctx context.Context, settings watch.Settings, original originalFSIDRead, read monitorQuorumRead) (*MonitorWatch, error) {
	var fsid string
	stream, err := watch.Start(ctx, watch.Spec[monitorSample, MonitorWatchEvent]{
		Name:     "watch monitors",
		Settings: settings,
		Read: func(ctx context.Context) (monitorSample, error) {
			pinned := fsid
			if pinned == "" {
				var err error
				if pinned, err = original(ctx); err != nil {
					return monitorSample{}, err
				}
			}
			report, err := read(ctx)
			if err != nil {
				if errors.Is(err, errClusterTerminated) {
					return monitorSample{}, watch.Fatal(err)
				}
				return monitorSample{}, err
			}
			if err := checkWatchFSID("watch monitors", "monmap", report.fsid, pinned); err != nil {
				return monitorSample{}, err
			}
			fsid = pinned
			return report.sample, nil
		},
		Equal: func(a, b monitorSample) bool {
			return a.leader == b.leader && slices.Equal(a.quorum, b.quorum) && slices.EqualFunc(a.monmap, b.monmap, sameDaemonAddress)
		},
		Event: func(m watch.Meta, s monitorSample, err error) MonitorWatchEvent {
			e := MonitorWatchEvent{Err: err}
			e.Sequence, e.Kind, e.Started, e.Finished = watchMeta(m)
			if err == nil {
				e.MonMapEpoch, e.ElectionEpoch, e.Leader = s.monMapEpoch, s.electionEpoch, s.leader
				e.Members, e.Quorum = cloneDaemonAddresses(s.monmap), slices.Clone(s.quorum)
			}
			return e
		},
	})
	if err != nil {
		return nil, err
	}
	return &MonitorWatch{stream: stream}, nil
}

func sameDaemonAddress(a, b DaemonAddress) bool {
	return a.Name == b.Name && slices.Equal(a.Addrs, b.Addrs)
}

func cloneDaemonAddresses(in []DaemonAddress) []DaemonAddress {
	if in == nil {
		return nil
	}
	out := make([]DaemonAddress, len(in))
	for i, member := range in {
		out[i] = DaemonAddress{Name: member.Name, Addrs: slices.Clone(member.Addrs)}
	}
	return out
}

// monitorQuorumReport reads the quorum status for one watch read. It takes no
// topology lock, as Observer.QuorumStatus, and selects the control container
// under ctx. A cluster whose Terminate has started is errClusterTerminated,
// also when it made the query fail.
func (c *Container) monitorQuorumReport(ctx context.Context) (monitorQuorumReport, error) {
	if c.native.calls.isClosed() {
		return monitorQuorumReport{}, errClusterTerminated
	}
	report, err := c.queryMonitorQuorumReport(ctx)
	if err != nil && c.native.calls.isClosed() {
		return monitorQuorumReport{}, errClusterTerminated
	}
	return report, err
}

func (c *Container) queryMonitorQuorumReport(ctx context.Context) (monitorQuorumReport, error) {
	control, err := c.conn().controlContainer(ctx)
	if err != nil {
		return monitorQuorumReport{}, fmt.Errorf("select monitor quorum control: %w", err)
	}
	data, err := monitorQuorumCommand(ctx, control, monitorQuorumProcessTimeout)
	if err != nil {
		return monitorQuorumReport{}, err
	}
	status, err := decodeMonitorQuorum(ctx, data)
	if err != nil {
		return monitorQuorumReport{}, err
	}
	return decodeMonitorQuorumReport(data, status)
}

// decodeMonitorQuorumReport decodes the fields of a validated quorum_status
// output that QuorumStatus does not carry: the epochs, the leader and the
// complete address vectors. Every field must be present.
func decodeMonitorQuorumReport(data []byte, status QuorumStatus) (monitorQuorumReport, error) {
	var native struct {
		ElectionEpoch *uint64 `json:"election_epoch"`
		Leader        *string `json:"quorum_leader_name"`
		MonMap        struct {
			Epoch *uint64 `json:"epoch"`
			Mons  []struct {
				Name        string              `json:"name"`
				PublicAddrs nativeAddressVector `json:"public_addrs"`
			} `json:"mons"`
		} `json:"monmap"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native.ElectionEpoch == nil || native.Leader == nil || native.MonMap.Epoch == nil || status.MonMap.FSID == "" {
		return monitorQuorumReport{}, errors.New("invalid native monitor quorum schema: missing epoch, leader or FSID")
	}
	quorum := slices.Sorted(slices.Values(status.QuorumNames))
	if *native.Leader != "" && !slices.Contains(quorum, *native.Leader) {
		return monitorQuorumReport{}, errors.New("invalid native monitor quorum leader")
	}
	monmap := make([]DaemonAddress, 0, len(native.MonMap.Mons))
	for _, mon := range native.MonMap.Mons {
		addrs, err := mon.PublicAddrs.entities()
		if err != nil {
			return monitorQuorumReport{}, fmt.Errorf("decode monmap member %s: %w", mon.Name, err)
		}
		monmap = append(monmap, DaemonAddress{Name: mon.Name, Addrs: addrs})
	}
	slices.SortFunc(monmap, func(a, b DaemonAddress) int { return strings.Compare(a.Name, b.Name) })
	return monitorQuorumReport{fsid: status.MonMap.FSID, sample: monitorSample{
		monMapEpoch: *native.MonMap.Epoch, electionEpoch: *native.ElectionEpoch, leader: *native.Leader,
		monmap: monmap, quorum: quorum,
	}}, nil
}
