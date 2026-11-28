package fault

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/netem"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/nshelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// NetemHelperImage is the netem helper that ceph-testcontainers-images
// publishes for contract 1 of its netem extension, for linux/amd64 and
// linux/arm64. ApplyNetworkProfile never selects it by itself: pass it, or
// another image that meets the contract, as NetworkProfile.HelperImage.
const NetemHelperImage = "ghcr.io/jsyoo5b/ceph-testcontainers-images:extension-netem-v1"

// MaxNetworkDelay bounds NetworkProfile.Delay.
const MaxNetworkDelay = 10 * time.Second

// MaxNetworkPeers bounds NetworkProfile.Peers.
const MaxNetworkPeers = 16

// NetworkRate is a bandwidth in bits per second, for NetworkProfile.Rate.
type NetworkRate uint64

// Units of NetworkRate, in powers of 1000 as tc counts them.
const (
	BitPerSecond     NetworkRate = 1
	KilobitPerSecond             = 1000 * BitPerSecond
	MegabitPerSecond             = 1000 * KilobitPerSecond
	GigabitPerSecond             = 1000 * MegabitPerSecond
)

// MinNetworkRate and MaxNetworkRate bound NetworkProfile.Rate.
const (
	MinNetworkRate = 64 * KilobitPerSecond
	MaxNetworkRate = 10 * GigabitPerSecond
)

// DefaultNetworkQueueLimit is the netem queue of a rate limit whose
// NetworkProfile.QueueLimit is zero, in packets: tc's default.
const DefaultNetworkQueueLimit = 1000

// MaxNetworkQueueLimit bounds NetworkProfile.QueueLimit.
const MaxNetworkQueueLimit = 100000

// String formats the rate in bit/s with the largest decimal prefix that
// divides it.
func (r NetworkRate) String() string { return netem.FormatRate(uint64(r)) }

// NetworkPacketFault is one of netem's packet faults, for NetworkProfile's
// Reorder, Duplicate and Corrupt: the percentage of the selected packets
// that it affects, and how much each packet's decision depends on the
// previous one's.
type NetworkPacketFault struct {
	// Percent is the probability that a packet is affected, from more than 0
	// to 100. Zero, with a zero Correlation, means no fault.
	Percent float64
	// Correlation, from 0 to 100, makes each packet's random decision this
	// percentage dependent on the previous packet's, so affected packets
	// come in runs. It needs a Percent.
	Correlation float64
}

// IsZero reports whether the fault is off.
//
//purpose:observation
func (f NetworkPacketFault) IsZero() bool {
	return f == NetworkPacketFault{}
}

// String formats the fault as tc's arguments do: the percentage, and the
// correlation when it is not zero.
func (f NetworkPacketFault) String() string { return netem.Fault(f).String() }

// validate checks a fault's percentages; name is its NetworkProfile field.
func (f NetworkPacketFault) validate(name string) error {
	switch {
	case math.IsNaN(f.Percent) || f.Percent < 0 || f.Percent > 100:
		return nshelper.InvalidRequest("network profile %s.Percent must be a percentage from 0 to 100", name)
	case math.IsNaN(f.Correlation) || f.Correlation < 0 || f.Correlation > 100:
		return nshelper.InvalidRequest("network profile %s.Correlation must be a percentage from 0 to 100", name)
	case f.Percent == 0 && f.Correlation != 0:
		return nshelper.InvalidRequest("network profile %s.Correlation needs a Percent", name)
	}
	return nil
}

// netemCommandTimeout bounds each command in a netem helper.
const netemCommandTimeout = 20 * time.Second

var (
	// ErrNetemImageUnsupported reports a helper image that does not meet the
	// netem contract: a missing or broken tc, ip, sh, sleep or python3, or a
	// tc without netem, or, for a Rate, a tc that cannot parse netem's rate,
	// or for a Reorder, Duplicate or Corrupt, a tc that cannot parse that
	// fault with a correlation. It belongs to ceph.ErrImageCapability.
	ErrNetemImageUnsupported = netem.ErrImageUnsupported
	// ErrNetemPermissionDenied reports an engine that did not give the helper
	// NET_ADMIN, or refused a qdisc change for lack of it. It belongs to
	// ceph.ErrEnvironmentUnavailable.
	ErrNetemPermissionDenied = netem.ErrPermissionDenied
	// ErrNetemKernelUnsupported reports a kernel without the netem qdisc,
	// for a profile with peers without the prio qdisc or the u32 classifier, or
	// for a Rate a netem qdisc that does not keep the rate, or for a Reorder,
	// Duplicate or Corrupt one that does not keep that fault, its correlation
	// or, for a Reorder, a gap of 1. It belongs to ceph.ErrEnvironmentUnavailable.
	ErrNetemKernelUnsupported = netem.ErrKernelUnsupported
)

// NetworkProfile configures netem shaping for ApplyNetworkProfile or ApplyLinkProfile.
type NetworkProfile struct {
	// HelperImage is the netem helper image: NetemHelperImage, or another image
	// that meets contract 1 of the netem extension. Required.
	HelperImage string
	// Plane selects the bridge whose endpoint of the target is shaped.
	Plane ceph.NetworkPlane
	// Delay is added to every packet that leaves the target through that
	// endpoint: at least 1ms and at most MaxNetworkDelay, in whole
	// microseconds. With a Loss, a Rate, a Duplicate or a Corrupt it may be
	// zero.
	Delay time.Duration
	// Jitter varies each packet's delay uniformly within ±Jitter. It is at
	// most Delay, in whole microseconds.
	Jitter time.Duration
	// Peer, when set, limits the profile to the packets that the target sends
	// to this address through the endpoint; every other packet leaves
	// unshaped. It is an IPv4 address, or an IPv6 address for an
	// endpoint that has one, such as a daemon's address from
	// DaemonAddresses on the same plane. Only packets that leave the target
	// are selected, unless Direction also shapes the peer's side.
	Peer netip.Addr
	// PeerPort, with Peer, limits the profile further to TCP packets to this
	// destination port, such as one of the peer's Messenger ports, and on
	// the peer's side of an Inbound profile to its packets from that port. Zero
	// selects every packet to Peer. An IPv6 packet with extension headers
	// before its TCP header is not selected.
	PeerPort uint16
	// Peers, instead of Peer and PeerPort, limits the profile to the packets
	// that the target sends to any of these peers, each with its own
	// optional TCP destination port, such as several OSDs of a daemon.
	// Every packet that one of them selects gets the same shaping; every
	// other packet leaves unshaped. It holds at most
	// MaxNetworkPeers distinct entries, of either family.
	Peers []NetworkPeer
	// Direction selects which side of the paths to the selected peers is
	// shaped: Outbound (the zero value) what the target sends, Inbound what
	// the peers send to the target, BothDirections both. Inbound and
	// BothDirections need a Peer or Peers; see NetworkDirection.
	Direction NetworkDirection
	// Phase selects which packets of a selected path are shaped: AllPackets
	// (the zero value), or ConnectionSetup, only TCP segments with SYN set.
	// ConnectionSetup needs a Peer or Peers; see NetworkPhase.
	Phase NetworkPhase
	// Loss drops this percentage of the packets that the profile applies to,
	// at random and independently, from more than 0 to 100. With 100 every
	// selected packet is dropped: with a Peer or Peers, the target's packets
	// to those peers, and only those, never arrive. Zero drops nothing.
	Loss float64
	// Rate, when set, limits the selected packets to this bandwidth, from
	// MinNetworkRate to MaxNetworkRate in whole bytes per second (a multiple
	// of 8 bit/s), with netem's rate in the same qdisc as the Delay and Loss.
	// netem sends each packet only after the previous one has taken its
	// length over the rate, so there is no burst: a packet counts with its
	// Ethernet header, as the kernel queues it, and a TCP segmentation
	// offload packet counts with all of its segments. The packets waiting
	// for the rate, and for the Delay, share one queue of QueueLimit
	// packets; one that arrives at a full queue is dropped and counted in
	// NetworkProfileStatus.Drops. Zero sets no limit. With a Loss, netem
	// drops first, then delays, then paces what is left. With Direction
	// Outbound it shapes only what the target sends; Inbound and
	// BothDirections apply the same Rate at each peer's endpoint. What an application
	// achieves through it is less, and is for the caller to measure; the
	// status's Rate and counters are configuration and counts, not a
	// throughput. Before it changes the target, ApplyNetworkProfile adds and
	// removes a netem qdisc with a rate on the loopback of its separate,
	// network-less helper and checks what the kernel reports, so a tc or
	// kernel without netem's rate is refused before the target changes.
	Rate NetworkRate
	// QueueLimit, with a Rate, is the netem queue in packets, from 1 to
	// MaxNetworkQueueLimit; zero means DefaultNetworkQueueLimit.
	QueueLimit int
	// Reorder, when set, sends this percentage of the selected packets at
	// once, ahead of the packets that wait for the Delay, so they arrive out
	// of order. It needs a Delay, which every other selected packet gets, and
	// takes no Jitter, which reorders packets by itself, and no Rate, which
	// the packets sent at once would bypass. netem's gap is 1: every packet
	// may be reordered.
	Reorder NetworkPacketFault
	// Duplicate, when set, sends a second copy of this percentage of the
	// selected packets. iproute2 6.1, the tc of NetemHelperImage, sends a
	// duplicate's correlation only together with a delay's correlation, so
	// with a Duplicate.Correlation the lease passes the delay with a zero
	// correlation (delay 0us 0us 0% without a Delay), the kernel's state
	// without one.
	Duplicate NetworkPacketFault
	// Corrupt, when set, flips one random bit in this percentage of the
	// selected packets, within the part of the packet that the kernel holds
	// in its first buffer: the headers, and the first payload bytes when they
	// are there too. netem first computes a checksum that the device would
	// have offloaded, so the receiver's checksum check sees the flipped bit,
	// and splits a segmentation offload packet, corrupting one segment.
	// netem decides per queued packet, for every fault: a percentage of
	// packets is not a percentage of TCP segments.
	Corrupt NetworkPacketFault
}

// NetworkDirection is the side of a profile's selected paths that it shapes.
// The netem helper shapes only what an endpoint sends, so the other side of a
// path is shaped at the peer's endpoint: for Inbound and BothDirections, every
// selected peer address must belong to a running container attached to the
// same bridge, and the lease applies the same profile there, selecting the
// packets that the peer sends to the target's address of that family. A
// peer's Port then selects the TCP source port of what the peer sends, so
// both directions of a conversation with one of the peer's ports, such as a
// Messenger port of an OSD, are selected by the same NetworkPeer.
type NetworkDirection int

const (
	// Outbound shapes what the target sends.
	Outbound NetworkDirection = iota
	// Inbound shapes what the selected peers send to the target, at each
	// peer's endpoint; nothing is applied to the target's own endpoint.
	Inbound
	// BothDirections shapes both, with one lease on the target's endpoint
	// and one on each peer's.
	BothDirections
)

func (d NetworkDirection) outbound() bool { return d == Outbound || d == BothDirections }
func (d NetworkDirection) inbound() bool  { return d == Inbound || d == BothDirections }

// String names the direction.
func (d NetworkDirection) String() string {
	switch d {
	case Outbound:
		return "outbound"
	case Inbound:
		return "inbound"
	case BothDirections:
		return "both directions"
	}
	return fmt.Sprintf("NetworkDirection(%d)", int(d))
}

// NetworkPhase is the part of a TCP connection that a profile shapes.
type NetworkPhase int

const (
	// AllPackets shapes every selected packet.
	AllPackets NetworkPhase = iota
	// ConnectionSetup shapes only the selected TCP segments with SYN set:
	// connection attempts, and the SYN-ACK that answers a peer's attempt.
	// Established connections keep their packets unshaped, so with a Loss of
	// 100 every session that exists stays up while every new connection,
	// reconnect included, fails on that path. Its u32 key sits at the TCP
	// flags of a header directly after a base IP header, like the port keys.
	ConnectionSetup
)

// String names the phase.
func (p NetworkPhase) String() string {
	switch p {
	case AllPackets:
		return "all packets"
	case ConnectionSetup:
		return "connection setup"
	}
	return fmt.Sprintf("NetworkPhase(%d)", int(p))
}

// NetworkPeer selects the packets that a profile's target sends to Addr, an
// IPv4 address or, for an endpoint that has one, an IPv6 address. With a
// Port, only TCP packets to that destination port are selected; an IPv6
// packet with extension headers before its TCP header is not.
type NetworkPeer struct {
	Addr netip.Addr
	Port uint16
}

// selection is the profile's peers: Peer and PeerPort as one entry, or Peers.
func (d NetworkProfile) selection() []NetworkPeer {
	if d.Peer.IsValid() {
		return []NetworkPeer{{Addr: d.Peer, Port: d.PeerPort}}
	}
	return slices.Clone(d.Peers)
}

// NetworkProfileStatus is what the kernel reports for a profile's interface,
// read through the helper. It is native state, not the requested NetworkProfile.
type NetworkProfileStatus struct {
	// Applied means that the profile's own netem qdisc is the root qdisc of the
	// interface it was applied to (with peers, that its own hierarchy is,
	// with the expected priority map and one filter for each peer), and that
	// nothing the lease did not add is under that root.
	Applied bool
	// Gone means that the shaped interface no longer exists: the target
	// stopped, restarted, was removed or was disconnected from the network,
	// and the profile ended with it.
	Gone bool
	// Interface is the shaped interface's current name.
	Interface string
	// RootKind and RootHandle describe the interface's current root qdisc.
	RootKind, RootHandle string
	// Delay and Jitter are the kernel's values while Applied.
	Delay, Jitter time.Duration
	// Loss is the kernel's loss percentage while Applied.
	Loss float64
	// Rate and QueueLimit are the netem qdisc's configured rate and queue
	// limit while Applied: the kernel's configuration, not a throughput that
	// anything achieved. Rate is zero without a rate limit.
	Rate       NetworkRate
	QueueLimit int
	// Reorder, Duplicate and Corrupt are the netem qdisc's configured packet
	// faults while Applied, and Gap its reorder gap (1 with a Reorder): the
	// kernel's configuration. None of them shows that a packet was affected;
	// the receiver's transport counters can.
	Reorder, Duplicate, Corrupt NetworkPacketFault
	Gap                         int
	// Packets, Bytes and Drops are the netem qdisc's counters while Applied.
	// With peers they count only the selected packets, of all peers. Drops
	// includes the packets that Loss dropped and those that found the queue
	// full.
	Packets, Bytes, Drops uint64
	// Overlimits and Requeues are the netem qdisc's other counters, and
	// Backlog (bytes) and QueueLength (packets) what waits in its queue, all
	// while Applied. With a Reorder, Requeues counts the packets that netem
	// sent ahead. They are counters, not rates: to know a throughput,
	// measure the workload, or compare Bytes over a period you time.
	Overlimits, Requeues, Backlog, QueueLength uint64
	// Qdiscs is tc's JSON output for the interface.
	Qdiscs json.RawMessage
	// Filters is tc's egress filter output for the interface, with the keys
	// of each u32 filter under "matches".
	Filters json.RawMessage

	// state is the judgement of internal/netem that the fields report.
	state netem.State
}

// NetworkProfileLease owns one netem profile on a container's network endpoint,
// applied from a helper container in the target's network namespace that
// alone has NET_ADMIN. Restore removes the profile and the helper. A non-nil
// lease returned with an error must be restored: the change may have been
// applied. Restore is retryable. Its exported fields describe the profile.
// Status and Restore use the endpoint and filter identities captured at apply.
type NetworkProfileLease struct {
	// ContainerID and NetworkName identify the shaped endpoint.
	ContainerID, NetworkName string
	// SandboxID is the Docker network sandbox of the target when the profile
	// was applied, and Namespace the kernel's name of its network namespace.
	SandboxID, Namespace string
	// Interface, InterfaceIndex, MACAddress and Address identify the
	// shaped interface inside that namespace.
	Interface      string
	InterfaceIndex int
	MACAddress     string
	Address        netip.Addr
	// HelperImageID is the image of the helper that applied the profile.
	HelperImageID string
	// Delay and Jitter are the requested values.
	Delay, Jitter time.Duration
	// Peer and PeerPort are the requested Peer and PeerPort.
	Peer     netip.Addr
	PeerPort uint16
	// Peers is the whole selection: Peer and PeerPort as one entry, or the
	// requested Peers. Empty means the whole interface.
	Peers []NetworkPeer
	// Loss is the requested loss percentage.
	Loss float64
	// Rate is the requested rate, and QueueLimit the queue limit that the
	// lease installed with it: the requested one, or
	// DefaultNetworkQueueLimit. Both are zero without a Rate.
	Rate       NetworkRate
	QueueLimit int
	// Reorder, Duplicate and Corrupt are the requested packet faults.
	Reorder, Duplicate, Corrupt NetworkPacketFault
	// Direction and Phase are the requested ones.
	Direction NetworkDirection
	Phase     NetworkPhase
	// SourcePorts says that each of Peers' ports is the TCP source port of
	// what this endpoint sends, not the destination port: it is set on the
	// leases in PeerLeases, whose Peers are the target's addresses.
	SourcePorts bool
	// PeerLeases holds, for Inbound and BothDirections, one lease on each
	// selected peer's endpoint, shaping what that peer sends to the target.
	// Restore restores them with the rest; each has its own Status. With
	// Inbound alone the lease applies nothing to the target's endpoint, and
	// its own Status is refused. The slice describes the leases captured at
	// apply and editing it does not change which leases Restore restores.
	PeerLeases []*NetworkProfileLease

	mu                                           sync.Mutex
	id, network, sandbox, namespace, helperImage string
	interfaceIndex                               int
	inboundLeases                                []*NetworkProfileLease
	// shapes reports that the lease owns a profile on its own endpoint;
	// an Inbound lease only holds its peers' leases.
	shapes  bool
	backend netemBackend
	helper  testcontainers.Container
	// shape is the hierarchy that the lease owns: its root qdisc, the netem
	// qdisc or with peers the prio qdisc whose third band leads to the netem
	// child, and the peer filters. params is the netem qdisc it installs.
	shape    netem.Shape
	params   netem.Params
	pending  string
	complete bool
	release  func()
}

// netemBackend runs the Docker and helper operations of a profile; tests
// replace it with a model of namespaces and qdiscs.
//
//testdouble:dependency-port
type netemBackend interface {
	// target inspects id and its endpoint on network. A removed container
	// returns an error for which errdefs.IsNotFound is true.
	target(ctx context.Context, id, network string) (nshelper.Target, error)
	// endpoints maps every address of the running containers attached to
	// network to the container's ID.
	endpoints(ctx context.Context, network string) (map[netip.Addr]string, error)
	// startHelper starts an idle helper from image in networkMode with only
	// NET_ADMIN. A non-nil helper returned with an error must be removed.
	startHelper(ctx context.Context, image string, networkMode container.NetworkMode) (testcontainers.Container, error)
	// helper reports whether helper still runs, and its image ID.
	helper(ctx context.Context, helper testcontainers.Container) (bool, string, error)
	removeHelper(ctx context.Context, helper testcontainers.Container) error
	run(ctx context.Context, helper testcontainers.Container, label string, argv ...string) (supervised.Report, error)
	cancel(ctx context.Context, helper testcontainers.Container, label, marker string) error
}

// validateNetworkProfile checks a NetworkProfile before anything runs.
func validateNetworkProfile(profile NetworkProfile) error {
	return validateProfile(profile, true)
}

// validateProfile checks a NetworkProfile, with a Ceph network plane or, for
// ApplyLinkProfile, without one.
func validateProfile(profile NetworkProfile, plane bool) error {
	if strings.TrimSpace(profile.HelperImage) == "" {
		// The extension is not selected.
		return &ceph.OperationError{Kind: ceph.ErrImageCapability, Capability: "netem extension",
			Err: errors.New("network profile needs NetworkProfile.HelperImage: the netem helper is never selected implicitly")}
	}
	// Every check runs: allowing a zero delay with a Loss must not skip the
	// selection's checks. Peer and PeerPort are the one-peer form of Peers:
	// past the checks of the form itself, both are checked as the one
	// selection that every later stage uses.
	peers := profile.selection()
	noDelay := profile.Delay == 0 && (profile.Loss > 0 || profile.Rate > 0 || !profile.Duplicate.IsZero() || !profile.Corrupt.IsZero())
	switch {
	case plane && profile.Plane != ceph.PublicNetworkPlane && profile.Plane != ceph.ClusterNetworkPlane:
		return nshelper.InvalidRequest("invalid Ceph network plane")
	case !plane && profile.Plane != "":
		return nshelper.InvalidRequest("a link profile names its network; leave NetworkProfile.Plane empty")
	case math.IsNaN(profile.Loss) || profile.Loss < 0 || profile.Loss > 100:
		return nshelper.InvalidRequest("network profile Loss must be a percentage from 0 to 100")
	case !noDelay && (profile.Delay < time.Millisecond || profile.Delay > MaxNetworkDelay):
		return nshelper.InvalidRequest("network profile Delay must be between 1ms and %v, or zero with a Loss, a Rate, a Duplicate or a Corrupt", MaxNetworkDelay)
	case profile.Rate != 0 && (profile.Rate < MinNetworkRate || profile.Rate > MaxNetworkRate):
		return nshelper.InvalidRequest("network profile Rate must be between %v and %v, or zero", MinNetworkRate, MaxNetworkRate)
	case profile.Rate%8 != 0:
		return nshelper.InvalidRequest("network profile Rate must be whole bytes per second, a multiple of 8 bit/s")
	case profile.QueueLimit != 0 && profile.Rate == 0:
		return nshelper.InvalidRequest("network profile QueueLimit needs a Rate")
	case profile.QueueLimit < 0 || profile.QueueLimit > MaxNetworkQueueLimit:
		return nshelper.InvalidRequest("network profile QueueLimit must be between 1 and %d packets, or zero for %d", MaxNetworkQueueLimit, DefaultNetworkQueueLimit)
	case profile.Jitter < 0 || profile.Jitter > profile.Delay:
		return nshelper.InvalidRequest("network profile Jitter must be between 0 and the Delay")
	case profile.Delay%time.Microsecond != 0 || profile.Jitter%time.Microsecond != 0:
		return nshelper.InvalidRequest("network profile Delay and Jitter must be whole microseconds")
	case profile.PeerPort != 0 && !profile.Peer.IsValid():
		return nshelper.InvalidRequest("network profile PeerPort needs a Peer")
	case profile.Peer.IsValid() && profile.Peers != nil:
		return nshelper.InvalidRequest("network profile takes either Peer and PeerPort or Peers")
	case profile.Peers != nil && (len(profile.Peers) == 0 || len(profile.Peers) > MaxNetworkPeers):
		return nshelper.InvalidRequest("network profile Peers needs 1 to %d peers", MaxNetworkPeers)
	case profile.Direction < Outbound || profile.Direction > BothDirections:
		return nshelper.InvalidRequest("network profile Direction %v is not Outbound, Inbound or BothDirections", profile.Direction)
	case profile.Phase < AllPackets || profile.Phase > ConnectionSetup:
		return nshelper.InvalidRequest("network profile Phase %v is not AllPackets or ConnectionSetup", profile.Phase)
	case profile.Direction != Outbound && len(peers) == 0:
		return nshelper.InvalidRequest("network profile Direction %v needs a Peer or Peers: the other direction is shaped at each peer's endpoint", profile.Direction)
	case profile.Phase == ConnectionSetup && len(peers) == 0:
		return nshelper.InvalidRequest("network profile Phase %v needs a Peer or Peers", profile.Phase)
	}
	for _, fault := range []struct {
		name  string
		fault NetworkPacketFault
	}{{"Reorder", profile.Reorder}, {"Duplicate", profile.Duplicate}, {"Corrupt", profile.Corrupt}} {
		if err := fault.fault.validate(fault.name); err != nil {
			return err
		}
	}
	if !profile.Reorder.IsZero() {
		switch {
		case profile.Delay == 0:
			return nshelper.InvalidRequest("network profile Reorder needs a Delay: netem reorders by sending some packets ahead of the delayed ones")
		case profile.Jitter != 0:
			return nshelper.InvalidRequest("network profile Reorder takes no Jitter: a jitter reorders packets by itself, and the two cannot be told apart")
		case profile.Rate != 0:
			return nshelper.InvalidRequest("network profile Reorder takes no Rate: netem sends the reordered packets past the rate limit")
		}
	}
	seen := map[NetworkPeer]bool{}
	for _, peer := range peers {
		switch {
		case !peer.Addr.IsValid() || peer.Addr.Is4In6() || peer.Addr.Zone() != "" || peer.Addr.IsUnspecified():
			return nshelper.InvalidRequest("network profile peer %v is not a plain IPv4 or IPv6 address", peer.Addr)
		case seen[peer]:
			return nshelper.InvalidRequest("network profile Peers has %v port %d twice", peer.Addr, peer.Port)
		}
		seen[peer] = true
	}
	return nil
}

// ApplyNetworkProfile applies netem shaping to the packets that target sends
// through its endpoint on the plane's bridge, below Ceph.
// It is an opt-in extension: profile.HelperImage names the netem helper, which
// runs in the target's network namespace with NET_ADMIN, while the target
// keeps its own image and capabilities. The target is an owned daemon or a
// caller-owned container attached to that bridge; host-network clusters,
// stopped or paused targets and targets that share another container's
// network namespace are refused. With a Peer or Peers, only the packets to
// those peers are shaped, through one prio root, one netem child and one u32
// filter per peer, all owned by the one lease: to cut a daemon's paths to
// several peers at once, list them in one profile. A Rate limits the bandwidth
// of the same packets in the same netem qdisc, alone or with a Delay and a
// Loss. Reorder, Duplicate and Corrupt add netem's packet faults to the same
// qdisc: a Reorder with a Delay and without Jitter or a Rate, a Duplicate or
// a Corrupt with any of the others.
//
// Before it changes anything, it checks the helper image against the netem
// contract, NET_ADMIN and the kernel's netem, with a Rate netem's rate in tc
// and the kernel, with a packet fault that fault in tc and the kernel, and
// with peers its prio qdisc and u32 classifier, in a
// separate helper without a network, and returns ErrNetemImageUnsupported,
// ErrNetemPermissionDenied or
// ErrNetemKernelUnsupported. It refuses an interface whose root qdisc is not
// the kernel's default and a target that already has a profile on that bridge.
// A missing HelperImage is ceph.ErrImageCapability, the refusals are
// ceph.ErrInvalidRequest, and a tc command that fails otherwise is a
// *ceph.NativeFailure.
//
// The profile lasts until Restore, or until the target stops or leaves the
// bridge, which removes its interface. Terminate restores profiles that are
// still held; restore a caller-owned target's profile before removing it.
//
// With a Peer or Peers the lease owns a prio root qdisc whose third band
// leads to the netem child, and one u32 filter per peer that sends its
// packets there; every priority maps to the first band, so nothing else is
// shaped. IPv4 peers' filters are in preference 10 and u32 table 800:, with
// handles 800::1, 800::2 and so on in the given order; IPv6 peers' are in
// the next preference and table (11 and 801: after IPv4 peers, otherwise 10
// and 800:). They are added root first and filters last, so no packet is
// shaped before the hierarchy is complete.
//
// Direction Inbound or BothDirections also shapes what each selected peer
// sends to the target: every peer address is resolved to the running
// container that has it on the bridge, before anything changes, and the same
// profile is applied to that container's endpoint, selecting the target's
// address of the peer's family and the peer's port as a TCP source port. One
// lease per peer container is in the returned lease's PeerLeases, each reserved
// and restored by Terminate on its own as well; with Inbound alone nothing is
// applied to the target. Phase ConnectionSetup adds a u32 key on the TCP SYN
// bit to every filter, so only connection setup is shaped.
//
// TCP absorbs packet faults before Ceph sees them: the receiver reorders
// segments back, discards duplicates (answering with a D-SACK) and drops a
// segment whose checksum fails, which the sender retransmits. A Ceph
// operation that returns the right bytes therefore does not show that a
// packet was altered; read the receiver's kernel counters for the transport
// effect.
//
//purpose:condition
func ApplyNetworkProfile(ctx context.Context, c *ceph.Container, target testcontainers.Container, profile NetworkProfile) (*NetworkProfileLease, error) {
	return temporaryNetworkProfile(ctx, c, target, profile, nil)
}

func temporaryNetworkProfile(ctx context.Context, c *ceph.Container, target testcontainers.Container, profile NetworkProfile, backend netemBackend) (*NetworkProfileLease, error) {
	if err := requireCluster(c, "a network profile"); err != nil {
		return nil, err
	}
	if backend == nil {
		backend = dockerNetemBackend{label: faulthook.OwnedLabel(c)}
	}
	if err := validateNetworkProfile(profile); err != nil {
		return nil, err
	}
	if target == nil || target.GetContainerID() == "" {
		return nil, nshelper.InvalidRequest("network profile requires a container")
	}
	name, err := admitNetworkProfileTarget(ctx, c, target, profile.Plane)
	if err != nil {
		return nil, err
	}
	// Each endpoint's lease is reserved and held for Terminate on its own,
	// so a profile on a peer's endpoint is restored even when the lease that
	// lists it is lost.
	hold := func(id string, apply func() (*NetworkProfileLease, error)) (*NetworkProfileLease, error) {
		return holdEndpoint(ctx, c, name, id, apply)
	}
	return applyDirectedProfile(ctx, backend, target.GetContainerID(), name, profile, hold)
}

// holdEndpoint reserves the endpoint of container id on the cluster's bridge
// name, applies its lease and keeps the lease for Terminate. When Terminate
// overtook the apply, it did not see the lease, which is restored here.
func holdEndpoint(ctx context.Context, c *ceph.Container, name, id string, apply func() (*NetworkProfileLease, error)) (*NetworkProfileLease, error) {
	token, err := reserveNetworkProfile(ctx, c, name, id)
	if err != nil {
		return nil, err
	}
	lease, err := apply()
	if closed := settleNetworkProfile(c, token, lease); closed && lease != nil {
		native, _ := faulthook.Terminated()
		return lease, errors.Join(err, native, lease.Restore(ctx))
	}
	return lease, err
}

// endpointHold applies one endpoint's lease: for a cluster's planes it
// reserves the endpoint and keeps the lease for Terminate, for a link it
// only applies it.
type endpointHold func(id string, apply func() (*NetworkProfileLease, error)) (*NetworkProfileLease, error)

// inboundPeer is one selected peer's endpoint for Inbound: the container
// that has the peer's addresses on the network, and the target's addresses
// with the peers' ports, which select what that peer sends to the target.
type inboundPeer struct {
	id    string
	peers []NetworkPeer
}

// applyDirectedProfile applies profile in its Direction: on the target's
// endpoint for Outbound, on each selected peer's endpoint for Inbound, and
// on both for BothDirections. Every peer is resolved before anything is
// applied, so a peer that is not a container on the network changes
// nothing. A non-nil lease returned with an error must be restored.
func applyDirectedProfile(ctx context.Context, backend netemBackend, id, network string, profile NetworkProfile, hold endpointHold) (*NetworkProfileLease, error) {
	var inbound []inboundPeer
	var state nshelper.Target
	if profile.Direction.inbound() {
		var err error
		if state, inbound, err = planInbound(ctx, backend, id, network, profile); err != nil {
			return nil, err
		}
	}
	var top *NetworkProfileLease
	if profile.Direction.outbound() {
		lease, err := hold(id, func() (*NetworkProfileLease, error) {
			return applyEndpointProfile(ctx, backend, id, network, profile, false)
		})
		if lease == nil || err != nil {
			return lease, err
		}
		top = lease
	} else {
		top = newNetworkProfileLease(backend, nil, id, network, state, "", profile.selection(), profile)
		top.shapes = false
	}
	top.Direction = profile.Direction
	for _, peer := range inbound {
		peerProfile := profile
		peerProfile.Direction, peerProfile.Peer, peerProfile.PeerPort, peerProfile.Peers = Outbound, netip.Addr{}, 0, peer.peers
		lease, err := hold(peer.id, func() (*NetworkProfileLease, error) {
			return applyEndpointProfile(ctx, backend, peer.id, network, peerProfile, true)
		})
		if lease != nil {
			top.addPeerLease(lease)
		}
		if err != nil {
			if !top.shapes && lease == nil && len(top.peerLeases()) == 0 {
				return nil, err
			}
			return top, err
		}
	}
	return top, nil
}

// addPeerLease publishes one more peer lease. With BothDirections, Terminate
// may already hold the outer lease and restore it meanwhile. Keep the
// restoration list private and publish a distinct copy.
func (l *NetworkProfileLease) addPeerLease(peer *NetworkProfileLease) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inboundLeases = append(l.inboundLeases, peer)
	l.PeerLeases = slices.Clone(l.inboundLeases)
}

// peerLeases returns the owned peer leases published so far.
func (l *NetworkProfileLease) peerLeases() []*NetworkProfileLease {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.inboundLeases)
}

// planInbound resolves every selected peer address to the container that
// has it on network, and lists, per container in the order of their first
// peer, the target's address of each peer's family with that peer's port.
// A peer address that no running container on the network has, or that is
// the target's own, is refused, as is an IPv6 peer of a target without an
// IPv6 address.
func planInbound(ctx context.Context, backend netemBackend, id, network string, profile NetworkProfile) (nshelper.Target, []inboundPeer, error) {
	state, err := backend.target(ctx, id, network)
	if err != nil {
		return nshelper.Target{}, nil, faulthook.ClassifyDocker(ctx, fmt.Errorf("inspect the network profile target: %w", err))
	}
	if err := admitNetemTarget(state); err != nil {
		return nshelper.Target{}, nil, err
	}
	endpoints, err := backend.endpoints(ctx, network)
	if err != nil {
		return nshelper.Target{}, nil, faulthook.ClassifyDocker(ctx, fmt.Errorf("inspect the endpoints of %s: %w", network, err))
	}
	var plan []inboundPeer
	for _, peer := range profile.selection() {
		owner, ok := endpoints[peer.Addr]
		switch {
		case !ok:
			return nshelper.Target{}, nil, nshelper.InvalidRequest("network profile peer %s is not the address of a running container on %s: %v is shaped at the peer's endpoint", peer.Addr, network, profile.Direction)
		case owner == id:
			return nshelper.Target{}, nil, nshelper.InvalidRequest("network profile peer %s is the target's own address", peer.Addr)
		}
		address := state.Address
		if peer.Addr.Is6() {
			if address = state.Address6; !address.IsValid() {
				return nshelper.Target{}, nil, nshelper.InvalidRequest("network profile peer %s is IPv6, and the target has no IPv6 address on %s", peer.Addr, network)
			}
		}
		i := slices.IndexFunc(plan, func(p inboundPeer) bool { return p.id == owner })
		if i < 0 {
			plan, i = append(plan, inboundPeer{id: owner}), len(plan)
		}
		selected := NetworkPeer{Addr: address, Port: peer.Port}
		if !slices.Contains(plan[i].peers, selected) {
			plan[i].peers = append(plan[i].peers, selected)
		}
	}
	return state, plan, nil
}

// ApplyLinkProfile applies the same netem profile on another bridge than the
// cluster's planes, for fixtures that own one, such as the private HTTP
// bridge between the gateways of an RGW multisite: it shapes the
// packets that ctr sends through its endpoint on the bridge named network.
// Like InterruptLink, the caller owns ctr, the network and the returned
// lease, and restores the lease before it removes either: no cluster's
// Terminate knows it. profile.Plane stays empty; every other field, check and
// error is that of ApplyNetworkProfile, and a network that is not a bridge
// is refused.
//
//purpose:condition
func ApplyLinkProfile(ctx context.Context, ctr testcontainers.Container, network string, profile NetworkProfile) (*NetworkProfileLease, error) {
	return temporaryLinkProfile(ctx, dockerNetemBackend{}, ctr, network, profile)
}

func temporaryLinkProfile(ctx context.Context, backend netemBackend, ctr testcontainers.Container, network string, profile NetworkProfile) (*NetworkProfileLease, error) {
	if err := validateProfile(profile, false); err != nil {
		return nil, err
	}
	switch {
	case ctr == nil || ctr.GetContainerID() == "":
		return nil, nshelper.InvalidRequest("network profile requires a container")
	case strings.TrimSpace(network) == "":
		return nil, nshelper.InvalidRequest("a link profile requires a network name")
	}
	return applyDirectedProfile(ctx, backend, ctr.GetContainerID(), network, profile,
		func(_ string, apply func() (*NetworkProfileLease, error)) (*NetworkProfileLease, error) {
			return apply()
		})
}

// admitNetworkProfileTarget returns the bridge of plane, after checking that
// the cluster is open and on bridges and that target is attached to it.
func admitNetworkProfileTarget(ctx context.Context, c *ceph.Container, target testcontainers.Container, plane ceph.NetworkPlane) (string, error) {
	var closed, hostNetwork, owned bool
	var name string
	err := faulthook.WithTopology(ctx, c, func() error {
		facts, factsErr := ceph.FactsOf(c)
		if factsErr != nil {
			return factsErr
		}
		closed, hostNetwork = faulthook.Closed(c), facts.UsesHostNetwork
		name = facts.NetworkName
		if plane == ceph.ClusterNetworkPlane {
			name = facts.ClusterNetworkName
		}
		owned = faulthook.Owns(c, target.GetContainerID())
		return nil
	})
	if err != nil {
		return "", err
	}
	_, terminated := faulthook.Terminated()
	switch {
	case closed:
		return "", terminated
	case hostNetwork:
		return "", nshelper.InvalidRequest("network profile requires a bridge cluster: a profile on a host network would shape the host")
	case name == "":
		return "", nshelper.InvalidRequest("Ceph bridge is not initialized")
	}
	if !owned {
		attached, err := target.Networks(ctx)
		if err != nil {
			return "", faulthook.ClassifyDocker(ctx, fmt.Errorf("inspect the container's networks: %w", err))
		}
		if !slices.Contains(attached, name) {
			return "", nshelper.InvalidRequest("the container is not attached to Ceph network %s", name)
		}
	}
	return name, nil
}

// reserveNetworkProfile reserves the endpoint of container id on the bridge
// name until settleNetworkProfile.
func reserveNetworkProfile(ctx context.Context, c *ceph.Container, name, id string) (faulthook.Token, error) {
	_, terminated := faulthook.Terminated()
	var token faulthook.Token
	err := faulthook.WithTopology(ctx, c, func() error {
		if faulthook.Closed(c) {
			return terminated
		}
		var reserveErr error
		token, _, reserveErr = faulthook.Reserve(ctx, c, faulthook.NetworkProfile, name+"/"+id)
		return reserveErr
	})
	return token, err
}

// settleNetworkProfile keeps a returned lease for Terminate, or frees the
// reservation when there is none. It reports whether Terminate has started.
func settleNetworkProfile(c *ceph.Container, token faulthook.Token, lease *NetworkProfileLease) (closed bool) {
	faulthook.WithTopologyNoContext(c, func() {
		held := faulthook.Held{}
		if lease != nil {
			lease.release = func() { faulthook.WithTopologyNoContext(c, func() { faulthook.Release(c, token) }) }
			held = faulthook.Held{Handle: lease, Undo: lease.Restore}
		}
		// token came from this cluster's reservation; no arbitrary token is accepted.
		closed, _ = faulthook.Settle(c, token, held)
	})
	return closed
}

// applyEndpointProfile checks the helper and the target, then applies the profile.
// It returns a lease whenever the change may have reached the kernel.
// With source, each peer's port is the TCP source port of what the
// endpoint sends: the lease shapes a peer's side of an Inbound profile.
func applyEndpointProfile(ctx context.Context, backend netemBackend, id, network string, profile NetworkProfile, source bool) (*NetworkProfileLease, error) {
	state, peers, imageID, err := admitEndpointProfile(ctx, backend, id, network, profile, source)
	if err != nil {
		return nil, err
	}
	// The checked image's ID, not its tag, which may move.
	helper, err := startNetemHelper(ctx, backend, imageID, container.NetworkMode("container:"+id))
	if err != nil {
		return nil, err
	}
	lease := newNetworkProfileLease(backend, helper, id, network, state, imageID, peers, profile)
	lease.SourcePorts, lease.shape.Match.Source = source, source
	if err := lease.locate(ctx, state); err != nil {
		return lease.discard(ctx, err)
	}
	lease.pickHandles()
	if keep, err := lease.runSteps(ctx, lease.shape.Steps(lease.Interface, lease.params.Arguments())); err != nil {
		if !keep {
			return lease.discard(ctx, err)
		}
		return lease, err
	}
	return lease, lease.verifyApplied(ctx)
}

// pickHandles picks the lease's root handle and, with peers, a different
// handle for its netem child.
func (l *NetworkProfileLease) pickHandles() {
	random := func() string { return fmt.Sprintf("%x:", 0x1000+rand.IntN(0xe000)) }
	l.shape.Handle = random()
	if len(l.shape.Peers) == 0 {
		return
	}
	for l.shape.Child = random(); l.shape.Child == l.shape.Handle; {
		l.shape.Child = random()
	}
}

// verifyApplied reads the kernel's qdisc back and requires exactly the
// requested profile.
func (l *NetworkProfileLease) verifyApplied(ctx context.Context) error {
	status, err := l.read(ctx)
	if err != nil {
		return fmt.Errorf("read the netem profile: %w", err)
	}
	return l.params.Verify(status.state)
}

// admitEndpointProfile checks the target's endpoint, that every IPv6 peer has an
// IPv6 target address, and the helper image, before anything is started. It
// returns the target, the selected peers and the checked image's ID.
func admitEndpointProfile(ctx context.Context, backend netemBackend, id, network string, profile NetworkProfile, source bool) (nshelper.Target, []NetworkPeer, string, error) {
	state, err := backend.target(ctx, id, network)
	if err != nil {
		return nshelper.Target{}, nil, "", faulthook.ClassifyDocker(ctx, fmt.Errorf("inspect the network profile target: %w", err))
	}
	if err := admitNetemTarget(state); err != nil {
		return nshelper.Target{}, nil, "", err
	}
	peers := profile.selection()
	for _, peer := range peers {
		if peer.Addr.Is6() && !state.Address6.IsValid() {
			return nshelper.Target{}, nil, "", nshelper.InvalidRequest("network profile peer %s is IPv6, and the target has no IPv6 address on %s", peer.Addr, network)
		}
	}
	imageID, err := preflightNetem(ctx, backend, profile.HelperImage, netem.Preflight{Rate: profile.Rate > 0,
		Faults: profile.params().Requested(), Peers: len(peers) > 0, Match: netem.Match{Source: source, Setup: profile.Phase == ConnectionSetup}})
	if err != nil {
		return nshelper.Target{}, nil, "", err
	}
	return state, peers, imageID, nil
}

// newNetworkProfileLease records the requested profile and the target it is
// applied to; the interface, handles and kernel state are filled in later.
func newNetworkProfileLease(backend netemBackend, helper testcontainers.Container, id, network string, state nshelper.Target, imageID string, peers []NetworkPeer, profile NetworkProfile) *NetworkProfileLease {
	lease := &NetworkProfileLease{ContainerID: id, NetworkName: network, SandboxID: state.SandboxID,
		MACAddress: state.MAC, Address: state.Address, HelperImageID: imageID, Delay: profile.Delay,
		Jitter: profile.Jitter, Peer: profile.Peer, PeerPort: profile.PeerPort, Peers: slices.Clone(peers), Loss: profile.Loss, Direction: profile.Direction,
		Phase: profile.Phase, backend: backend, helper: helper, shapes: true,
		id: id, network: network, sandbox: state.SandboxID, helperImage: imageID,
		shape: netem.Shape{Peers: netemPeers(peers), Match: netem.Match{Setup: profile.Phase == ConnectionSetup}}, params: profile.params()}
	lease.Rate, lease.QueueLimit = NetworkRate(lease.params.Rate), lease.params.Limit
	lease.Reorder, lease.Duplicate, lease.Corrupt = profile.Reorder, profile.Duplicate, profile.Corrupt
	return lease
}

// params is the profile's netem qdisc, with the queue limit of a Rate
// resolved.
func (d NetworkProfile) params() netem.Params {
	params := netem.Params{Delay: d.Delay, Jitter: d.Jitter, Loss: d.Loss,
		Reorder: netem.Fault(d.Reorder), Duplicate: netem.Fault(d.Duplicate), Corrupt: netem.Fault(d.Corrupt)}
	if d.Rate > 0 {
		params.Rate, params.Limit = uint64(d.Rate), d.QueueLimit
		if params.Limit == 0 {
			params.Limit = DefaultNetworkQueueLimit
		}
	}
	return params
}

// netemPeers converts a selection for internal/netem.
func netemPeers(peers []NetworkPeer) []netem.Peer {
	converted := make([]netem.Peer, len(peers))
	for i, peer := range peers {
		converted[i] = netem.Peer(peer)
	}
	return converted
}

// discard removes the helper of a lease that changed nothing in the kernel.
func (l *NetworkProfileLease) discard(ctx context.Context, err error) (*NetworkProfileLease, error) {
	return nil, errors.Join(err, removeNetemHelper(ctx, l.backend, l.helper))
}

// locate finds the one interface with the target's address and MAC, requires
// its root qdisc to be the default one and records it. It then rechecks the
// target: a restart between the first inspection and the helper's start would
// have given the helper a namespace without this endpoint.
func (l *NetworkProfileLease) locate(ctx context.Context, state nshelper.Target) error {
	report, err := inspectNetem(ctx, l.backend, l.helper, "inspect", state.Address.String(), state.MAC)
	if err != nil {
		return err
	}
	if len(report.Links) != 1 {
		return fmt.Errorf("found %d interfaces with the target's address and MAC on %s", len(report.Links), l.network)
	}
	root, err := report.Root()
	if err != nil {
		return err
	}
	if root.Handle != "0:" {
		return nshelper.InvalidRequest("the target's interface has a %s root qdisc (handle %s) that the profile would replace", root.Kind, root.Handle)
	}
	l.namespace, l.interfaceIndex = report.Namespace, report.Links[0].Index
	l.Namespace, l.Interface, l.InterfaceIndex = l.namespace, report.Links[0].Name, l.interfaceIndex
	if again, err := l.backend.target(ctx, l.id, l.network); err != nil || again.SandboxID != state.SandboxID {
		return errors.Join(errors.New("the network profile target restarted while it was being checked"), err)
	}
	return nil
}

// runSteps runs the planned commands in order. On failure keep reports
// whether the change may have reached the kernel, so the lease must be
// returned; otherwise nothing changed and the helper can be discarded.
func (l *NetworkProfileLease) runSteps(ctx context.Context, steps []netem.Step) (keep bool, err error) {
	for i, step := range steps {
		result, err := l.backend.run(ctx, l.helper, step.Label, step.Argv...)
		if err != nil {
			// The command may have reached the kernel; only the lease can tell.
			if marker, unconfirmed := supervised.Unconfirmed(err); unconfirmed {
				l.pending = marker
			}
			return true, fmt.Errorf("%s: %w", step.Label, err)
		}
		if result.ExitCode == 0 && !result.TimedOut {
			continue
		}
		cause := netem.CommandError(step.Label, result)
		if i > 0 {
			// The root is the lease's; Restore removes it with the rest.
			return true, cause
		}
		// tc changes nothing when the kernel refuses the request.
		status, statusErr := l.read(ctx)
		if statusErr != nil || status.Applied || status.RootHandle == l.shape.Handle {
			return true, errors.Join(cause, statusErr)
		}
		return false, cause
	}
	return false, nil
}

// admitNetemTarget refuses targets whose namespace the helper cannot join.
func admitNetemTarget(state nshelper.Target) error {
	return nshelper.Admit(state, "network profile", "the container", func(state nshelper.Target) error {
		if !state.Address.Is4() || state.MAC == "" {
			return nshelper.InvalidRequest("network profile requires an IPv4 endpoint with a MAC address")
		}
		return nil
	})
}

// preflightNetem checks the helper image, NET_ADMIN and the kernel's netem,
// for a Rate also netem's rate in tc and the kernel, for faults (netem
// keywords) those packet faults in tc and the kernel, and for a profile with
// peers also the kernel's prio qdisc and u32 classifier, in a helper without
// a network, and returns the helper's image ID.
func preflightNetem(ctx context.Context, backend netemBackend, image string, preflight netem.Preflight) (string, error) {
	helper, err := backend.startHelper(ctx, image, "none")
	if err != nil {
		return "", errors.Join(faulthook.ClassifyDocker(ctx, fmt.Errorf("start the netem preflight helper: %w", err)), removeNetemHelper(ctx, backend, helper))
	}
	imageID, err := func() (string, error) {
		_, imageID, err := backend.helper(ctx, helper)
		if err != nil {
			return "", fmt.Errorf("inspect the netem preflight helper: %w", err)
		}
		report, err := runNetemScript(ctx, backend, helper, "check the netem helper", preflight.Args()...)
		if err != nil {
			return "", err
		}
		if err := preflight.Check(report); err != nil {
			return "", err
		}
		return imageID, nil
	}()
	return imageID, errors.Join(err, removeNetemHelper(ctx, backend, helper))
}

// runNetemScript runs netem.Script in helper.
func runNetemScript(ctx context.Context, backend netemBackend, helper testcontainers.Container, label string, args ...string) (netem.Report, error) {
	result, err := backend.run(ctx, helper, label, append([]string{"python3", "-c", netem.Script}, args...)...)
	var exit *supervised.ExitError
	if errors.As(err, &exit) && (exit.Code == 126 || exit.Code == 127) {
		return netem.Report{}, fmt.Errorf("%w: it cannot run python3", ErrNetemImageUnsupported)
	}
	if err != nil {
		return netem.Report{}, fmt.Errorf("%s: %w", label, err)
	}
	if result.TimedOut {
		// Too slow says nothing about the image.
		return netem.Report{}, fmt.Errorf("%s timed out", label)
	}
	if result.ExitCode != 0 || result.StdoutTruncated {
		// The contract's script failed: the image, whatever tc reported.
		return netem.Report{}, fmt.Errorf("%w: %v", ErrNetemImageUnsupported, netem.CommandError(label, result))
	}
	var report netem.Report
	if err := json.Unmarshal(result.Stdout, &report); err != nil {
		return netem.Report{}, fmt.Errorf("%s: parse its report: %w", label, err)
	}
	return report, nil
}

// startNetemHelper starts a helper from image, an image ID, in networkMode,
// and checks that it runs that image.
func startNetemHelper(ctx context.Context, backend netemBackend, image string, networkMode container.NetworkMode) (testcontainers.Container, error) {
	helper, err := backend.startHelper(ctx, image, networkMode)
	if err != nil {
		return nil, errors.Join(faulthook.ClassifyDocker(ctx, fmt.Errorf("start the netem helper: %w", err)), removeNetemHelper(ctx, backend, helper))
	}
	if _, started, err := backend.helper(ctx, helper); err != nil || started != image {
		return nil, errors.Join(fmt.Errorf("the netem helper runs image %q instead of %s", started, image), err,
			removeNetemHelper(ctx, backend, helper))
	}
	return helper, nil
}

// inspectNetem reads the namespace, interface and qdiscs that helper sees,
// selecting the interface as netem.Script's args say.
func inspectNetem(ctx context.Context, backend netemBackend, helper testcontainers.Container, args ...string) (netem.Report, error) {
	report, err := runNetemScript(ctx, backend, helper, "inspect the shaped interface", args...)
	if err == nil && report.Error != "" {
		err = fmt.Errorf("inspect the shaped interface: %s", report.Error)
	}
	return report, err
}

// removeNetemHelper removes helper, also after ctx ended: a helper of a
// canceled call has no lease that could remove it later. A cluster's helper
// that it cannot remove keeps the cluster's label, so Terminate removes it.
// An ApplyLinkProfile helper has no cluster and no such label: only the
// testcontainers session reaper removes it.
func removeNetemHelper(ctx context.Context, backend netemBackend, helper testcontainers.Container) error {
	if helper == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), netemCommandTimeout)
	defer cancel()
	if err := backend.removeHelper(ctx, helper); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove the netem helper: %w", err)
	}
	return nil
}

// Status reads the shaped interface's qdiscs through the helper. After the
// target restarted, stopped, was removed or left the bridge, it reports Gone.
// With peers it reports Applied only when the root, the netem child and
// exactly the lease's filters, with the requested address and port keys,
// are present and nothing else is under the root; Filters holds tc's filter
// dump with each u32 filter's keys under matches. The counters are the
// child's, so they count only selected packets. A nil lease is refused with
// an error matching ceph.ErrInvalidRequest.
//
//purpose:observation
func (l *NetworkProfileLease) Status(ctx context.Context) (NetworkProfileStatus, error) {
	if l == nil {
		return NetworkProfileStatus{}, unavailable("network profile")
	}
	if err := lockMutex(ctx, &l.mu); err != nil {
		return NetworkProfileStatus{}, err
	}
	defer l.mu.Unlock()
	if l.complete {
		return NetworkProfileStatus{}, nshelper.InvalidRequest("the network profile was restored")
	}
	if !l.shapes {
		return NetworkProfileStatus{}, nshelper.InvalidRequest("an Inbound profile shapes only its peers' endpoints: read the Status of each lease in PeerLeases")
	}
	if err := l.settle(ctx); err != nil {
		return NetworkProfileStatus{}, err
	}
	return l.read(ctx)
}

// settle confirms that no earlier tc command of the lease can still run, and
// gives the lease a running helper in the target's namespace when the target
// still has it.
func (l *NetworkProfileLease) settle(ctx context.Context) error {
	if l.helper != nil {
		running, _, err := l.backend.helper(ctx, l.helper)
		switch {
		case errdefs.IsNotFound(err):
			// A removed helper runs nothing anymore.
			l.helper, l.pending = nil, ""
		case err != nil:
			return fmt.Errorf("inspect the netem helper: %w", err)
		case !running:
			if err := removeNetemHelper(ctx, l.backend, l.helper); err != nil {
				return err
			}
			l.helper, l.pending = nil, ""
		}
	}
	if l.pending != "" {
		if err := l.backend.cancel(ctx, l.helper, "netem profile", l.pending); err != nil {
			return fmt.Errorf("confirm that the earlier tc command stopped: %w", err)
		}
		l.pending = ""
	}
	if l.helper != nil {
		return nil
	}
	state, err := l.backend.target(ctx, l.id, l.network)
	if errdefs.IsNotFound(err) || err == nil && (!state.Running || state.SandboxID != l.sandbox) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect the network profile target: %w", err)
	}
	// The validated image, by ID: its tag may now name another image.
	helper, err := startNetemHelper(ctx, l.backend, l.helperImage, container.NetworkMode("container:"+l.id))
	if err != nil {
		return fmt.Errorf("replace the netem helper: %w", err)
	}
	l.helper = helper
	return nil
}

// read reports the shaped interface's state; the caller holds l.mu and has
// settled the lease. Without a helper the namespace no longer exists.
func (l *NetworkProfileLease) read(ctx context.Context) (NetworkProfileStatus, error) {
	if l.helper == nil {
		return NetworkProfileStatus{Gone: true}, nil
	}
	// The interface is found by its index: its address, MAC and name may
	// change, but a reconnect creates a new interface with a new index.
	report, err := inspectNetem(ctx, l.backend, l.helper, "index", strconv.Itoa(l.interfaceIndex))
	if err != nil {
		return NetworkProfileStatus{}, err
	}
	// Another namespace, or no interface with that index, is not the shaped
	// interface; its qdiscs are not the lease's.
	if report.Namespace != l.namespace || len(report.Links) != 1 {
		return NetworkProfileStatus{Gone: true}, nil
	}
	state, err := l.shape.Inspect(report)
	if err != nil {
		return NetworkProfileStatus{}, err
	}
	return NetworkProfileStatus{Applied: state.Applied, Interface: report.Links[0].Name, RootKind: state.RootKind, RootHandle: state.RootHandle,
		Delay: state.Delay, Jitter: state.Jitter, Loss: state.Loss, Rate: NetworkRate(state.Rate), QueueLimit: state.Limit,
		Reorder: NetworkPacketFault(state.Reorder), Duplicate: NetworkPacketFault(state.Duplicate), Corrupt: NetworkPacketFault(state.Corrupt), Gap: state.Gap,
		Packets: state.Packets, Bytes: state.Bytes, Drops: state.Drops,
		Overlimits: state.Overlimits, Requeues: state.Requeues, Backlog: state.Backlog, QueueLength: state.QueueLength,
		Qdiscs: report.Qdiscs, Filters: report.Filters, state: state}, nil
}

// Restore removes the profile's qdisc and then the helper, after restoring
// each owned peer lease. A profile whose interface is gone needs no removal. A
// root qdisc that someone replaced, or a filter or qdisc that someone added
// under the lease's root, is refused, not removed, and the lease stays
// retryable. A nil lease has nothing to restore.
//
//purpose:condition,lifetime
func (l *NetworkProfileLease) Restore(ctx context.Context) error {
	if l == nil {
		return nil
	}
	if err := lockMutex(ctx, &l.mu); err != nil {
		return err
	}
	defer l.mu.Unlock()
	// Every peer lease is restored, even after the outer lease completed: a
	// peer lease published after an earlier Restore is still restored here.
	// Each peer has its own mutex, so holding l.mu keeps the private list
	// stable without nesting the same lock.
	var inbound error
	for _, peer := range l.inboundLeases {
		inbound = errors.Join(inbound, peer.Restore(ctx))
	}
	if inbound != nil {
		return inbound
	}
	if l.complete {
		return nil
	}
	if !l.shapes {
		l.complete = true
		return nil
	}
	if err := l.settle(ctx); err != nil {
		return err
	}
	status, err := l.read(ctx)
	if err != nil {
		return err
	}
	if !status.Gone {
		// The lease's root goes, also when a selective profile is incomplete,
		// only while everything under it is what the apply added: removing
		// the root removes all of it, so foreign state there is refused even
		// when the profile itself is still applied.
		if status.RootHandle == l.shape.Handle {
			if err := l.shape.OwnedPrefix(status.Qdiscs, status.Filters); err != nil {
				return nshelper.InvalidRequest("the shaped interface's root %s carries state that the lease did not add (%v); remove it, then Restore again", l.shape.Handle, err)
			}
			result, err := l.backend.run(ctx, l.helper, "remove the netem profile", "tc", "qdisc", "del", "dev", status.Interface, "root", "handle", l.shape.Handle)
			if err != nil {
				if marker, unconfirmed := supervised.Unconfirmed(err); unconfirmed {
					l.pending = marker
				}
				return fmt.Errorf("remove the netem profile: %w", err)
			}
			if result.ExitCode != 0 || result.TimedOut {
				return netem.CommandError("remove the netem profile", result)
			}
			if status, err = l.read(ctx); err != nil {
				return err
			}
		}
		if !status.Gone && status.RootHandle != "0:" {
			return nshelper.InvalidRequest("the shaped interface's root qdisc is now %s %s, which the lease does not own", status.RootKind, status.RootHandle)
		}
	}
	if err := removeNetemHelper(ctx, l.backend, l.helper); err != nil {
		return err
	}
	l.helper, l.complete = nil, true
	if l.release != nil {
		l.release()
	}
	return nil
}

// dockerNetemBackend runs helpers as owned containers of a cluster, or
// without a cluster's label for ApplyLinkProfile.
type dockerNetemBackend struct {
	label testcontainers.CustomizeRequestOption
}

func (dockerNetemBackend) target(ctx context.Context, id, network string) (nshelper.Target, error) {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nshelper.Target{}, err
	}
	defer func() { _ = docker.Close() }()
	return nshelper.Inspect(ctx, docker, id, network)
}

func (dockerNetemBackend) endpoints(ctx context.Context, network string) (map[netip.Addr]string, error) {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = docker.Close() }()
	inspection, err := docker.NetworkInspect(ctx, network, mobycl.NetworkInspectOptions{})
	if err != nil {
		return nil, err
	}
	addresses := map[netip.Addr]string{}
	for id, endpoint := range inspection.Network.Containers {
		for _, prefix := range []netip.Prefix{endpoint.IPv4Address, endpoint.IPv6Address} {
			if prefix.IsValid() {
				addresses[prefix.Addr()] = id
			}
		}
	}
	return addresses, nil
}

// netemHelperOptions run an idle helper in networkMode as root, as the
// contract says whatever the image's USER, with NET_ADMIN as its only
// capability.
func netemHelperOptions(networkMode container.NetworkMode) []testcontainers.ContainerCustomizer {
	return append([]testcontainers.ContainerCustomizer{
		ceph.WithIdleEntrypoint(),
		testcontainers.WithConfigModifier(func(config *container.Config) { config.User = "0:0" }),
	}, nshelper.Confine(networkMode, "NET_ADMIN", "netem-helper")...)
}

func (b dockerNetemBackend) startHelper(ctx context.Context, image string, networkMode container.NetworkMode) (testcontainers.Container, error) {
	options := netemHelperOptions(networkMode)
	if b.label != nil {
		options = append(options, b.label)
	}
	helper, err := testcontainers.Run(ctx, image, options...)
	if helper == nil {
		return nil, err
	}
	return helper, err
}

func (dockerNetemBackend) helper(ctx context.Context, helper testcontainers.Container) (bool, string, error) {
	inspection, err := helper.Inspect(ctx)
	if err != nil {
		return false, "", err
	}
	running := inspection.State != nil && inspection.State.Running && !inspection.State.Paused
	return running, inspection.Image, nil
}

func (dockerNetemBackend) removeHelper(ctx context.Context, helper testcontainers.Container) error {
	return helper.Terminate(ctx, testcontainers.StopTimeout(0))
}

func (dockerNetemBackend) run(ctx context.Context, helper testcontainers.Container, label string, argv ...string) (supervised.Report, error) {
	return supervised.Run(ctx, helper, label, netemCommandTimeout, 1<<20, 64<<10, argv...)
}

func (dockerNetemBackend) cancel(ctx context.Context, helper testcontainers.Container, label, marker string) error {
	return supervised.Cancel(ctx, helper, label, marker)
}

// ApplyZoneProfile applies netem shaping at the boundary of a cluster with
// zones (ceph.WithZoneConfig): to the packets that leave zone on profile.Plane,
// which the zone's router forwards and only those. Every other zone's traffic,
// and traffic within any zone, keeps its path. To delay or cut both
// directions between two zones, apply a profile to each zone; one lease per
// router and plane never shapes a packet twice, and the router's counters
// attribute each direction. A Loss of 100 partitions the boundary in that
// direction. With a Peer or Peers only the packets to those addresses are
// shaped, as by ApplyNetworkProfile. The direction is the zone's egress, so
// Direction must stay Outbound.
//
// It refuses a cluster without zones, an unknown zone or plane, and, while
// any container on the bridge of any configured plane, not only profile.Plane,
// is neither a router nor a member enrolled for its current start, the
// shaping itself: such a container would reach the other zones directly (see
// ceph.ZoneLayout.Unzoned). The
// helper checks, the refusals and the errors are those of
// ApplyNetworkProfile, and so are the lease and its restoration: Terminate
// restores a held lease first, before the boundary is released.
//
//purpose:condition
func ApplyZoneProfile(ctx context.Context, c *ceph.Container, zone string, profile NetworkProfile) (*NetworkProfileLease, error) {
	if err := requireCluster(c, "a zone profile"); err != nil {
		return nil, err
	}
	if err := validateNetworkProfile(profile); err != nil {
		return nil, err
	}
	if profile.Direction != Outbound {
		return nil, nshelper.InvalidRequest("a zone profile shapes the zone's egress: Direction must be Outbound, apply a profile to the other zone for its direction")
	}
	router, name, err := faulthook.ZoneRouter(ctx, c, zone, profile.Plane)
	if err != nil {
		return nil, err
	}
	backend := dockerNetemBackend{label: faulthook.OwnedLabel(c)}
	return holdEndpoint(ctx, c, name, router.GetContainerID(), func() (*NetworkProfileLease, error) {
		return applyEndpointProfile(ctx, backend, router.GetContainerID(), name, profile, false)
	})
}
