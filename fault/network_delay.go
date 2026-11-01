package fault

import (
	"context"
	"encoding/binary"
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
func (r NetworkRate) String() string {
	for _, unit := range []struct {
		size   NetworkRate
		suffix string
	}{{GigabitPerSecond, "Gbit/s"}, {MegabitPerSecond, "Mbit/s"}, {KilobitPerSecond, "Kbit/s"}} {
		if r >= unit.size && r%unit.size == 0 {
			return strconv.FormatUint(uint64(r/unit.size), 10) + unit.suffix
		}
	}
	return strconv.FormatUint(uint64(r), 10) + "bit/s"
}

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
func (f NetworkPacketFault) String() string {
	text := strconv.FormatFloat(f.Percent, 'f', -1, 64) + "%"
	if f.Correlation != 0 {
		text += " " + strconv.FormatFloat(f.Correlation, 'f', -1, 64) + "%"
	}
	return text
}

// matches reports whether the kernel's fault is the requested one: the
// kernel keeps each percentage as a 32-bit probability.
func (f NetworkPacketFault) matches(kernel NetworkPacketFault) bool {
	return math.Abs(f.Percent-kernel.Percent) <= 1e-4 && math.Abs(f.Correlation-kernel.Correlation) <= 1e-4
}

// validate checks a fault's percentages; name is its NetworkProfile field.
func (f NetworkPacketFault) validate(name string) error {
	switch {
	case math.IsNaN(f.Percent) || f.Percent < 0 || f.Percent > 100:
		return invalidRequest("network delay %s.Percent must be a percentage from 0 to 100", name)
	case math.IsNaN(f.Correlation) || f.Correlation < 0 || f.Correlation > 100:
		return invalidRequest("network delay %s.Correlation must be a percentage from 0 to 100", name)
	case f.Percent == 0 && f.Correlation != 0:
		return invalidRequest("network delay %s.Correlation needs a Percent", name)
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
	ErrNetemImageUnsupported error = &categoryError{text: "the netem helper image does not meet the netem contract", kind: ceph.ErrImageCapability}
	// ErrNetemPermissionDenied reports an engine that did not give the helper
	// NET_ADMIN, or refused a qdisc change for lack of it. It belongs to
	// ceph.ErrEnvironmentUnavailable.
	ErrNetemPermissionDenied error = &categoryError{text: "the netem helper has no NET_ADMIN", kind: ceph.ErrEnvironmentUnavailable}
	// ErrNetemKernelUnsupported reports a kernel without the netem qdisc,
	// for a delay with peers without the prio qdisc or the u32 classifier, or
	// for a Rate a netem qdisc that does not keep the rate, or for a Reorder,
	// Duplicate or Corrupt one that does not keep that fault, its correlation
	// or, for a Reorder, a gap of 1. It belongs to ceph.ErrEnvironmentUnavailable.
	ErrNetemKernelUnsupported error = &categoryError{text: "the engine's kernel lacks a qdisc or classifier that the delay needs", kind: ceph.ErrEnvironmentUnavailable}
)

// NetworkProfile configures netem shaping for ApplyNetworkProfile or ApplyLinkProfile.
type NetworkProfile struct {
	// HelperImage is the netem helper image: NetemHelperImage, or another image
	// that meets contract 1 of the netem extension. Required.
	HelperImage string
	// Plane selects the bridge whose endpoint of the target is delayed.
	Plane ceph.NetworkPlane
	// Delay is added to every packet that leaves the target through that
	// endpoint: at least 1ms and at most MaxNetworkDelay, in whole
	// microseconds. With a Loss, a Rate, a Duplicate or a Corrupt it may be
	// zero.
	Delay time.Duration
	// Jitter varies each packet's delay uniformly within ±Jitter. It is at
	// most Delay, in whole microseconds.
	Jitter time.Duration
	// Peer, when set, limits the delay to the packets that the target sends
	// to this address through the endpoint; every other packet leaves
	// without delay. It is an IPv4 address, or an IPv6 address for an
	// endpoint that has one, such as a daemon's address from
	// DaemonAddresses on the same plane. Only packets that leave the target
	// are selected, unless Direction also shapes the peer's side.
	Peer netip.Addr
	// PeerPort, with Peer, limits the delay further to TCP packets to this
	// destination port, such as one of the peer's Messenger ports, and on
	// the peer's side of an Inbound delay to its packets from that port. Zero
	// selects every packet to Peer. An IPv6 packet with extension headers
	// before its TCP header is not selected.
	PeerPort uint16
	// Peers, instead of Peer and PeerPort, limits the delay to the packets
	// that the target sends to any of these peers, each with its own
	// optional TCP destination port, such as several OSDs of a daemon.
	// Every packet that one of them selects gets the same delay and loss;
	// every other packet leaves without delay. It holds at most
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
	// Loss drops this percentage of the packets that the delay applies to,
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
	// throughput. The preflight adds and removes a netem qdisc with a rate on
	// its loopback and checks what the kernel reports, so a tc or kernel
	// without netem's rate is refused before the target changes.
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

// namedFault is one of a delay's packet faults with its netem keyword.
type namedFault struct {
	name  string
	fault NetworkPacketFault
}

// packetFaults are the delay's packet faults, set or not, in tc's order.
func (d NetworkProfile) packetFaults() []namedFault {
	return []namedFault{{"reorder", d.Reorder}, {"duplicate", d.Duplicate}, {"corrupt", d.Corrupt}}
}

// requestedFaults are the netem keywords of the delay's packet faults that
// are set, in tc's order.
func (d NetworkProfile) requestedFaults() []string {
	var names []string
	for _, fault := range d.packetFaults() {
		if !fault.fault.IsZero() {
			names = append(names, fault.name)
		}
	}
	return names
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

// NetworkPeer selects the packets that a delay's target sends to Addr, an
// IPv4 address or, for an endpoint that has one, an IPv6 address. With a
// Port, only TCP packets to that destination port are selected; an IPv6
// packet with extension headers before its TCP header is not.
type NetworkPeer struct {
	Addr netip.Addr
	Port uint16
}

// selection is the delay's peers: Peer and PeerPort as one entry, or Peers.
func (d NetworkProfile) selection() []NetworkPeer {
	if d.Peer.IsValid() {
		return []NetworkPeer{{Addr: d.Peer, Port: d.PeerPort}}
	}
	return slices.Clone(d.Peers)
}

// NetworkProfileStatus is what the kernel reports for a profile's interface,
// read through the helper. It is native state, not the requested NetworkProfile.
type NetworkProfileStatus struct {
	// Applied means that the delay's own netem qdisc is the root qdisc of the
	// interface it was applied to (with peers, that its own hierarchy is,
	// with one filter for each peer), and that nothing the lease did not add
	// is under that root.
	Applied bool
	// Gone means that the delayed interface no longer exists: the target
	// stopped, restarted, was removed or was disconnected from the network,
	// and the delay ended with it.
	Gone bool
	// Interface is the delayed interface's current name.
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

	// mismatch says why the lease's own root does not hold exactly the
	// requested hierarchy, with nothing foreign under it.
	mismatch error
}

// NetworkProfileLease owns one netem profile on a container's network endpoint,
// applied from a helper container in the target's network namespace that
// alone has NET_ADMIN. Restore removes the delay and the helper. A non-nil
// lease returned with an error must be restored: the change may have been
// applied. Restore is retryable.
type NetworkProfileLease struct {
	// ContainerID and NetworkName identify the delayed endpoint.
	ContainerID, NetworkName string
	// SandboxID is the Docker network sandbox of the target when the delay
	// was applied, and Namespace the kernel's name of its network namespace.
	SandboxID, Namespace string
	// Interface, InterfaceIndex, MACAddress and Address identify the
	// delayed interface inside that namespace.
	Interface      string
	InterfaceIndex int
	MACAddress     string
	Address        netip.Addr
	// HelperImageID is the image of the helper that applied the delay.
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
	// its own Status is refused.
	PeerLeases []*NetworkProfileLease

	mu sync.Mutex
	// shapes reports that the lease owns a profile on its own endpoint;
	// an Inbound lease only holds its peers' leases.
	shapes  bool
	backend netemBackend
	helper  testcontainers.Container
	// handle is the root qdisc that the lease owns: the netem qdisc, or with
	// peers the prio qdisc whose third band leads to the netem child.
	handle   string
	child    string
	pending  string
	complete bool
	release  func()
}

// netemTarget is the Docker state of a delay's target.
type netemTarget struct {
	running, paused bool
	networkMode     container.NetworkMode
	sandboxID       string
	// driver is the driver of the network that the endpoint is on.
	driver   string
	address  netip.Addr
	address6 netip.Addr
	mac      string
}

// netemBackend runs the Docker and helper operations of a delay; tests
// replace it with a model of namespaces and qdiscs.
//
//testdouble:dependency-port
type netemBackend interface {
	// target inspects id and its endpoint on network. A removed container
	// returns an error for which errdefs.IsNotFound is true.
	target(ctx context.Context, id, network string) (netemTarget, error)
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

// netemScript reports, as one JSON object, what a helper sees. "preflight"
// checks the contract and netem on the loopback of the helper's own
// namespace, with "rate" also netem's rate, with "reorder", "duplicate" or
// "corrupt" those packet faults with a correlation, and with "peers" the prio qdisc
// and u32 classifier, its filter also with a TCP source port key with
// "sport", and with a destination port and a SYN key with "setup"; "inspect ADDRESS MAC" finds the interface with that address and
// MAC in the namespace the helper joined, and "index N" the interface with
// that index; both read its qdiscs and egress filters. tc prints each u32
// key as another "match" member of one object, so the script collects them
// into a "matches" list before they reach a JSON decoder that would keep only
// the last.
const netemScript = `import json,os,shutil,subprocess,sys
def pairs(items):
 d={}
 for k,v in items:
  if k=="match": d.setdefault("matches",[]).append(v)
  else: d[k]=v
 return d
def run(*argv):
 p=subprocess.run(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 return p.returncode,p.stdout.decode(errors="replace"),p.stderr.decode(errors="replace").strip()[-400:]
def net_admin():
 with open("/proc/self/status") as f:
  for line in f:
   if line.startswith("CapEff:"): return bool(int(line.split()[1],16)>>12&1)
 return False
out={"namespace":os.readlink("/proc/self/ns/net"),"net_admin":net_admin()}
if sys.argv[1]=="preflight":
 out["missing"]=[n for n in ("sh","sleep","tc","ip") if shutil.which(n) is None]
 if not out["missing"]:
  broken=[]
  for tool in ("tc","ip"):
   code,o,e=run(tool,"-V")
   out[tool]=(o+e).strip()[:200]
   if code or "iproute2" not in o+e: broken.append(tool+" -V")
  for argv in (("tc","-j","-s","qdisc","show","dev","lo"),("ip","-j","address","show")):
   code,o,e=run(*argv)
   try: json.loads(o)
   except ValueError: broken.append(" ".join(argv))
  out["broken"]=broken
  if not broken:
   out["add"]=run("tc","qdisc","add","dev","lo","root","handle","c7e1:","netem","delay","1ms")[0::2]
   if out["add"][0]==0:
    out["shown"]=run("tc","-j","qdisc","show","dev","lo")[1]
    out["delete"]=run("tc","qdisc","del","dev","lo","root","handle","c7e1:")[0::2]
    if "rate" in sys.argv[2:] and out["delete"][0]==0:
     out["rate"]=run("tc","qdisc","add","dev","lo","root","handle","c7e3:","netem","rate","1000000bit","limit","100")[0::2]
     if out["rate"][0]==0:
      out["rate_shown"]=run("tc","-j","qdisc","show","dev","lo")[1]
      out["rate_delete"]=run("tc","qdisc","del","dev","lo","root","handle","c7e3:")[0::2]
    faults=[f for f in ("reorder","duplicate","corrupt") if f in sys.argv[2:]]
    if faults and out["delete"][0]==0:
     argv=["tc","qdisc","add","dev","lo","root","handle","c7e4:","netem"]
     if "reorder" in faults or "duplicate" in faults: argv+=["delay","1ms" if "reorder" in faults else "0us","0us","0%"]
     for f,p,c in (("reorder","25%","50%"),("duplicate","3%","10%"),("corrupt","2%","5%")):
      if f in faults: argv+=[f,p,c]
     out["faults"]=run(*argv)[0::2]
     if out["faults"][0]==0:
      out["faults_shown"]=run("tc","-j","qdisc","show","dev","lo")[1]
      out["faults_delete"]=run("tc","qdisc","del","dev","lo","root","handle","c7e4:")[0::2]
    if "peers" in sys.argv[2:] and out["delete"][0]==0:
     out["prio"]=run("tc","qdisc","add","dev","lo","root","handle","c7e2:","prio","bands","3","priomap",*["0"]*16)[0::2]
     if out["prio"][0]==0:
      keys=["match","ip","dst","127.0.0.2/32"]
      if "sport" in sys.argv[2:] or "setup" in sys.argv[2:]: keys+=["match","ip","protocol","6","0xff","match","ip","sport" if "sport" in sys.argv[2:] else "dport","7000","0xffff"]
      if "setup" in sys.argv[2:]: keys+=["match","u8","0x02","0x02","at","33"]
      out["u32"]=run("tc","filter","add","dev","lo","parent","c7e2:","protocol","ip","pref","10","handle","800::1","u32",*keys,"flowid","c7e2:3")[0::2]
      if out["u32"][0]==0:
       o=run("tc","-j","filter","show","dev","lo")[1]
       try: out["u32_shown"]=json.dumps(json.loads(o,object_pairs_hook=pairs))
       except ValueError: out["u32_shown"]=o
      out["prio_delete"]=run("tc","qdisc","del","dev","lo","root","handle","c7e2:")[0::2]
else:
 code,o,e=run("ip","-j","address","show")
 if code: out["error"]="ip: "+e
 else:
  links=json.loads(o)
  if sys.argv[1]=="inspect":
   address,mac=sys.argv[2],sys.argv[3]
   links=[l for l in links if l.get("address")==mac and any(a.get("local")==address for a in l.get("addr_info",[]))]
  else:
   links=[l for l in links if l.get("ifindex")==int(sys.argv[2])]
  out["interfaces"]=[{"name":l["ifname"],"index":l["ifindex"]} for l in links]
  if len(links)==1:
   code,o,e=run("tc","-j","-s","qdisc","show","dev",links[0]["ifname"])
   if code: out["error"]="tc: "+e
   else:
    out["qdiscs"]=json.loads(o)
    code,o,e=run("tc","-j","filter","show","dev",links[0]["ifname"])
    if code: out["error"]="tc filter: "+e
    else: out["filters"]=json.loads(o,object_pairs_hook=pairs) if o.strip() else []
print(json.dumps(out))
`

type netemQdisc struct {
	Kind    string `json:"kind"`
	Handle  string `json:"handle"`
	Parent  string `json:"parent"`
	Root    bool   `json:"root"`
	Options struct {
		Delay struct {
			Delay  float64 `json:"delay"`
			Jitter float64 `json:"jitter"`
		} `json:"delay"`
		// Loss is a fraction; tc prints no delay member for a zero delay.
		Loss struct {
			Loss float64 `json:"loss"`
		} `json:"loss-random"`
		// Rate is netem's rate; tc prints no rate member without one.
		Rate  netemRate `json:"rate"`
		Limit int       `json:"limit"`
		// Reorder, Duplicate and Corrupt are netem's packet faults, and Gap
		// its reorder gap; tc prints no fault member without the fault.
		Reorder   netemFault  `json:"reorder"`
		Duplicate netemFault  `json:"duplicate"`
		Corrupt   netemFault  `json:"corrupt"`
		Gap       netemNumber `json:"gap"`
	} `json:"options"`
	Bytes      uint64 `json:"bytes"`
	Packets    uint64 `json:"packets"`
	Drops      uint64 `json:"drops"`
	Overlimits uint64 `json:"overlimits"`
	Requeues   uint64 `json:"requeues"`
	Backlog    uint64 `json:"backlog"`
	Qlen       uint64 `json:"qlen"`
}

// netemRate is netem's rate member, an object whose rate is in bytes per
// second. Other qdiscs, such as a foreign tbf, print a plain number under
// the same key; it is not netem's rate, and reads as zero.
type netemRate struct {
	Rate uint64 `json:"rate"`
}

func (r *netemRate) UnmarshalJSON(data []byte) error {
	if trimmed := strings.TrimSpace(string(data)); !strings.HasPrefix(trimmed, "{") {
		*r = netemRate{}
		return nil
	}
	type object netemRate
	return json.Unmarshal(data, (*object)(r))
}

// netemFault is one of netem's reorder, duplicate and corrupt members: an
// object with the probability under the member's own name and its
// correlation, both fractions. Another qdisc's member of the same name that is
// not such an object is not netem's, and reads as no fault.
type netemFault map[string]float64

func (f *netemFault) UnmarshalJSON(data []byte) error {
	*f = nil
	var members map[string]any
	if json.Unmarshal(data, &members) != nil {
		return nil
	}
	*f = netemFault{}
	for key, value := range members {
		if number, ok := value.(float64); ok {
			(*f)[key] = number
		}
	}
	return nil
}

// fault is the member in percentages; name is its probability's key.
func (f netemFault) fault(name string) NetworkPacketFault {
	percent := func(fraction float64) float64 { return math.Round(fraction*100*1e6) / 1e6 }
	return NetworkPacketFault{Percent: percent(f[name]), Correlation: percent(f["correlation"])}
}

// netemNumber is netem's gap, a whole number; another qdisc's member of the
// same name that is not one reads as zero.
type netemNumber int

func (n *netemNumber) UnmarshalJSON(data []byte) error {
	var value float64
	if json.Unmarshal(data, &value) != nil || value != math.Trunc(value) || math.Abs(value) > math.MaxInt32 {
		*n = 0
		return nil
	}
	*n = netemNumber(value)
	return nil
}

type netemReport struct {
	Namespace string   `json:"namespace"`
	NetAdmin  bool     `json:"net_admin"`
	Missing   []string `json:"missing"`
	Broken    []string `json:"broken"`
	Add       []any    `json:"add"`
	Shown     string   `json:"shown"`
	Delete    []any    `json:"delete"`
	// Prio, U32, U32Shown and PrioDelete are the preflight of a delay with
	// peers: a prio root and a u32 filter on the helper's loopback.
	Prio       []any  `json:"prio"`
	U32        []any  `json:"u32"`
	U32Shown   string `json:"u32_shown"`
	PrioDelete []any  `json:"prio_delete"`
	// Rate, RateShown and RateDelete are the preflight of a Rate: a netem
	// qdisc with a rate and a queue limit on the helper's loopback.
	Rate       []any  `json:"rate"`
	RateShown  string `json:"rate_shown"`
	RateDelete []any  `json:"rate_delete"`
	// Faults, FaultsShown and FaultsDelete are the preflight of a Reorder,
	// Duplicate or Corrupt: a netem qdisc with the requested faults, each
	// with a correlation, on the helper's loopback.
	Faults       []any           `json:"faults"`
	FaultsShown  string          `json:"faults_shown"`
	FaultsDelete []any           `json:"faults_delete"`
	Error        string          `json:"error"`
	Qdiscs       json.RawMessage `json:"qdiscs"`
	Filters      json.RawMessage `json:"filters"`
	Links        []struct {
		Name  string `json:"name"`
		Index int    `json:"index"`
	} `json:"interfaces"`
}

// root returns the root qdisc of the inspected interface.
func (r netemReport) root() (netemQdisc, error) {
	var qdiscs []netemQdisc
	if err := json.Unmarshal(r.Qdiscs, &qdiscs); err != nil {
		return netemQdisc{}, fmt.Errorf("parse tc qdisc output: %w", err)
	}
	for _, qdisc := range qdiscs {
		if qdisc.Root {
			return qdisc, nil
		}
	}
	return netemQdisc{}, errors.New("tc reported no root qdisc")
}

func seconds(value float64) time.Duration {
	return time.Duration(math.Round(value*1e6)) * time.Microsecond
}

// validateNetworkDelay checks a NetworkProfile before anything runs.
func validateNetworkDelay(delay NetworkProfile) error {
	return validateDelay(delay, true)
}

// validateDelay checks a NetworkProfile, with a Ceph network plane or, for
// ApplyLinkProfile, without one.
func validateDelay(delay NetworkProfile, plane bool) error {
	if strings.TrimSpace(delay.HelperImage) == "" {
		// The extension is not selected.
		return &ceph.OperationError{Kind: ceph.ErrImageCapability, Capability: "netem extension",
			Err: errors.New("network delay needs NetworkProfile.HelperImage: the netem helper is never selected implicitly")}
	}
	// Every check runs: allowing a zero delay with a Loss must not skip the
	// selection's checks.
	noDelay := delay.Delay == 0 && (delay.Loss > 0 || delay.Rate > 0 || !delay.Duplicate.IsZero() || !delay.Corrupt.IsZero())
	switch {
	case plane && delay.Plane != ceph.PublicNetworkPlane && delay.Plane != ceph.ClusterNetworkPlane:
		return invalidRequest("invalid Ceph network plane")
	case !plane && delay.Plane != "":
		return invalidRequest("a link delay names its network; leave NetworkProfile.Plane empty")
	case math.IsNaN(delay.Loss) || delay.Loss < 0 || delay.Loss > 100:
		return invalidRequest("network delay Loss must be a percentage from 0 to 100")
	case !noDelay && (delay.Delay < time.Millisecond || delay.Delay > MaxNetworkDelay):
		return invalidRequest("network delay must be between 1ms and %v, or zero with a Loss, a Rate, a Duplicate or a Corrupt", MaxNetworkDelay)
	case delay.Rate != 0 && (delay.Rate < MinNetworkRate || delay.Rate > MaxNetworkRate):
		return invalidRequest("network delay Rate must be between %v and %v, or zero", MinNetworkRate, MaxNetworkRate)
	case delay.Rate%8 != 0:
		return invalidRequest("network delay Rate must be whole bytes per second, a multiple of 8 bit/s")
	case delay.QueueLimit != 0 && delay.Rate == 0:
		return invalidRequest("network delay QueueLimit needs a Rate")
	case delay.QueueLimit < 0 || delay.QueueLimit > MaxNetworkQueueLimit:
		return invalidRequest("network delay QueueLimit must be between 1 and %d packets, or zero for %d", MaxNetworkQueueLimit, DefaultNetworkQueueLimit)
	case delay.Jitter < 0 || delay.Jitter > delay.Delay:
		return invalidRequest("network delay jitter must be between 0 and the delay")
	case delay.Delay%time.Microsecond != 0 || delay.Jitter%time.Microsecond != 0:
		return invalidRequest("network delay and jitter must be whole microseconds")
	case delay.PeerPort != 0 && !delay.Peer.IsValid():
		return invalidRequest("network delay PeerPort needs a Peer")
	case delay.Peer.IsValid() && (delay.Peer.Is4In6() || delay.Peer.Zone() != "" || delay.Peer.IsUnspecified()):
		return invalidRequest("network delay Peer %s is not a plain IPv4 or IPv6 address", delay.Peer)
	case delay.Peer.IsValid() && delay.Peers != nil:
		return invalidRequest("network delay takes either Peer and PeerPort or Peers")
	case delay.Peers != nil && (len(delay.Peers) == 0 || len(delay.Peers) > MaxNetworkPeers):
		return invalidRequest("network delay Peers needs 1 to %d peers", MaxNetworkPeers)
	case delay.Direction < Outbound || delay.Direction > BothDirections:
		return invalidRequest("network delay Direction %v is not Outbound, Inbound or BothDirections", delay.Direction)
	case delay.Phase < AllPackets || delay.Phase > ConnectionSetup:
		return invalidRequest("network delay Phase %v is not AllPackets or ConnectionSetup", delay.Phase)
	case delay.Direction != Outbound && !delay.Peer.IsValid() && delay.Peers == nil:
		return invalidRequest("network delay Direction %v needs a Peer or Peers: the other direction is shaped at each peer's endpoint", delay.Direction)
	case delay.Phase == ConnectionSetup && !delay.Peer.IsValid() && delay.Peers == nil:
		return invalidRequest("network delay Phase %v needs a Peer or Peers", delay.Phase)
	}
	for _, fault := range []namedFault{{"Reorder", delay.Reorder}, {"Duplicate", delay.Duplicate}, {"Corrupt", delay.Corrupt}} {
		if err := fault.fault.validate(fault.name); err != nil {
			return err
		}
	}
	if !delay.Reorder.IsZero() {
		switch {
		case delay.Delay == 0:
			return invalidRequest("network delay Reorder needs a Delay: netem reorders by sending some packets ahead of the delayed ones")
		case delay.Jitter != 0:
			return invalidRequest("network delay Reorder takes no Jitter: a jitter reorders packets by itself, and the two cannot be told apart")
		case delay.Rate != 0:
			return invalidRequest("network delay Reorder takes no Rate: netem sends the reordered packets past the rate limit")
		}
	}
	seen := map[NetworkPeer]bool{}
	for _, peer := range delay.Peers {
		switch {
		case !peer.Addr.IsValid() || peer.Addr.Is4In6() || peer.Addr.Zone() != "" || peer.Addr.IsUnspecified():
			return invalidRequest("network delay peer %v is not a plain IPv4 or IPv6 address", peer.Addr)
		case seen[peer]:
			return invalidRequest("network delay Peers has %v port %d twice", peer.Addr, peer.Port)
		}
		seen[peer] = true
	}
	return nil
}

// netemStep is one tc command of a delay's change.
type netemStep struct {
	label string
	argv  []string
}

// peerBand is the prio band of a selective delay's root that leads to its
// netem child; the filter sends the selected packets there.
const peerBand = "3"

// peerLeaf is one u32 filter of a delay with peers: the filter that selects
// one peer's packets.
type peerLeaf struct {
	peer NetworkPeer
	// protocol is tc's protocol of the filter, ip or ipv6.
	protocol string
	// pref is the filter's preference, table the u32 table that the kernel
	// creates for that preference, and handle the filter's own key node.
	pref, table, handle string
}

// planPeerLeaves lays out the filters of a delay with peers: one preference
// per family, IPv4 first, each with its own u32 table, and one key node per
// peer in the caller's order. A u32 filter has one protocol, so the families
// cannot share a preference. The kernel numbers the u32 tables of one qdisc
// from 800: in the order that their preferences are created; the filters are
// added in this order, and each names its table, so a table that the kernel
// numbered otherwise refuses the filter or shows it under another preference,
// and neither is accepted as applied. One peer of one family gives the
// single filter pref 10, handle 800::1.
func planPeerLeaves(peers []NetworkPeer) []peerLeaf {
	var leaves []peerLeaf
	group := 0
	for _, family := range []struct {
		protocol string
		match    func(netip.Addr) bool
	}{{"ip", netip.Addr.Is4}, {"ipv6", netip.Addr.Is6}} {
		node := 0
		for _, peer := range peers {
			if !family.match(peer.Addr) {
				continue
			}
			node++
			table := fmt.Sprintf("%x:", 0x800+group)
			leaves = append(leaves, peerLeaf{peer: peer, protocol: family.protocol, pref: strconv.Itoa(10 + group),
				table: table, handle: fmt.Sprintf("%s:%x", table, node)})
		}
		if node > 0 {
			group++
		}
	}
	return leaves
}

// u32Key is one 32-bit match of a u32 filter, at a byte offset of the IP
// header, as tc prints it.
type u32Key struct {
	Off   int
	Value uint32
	Mask  uint32
}

// peerMatch is how a lease's filters read their peers: whether each port is
// a TCP source port rather than a destination port, and whether only
// segments with SYN set are selected.
type peerMatch struct {
	source, setup bool
}

// tcpFlags is the offset of the TCP flags byte in a TCP header, and tcpSYN
// its SYN bit.
const (
	tcpFlags = 13
	tcpSYN   = 0x02
)

// peerFilterKeys are the u32 keys that select packets to peer and, when port
// is not zero, TCP packets to that destination port, or from that source port
// with match.source, and with match.setup only TCP segments with SYN set. tc
// aligns every key to a 32-bit word: the IPv4 protocol is at offset 8, the
// IPv6 next header at offset 4, the destination port is the low half and the
// source port the high half of the word at offset 20 (IPv4) or 40 (IPv6), and
// the SYN bit is in the word at offset 32 (52), right after a header without
// options or extension headers.
func peerFilterKeys(peer netip.Addr, port uint16, match peerMatch) []u32Key {
	var keys []u32Key
	address := peer.AsSlice()
	tcp := 20
	protocol := u32Key{Off: 8, Value: 6 << 16, Mask: 0xff << 16}
	if peer.Is4() {
		keys = append(keys, u32Key{Off: 16, Value: binary.BigEndian.Uint32(address), Mask: 0xffffffff})
	} else {
		for word := range 4 {
			keys = append(keys, u32Key{Off: 24 + 4*word, Value: binary.BigEndian.Uint32(address[4*word:]), Mask: 0xffffffff})
		}
		tcp, protocol = 40, u32Key{Off: 4, Value: 6 << 8, Mask: 0xff << 8}
	}
	if port != 0 || match.setup {
		keys = append(keys, protocol)
	}
	switch {
	case port != 0 && match.source:
		keys = append(keys, u32Key{Off: tcp, Value: uint32(port) << 16, Mask: 0xffff << 16})
	case port != 0:
		keys = append(keys, u32Key{Off: tcp, Value: uint32(port), Mask: 0xffff})
	}
	if match.setup {
		flags := tcp + tcpFlags
		shift := 8 * (3 - flags%4)
		keys = append(keys, u32Key{Off: flags - flags%4, Value: tcpSYN << shift, Mask: tcpSYN << shift})
	}
	return keys
}

// peerFilterArgs is the tc command that adds one filter of a delay with peers.
func peerFilterArgs(device, root string, leaf peerLeaf, match peerMatch) []string {
	family, bits, tcp := "ip", "/32", 20
	if leaf.peer.Addr.Is6() {
		family, bits, tcp = "ip6", "/128", 40
	}
	argv := []string{"tc", "filter", "add", "dev", device, "parent", root, "protocol", leaf.protocol, "pref", leaf.pref,
		"handle", leaf.handle, "u32", "match", family, "dst", leaf.peer.Addr.String() + bits}
	if leaf.peer.Port != 0 || match.setup {
		argv = append(argv, "match", family, "protocol", "6", "0xff")
	}
	if leaf.peer.Port != 0 {
		field := "dport"
		if match.source {
			field = "sport"
		}
		argv = append(argv, "match", family, field, strconv.Itoa(int(leaf.peer.Port)), "0xffff")
	}
	if match.setup {
		argv = append(argv, "match", "u8", fmt.Sprintf("0x%02x", tcpSYN), fmt.Sprintf("0x%02x", tcpSYN), "at", strconv.Itoa(tcp+tcpFlags))
	}
	return append(argv, "flowid", root+peerBand)
}

// netemFilter is one entry of tc's filter dump, with the u32 keys that
// netemScript collected under "matches".
type netemFilter struct {
	Parent   string `json:"parent"`
	Protocol string `json:"protocol"`
	Pref     int    `json:"pref"`
	Kind     string `json:"kind"`
	Options  struct {
		Handle  string `json:"fh"`
		FlowID  string `json:"flowid"`
		Matches []struct {
			Value   string `json:"value"`
			Mask    string `json:"mask"`
			OffMask string `json:"offmask"`
			Off     int    `json:"off"`
		} `json:"matches"`
	} `json:"options"`
}

// checkPeerFilter reports why the egress filters of an interface are not
// exactly the filters of a delay with these peers under root, or nil. Header
// entries, which tc prints for each preference's chain and its u32 table, may
// accompany the filters; any other filter is foreign state.
func checkPeerFilter(data json.RawMessage, root string, peers []NetworkPeer, match peerMatch) error {
	return checkPeerFilters(data, root, peers, match, true)
}

// checkPeerFilters is checkPeerFilter that, unless required, also accepts
// only some of the filters, or none: the state of an apply that stopped
// before its last filter.
func checkPeerFilters(data json.RawMessage, root string, peers []NetworkPeer, match peerMatch, required bool) error {
	var filters []netemFilter
	if err := json.Unmarshal(data, &filters); err != nil {
		return fmt.Errorf("parse tc filter output: %w", err)
	}
	leaves := planPeerLeaves(peers)
	seen := map[string]bool{}
	for _, filter := range filters {
		pref := strconv.Itoa(filter.Pref)
		group := slices.IndexFunc(leaves, func(leaf peerLeaf) bool { return leaf.protocol == filter.Protocol && leaf.pref == pref })
		if filter.Parent != root || group < 0 || filter.Kind != "u32" {
			return fmt.Errorf("a filter with parent %s, protocol %s, preference %d and kind %s is not the delay's", filter.Parent, filter.Protocol, filter.Pref, filter.Kind)
		}
		if len(filter.Options.Matches) == 0 && filter.Options.FlowID == "" {
			// The preference's chain, or its u32 table.
			if filter.Options.Handle != "" && filter.Options.Handle != leaves[group].table {
				return fmt.Errorf("preference %d has the u32 table %s, not the delay's %s", filter.Pref, filter.Options.Handle, leaves[group].table)
			}
			continue
		}
		i := slices.IndexFunc(leaves, func(leaf peerLeaf) bool {
			return leaf.protocol == filter.Protocol && leaf.pref == pref && leaf.handle == filter.Options.Handle
		})
		if i < 0 {
			return fmt.Errorf("the filter %s of preference %d is not one of the delay's", filter.Options.Handle, filter.Pref)
		}
		leaf := leaves[i]
		if seen[leaf.handle] {
			return fmt.Errorf("the interface has the filter %s twice", leaf.handle)
		}
		seen[leaf.handle] = true
		if filter.Options.FlowID != root+peerBand {
			return fmt.Errorf("the filter %s leads to %s, not %s", leaf.handle, filter.Options.FlowID, root+peerBand)
		}
		var got []u32Key
		for _, match := range filter.Options.Matches {
			value, err1 := strconv.ParseUint(match.Value, 16, 32)
			mask, err2 := strconv.ParseUint(match.Mask, 16, 32)
			if err1 != nil || err2 != nil || match.OffMask != "" {
				return fmt.Errorf("the filter %s has a key tc printed as %+v", leaf.handle, match)
			}
			got = append(got, u32Key{Off: match.Off, Value: uint32(value), Mask: uint32(mask)})
		}
		if want := peerFilterKeys(leaf.peer.Addr, leaf.peer.Port, match); !slices.Equal(got, want) {
			return fmt.Errorf("the filter %s has the keys %v, not %v", leaf.handle, got, want)
		}
	}
	if required {
		for _, leaf := range leaves {
			if !seen[leaf.handle] {
				return fmt.Errorf("the filter %s for %v port %d is missing", leaf.handle, leaf.peer.Addr, leaf.peer.Port)
			}
		}
	}
	return nil
}

// ApplyNetworkProfile applies netem shaping to the packets that target sends
// through its endpoint on the plane's bridge, below Ceph.
// It is an opt-in extension: delay.HelperImage names the netem helper, which
// runs in the target's network namespace with NET_ADMIN, while the target
// keeps its own image and capabilities. The target is an owned daemon or a
// caller-owned container attached to that bridge; host-network clusters,
// stopped or paused targets and targets that share another container's
// network namespace are refused. With a Peer or Peers, only the packets to
// those peers are delayed, through one prio root, one netem child and one u32
// filter per peer, all owned by the one lease: to cut a daemon's paths to
// several peers at once, list them in one delay. A Rate limits the bandwidth
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
// the kernel's default and a target that already has a delay on that bridge.
// A missing HelperImage is ceph.ErrImageCapability, the refusals are
// ceph.ErrInvalidRequest, and a tc command that fails otherwise is a
// *ceph.NativeFailure.
//
// The delay lasts until Restore, or until the target stops or leaves the
// bridge, which removes its interface. Terminate restores delays that are
// still held; restore a caller-owned target's delay before removing it.
//
// With a Peer or Peers the lease owns a prio root qdisc whose third band
// leads to the netem child, and one u32 filter per peer that sends its
// packets there; every priority maps to the first band, so nothing else is
// delayed. IPv4 peers' filters are in preference 10 and u32 table 800:, with
// handles 800::1, 800::2 and so on in the given order; IPv6 peers' are in
// the next preference and table (11 and 801: after IPv4 peers, otherwise 10
// and 800:). They are added root first and filters last, so no packet is
// delayed before the hierarchy is complete.
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
func ApplyNetworkProfile(ctx context.Context, c *ceph.Container, target testcontainers.Container, delay NetworkProfile) (*NetworkProfileLease, error) {
	return temporaryNetworkDelay(ctx, c, target, delay, nil)
}

func temporaryNetworkDelay(ctx context.Context, c *ceph.Container, target testcontainers.Container, delay NetworkProfile, backend netemBackend) (*NetworkProfileLease, error) {
	if backend == nil {
		backend = dockerNetemBackend{label: faulthook.OwnedLabel(c)}
	}
	if err := validateNetworkDelay(delay); err != nil {
		return nil, err
	}
	if target == nil || target.GetContainerID() == "" {
		return nil, invalidRequest("network delay requires a container")
	}
	name, err := admitNetworkDelayTarget(ctx, c, target, delay.Plane)
	if err != nil {
		return nil, err
	}
	// Each endpoint's lease is reserved and held for Terminate on its own,
	// so a delay on a peer's endpoint is restored even when the lease that
	// lists it is lost.
	hold := func(id string, apply func() (*NetworkProfileLease, error)) (*NetworkProfileLease, error) {
		token, err := reserveNetworkDelay(ctx, c, name, id)
		if err != nil {
			return nil, err
		}
		lease, err := apply()
		if closed := settleNetworkDelay(c, token, lease); closed && lease != nil {
			// Terminate ran meanwhile and did not see this delay.
			native, _ := faulthook.Terminated()
			return lease, errors.Join(err, native, lease.Restore(ctx))
		}
		return lease, err
	}
	return applyDirectedDelay(ctx, backend, target.GetContainerID(), name, delay, hold)
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

// applyDirectedDelay applies delay in its Direction: on the target's
// endpoint for Outbound, on each selected peer's endpoint for Inbound, and
// on both for BothDirections. Every peer is resolved before anything is
// applied, so a peer that is not a container on the network changes
// nothing. A non-nil lease returned with an error must be restored.
func applyDirectedDelay(ctx context.Context, backend netemBackend, id, network string, delay NetworkProfile, hold endpointHold) (*NetworkProfileLease, error) {
	var inbound []inboundPeer
	var state netemTarget
	if delay.Direction.inbound() {
		var err error
		if state, inbound, err = planInbound(ctx, backend, id, network, delay); err != nil {
			return nil, err
		}
	}
	var top *NetworkProfileLease
	if delay.Direction.outbound() {
		lease, err := hold(id, func() (*NetworkProfileLease, error) {
			return applyNetworkDelay(ctx, backend, id, network, delay, false)
		})
		if lease == nil || err != nil {
			return lease, err
		}
		top = lease
	} else {
		top = newNetworkProfileLease(backend, nil, id, network, state, "", delay.selection(), delay)
		top.shapes = false
	}
	top.Direction = delay.Direction
	for _, peer := range inbound {
		profile := delay
		profile.Direction, profile.Peer, profile.PeerPort, profile.Peers = Outbound, netip.Addr{}, 0, peer.peers
		lease, err := hold(peer.id, func() (*NetworkProfileLease, error) {
			return applyNetworkDelay(ctx, backend, peer.id, network, profile, true)
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
// may already hold the outer lease and restore it meanwhile, so PeerLeases is
// written and read only under l.mu.
func (l *NetworkProfileLease) addPeerLease(peer *NetworkProfileLease) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.PeerLeases = append(l.PeerLeases, peer)
}

// peerLeases returns the peer leases published so far.
func (l *NetworkProfileLease) peerLeases() []*NetworkProfileLease {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.PeerLeases)
}

// planInbound resolves every selected peer address to the container that
// has it on network, and lists, per container in the order of their first
// peer, the target's address of each peer's family with that peer's port.
// A peer address that no running container on the network has, or that is
// the target's own, is refused, as is an IPv6 peer of a target without an
// IPv6 address.
func planInbound(ctx context.Context, backend netemBackend, id, network string, delay NetworkProfile) (netemTarget, []inboundPeer, error) {
	state, err := backend.target(ctx, id, network)
	if err != nil {
		return netemTarget{}, nil, faulthook.ClassifyDocker(ctx, fmt.Errorf("inspect the network delay target: %w", err))
	}
	if err := admitNetemTarget(state); err != nil {
		return netemTarget{}, nil, err
	}
	endpoints, err := backend.endpoints(ctx, network)
	if err != nil {
		return netemTarget{}, nil, faulthook.ClassifyDocker(ctx, fmt.Errorf("inspect the endpoints of %s: %w", network, err))
	}
	var plan []inboundPeer
	for _, peer := range delay.selection() {
		owner, ok := endpoints[peer.Addr]
		switch {
		case !ok:
			return netemTarget{}, nil, invalidRequest("network delay peer %s is not the address of a running container on %s: %v is shaped at the peer's endpoint", peer.Addr, network, delay.Direction)
		case owner == id:
			return netemTarget{}, nil, invalidRequest("network delay peer %s is the target's own address", peer.Addr)
		}
		address := state.address
		if peer.Addr.Is6() {
			if address = state.address6; !address.IsValid() {
				return netemTarget{}, nil, invalidRequest("network delay peer %s is IPv6, and the target has no IPv6 address on %s", peer.Addr, network)
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
// bridge between the gateways of an RGW multisite: it delays or drops the
// packets that ctr sends through its endpoint on the bridge named network.
// Like InterruptLink, the caller owns ctr, the network and the returned
// lease, and restores the lease before it removes either: no cluster's
// Terminate knows it. delay.Plane stays empty; every other field, check and
// error is that of ApplyNetworkProfile, and a network that is not a bridge
// is refused.
//
//purpose:condition
func ApplyLinkProfile(ctx context.Context, ctr testcontainers.Container, network string, delay NetworkProfile) (*NetworkProfileLease, error) {
	return temporaryLinkDelay(ctx, dockerNetemBackend{}, ctr, network, delay)
}

func temporaryLinkDelay(ctx context.Context, backend netemBackend, ctr testcontainers.Container, network string, delay NetworkProfile) (*NetworkProfileLease, error) {
	if err := validateDelay(delay, false); err != nil {
		return nil, err
	}
	switch {
	case ctr == nil || ctr.GetContainerID() == "":
		return nil, invalidRequest("network delay requires a container")
	case strings.TrimSpace(network) == "":
		return nil, invalidRequest("a link delay requires a network name")
	}
	return applyDirectedDelay(ctx, backend, ctr.GetContainerID(), network, delay,
		func(_ string, apply func() (*NetworkProfileLease, error)) (*NetworkProfileLease, error) {
			return apply()
		})
}

// admitNetworkDelayTarget returns the bridge of plane, after checking that
// the cluster is open and on bridges and that target is attached to it.
func admitNetworkDelayTarget(ctx context.Context, c *ceph.Container, target testcontainers.Container, plane ceph.NetworkPlane) (string, error) {
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
		return "", invalidRequest("network delay requires a bridge cluster: a delay on a host network would delay the host")
	case name == "":
		return "", invalidRequest("Ceph bridge is not initialized")
	}
	if !owned {
		attached, err := target.Networks(ctx)
		if err != nil {
			return "", faulthook.ClassifyDocker(ctx, fmt.Errorf("inspect the container's networks: %w", err))
		}
		if !slices.Contains(attached, name) {
			return "", invalidRequest("the container is not attached to Ceph network %s", name)
		}
	}
	return name, nil
}

// reserveNetworkDelay reserves the endpoint of container id on the bridge
// name until settleNetworkDelay.
func reserveNetworkDelay(ctx context.Context, c *ceph.Container, name, id string) (faulthook.Token, error) {
	_, terminated := faulthook.Terminated()
	var token faulthook.Token
	err := faulthook.WithTopology(ctx, c, func() error {
		if faulthook.Closed(c) {
			return terminated
		}
		var reserveErr error
		token, _, reserveErr = faulthook.Reserve(ctx, c, faulthook.NetworkDelay, name+"/"+id)
		return reserveErr
	})
	return token, err
}

// settleNetworkDelay keeps a returned lease for Terminate, or frees the
// reservation when there is none. It reports whether Terminate has started.
func settleNetworkDelay(c *ceph.Container, token faulthook.Token, lease *NetworkProfileLease) (closed bool) {
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

// applyNetworkDelay checks the helper and the target, then applies the delay.
// It returns a lease whenever the change may have reached the kernel.
// With source, each peer's port is the TCP source port of what the
// endpoint sends: the lease shapes a peer's side of an Inbound delay.
func applyNetworkDelay(ctx context.Context, backend netemBackend, id, network string, delay NetworkProfile, source bool) (*NetworkProfileLease, error) {
	state, peers, imageID, err := admitNetworkDelay(ctx, backend, id, network, delay, source)
	if err != nil {
		return nil, err
	}
	// The checked image's ID, not its tag, which may move.
	helper, err := startNetemHelper(ctx, backend, imageID, container.NetworkMode("container:"+id))
	if err != nil {
		return nil, err
	}
	lease := newNetworkProfileLease(backend, helper, id, network, state, imageID, peers, delay)
	lease.SourcePorts = source
	if err := lease.locate(ctx, state); err != nil {
		return lease.discard(ctx, err)
	}
	lease.handle = fmt.Sprintf("%x:", 0x1000+rand.IntN(0xe000))
	if keep, err := lease.runSteps(ctx, lease.plan(netemArguments(delay, lease.Rate, lease.QueueLimit))); err != nil {
		if !keep {
			return lease.discard(ctx, err)
		}
		return lease, err
	}
	return lease, lease.verifyApplied(ctx, delay)
}

// admitNetworkDelay checks the target's endpoint, that every IPv6 peer has an
// IPv6 target address, and the helper image, before anything is started. It
// returns the target, the selected peers and the checked image's ID.
func admitNetworkDelay(ctx context.Context, backend netemBackend, id, network string, delay NetworkProfile, source bool) (netemTarget, []NetworkPeer, string, error) {
	state, err := backend.target(ctx, id, network)
	if err != nil {
		return netemTarget{}, nil, "", faulthook.ClassifyDocker(ctx, fmt.Errorf("inspect the network delay target: %w", err))
	}
	if err := admitNetemTarget(state); err != nil {
		return netemTarget{}, nil, "", err
	}
	peers := delay.selection()
	for _, peer := range peers {
		if peer.Addr.Is6() && !state.address6.IsValid() {
			return netemTarget{}, nil, "", invalidRequest("network delay peer %s is IPv6, and the target has no IPv6 address on %s", peer.Addr, network)
		}
	}
	imageID, err := preflightNetem(ctx, backend, delay.HelperImage, len(peers) > 0, delay.Rate > 0, delay.requestedFaults(),
		peerMatch{source: source, setup: delay.Phase == ConnectionSetup})
	if err != nil {
		return netemTarget{}, nil, "", err
	}
	return state, peers, imageID, nil
}

// newNetworkProfileLease records the requested profile and the target it is
// applied to; the interface, handles and kernel state are filled in later.
func newNetworkProfileLease(backend netemBackend, helper testcontainers.Container, id, network string, state netemTarget, imageID string, peers []NetworkPeer, delay NetworkProfile) *NetworkProfileLease {
	lease := &NetworkProfileLease{ContainerID: id, NetworkName: network, SandboxID: state.sandboxID,
		MACAddress: state.mac, Address: state.address, HelperImageID: imageID, Delay: delay.Delay,
		Jitter: delay.Jitter, Peer: delay.Peer, PeerPort: delay.PeerPort, Peers: peers, Loss: delay.Loss, Direction: delay.Direction,
		Phase: delay.Phase, backend: backend, helper: helper, shapes: true}
	if delay.Rate > 0 {
		lease.Rate, lease.QueueLimit = delay.Rate, delay.QueueLimit
		if lease.QueueLimit == 0 {
			lease.QueueLimit = DefaultNetworkQueueLimit
		}
	}
	lease.Reorder, lease.Duplicate, lease.Corrupt = delay.Reorder, delay.Duplicate, delay.Corrupt
	return lease
}

// discard removes the helper of a lease that changed nothing in the kernel.
func (l *NetworkProfileLease) discard(ctx context.Context, err error) (*NetworkProfileLease, error) {
	return nil, errors.Join(err, removeNetemHelper(ctx, l.backend, l.helper))
}

// locate finds the one interface with the target's address and MAC, requires
// its root qdisc to be the default one and records it. It then rechecks the
// target: a restart between the first inspection and the helper's start would
// have given the helper a namespace without this endpoint.
func (l *NetworkProfileLease) locate(ctx context.Context, state netemTarget) error {
	report, err := inspectNetem(ctx, l.backend, l.helper, "inspect", state.address.String(), state.mac)
	if err != nil {
		return err
	}
	if len(report.Links) != 1 {
		return fmt.Errorf("found %d interfaces with the target's address and MAC on %s", len(report.Links), l.NetworkName)
	}
	root, err := report.root()
	if err != nil {
		return err
	}
	if root.Handle != "0:" {
		return invalidRequest("the target's interface has a %s root qdisc (handle %s) that the delay would replace", root.Kind, root.Handle)
	}
	l.Namespace, l.Interface, l.InterfaceIndex = report.Namespace, report.Links[0].Name, report.Links[0].Index
	if again, err := l.backend.target(ctx, l.ContainerID, l.NetworkName); err != nil || again.sandboxID != state.sandboxID {
		return errors.Join(errors.New("the network delay target restarted while it was being checked"), err)
	}
	return nil
}

// match is how the lease's filters read its peers.
func (l *NetworkProfileLease) match() peerMatch {
	return peerMatch{source: l.SourcePorts, setup: l.Phase == ConnectionSetup}
}

// netemArguments is the netem qdisc's parameter list for delay, with rate and
// queueLimit as the lease resolved them.
func netemArguments(delay NetworkProfile, rate NetworkRate, queueLimit int) []string {
	netem := []string{"netem"}
	// tc (iproute2 6.1, as in NetemHelperImage) sends a duplicate's
	// correlation only together with a delay's correlation, and drops it
	// silently otherwise. A zero delay correlation, which the kernel also
	// has without one, makes it send both.
	dupCorrelation := delay.Duplicate.Correlation != 0
	if delay.Delay > 0 || dupCorrelation {
		netem = append(netem, "delay", fmt.Sprintf("%dus", delay.Delay.Microseconds()))
		if delay.Jitter > 0 || dupCorrelation {
			netem = append(netem, fmt.Sprintf("%dus", delay.Jitter.Microseconds()))
		}
		if dupCorrelation {
			netem = append(netem, "0%")
		}
	}
	if delay.Loss > 0 {
		netem = append(netem, "loss", strconv.FormatFloat(delay.Loss, 'f', -1, 64)+"%")
	}
	for _, fault := range delay.packetFaults() {
		if !fault.fault.IsZero() {
			netem = append(append(netem, fault.name), strings.Fields(fault.fault.String())...)
		}
	}
	if rate > 0 {
		netem = append(netem, "rate", strconv.FormatUint(uint64(rate), 10)+"bit", "limit", strconv.Itoa(queueLimit))
	}
	return netem
}

// plan is the tc commands that install netem under the lease's root handle:
// netem itself as the root, or, with peers, a prio root whose every priority
// is in band 1, netem on the peer band and the peer filters added last, so
// nothing is delayed until the filters select the peers' packets. It picks
// the child handle.
func (l *NetworkProfileLease) plan(netem []string) []netemStep {
	if len(l.Peers) == 0 {
		return []netemStep{{"add the netem delay", append([]string{"tc", "qdisc", "add", "dev", l.Interface, "root", "handle", l.handle}, netem...)}}
	}
	l.child = fmt.Sprintf("%x:", 0x1000+rand.IntN(0xe000))
	for l.child == l.handle {
		l.child = fmt.Sprintf("%x:", 0x1000+rand.IntN(0xe000))
	}
	priomap := slices.Repeat([]string{"0"}, 16)
	steps := []netemStep{
		{"add the delay's prio root", append([]string{"tc", "qdisc", "add", "dev", l.Interface, "root", "handle", l.handle, "prio", "bands", "3", "priomap"}, priomap...)},
		{"add the netem delay", append([]string{"tc", "qdisc", "add", "dev", l.Interface, "parent", l.handle + peerBand, "handle", l.child}, netem...)},
	}
	leaves := planPeerLeaves(l.Peers)
	for i, leaf := range leaves {
		label := "add the peer filter"
		if len(leaves) > 1 {
			label = fmt.Sprintf("add peer filter %d of %d", i+1, len(leaves))
		}
		steps = append(steps, netemStep{label, peerFilterArgs(l.Interface, l.handle, leaf, l.match())})
	}
	return steps
}

// runSteps runs the planned commands in order. On failure keep reports
// whether the change may have reached the kernel, so the lease must be
// returned; otherwise nothing changed and the helper can be discarded.
func (l *NetworkProfileLease) runSteps(ctx context.Context, steps []netemStep) (keep bool, err error) {
	for i, step := range steps {
		result, err := l.backend.run(ctx, l.helper, step.label, step.argv...)
		if err != nil {
			// The command may have reached the kernel; only the lease can tell.
			if marker, unconfirmed := supervised.Unconfirmed(err); unconfirmed {
				l.pending = marker
			}
			return true, fmt.Errorf("%s: %w", step.label, err)
		}
		if result.ExitCode == 0 && !result.TimedOut {
			continue
		}
		cause := netemCommandError(step.label, result)
		if i > 0 {
			// The root is the lease's; Restore removes it with the rest.
			return true, cause
		}
		// tc changes nothing when the kernel refuses the request.
		status, statusErr := l.read(ctx)
		if statusErr != nil || status.Applied || status.RootHandle == l.handle {
			return true, errors.Join(cause, statusErr)
		}
		return false, cause
	}
	return false, nil
}

// verifyApplied reads the kernel's qdisc back and requires exactly the
// requested delay, jitter, loss, rate, queue limit and packet faults.
func (l *NetworkProfileLease) verifyApplied(ctx context.Context, delay NetworkProfile) error {
	status, err := l.read(ctx)
	if err != nil {
		return fmt.Errorf("read the netem delay: %w", err)
	}
	// The kernel keeps the loss as a 32-bit probability.
	if !status.Applied || status.Delay != delay.Delay || status.Jitter != delay.Jitter || math.Abs(status.Loss-delay.Loss) > 1e-4 ||
		status.Rate != l.Rate || l.Rate > 0 && status.QueueLimit != l.QueueLimit {
		return errors.Join(fmt.Errorf("the kernel reports root %s %s with delay %v, jitter %v, loss %v%%, rate %v and queue limit %d instead of the requested delay",
			status.RootKind, status.RootHandle, status.Delay, status.Jitter, status.Loss, status.Rate, status.QueueLimit), status.mismatch)
	}
	return l.checkFaults(status)
}

// checkFaults compares the kernel's packet faults and reorder gap with the
// request: a fault that the kernel did not keep, or keeps without being
// asked, is an error.
func (l *NetworkProfileLease) checkFaults(status NetworkProfileStatus) error {
	kernel := map[string]NetworkPacketFault{"reorder": status.Reorder, "duplicate": status.Duplicate, "corrupt": status.Corrupt}
	for _, fault := range (NetworkProfile{Reorder: l.Reorder, Duplicate: l.Duplicate, Corrupt: l.Corrupt}).packetFaults() {
		if !fault.fault.matches(kernel[fault.name]) {
			return fmt.Errorf("the kernel reports %s %v instead of the requested %v", fault.name, kernel[fault.name], fault.fault)
		}
	}
	wantGap := 0
	if !l.Reorder.IsZero() {
		wantGap = 1
	}
	if status.Gap != wantGap {
		return fmt.Errorf("the kernel reports a reorder gap of %d instead of %d", status.Gap, wantGap)
	}
	return nil
}

// admitNetemTarget refuses targets whose namespace the helper cannot join.
func admitNetemTarget(state netemTarget) error {
	switch {
	case !state.running || state.paused:
		return invalidRequest("network delay requires a running, unpaused container")
	case state.networkMode.IsHost() || state.networkMode.IsNone() || state.networkMode.IsContainer():
		return invalidRequest("network delay cannot join a container in network mode %s", state.networkMode)
	case state.sandboxID == "":
		return invalidRequest("the container has no network sandbox")
	case !state.address.Is4() || state.mac == "":
		return invalidRequest("network delay requires an IPv4 endpoint with a MAC address")
	case state.driver != "bridge":
		return invalidRequest("network delay requires a bridge network, not a %q one", state.driver)
	}
	return nil
}

// preflightNetem checks the helper image, NET_ADMIN and the kernel's netem,
// for a Rate also netem's rate in tc and the kernel, for faults (netem
// keywords) those packet faults in tc and the kernel, and for a delay with
// peers also the kernel's prio qdisc and u32 classifier, in a helper without
// a network, and returns the helper's image ID.
func preflightNetem(ctx context.Context, backend netemBackend, image string, peers, rate bool, faults []string, match peerMatch) (string, error) {
	helper, err := backend.startHelper(ctx, image, "none")
	if err != nil {
		return "", errors.Join(faulthook.ClassifyDocker(ctx, fmt.Errorf("start the netem preflight helper: %w", err)), removeNetemHelper(ctx, backend, helper))
	}
	imageID, err := func() (string, error) {
		_, imageID, err := backend.helper(ctx, helper)
		if err != nil {
			return "", fmt.Errorf("inspect the netem preflight helper: %w", err)
		}
		args := []string{"preflight"}
		if rate {
			args = append(args, "rate")
		}
		args = append(args, faults...)
		if peers {
			args = append(args, "peers")
			if match.source {
				args = append(args, "sport")
			}
			if match.setup {
				args = append(args, "setup")
			}
		}
		report, err := runNetemScript(ctx, backend, helper, "check the netem helper", args...)
		if err != nil {
			return "", err
		}
		// The helper's script wrote this report; anything that it leaves out
		// or gets wrong is a helper that does not meet the contract.
		switch {
		case report.Missing == nil:
			return "", fmt.Errorf("%w: the preflight report lists no missing executables", ErrNetemImageUnsupported)
		case len(report.Missing) > 0:
			return "", fmt.Errorf("%w: missing %s", ErrNetemImageUnsupported, strings.Join(report.Missing, ", "))
		case report.Broken == nil:
			return "", fmt.Errorf("%w: the preflight report lists no broken commands", ErrNetemImageUnsupported)
		case len(report.Broken) > 0:
			return "", fmt.Errorf("%w: failed %s", ErrNetemImageUnsupported, strings.Join(report.Broken, ", "))
		case !report.NetAdmin:
			return "", ErrNetemPermissionDenied
		}
		code, text, err := netemResult(report.Add, "add")
		if err != nil {
			return "", err
		}
		if code != 0 {
			return "", classifyNetem("add netem on the helper's loopback", code, text)
		}
		var shown []netemQdisc
		if err := json.Unmarshal([]byte(report.Shown), &shown); err != nil || len(shown) == 0 || shown[0].Kind != "netem" {
			return "", fmt.Errorf("%w: tc did not show the netem qdisc it added", ErrNetemImageUnsupported)
		}
		if code, text, err = netemResult(report.Delete, "delete"); err != nil {
			return "", err
		}
		if code != 0 {
			return "", classifyNetem("remove netem from the helper's loopback", code, text)
		}
		if rate {
			if err := preflightRate(report); err != nil {
				return "", err
			}
		}
		if len(faults) > 0 {
			if err := preflightFaults(report, faults); err != nil {
				return "", err
			}
		}
		if peers {
			if err := preflightPeers(report, match); err != nil {
				return "", err
			}
		}
		return imageID, nil
	}()
	return imageID, errors.Join(err, removeNetemHelper(ctx, backend, helper))
}

// preflightRate checks the preflight's netem rate on the helper's loopback:
// tc must parse netem's rate and limit, and the kernel must keep both.
func preflightRate(report netemReport) error {
	code, text, err := netemResult(report.Rate, "rate")
	if err != nil {
		return err
	}
	if code != 0 {
		return classifyNetem("add a netem rate on the helper's loopback", code, text)
	}
	var shown []netemQdisc
	if err := json.Unmarshal([]byte(report.RateShown), &shown); err != nil {
		return fmt.Errorf("%w: tc did not show the netem rate it added", ErrNetemImageUnsupported)
	}
	i := slices.IndexFunc(shown, func(qdisc netemQdisc) bool { return qdisc.Kind == "netem" && qdisc.Handle == "c7e3:" })
	switch {
	case i < 0:
		return fmt.Errorf("%w: tc did not show the netem rate it added", ErrNetemImageUnsupported)
	case shown[i].Options.Rate.Rate != 125000 || shown[i].Options.Limit != 100:
		return fmt.Errorf("%w: netem kept a rate of %d bytes/s and a limit of %d instead of 125000 and 100", ErrNetemKernelUnsupported,
			shown[i].Options.Rate.Rate, shown[i].Options.Limit)
	}
	if code, text, err = netemResult(report.RateDelete, "rate_delete"); err != nil {
		return err
	}
	if code != 0 {
		return classifyNetem("remove the netem rate from the helper's loopback", code, text)
	}
	return nil
}

// preflightFault is the packet fault that the preflight adds for each
// netem keyword, with a correlation, as netemScript's arguments say.
var preflightFault = map[string]NetworkPacketFault{
	"reorder":   {Percent: 25, Correlation: 50},
	"duplicate": {Percent: 3, Correlation: 10},
	"corrupt":   {Percent: 2, Correlation: 5},
}

// preflightFaults checks the preflight's netem packet faults on the
// helper's loopback: tc must parse each requested fault with its
// correlation, and the kernel must keep each, and for a reorder the gap.
func preflightFaults(report netemReport, faults []string) error {
	code, text, err := netemResult(report.Faults, "faults")
	if err != nil {
		return err
	}
	if code != 0 {
		return classifyNetem("add netem's "+strings.Join(faults, ", ")+" on the helper's loopback", code, text)
	}
	var shown []netemQdisc
	if err := json.Unmarshal([]byte(report.FaultsShown), &shown); err != nil {
		return fmt.Errorf("%w: tc did not show the netem packet faults it added", ErrNetemImageUnsupported)
	}
	i := slices.IndexFunc(shown, func(qdisc netemQdisc) bool { return qdisc.Kind == "netem" && qdisc.Handle == "c7e4:" })
	if i < 0 {
		return fmt.Errorf("%w: tc did not show the netem packet faults it added", ErrNetemImageUnsupported)
	}
	options := shown[i].Options
	kernel := map[string]netemFault{"reorder": options.Reorder, "duplicate": options.Duplicate, "corrupt": options.Corrupt}
	for _, name := range faults {
		if got := kernel[name].fault(name); !preflightFault[name].matches(got) {
			return fmt.Errorf("%w: netem kept %s %v instead of %v", ErrNetemKernelUnsupported, name, got, preflightFault[name])
		}
	}
	if slices.Contains(faults, "reorder") && options.Gap != 1 {
		return fmt.Errorf("%w: netem kept a reorder gap of %d instead of 1", ErrNetemKernelUnsupported, options.Gap)
	}
	if code, text, err = netemResult(report.FaultsDelete, "faults_delete"); err != nil {
		return err
	}
	if code != 0 {
		return classifyNetem("remove the netem packet faults from the helper's loopback", code, text)
	}
	return nil
}

// preflightPeers checks the preflight's prio root and u32 filter on the
// helper's loopback: what a delay with peers adds above netem.
func preflightPeers(report netemReport, match peerMatch) error {
	code, text, err := netemResult(report.Prio, "prio")
	if err != nil {
		return err
	}
	if code != 0 {
		return classifyNetem("add a prio qdisc on the helper's loopback", code, text)
	}
	if code, text, err = netemResult(report.U32, "u32"); err != nil {
		return err
	}
	if code != 0 {
		return classifyNetem("add a u32 filter on the helper's loopback", code, text)
	}
	var shown []netemFilter
	if err := json.Unmarshal([]byte(report.U32Shown), &shown); err != nil {
		return fmt.Errorf("%w: tc did not show the u32 filter it added", ErrNetemImageUnsupported)
	}
	i := slices.IndexFunc(shown, func(filter netemFilter) bool { return filter.Kind == "u32" && filter.Options.FlowID == "c7e2:3" })
	if i < 0 {
		return fmt.Errorf("%w: tc did not show the u32 filter it added", ErrNetemImageUnsupported)
	}
	if match.source || match.setup {
		// The source port and SYN keys must reach the kernel as the
		// lease will read them back.
		var port uint16 = 7000
		var got []u32Key
		for _, key := range shown[i].Options.Matches {
			value, err1 := strconv.ParseUint(key.Value, 16, 32)
			mask, err2 := strconv.ParseUint(key.Mask, 16, 32)
			if err1 != nil || err2 != nil || key.OffMask != "" {
				return fmt.Errorf("%w: tc printed the u32 key %+v", ErrNetemImageUnsupported, key)
			}
			got = append(got, u32Key{Off: key.Off, Value: uint32(value), Mask: uint32(mask)})
		}
		if want := peerFilterKeys(netip.MustParseAddr("127.0.0.2"), port, match); !slices.Equal(got, want) {
			return fmt.Errorf("%w: the u32 filter kept the keys %v instead of %v", ErrNetemKernelUnsupported, got, want)
		}
	}
	if code, text, err = netemResult(report.PrioDelete, "prio_delete"); err != nil {
		return err
	}
	if code != 0 {
		return classifyNetem("remove the prio qdisc from the helper's loopback", code, text)
	}
	return nil
}

// netemResult reads one [exit code, stderr] pair of the preflight report.
func netemResult(value []any, name string) (int, string, error) {
	malformed := fmt.Errorf("%w: the preflight report has no valid %s result", ErrNetemImageUnsupported, name)
	if len(value) != 2 {
		return 0, "", malformed
	}
	code, isNumber := value[0].(float64)
	text, isText := value[1].(string)
	if !isNumber || !isText || code != math.Trunc(code) || code < 0 || code > 255 {
		return 0, "", malformed
	}
	return int(code), text, nil
}

// runNetemScript runs netemScript in helper.
func runNetemScript(ctx context.Context, backend netemBackend, helper testcontainers.Container, label string, args ...string) (netemReport, error) {
	result, err := backend.run(ctx, helper, label, append([]string{"python3", "-c", netemScript}, args...)...)
	var exit *supervised.ExitError
	if errors.As(err, &exit) && (exit.Code == 126 || exit.Code == 127) {
		return netemReport{}, fmt.Errorf("%w: it cannot run python3", ErrNetemImageUnsupported)
	}
	if err != nil {
		return netemReport{}, fmt.Errorf("%s: %w", label, err)
	}
	if result.TimedOut {
		// Too slow says nothing about the image.
		return netemReport{}, fmt.Errorf("%s timed out", label)
	}
	if result.ExitCode != 0 || result.StdoutTruncated {
		// The contract's script failed: the image, whatever tc reported.
		return netemReport{}, fmt.Errorf("%w: %v", ErrNetemImageUnsupported, netemCommandError(label, result))
	}
	var report netemReport
	if err := json.Unmarshal(result.Stdout, &report); err != nil {
		return netemReport{}, fmt.Errorf("%s: parse its report: %w", label, err)
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
// selecting the interface as netemScript's args say.
func inspectNetem(ctx context.Context, backend netemBackend, helper testcontainers.Container, args ...string) (netemReport, error) {
	report, err := runNetemScript(ctx, backend, helper, "inspect the delayed interface", args...)
	if err == nil && report.Error != "" {
		err = fmt.Errorf("inspect the delayed interface: %s", report.Error)
	}
	return report, err
}

func netemCommandError(label string, result supervised.Report) error {
	if result.TimedOut {
		return fmt.Errorf("%s timed out", label)
	}
	return classifyNetem(label, result.ExitCode, strings.TrimSpace(string(result.Stderr)))
}

// classifyNetem turns tc's exit code and error text into the error kinds of
// the contract, or a *ceph.NativeFailure of tc.
func classifyNetem(label string, code int, text string) error {
	switch {
	case strings.Contains(text, "Operation not permitted"):
		return fmt.Errorf("%s: %w", label, ErrNetemPermissionDenied)
	case strings.Contains(text, "Specified qdisc kind is unknown"):
		return fmt.Errorf("%s: %w: no such qdisc", label, ErrNetemKernelUnsupported)
	case strings.Contains(text, "TC classifier not found"):
		return fmt.Errorf("%s: %w: no such classifier", label, ErrNetemKernelUnsupported)
	case strings.Contains(text, "Unknown qdisc") || strings.Contains(text, "Unknown filter") || strings.Contains(text, "unparsable") ||
		strings.Contains(text, `What is "`):
		return fmt.Errorf("%s: %w: tc cannot parse it", label, ErrNetemImageUnsupported)
	}
	return fmt.Errorf("%s: %w", label, faulthook.NativeFailure("tc", code, nil, []byte(text)))
}

// removeNetemHelper removes helper, also after ctx ended: a helper of a
// canceled call has no lease that could remove it later. One it cannot
// remove keeps the cluster's label, so Terminate removes it.
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

// Status reads the delayed interface's qdiscs through the helper. After the
// target restarted, stopped, was removed or left the bridge, it reports Gone.
// With peers it reports Applied only when the root, the netem child and
// exactly the lease's filters, with the requested address and port keys,
// are present and nothing else is under the root; Filters holds tc's filter
// dump with each u32 filter's keys under matches. The counters are the
// child's, so they count only selected packets.
//
//purpose:observation
func (l *NetworkProfileLease) Status(ctx context.Context) (NetworkProfileStatus, error) {
	if err := lockMutex(ctx, &l.mu); err != nil {
		return NetworkProfileStatus{}, err
	}
	defer l.mu.Unlock()
	if l.complete {
		return NetworkProfileStatus{}, invalidRequest("the network delay was restored")
	}
	if !l.shapes {
		return NetworkProfileStatus{}, invalidRequest("an Inbound delay shapes only its peers' endpoints: read the Status of each lease in PeerLeases")
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
		if err := l.backend.cancel(ctx, l.helper, "netem delay", l.pending); err != nil {
			return fmt.Errorf("confirm that the earlier tc command stopped: %w", err)
		}
		l.pending = ""
	}
	if l.helper != nil {
		return nil
	}
	state, err := l.backend.target(ctx, l.ContainerID, l.NetworkName)
	if errdefs.IsNotFound(err) || err == nil && (!state.running || state.sandboxID != l.SandboxID) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect the network delay target: %w", err)
	}
	// The validated image, by ID: its tag may now name another image.
	helper, err := startNetemHelper(ctx, l.backend, l.HelperImageID, container.NetworkMode("container:"+l.ContainerID))
	if err != nil {
		return fmt.Errorf("replace the netem helper: %w", err)
	}
	l.helper = helper
	return nil
}

// read reports the delayed interface's state; the caller holds l.mu and has
// settled the lease. Without a helper the namespace no longer exists.
func (l *NetworkProfileLease) read(ctx context.Context) (NetworkProfileStatus, error) {
	if l.helper == nil {
		return NetworkProfileStatus{Gone: true}, nil
	}
	// The interface is found by its index: its address, MAC and name may
	// change, but a reconnect creates a new interface with a new index.
	report, err := inspectNetem(ctx, l.backend, l.helper, "index", strconv.Itoa(l.InterfaceIndex))
	if err != nil {
		return NetworkProfileStatus{}, err
	}
	// Another namespace, or no interface with that index, is not the delayed
	// interface; its qdiscs are not the lease's.
	if report.Namespace != l.Namespace || len(report.Links) != 1 {
		return NetworkProfileStatus{Gone: true}, nil
	}
	root, err := report.root()
	if err != nil {
		return NetworkProfileStatus{}, err
	}
	status := NetworkProfileStatus{Interface: report.Links[0].Name, RootKind: root.Kind, RootHandle: root.Handle, Qdiscs: report.Qdiscs, Filters: report.Filters}
	netem := root
	switch {
	case len(l.Peers) == 0:
		if root.Kind == "netem" && root.Handle == l.handle {
			status.mismatch = l.ownedPrefix(status)
			status.Applied = status.mismatch == nil
		}
	case root.Kind == "prio" && root.Handle == l.handle:
		// The delay is applied only as the whole hierarchy: the root, its
		// netem child in the selected band, and the one filter to it.
		var qdiscs []netemQdisc
		if err := json.Unmarshal(report.Qdiscs, &qdiscs); err != nil {
			return NetworkProfileStatus{}, fmt.Errorf("parse tc qdisc output: %w", err)
		}
		children := 0
		for _, qdisc := range qdiscs {
			if qdisc.Kind == "netem" && qdisc.Handle == l.child && qdisc.Parent == l.handle+peerBand {
				netem, children = qdisc, children+1
			}
		}
		status.mismatch = checkPeerFilter(report.Filters, l.handle, l.Peers, l.match())
		if children != 1 {
			status.mismatch = errors.Join(status.mismatch, fmt.Errorf("the prio root has no netem child %s in band %s", l.child, l.handle+peerBand))
		} else if err := l.ownedPrefix(status); status.mismatch == nil && err != nil {
			status.mismatch = errors.Join(status.mismatch, err)
		}
		status.Applied = status.mismatch == nil
	}
	if status.Applied {
		status.Delay, status.Jitter = seconds(netem.Options.Delay.Delay), seconds(netem.Options.Delay.Jitter)
		status.Loss = math.Round(netem.Options.Loss.Loss*100*1e6) / 1e6
		status.Packets, status.Bytes, status.Drops = netem.Packets, netem.Bytes, netem.Drops
		status.Overlimits, status.Requeues, status.Backlog, status.QueueLength = netem.Overlimits, netem.Requeues, netem.Backlog, netem.Qlen
		status.Rate, status.QueueLimit = NetworkRate(netem.Options.Rate.Rate)*8, netem.Options.Limit
		status.Reorder, status.Duplicate = netem.Options.Reorder.fault("reorder"), netem.Options.Duplicate.fault("duplicate")
		status.Corrupt, status.Gap = netem.Options.Corrupt.fault("corrupt"), int(netem.Options.Gap)
	}
	return status, nil
}

// ownedPrefix checks that the qdiscs and filters under the lease's root are
// a prefix of what applying the delay adds: the root, and for a selective
// delay its netem child in the selected band and then the one filter.
// Removing the root removes everything under it, so anything else there is
// foreign; qdiscs outside the root, such as an ingress qdisc, are not.
func (l *NetworkProfileLease) ownedPrefix(status NetworkProfileStatus) error {
	var qdiscs []netemQdisc
	if err := json.Unmarshal(status.Qdiscs, &qdiscs); err != nil {
		return fmt.Errorf("parse tc qdisc output: %w", err)
	}
	rootKind := "netem"
	if len(l.Peers) > 0 {
		rootKind = "prio"
	}
	// A qdisc is under the root when its parent is the root or a qdisc under
	// it; tc does not promise to list a parent first.
	under := map[string]bool{}
	for changed := true; changed; {
		changed = false
		for _, qdisc := range qdiscs {
			major, _, _ := strings.Cut(qdisc.Handle, ":")
			parent, _, _ := strings.Cut(qdisc.Parent, ":")
			if !under[major] && (qdisc.Root || under[parent]) {
				under[major], changed = true, true
			}
		}
	}
	for _, qdisc := range qdiscs {
		if major, _, _ := strings.Cut(qdisc.Handle, ":"); !under[major] {
			continue
		}
		switch {
		case qdisc.Root && qdisc.Kind == rootKind && qdisc.Handle == l.handle:
		case len(l.Peers) > 0 && !qdisc.Root && qdisc.Kind == "netem" && qdisc.Handle == l.child && qdisc.Parent == l.handle+peerBand:
		case qdisc.Root:
			return fmt.Errorf("the root is %s %s, not the delay's %s", qdisc.Kind, qdisc.Handle, rootKind)
		default:
			return fmt.Errorf("a %s qdisc %s under %s", qdisc.Kind, qdisc.Handle, qdisc.Parent)
		}
	}
	if len(l.Peers) > 0 {
		return checkPeerFilters(status.Filters, l.handle, l.Peers, l.match(), false)
	}
	var filters []netemFilter
	if err := json.Unmarshal(status.Filters, &filters); err != nil {
		return fmt.Errorf("parse tc filter output: %w", err)
	}
	if len(filters) > 0 {
		return fmt.Errorf("%d filters under the root", len(filters))
	}
	return nil
}

// Restore removes the delay's qdisc and then the helper, after restoring
// every lease in PeerLeases. A delay whose interface is gone needs no removal. A
// root qdisc that someone replaced, or a filter or qdisc that someone added
// under the lease's root, is refused, not removed, and the lease stays
// retryable.
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
	// Each peer has its own mutex, so holding l.mu keeps PeerLeases stable
	// without nesting the same lock.
	var inbound error
	for _, peer := range l.PeerLeases {
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
		// The lease's root goes, also when a selective delay is incomplete,
		// only while everything under it is what the apply added: removing
		// the root removes all of it, so foreign state there is refused even
		// when the delay itself is still applied.
		if status.RootHandle == l.handle {
			if err := l.ownedPrefix(status); err != nil {
				return invalidRequest("the delayed interface's root %s carries state that the lease did not add (%v); remove it, then Restore again", l.handle, err)
			}
			result, err := l.backend.run(ctx, l.helper, "remove the netem delay", "tc", "qdisc", "del", "dev", status.Interface, "root", "handle", l.handle)
			if err != nil {
				if marker, unconfirmed := supervised.Unconfirmed(err); unconfirmed {
					l.pending = marker
				}
				return fmt.Errorf("remove the netem delay: %w", err)
			}
			if result.ExitCode != 0 || result.TimedOut {
				return netemCommandError("remove the netem delay", result)
			}
			if status, err = l.read(ctx); err != nil {
				return err
			}
		}
		if !status.Gone && status.RootHandle != "0:" {
			return invalidRequest("the delayed interface's root qdisc is now %s %s, which the lease does not own", status.RootKind, status.RootHandle)
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

func (dockerNetemBackend) target(ctx context.Context, id, network string) (netemTarget, error) {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return netemTarget{}, err
	}
	defer func() { _ = docker.Close() }()
	inspection, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return netemTarget{}, err
	}
	var state netemTarget
	if current := inspection.Container.State; current != nil {
		state.running, state.paused = current.Running && !current.Restarting, current.Paused
	}
	if inspection.Container.HostConfig != nil {
		state.networkMode = inspection.Container.HostConfig.NetworkMode
	}
	if settings := inspection.Container.NetworkSettings; settings != nil {
		state.sandboxID = settings.SandboxID
		if endpoint := settings.Networks[network]; endpoint != nil {
			state.address, state.address6, state.mac = endpoint.IPAddress, endpoint.GlobalIPv6Address, endpoint.MacAddress.String()
			inspection, err := docker.NetworkInspect(ctx, network, mobycl.NetworkInspectOptions{})
			if err != nil {
				return netemTarget{}, fmt.Errorf("inspect network %s: %w", network, err)
			}
			state.driver = inspection.Network.Driver
		}
	}
	return state, nil
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
	return []testcontainers.ContainerCustomizer{
		ceph.WithIdleEntrypoint(),
		testcontainers.WithConfigModifier(func(config *container.Config) { config.User = "0:0" }),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.NetworkMode = networkMode
			host.CapDrop = []string{"ALL"}
			host.CapAdd = []string{"NET_ADMIN"}
		}),
		testcontainers.WithLabels(map[string]string{"org.testcontainers.ceph.role": "netem-helper"}),
	}
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
