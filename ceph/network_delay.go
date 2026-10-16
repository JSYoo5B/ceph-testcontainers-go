package ceph

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

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// NetemHelperImage is the netem helper that ceph-testcontainers-images
// publishes for contract 1 of its netem extension, for linux/amd64 and
// linux/arm64. TemporaryNetworkDelay never selects it by itself: pass it, or
// another image that meets the contract, as NetworkDelay.HelperImage.
const NetemHelperImage = "ghcr.io/jsyoo5b/ceph-testcontainers-images:extension-netem-v1"

// MaxNetworkDelay bounds NetworkDelay.Delay.
const MaxNetworkDelay = 10 * time.Second

// netemCommandTimeout bounds each command in a netem helper.
const netemCommandTimeout = 20 * time.Second

var (
	// ErrNetemImageUnsupported reports a helper image that does not meet the
	// netem contract: a missing or broken tc, ip, sh, sleep or python3, or a
	// tc without netem. It belongs to ErrImageCapability.
	ErrNetemImageUnsupported error = &categoryError{text: "the netem helper image does not meet the netem contract", kind: ErrImageCapability}
	// ErrNetemPermissionDenied reports an engine that did not give the helper
	// NET_ADMIN, or refused a qdisc change for lack of it. It belongs to
	// ErrEnvironmentUnavailable.
	ErrNetemPermissionDenied error = &categoryError{text: "the netem helper has no NET_ADMIN", kind: ErrEnvironmentUnavailable}
	// ErrNetemKernelUnsupported reports a kernel without the netem qdisc. It
	// belongs to ErrEnvironmentUnavailable.
	ErrNetemKernelUnsupported error = &categoryError{text: "the engine's kernel has no netem qdisc", kind: ErrEnvironmentUnavailable}
)

// NetworkDelay configures TemporaryNetworkDelay.
type NetworkDelay struct {
	// HelperImage is the netem helper image: NetemHelperImage, or another image
	// that meets contract 1 of the netem extension. Required.
	HelperImage string
	// Plane selects the bridge whose endpoint of the target is delayed.
	Plane NetworkPlane
	// Delay is added to every packet that leaves the target through that
	// endpoint: at least 1ms and at most MaxNetworkDelay, in whole
	// microseconds. With a Loss it may be zero.
	Delay time.Duration
	// Jitter varies each packet's delay uniformly within ±Jitter. It is at
	// most Delay, in whole microseconds.
	Jitter time.Duration
	// Peer, when set, limits the delay to the packets that the target sends
	// to this address through the endpoint; every other packet leaves
	// without delay. It is an IPv4 address, or an IPv6 address for an
	// endpoint that has one, such as a daemon's address from
	// DaemonAddresses on the same plane. Only packets that leave the target
	// are selected: to delay the other direction, delay the peer's endpoint
	// with the target's address.
	Peer netip.Addr
	// PeerPort, with Peer, limits the delay further to TCP packets to this
	// destination port, such as one of the peer's Messenger ports. Zero
	// selects every packet to Peer. An IPv6 packet with extension headers
	// before its TCP header is not selected.
	PeerPort uint16
	// Loss drops this percentage of the packets that the delay applies to,
	// at random and independently, from more than 0 to 100. With 100 every
	// selected packet is dropped: with a Peer, the target's packets to that
	// peer, and only those, never arrive. Zero drops nothing.
	Loss float64
}

// NetworkDelayStatus is what the kernel reports for a delay's interface,
// read through the helper. It is native state, not the requested NetworkDelay.
type NetworkDelayStatus struct {
	// Applied means that the delay's own netem qdisc is the root qdisc of the
	// interface it was applied to (with a Peer, that its own hierarchy is),
	// and that nothing the lease did not add is under that root.
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
	// Packets, Bytes and Drops are the netem qdisc's counters while Applied.
	// With a Peer they count only the selected packets.
	Packets, Bytes, Drops uint64
	// Qdiscs is tc's JSON output for the interface.
	Qdiscs json.RawMessage
	// Filters is tc's egress filter output for the interface, with the keys
	// of each u32 filter under "matches".
	Filters json.RawMessage

	// mismatch says why the lease's own root does not hold exactly the
	// requested hierarchy, with nothing foreign under it.
	mismatch error
}

// NetworkDelayLease owns one netem delay on a container's network endpoint,
// applied from a helper container in the target's network namespace that
// alone has NET_ADMIN. Restore removes the delay and the helper. A non-nil
// lease returned with an error must be restored: the change may have been
// applied. Restore is retryable.
type NetworkDelayLease struct {
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
	// Peer and PeerPort are the requested selection; an invalid Peer means
	// the whole interface.
	Peer     netip.Addr
	PeerPort uint16
	// Loss is the requested loss percentage.
	Loss float64

	mu      sync.Mutex
	backend netemBackend
	helper  testcontainers.Container
	// handle is the root qdisc that the lease owns: the netem qdisc, or with
	// a Peer the prio qdisc whose third band leads to the netem child.
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
	address         netip.Addr
	address6        netip.Addr
	mac             string
}

// netemBackend runs the Docker and helper operations of a delay; tests
// replace it with a model of namespaces and qdiscs.
type netemBackend interface {
	// target inspects id and its endpoint on network. A removed container
	// returns an error for which errdefs.IsNotFound is true.
	target(ctx context.Context, id, network string) (netemTarget, error)
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
// namespace; "inspect ADDRESS MAC" finds the interface with that address and
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
	} `json:"options"`
	Bytes   uint64 `json:"bytes"`
	Packets uint64 `json:"packets"`
	Drops   uint64 `json:"drops"`
}

type netemReport struct {
	Namespace string          `json:"namespace"`
	NetAdmin  bool            `json:"net_admin"`
	Missing   []string        `json:"missing"`
	Broken    []string        `json:"broken"`
	Add       []any           `json:"add"`
	Shown     string          `json:"shown"`
	Delete    []any           `json:"delete"`
	Error     string          `json:"error"`
	Qdiscs    json.RawMessage `json:"qdiscs"`
	Filters   json.RawMessage `json:"filters"`
	Links     []struct {
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

// validateNetworkDelay checks a NetworkDelay before anything runs.
func validateNetworkDelay(delay NetworkDelay) error {
	if strings.TrimSpace(delay.HelperImage) == "" {
		// The extension is not selected.
		return &OperationError{Kind: ErrImageCapability, Capability: "netem extension",
			Err: errors.New("network delay needs NetworkDelay.HelperImage: the netem helper is never selected implicitly")}
	}
	// Every check runs: allowing a zero delay with a Loss must not skip the
	// selection's checks.
	lossOnly := delay.Loss > 0 && delay.Delay == 0
	switch {
	case delay.Plane != PublicNetworkPlane && delay.Plane != ClusterNetworkPlane:
		return invalidRequest("invalid Ceph network plane")
	case math.IsNaN(delay.Loss) || delay.Loss < 0 || delay.Loss > 100:
		return invalidRequest("network delay Loss must be a percentage from 0 to 100")
	case !lossOnly && (delay.Delay < time.Millisecond || delay.Delay > MaxNetworkDelay):
		return invalidRequest("network delay must be between 1ms and %v, or zero with a Loss", MaxNetworkDelay)
	case delay.Jitter < 0 || delay.Jitter > delay.Delay:
		return invalidRequest("network delay jitter must be between 0 and the delay")
	case delay.Delay%time.Microsecond != 0 || delay.Jitter%time.Microsecond != 0:
		return invalidRequest("network delay and jitter must be whole microseconds")
	case delay.PeerPort != 0 && !delay.Peer.IsValid():
		return invalidRequest("network delay PeerPort needs a Peer")
	case delay.Peer.IsValid() && (delay.Peer.Is4In6() || delay.Peer.Zone() != "" || delay.Peer.IsUnspecified()):
		return invalidRequest("network delay Peer %s is not a plain IPv4 or IPv6 address", delay.Peer)
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

// peerFilterPref and peerFilterHandle name the selective delay's one u32
// filter: its preference and its first table's first key node.
const (
	peerFilterPref   = "10"
	peerFilterHandle = "800::1"
)

// u32Key is one 32-bit match of a u32 filter, at a byte offset of the IP
// header, as tc prints it.
type u32Key struct {
	Off   int
	Value uint32
	Mask  uint32
}

// peerFilterKeys are the u32 keys that select packets to peer and, when port
// is not zero, TCP packets to that port. tc aligns every key to a 32-bit word:
// the IPv4 protocol is at offset 8, the IPv6 next header at offset 4, and the
// destination port is the low half of the word at offset 20 (IPv4) or 40
// (IPv6), right after a header without options or extension headers.
func peerFilterKeys(peer netip.Addr, port uint16) []u32Key {
	var keys []u32Key
	address := peer.AsSlice()
	if peer.Is4() {
		keys = append(keys, u32Key{Off: 16, Value: binary.BigEndian.Uint32(address), Mask: 0xffffffff})
		if port != 0 {
			keys = append(keys, u32Key{Off: 8, Value: 6 << 16, Mask: 0xff << 16}, u32Key{Off: 20, Value: uint32(port), Mask: 0xffff})
		}
		return keys
	}
	for word := range 4 {
		keys = append(keys, u32Key{Off: 24 + 4*word, Value: binary.BigEndian.Uint32(address[4*word:]), Mask: 0xffffffff})
	}
	if port != 0 {
		keys = append(keys, u32Key{Off: 4, Value: 6 << 8, Mask: 0xff << 8}, u32Key{Off: 40, Value: uint32(port), Mask: 0xffff})
	}
	return keys
}

// peerFilterArgs is the tc command that adds the selective delay's filter.
func peerFilterArgs(device, root string, peer netip.Addr, port uint16) []string {
	protocol, family, bits := "ip", "ip", "/32"
	if peer.Is6() {
		protocol, family, bits = "ipv6", "ip6", "/128"
	}
	argv := []string{"tc", "filter", "add", "dev", device, "parent", root, "protocol", protocol, "pref", peerFilterPref,
		"handle", peerFilterHandle, "u32", "match", family, "dst", peer.String() + bits}
	if port != 0 {
		argv = append(argv, "match", family, "protocol", "6", "0xff", "match", family, "dport", strconv.Itoa(int(port)), "0xffff")
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
// exactly the selective delay's one filter under root, or nil. Header
// entries, which tc prints for the filter's chain and its u32 table, may
// accompany the filter; any other filter is foreign state.
func checkPeerFilter(data json.RawMessage, root string, peer netip.Addr, port uint16) error {
	return checkPeerFilters(data, root, peer, port, true)
}

// checkPeerFilters is checkPeerFilter that, unless required, also accepts no
// filter with keys: the state of an apply that stopped before its filter.
func checkPeerFilters(data json.RawMessage, root string, peer netip.Addr, port uint16, required bool) error {
	var filters []netemFilter
	if err := json.Unmarshal(data, &filters); err != nil {
		return fmt.Errorf("parse tc filter output: %w", err)
	}
	protocol := "ip"
	if peer.Is6() {
		protocol = "ipv6"
	}
	var leaves []netemFilter
	for _, filter := range filters {
		if filter.Parent != root || filter.Protocol != protocol || strconv.Itoa(filter.Pref) != peerFilterPref || filter.Kind != "u32" {
			return fmt.Errorf("a filter with parent %s, protocol %s, preference %d and kind %s is not the delay's", filter.Parent, filter.Protocol, filter.Pref, filter.Kind)
		}
		if len(filter.Options.Matches) > 0 || filter.Options.FlowID != "" {
			leaves = append(leaves, filter)
		}
	}
	if len(leaves) == 0 && !required {
		return nil
	}
	if len(leaves) != 1 {
		return fmt.Errorf("the interface has %d u32 filters with keys, not the delay's one", len(leaves))
	}
	leaf := leaves[0]
	if leaf.Options.Handle != peerFilterHandle || leaf.Options.FlowID != root+peerBand {
		return fmt.Errorf("the filter %s leads to %s, not %s to %s", leaf.Options.Handle, leaf.Options.FlowID, peerFilterHandle, root+peerBand)
	}
	var got []u32Key
	for _, match := range leaf.Options.Matches {
		value, err1 := strconv.ParseUint(match.Value, 16, 32)
		mask, err2 := strconv.ParseUint(match.Mask, 16, 32)
		if err1 != nil || err2 != nil || match.OffMask != "" {
			return fmt.Errorf("the filter has a key tc printed as %+v", match)
		}
		got = append(got, u32Key{Off: match.Off, Value: uint32(value), Mask: uint32(mask)})
	}
	if want := peerFilterKeys(peer, port); !slices.Equal(got, want) {
		return fmt.Errorf("the filter's keys %v are not %v", got, want)
	}
	return nil
}

// TemporaryNetworkDelay delays every packet that target sends through its
// endpoint on the plane's bridge, below Ceph, with the kernel's netem qdisc.
// It is an opt-in extension: delay.HelperImage names the netem helper, which
// runs in the target's network namespace with NET_ADMIN, while the target
// keeps its own image and capabilities. The target is an owned daemon or a
// caller-owned container attached to that bridge; host-network clusters,
// stopped or paused targets and targets that share another container's
// network namespace are refused.
//
// Before it changes anything, it checks the helper image against the netem
// contract, NET_ADMIN and the kernel's netem in a separate helper without a
// network, and returns ErrNetemImageUnsupported, ErrNetemPermissionDenied or
// ErrNetemKernelUnsupported. It refuses an interface whose root qdisc is not
// the kernel's default and a target that already has a delay on that bridge.
// A missing HelperImage is ErrImageCapability, the refusals are
// ErrInvalidRequest, and a tc command that fails otherwise is a
// *NativeFailure.
//
// The delay lasts until Restore, or until the target stops or leaves the
// bridge, which removes its interface. Terminate restores delays that are
// still held; restore a caller-owned target's delay before removing it.
func (c *Container) TemporaryNetworkDelay(ctx context.Context, target testcontainers.Container, delay NetworkDelay) (*NetworkDelayLease, error) {
	if err := validateNetworkDelay(delay); err != nil {
		return nil, err
	}
	if target == nil || target.GetContainerID() == "" {
		return nil, invalidRequest("network delay requires a container")
	}
	id := target.GetContainerID()
	name, key, err := c.reserveNetworkDelay(ctx, target, delay.Plane)
	if err != nil {
		return nil, err
	}
	lease, err := applyNetworkDelay(ctx, c.netemBackend(), id, name, delay)
	if closed := c.settleNetworkDelay(key, lease); closed && lease != nil {
		// Terminate ran meanwhile and did not see this delay.
		return lease, errors.Join(err, errNativeTerminated, lease.Restore(ctx))
	}
	return lease, err
}

// reserveNetworkDelay admits target for a delay on plane and reserves its
// endpoint until settleNetworkDelay.
func (c *Container) reserveNetworkDelay(ctx context.Context, target testcontainers.Container, plane NetworkPlane) (string, string, error) {
	if err := c.lockTopology(ctx); err != nil {
		return "", "", err
	}
	closed, hostNetwork := c.closed, c.UsesHostNetwork()
	name := c.NetworkName()
	if plane == ClusterNetworkPlane {
		name = c.ClusterNetworkName()
	}
	owned := c.ownsNativeTarget(target.GetContainerID())
	c.mu.Unlock()
	switch {
	case closed:
		return "", "", errClusterTerminated
	case hostNetwork:
		return "", "", invalidRequest("network delay requires a bridge cluster: a delay on a host network would delay the host")
	case name == "":
		return "", "", invalidRequest("Ceph bridge is not initialized")
	}
	if !owned {
		attached, err := target.Networks(ctx)
		if err != nil {
			return "", "", classifyDocker(ctx, fmt.Errorf("inspect the container's networks: %w", err))
		}
		if !slices.Contains(attached, name) {
			return "", "", invalidRequest("the container is not attached to Ceph network %s", name)
		}
	}
	key := name + "/" + target.GetContainerID()
	if err := c.lockTopology(ctx); err != nil {
		return "", "", err
	}
	defer c.mu.Unlock()
	if c.closed {
		return "", "", errClusterTerminated
	}
	// A completed lease leaves c.delays, so a held one still needs Restore.
	// Its mutex is not taken here: Restore holds it while it takes c.mu.
	if previous, held := c.delays[key]; held {
		if previous == nil {
			return "", "", invalidRequest("another network delay of this endpoint is being applied")
		}
		return "", "", invalidRequest("restore the existing network delay of this endpoint first")
	}
	if c.delays == nil {
		c.delays = make(map[string]*NetworkDelayLease)
	}
	c.delays[key] = nil
	return name, key, nil
}

// settleNetworkDelay keeps a returned lease for Terminate, or frees the
// reservation when there is none. It reports whether Terminate has started.
func (c *Container) settleNetworkDelay(key string, lease *NetworkDelayLease) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if lease == nil {
		delete(c.delays, key)
		return c.closed
	}
	c.delays[key] = lease
	lease.release = func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.delays[key] == lease {
			delete(c.delays, key)
		}
	}
	return c.closed
}

// applyNetworkDelay checks the helper and the target, then applies the delay.
// It returns a lease whenever the change may have reached the kernel.
func applyNetworkDelay(ctx context.Context, backend netemBackend, id, network string, delay NetworkDelay) (*NetworkDelayLease, error) {
	state, err := backend.target(ctx, id, network)
	if err != nil {
		return nil, classifyDocker(ctx, fmt.Errorf("inspect the network delay target: %w", err))
	}
	if err := admitNetemTarget(state); err != nil {
		return nil, err
	}
	if delay.Peer.Is6() && !state.address6.IsValid() {
		return nil, invalidRequest("network delay Peer %s is IPv6, and the target has no IPv6 address on %s", delay.Peer, network)
	}
	imageID, err := preflightNetem(ctx, backend, delay.HelperImage)
	if err != nil {
		return nil, err
	}
	// The checked image's ID, not its tag, which may move.
	helper, err := startNetemHelper(ctx, backend, imageID, container.NetworkMode("container:"+id))
	if err != nil {
		return nil, err
	}
	lease := &NetworkDelayLease{ContainerID: id, NetworkName: network, SandboxID: state.sandboxID,
		MACAddress: state.mac, Address: state.address, HelperImageID: imageID, Delay: delay.Delay,
		Jitter: delay.Jitter, Peer: delay.Peer, PeerPort: delay.PeerPort, Loss: delay.Loss, backend: backend, helper: helper}
	discard := func(err error) (*NetworkDelayLease, error) {
		return nil, errors.Join(err, removeNetemHelper(ctx, backend, helper))
	}
	report, err := inspectNetem(ctx, backend, helper, "inspect", state.address.String(), state.mac)
	if err != nil {
		return discard(err)
	}
	if len(report.Links) != 1 {
		return discard(fmt.Errorf("found %d interfaces with the target's address and MAC on %s", len(report.Links), network))
	}
	root, err := report.root()
	if err != nil {
		return discard(err)
	}
	if root.Handle != "0:" {
		return discard(invalidRequest("the target's interface has a %s root qdisc (handle %s) that the delay would replace", root.Kind, root.Handle))
	}
	lease.Namespace, lease.Interface, lease.InterfaceIndex = report.Namespace, report.Links[0].Name, report.Links[0].Index
	// A restart between the first inspection and the helper's start would
	// have given the helper a namespace without this endpoint.
	if again, err := backend.target(ctx, id, network); err != nil || again.sandboxID != state.sandboxID {
		return discard(errors.Join(errors.New("the network delay target restarted while it was being checked"), err))
	}
	lease.handle = fmt.Sprintf("%x:", 0x1000+rand.IntN(0xe000))
	netem := []string{"netem"}
	if delay.Delay > 0 {
		netem = append(netem, "delay", fmt.Sprintf("%dus", delay.Delay.Microseconds()))
		if delay.Jitter > 0 {
			netem = append(netem, fmt.Sprintf("%dus", delay.Jitter.Microseconds()))
		}
	}
	if delay.Loss > 0 {
		netem = append(netem, "loss", strconv.FormatFloat(delay.Loss, 'f', -1, 64)+"%")
	}
	steps := []netemStep{{"add the netem delay", append([]string{"tc", "qdisc", "add", "dev", lease.Interface, "root", "handle", lease.handle}, netem...)}}
	if delay.Peer.IsValid() {
		// The prio root first, with every priority in band 1, so nothing is
		// delayed until the filter, added last, selects the peer's packets.
		lease.child = fmt.Sprintf("%x:", 0x1000+rand.IntN(0xe000))
		for lease.child == lease.handle {
			lease.child = fmt.Sprintf("%x:", 0x1000+rand.IntN(0xe000))
		}
		priomap := slices.Repeat([]string{"0"}, 16)
		steps = []netemStep{
			{"add the delay's prio root", append([]string{"tc", "qdisc", "add", "dev", lease.Interface, "root", "handle", lease.handle, "prio", "bands", "3", "priomap"}, priomap...)},
			{"add the netem delay", append([]string{"tc", "qdisc", "add", "dev", lease.Interface, "parent", lease.handle + peerBand, "handle", lease.child}, netem...)},
			{"add the peer filter", peerFilterArgs(lease.Interface, lease.handle, delay.Peer, delay.PeerPort)},
		}
	}
	for i, step := range steps {
		result, err := backend.run(ctx, helper, step.label, step.argv...)
		if err != nil {
			// The command may have reached the kernel; only the lease can tell.
			if marker, unconfirmed := supervised.Unconfirmed(err); unconfirmed {
				lease.pending = marker
			}
			return lease, fmt.Errorf("%s: %w", step.label, err)
		}
		if result.ExitCode != 0 || result.TimedOut {
			cause := netemCommandError(step.label, result)
			if i > 0 {
				// The root is the lease's; Restore removes it with the rest.
				return lease, cause
			}
			// tc changes nothing when the kernel refuses the request.
			status, statusErr := lease.read(ctx)
			if statusErr != nil || status.Applied || status.RootHandle == lease.handle {
				return lease, errors.Join(cause, statusErr)
			}
			return discard(cause)
		}
	}
	status, err := lease.read(ctx)
	if err != nil {
		return lease, fmt.Errorf("read the netem delay: %w", err)
	}
	// The kernel keeps the loss as a 32-bit probability.
	if !status.Applied || status.Delay != delay.Delay || status.Jitter != delay.Jitter || math.Abs(status.Loss-delay.Loss) > 1e-4 {
		return lease, errors.Join(fmt.Errorf("the kernel reports root %s %s with delay %v, jitter %v and loss %v%% instead of the requested delay",
			status.RootKind, status.RootHandle, status.Delay, status.Jitter, status.Loss), status.mismatch)
	}
	return lease, nil
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
	}
	return nil
}

// preflightNetem checks the helper image, NET_ADMIN and the kernel's netem
// in a helper without a network, and returns the helper's image ID.
func preflightNetem(ctx context.Context, backend netemBackend, image string) (string, error) {
	helper, err := backend.startHelper(ctx, image, "none")
	if err != nil {
		return "", errors.Join(classifyDocker(ctx, fmt.Errorf("start the netem preflight helper: %w", err)), removeNetemHelper(ctx, backend, helper))
	}
	imageID, err := func() (string, error) {
		_, imageID, err := backend.helper(ctx, helper)
		if err != nil {
			return "", fmt.Errorf("inspect the netem preflight helper: %w", err)
		}
		report, err := runNetemScript(ctx, backend, helper, "check the netem helper", "preflight")
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
		return imageID, nil
	}()
	return imageID, errors.Join(err, removeNetemHelper(ctx, backend, helper))
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
		return nil, errors.Join(classifyDocker(ctx, fmt.Errorf("start the netem helper: %w", err)), removeNetemHelper(ctx, backend, helper))
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
// the contract, or a *NativeFailure of tc.
func classifyNetem(label string, code int, text string) error {
	switch {
	case strings.Contains(text, "Operation not permitted"):
		return fmt.Errorf("%s: %w", label, ErrNetemPermissionDenied)
	case strings.Contains(text, "Specified qdisc kind is unknown"):
		return fmt.Errorf("%s: %w", label, ErrNetemKernelUnsupported)
	case strings.Contains(text, "Unknown qdisc") || strings.Contains(text, "unparsable"):
		return fmt.Errorf("%s: %w: tc has no netem", label, ErrNetemImageUnsupported)
	}
	return fmt.Errorf("%s: %w", label, nativeFailure("tc", code, nil, []byte(text)))
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
func (l *NetworkDelayLease) Status(ctx context.Context) (NetworkDelayStatus, error) {
	if err := lockTopologyMutex(ctx, &l.mu); err != nil {
		return NetworkDelayStatus{}, err
	}
	defer l.mu.Unlock()
	if l.complete {
		return NetworkDelayStatus{}, invalidRequest("the network delay was restored")
	}
	if err := l.settle(ctx); err != nil {
		return NetworkDelayStatus{}, err
	}
	return l.read(ctx)
}

// settle confirms that no earlier tc command of the lease can still run, and
// gives the lease a running helper in the target's namespace when the target
// still has it.
func (l *NetworkDelayLease) settle(ctx context.Context) error {
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
func (l *NetworkDelayLease) read(ctx context.Context) (NetworkDelayStatus, error) {
	if l.helper == nil {
		return NetworkDelayStatus{Gone: true}, nil
	}
	// The interface is found by its index: its address, MAC and name may
	// change, but a reconnect creates a new interface with a new index.
	report, err := inspectNetem(ctx, l.backend, l.helper, "index", strconv.Itoa(l.InterfaceIndex))
	if err != nil {
		return NetworkDelayStatus{}, err
	}
	// Another namespace, or no interface with that index, is not the delayed
	// interface; its qdiscs are not the lease's.
	if report.Namespace != l.Namespace || len(report.Links) != 1 {
		return NetworkDelayStatus{Gone: true}, nil
	}
	root, err := report.root()
	if err != nil {
		return NetworkDelayStatus{}, err
	}
	status := NetworkDelayStatus{Interface: report.Links[0].Name, RootKind: root.Kind, RootHandle: root.Handle, Qdiscs: report.Qdiscs, Filters: report.Filters}
	netem := root
	switch {
	case !l.Peer.IsValid():
		if root.Kind == "netem" && root.Handle == l.handle {
			status.mismatch = l.ownedPrefix(status)
			status.Applied = status.mismatch == nil
		}
	case root.Kind == "prio" && root.Handle == l.handle:
		// The delay is applied only as the whole hierarchy: the root, its
		// netem child in the selected band, and the one filter to it.
		var qdiscs []netemQdisc
		if err := json.Unmarshal(report.Qdiscs, &qdiscs); err != nil {
			return NetworkDelayStatus{}, fmt.Errorf("parse tc qdisc output: %w", err)
		}
		children := 0
		for _, qdisc := range qdiscs {
			if qdisc.Kind == "netem" && qdisc.Handle == l.child && qdisc.Parent == l.handle+peerBand {
				netem, children = qdisc, children+1
			}
		}
		status.mismatch = checkPeerFilter(report.Filters, l.handle, l.Peer, l.PeerPort)
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
	}
	return status, nil
}

// ownedPrefix checks that the qdiscs and filters under the lease's root are
// a prefix of what applying the delay adds: the root, and for a selective
// delay its netem child in the selected band and then the one filter.
// Removing the root removes everything under it, so anything else there is
// foreign; qdiscs outside the root, such as an ingress qdisc, are not.
func (l *NetworkDelayLease) ownedPrefix(status NetworkDelayStatus) error {
	var qdiscs []netemQdisc
	if err := json.Unmarshal(status.Qdiscs, &qdiscs); err != nil {
		return fmt.Errorf("parse tc qdisc output: %w", err)
	}
	rootKind := "netem"
	if l.Peer.IsValid() {
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
		case l.Peer.IsValid() && !qdisc.Root && qdisc.Kind == "netem" && qdisc.Handle == l.child && qdisc.Parent == l.handle+peerBand:
		case qdisc.Root:
			return fmt.Errorf("the root is %s %s, not the delay's %s", qdisc.Kind, qdisc.Handle, rootKind)
		default:
			return fmt.Errorf("a %s qdisc %s under %s", qdisc.Kind, qdisc.Handle, qdisc.Parent)
		}
	}
	if l.Peer.IsValid() {
		return checkPeerFilters(status.Filters, l.handle, l.Peer, l.PeerPort, false)
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

// Restore removes the delay's qdisc and then the helper. A delay whose
// interface is gone needs no removal. A root qdisc that someone replaced, or
// a filter or qdisc that someone added under the lease's root, is refused,
// not removed, and the lease stays retryable.
func (l *NetworkDelayLease) Restore(ctx context.Context) error {
	if l == nil {
		return nil
	}
	if err := lockTopologyMutex(ctx, &l.mu); err != nil {
		return err
	}
	defer l.mu.Unlock()
	if l.complete {
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

// dockerNetemBackend runs helpers as owned containers of a cluster.
type dockerNetemBackend struct {
	label testcontainers.CustomizeRequestOption
}

func (c *Container) netemBackend() netemBackend {
	if c.newNetemBackend != nil {
		return c.newNetemBackend()
	}
	return dockerNetemBackend{label: c.ownedLabel()}
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
		}
	}
	return state, nil
}

// netemHelperOptions run an idle helper in networkMode as root, as the
// contract says whatever the image's USER, with NET_ADMIN as its only
// capability.
func netemHelperOptions(networkMode container.NetworkMode) []testcontainers.ContainerCustomizer {
	return []testcontainers.ContainerCustomizer{
		WithIdleEntrypoint(),
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
	helper, err := testcontainers.Run(ctx, image, append(netemHelperOptions(networkMode), b.label)...)
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
