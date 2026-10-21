package fault

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// fakeQdisc is an interface's root qdisc; nil means the kernel's default.
type fakeQdisc struct {
	kind, handle  string
	delay, jitter time.Duration
	loss          float64 // fraction
	// rate is in bytes per second, as the kernel keeps it; limit is the
	// queue limit in packets, tc's 1000 unless the command set one.
	rate    uint64
	limit   int
	packets uint64
	// The kernel's other counters and queue state.
	overlimits, requeues, backlog, qlen uint64
	// faults are netem's packet faults by keyword, in percent as tc's
	// arguments give them; gap is the reorder gap.
	faults map[string]NetworkPacketFault
	gap    int
}

// fakeNetemQdisc reads tc's netem arguments: [delay D [J]] [loss P%]
// [reorder|duplicate|corrupt P% [C%]]... [rate Nbit limit L]. Like tc, a
// reorder sets a gap of 1. Like tc 6.1, it keeps a duplicate's correlation
// only when the delay has a correlation too.
func fakeNetemQdisc(handle string, args []string) *fakeQdisc {
	qdisc := &fakeQdisc{kind: "netem", handle: handle, limit: 1000}
	delayCorrelation := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "rate":
			bits, _ := strconv.ParseUint(strings.TrimSuffix(args[i+1], "bit"), 10, 64)
			qdisc.rate = bits / 8
		case "limit":
			qdisc.limit, _ = strconv.Atoi(args[i+1])
		case "delay":
			qdisc.delay, _ = time.ParseDuration(args[i+1])
			if i+2 < len(args) && strings.HasSuffix(args[i+2], "us") {
				qdisc.jitter, _ = time.ParseDuration(args[i+2])
				delayCorrelation = i+3 < len(args) && strings.HasSuffix(args[i+3], "%")
			}
		case "loss":
			percent, _ := strconv.ParseFloat(strings.TrimSuffix(args[i+1], "%"), 64)
			qdisc.loss = percent / 100
		case "reorder", "duplicate", "corrupt":
			var fault NetworkPacketFault
			fault.Percent, _ = strconv.ParseFloat(strings.TrimSuffix(args[i+1], "%"), 64)
			if i+2 < len(args) && strings.HasSuffix(args[i+2], "%") {
				fault.Correlation, _ = strconv.ParseFloat(strings.TrimSuffix(args[i+2], "%"), 64)
			}
			if qdisc.faults == nil {
				qdisc.faults = map[string]NetworkPacketFault{}
			}
			qdisc.faults[args[i]] = fault
			if args[i] == "reorder" {
				qdisc.gap = 1
			}
		}
	}
	if fault, set := qdisc.faults["duplicate"]; set && !delayCorrelation {
		fault.Correlation = 0
		qdisc.faults["duplicate"] = fault
	}
	return qdisc
}

// fakeNetemOptions prints a netem qdisc's options as tc does: no delay
// member for a zero delay, the loss as a fraction.
func fakeNetemOptions(q *fakeQdisc) map[string]any {
	options := map[string]any{"limit": q.limit}
	if q.rate > 0 {
		options["rate"] = map[string]any{"rate": q.rate, "packetoverhead": 0, "cellsize": 0, "celloverhead": 0}
	}
	if q.delay > 0 {
		options["delay"] = map[string]any{"delay": q.delay.Seconds(), "jitter": q.jitter.Seconds(), "correlation": 0}
	}
	if q.loss > 0 {
		options["loss-random"] = map[string]any{"loss": q.loss, "correlation": 0}
	}
	// tc prints each fault as fractions, under its own keyword.
	for name, fault := range q.faults {
		options[name] = map[string]any{name: fault.Percent / 100, "correlation": fault.Correlation / 100}
	}
	options["ecn"], options["gap"] = false, q.gap
	return options
}

type fakeLink struct {
	name    string
	index   int
	mac     string
	address netip.Addr
	root    *fakeQdisc
	// child is a netem qdisc under a prio root, parent its band; filters
	// are the egress filter dump, u32 keys under "matches".
	child   *fakeQdisc
	parent  string
	filters []map[string]any
	// tables are the u32 tables under the root, by preference: the kernel
	// gives the first preference 800:, the next 801:, and so on.
	tables map[int]fakeU32Table
	// extra are qdiscs that someone else added, by parent; removing the
	// root removes those under it.
	extra []fakeExtraQdisc
}

type fakeU32Table struct {
	protocol, id string
}

type fakeExtraQdisc struct {
	kind, handle, parent string
}

// options prints a foreign qdisc's options as tc does: a tbf's rate is a
// number of bytes per second, not netem's rate object.
func (q fakeExtraQdisc) options() map[string]any {
	if q.kind == "tbf" {
		return map[string]any{"rate": 125000000, "burst": 65536, "lat": 0.05}
	}
	if q.kind == "faultlike" {
		// Members named like netem's faults that are not netem's.
		return map[string]any{"duplicate": 3, "corrupt": true, "reorder": "x", "gap": "none"}
	}
	return map[string]any{}
}

// fakeNamespace is a network namespace with its interfaces.
type fakeNamespace struct {
	id    int
	links []*fakeLink
}

type fakeNetemTarget struct {
	// address6 is the endpoint's IPv6 address on a dual-stack bridge.
	address6        netip.Addr
	running, paused bool
	mode            container.NetworkMode
	sandbox         int
	namespace       *fakeNamespace
	removed         bool
}

// fakeNetemHelper stands in for a helper container; the code under test only
// hands it back to the backend.
type fakeNetemHelper struct {
	testcontainers.Container
	id        string
	image     string
	namespace *fakeNamespace
	netAdmin  bool
	running   bool
	removed   bool
}

func (h *fakeNetemHelper) GetContainerID() string { return h.id }

// fakeNetem models a Docker engine with network namespaces, interfaces and
// their root qdiscs, the helper containers in them, and an image that may
// break the netem contract.
type fakeNetem struct {
	mu      sync.Mutex
	nextID  int
	targets map[string]*fakeNetemTarget
	helpers []*fakeNetemHelper
	kernel  bool // the kernel has netem
	noPrio  bool // the kernel lacks the prio qdisc
	noU32   bool // the kernel lacks the u32 classifier
	// driver is the driver of the network tc-public; newFakeNetem makes it a
	// bridge.
	driver string
	// tableGap skips numbers before the u32 table of a second preference,
	// as a kernel that numbered them otherwise would.
	tableGap int
	netAdmin bool // the engine grants NET_ADMIN
	imageID  string
	// tags resolves image references; an image ID resolves to itself.
	tags map[string]string
	// broken is an image ID without tc.
	broken    string
	missing   []string // contract executables the image lacks
	noPython  bool
	commands  []string
	cancelErr error
	// onAdd replaces a tc qdisc add; apply makes the change.
	onAdd func(apply func() supervised.Report) (supervised.Report, error)
	// onStart runs before a helper starts.
	onStart func(mode container.NetworkMode)
	// onRemove runs before a helper is removed.
	onRemove func()
	// editPreflight changes the preflight report before it is returned.
	editPreflight func(map[string]any)
	// slowPreflight makes the preflight run into its deadline.
	slowPreflight bool
	// onSelective may refuse a command of a selective delay before it runs.
	onSelective func(argv []string) (supervised.Report, bool)
	// noRateParse is an image whose tc cannot parse netem's rate.
	noRateParse bool
	// rateIgnored is a kernel whose netem does not keep a rate.
	rateIgnored bool
	// faultUnparsed is a packet fault keyword that the image's tc cannot
	// parse; faultIgnored are those that the kernel's netem does not keep,
	// with "gap" the reorder gap, and faultChanged those that it keeps
	// without their correlation.
	faultUnparsed string
	faultIgnored  map[string]bool
	faultChanged  map[string]bool
}

// faultUnparsedReport is tc's answer to a netem keyword that it cannot parse.
func faultUnparsedReport(name string) supervised.Report {
	return supervised.Report{ExitCode: 1, Stderr: []byte("What is \"" + name + "\"?\nUsage: ... netem [ limit PACKETS ]")}
}

// unparsedFault reports whether argv has the keyword that tc cannot parse.
func (n *fakeNetem) unparsedFault(argv []string) bool {
	return n.faultUnparsed != "" && slices.Contains(argv, n.faultUnparsed)
}

// netemQdisc is fakeNetemQdisc in this kernel; the caller holds n.mu.
func (n *fakeNetem) netemQdisc(handle string, args []string) *fakeQdisc {
	qdisc := fakeNetemQdisc(handle, args)
	if n.rateIgnored {
		qdisc.rate = 0
	}
	for name := range n.faultIgnored {
		if name == "gap" {
			qdisc.gap = 0
		}
		delete(qdisc.faults, name)
	}
	for name := range n.faultChanged {
		if fault, kept := qdisc.faults[name]; kept {
			fault.Correlation = 0
			qdisc.faults[name] = fault
		}
	}
	return qdisc
}

// rateUnparsed is tc's answer to a netem rate that it cannot parse.
var rateUnparsed = supervised.Report{ExitCode: 1, Stderr: []byte("What is \"rate\"?\nUsage: ... netem [ limit PACKETS ]")}

func newFakeNetem() *fakeNetem {
	n := &fakeNetem{targets: map[string]*fakeNetemTarget{}, kernel: true, netAdmin: true, imageID: "sha256:helper", driver: "bridge",
		tags: map[string]string{"netem-helper:test": "sha256:helper"}}
	n.targets["osd-0"] = &fakeNetemTarget{running: true, mode: "tc-public", sandbox: 1, namespace: n.namespace("10.88.0.5", "02:42:0a:58:00:05")}
	return n
}

func (n *fakeNetem) namespace(address, mac string) *fakeNamespace {
	n.nextID++
	return &fakeNamespace{id: n.nextID, links: []*fakeLink{
		{name: "lo", index: 1, mac: "00:00:00:00:00:00", address: netip.MustParseAddr("127.0.0.1")},
		{name: "eth0", index: 10 + n.nextID, mac: mac, address: netip.MustParseAddr(address)},
	}}
}

// restart gives the target a new namespace; helpers stay in the old one,
// which loses the endpoint.
func (n *fakeNetem) restart(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	target := n.targets[id]
	eth0 := target.namespace.links[1]
	target.namespace.links = target.namespace.links[:1]
	target.namespace = n.namespace(eth0.address.String(), eth0.mac)
	target.sandbox++
}

func (n *fakeNetem) root(id string) *fakeQdisc {
	n.mu.Lock()
	defer n.mu.Unlock()
	if links := n.targets[id].namespace.links; len(links) > 1 {
		return links[1].root
	}
	return nil
}

func (n *fakeNetem) setRoot(id string, root *fakeQdisc) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.targets[id].namespace.links[1].root = root
}

func (n *fakeNetem) liveHelpers() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	live := 0
	for _, helper := range n.helpers {
		if !helper.removed {
			live++
		}
	}
	return live
}

func (n *fakeNetem) target(_ context.Context, id, network string) (netemTarget, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	target := n.targets[id]
	if target == nil || target.removed {
		return netemTarget{}, errdefs.ErrNotFound
	}
	state := netemTarget{running: target.running, paused: target.paused, networkMode: target.mode,
		sandboxID: strconv.Itoa(target.sandbox)}
	if network == "tc-public" && len(target.namespace.links) > 1 {
		state.address, state.address6, state.mac = target.namespace.links[1].address, target.address6, target.namespace.links[1].mac
		state.driver = n.driver
	}
	return state, nil
}

func (n *fakeNetem) startHelper(_ context.Context, image string, mode container.NetworkMode) (testcontainers.Container, error) {
	if n.onStart != nil {
		n.onStart(mode)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nextID++
	if id, tagged := n.tags[image]; tagged {
		image = id
	}
	helper := &fakeNetemHelper{id: fmt.Sprintf("helper-%d", n.nextID), image: image, netAdmin: n.netAdmin, running: true}
	if mode == "none" {
		helper.namespace = &fakeNamespace{id: n.nextID, links: []*fakeLink{{name: "lo", index: 1, mac: "00:00:00:00:00:00", address: netip.MustParseAddr("127.0.0.1")}}}
	} else {
		target := n.targets[strings.TrimPrefix(string(mode), "container:")]
		if target == nil || target.removed || !target.running {
			return nil, errors.New("cannot join the target's namespace")
		}
		helper.namespace = target.namespace
	}
	n.helpers = append(n.helpers, helper)
	return helper, nil
}

func (n *fakeNetem) helper(_ context.Context, ctr testcontainers.Container) (bool, string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	helper := ctr.(*fakeNetemHelper)
	if helper.removed {
		return false, "", errdefs.ErrNotFound
	}
	return helper.running, helper.image, nil
}

func (n *fakeNetem) removeHelper(ctx context.Context, ctr testcontainers.Container) error {
	if n.onRemove != nil {
		n.onRemove()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	helper := ctr.(*fakeNetemHelper)
	if helper.removed {
		return errdefs.ErrNotFound
	}
	helper.removed = true
	return nil
}

func (n *fakeNetem) cancel(context.Context, testcontainers.Container, string, string) error {
	return n.cancelErr
}

func qdiscJSON(link *fakeLink) []map[string]any {
	if link.root == nil {
		qdiscs := []map[string]any{{"kind": "noqueue", "handle": "0:", "root": true, "options": map[string]any{}}}
		for _, extra := range link.extra {
			qdiscs = append(qdiscs, map[string]any{"kind": extra.kind, "handle": extra.handle, "parent": extra.parent, "options": extra.options()})
		}
		return qdiscs
	}
	counted := func(q *fakeQdisc, entry map[string]any) map[string]any {
		entry["options"], entry["packets"], entry["bytes"], entry["drops"] = fakeNetemOptions(q), q.packets, q.packets*100, 0
		entry["overlimits"], entry["requeues"], entry["backlog"], entry["qlen"] = q.overlimits, q.requeues, q.backlog, q.qlen
		return entry
	}
	qdiscs := []map[string]any{counted(link.root, map[string]any{"kind": link.root.kind, "handle": link.root.handle, "root": true})}
	if link.child != nil {
		qdiscs = append(qdiscs, counted(link.child, map[string]any{"kind": link.child.kind, "handle": link.child.handle, "parent": link.parent}))
	}
	for _, extra := range link.extra {
		qdiscs = append(qdiscs, map[string]any{"kind": extra.kind, "handle": extra.handle, "parent": extra.parent, "options": extra.options()})
	}
	return qdiscs
}

// fakeU32Keys encodes tc's u32 match arguments as tc prints them: one
// 32-bit word per key, in hex without leading zeros.
func fakeU32Keys(args []string) []map[string]any {
	key := func(off int, value, mask uint32) map[string]any {
		return map[string]any{"value": strconv.FormatUint(uint64(value), 16), "mask": strconv.FormatUint(uint64(mask), 16), "offmask": "", "off": off}
	}
	var keys []map[string]any
	for i := 0; i < len(args); i++ {
		if args[i] != "match" {
			continue
		}
		family, field := args[i+1], args[i+2]
		switch field {
		case "dst":
			prefix := netip.MustParsePrefix(args[i+3])
			address := prefix.Addr().AsSlice()
			if family == "ip" {
				keys = append(keys, key(16, binary.BigEndian.Uint32(address), 0xffffffff))
			} else {
				for word := range 4 {
					keys = append(keys, key(24+4*word, binary.BigEndian.Uint32(address[4*word:]), 0xffffffff))
				}
			}
		case "protocol":
			if family == "ip" {
				keys = append(keys, key(8, 6<<16, 0xff<<16))
			} else {
				keys = append(keys, key(4, 6<<8, 0xff<<8))
			}
		case "dport":
			port, _ := strconv.Atoi(args[i+3])
			if family == "ip" {
				keys = append(keys, key(20, uint32(port), 0xffff))
			} else {
				keys = append(keys, key(40, uint32(port), 0xffff))
			}
		}
	}
	return keys
}

func (n *fakeNetem) run(ctx context.Context, ctr testcontainers.Container, label string, argv ...string) (supervised.Report, error) {
	if err := ctx.Err(); err != nil {
		return supervised.Report{}, err
	}
	n.mu.Lock()
	helper := ctr.(*fakeNetemHelper)
	n.commands = append(n.commands, strings.Join(argv, " "))
	if helper.removed || !helper.running {
		n.mu.Unlock()
		return supervised.Report{}, &supervised.BackendError{Op: "exec", Err: errors.New("container is not running")}
	}
	if argv[0] == "python3" {
		defer n.mu.Unlock()
		if n.noPython {
			return supervised.Report{}, &supervised.ExitError{Label: label, Code: 127}
		}
		return n.script(helper, argv[3:])
	}
	if slices.Contains(n.missing, "tc") || helper.image == n.broken {
		n.mu.Unlock()
		return supervised.Report{ExitCode: 127, Stderr: []byte("tc: not found")}, nil
	}
	link := helper.namespace.link(argv[4])
	if link == nil {
		n.mu.Unlock()
		return supervised.Report{ExitCode: 1, Stderr: []byte(`Cannot find device "` + argv[4] + `"`)}, nil
	}
	if argv[1] == "filter" || argv[5] == "parent" || len(argv) > 8 && argv[8] == "prio" {
		defer n.mu.Unlock()
		if n.onSelective != nil {
			if report, refused := n.onSelective(argv); refused {
				return report, nil
			}
		}
		switch {
		case argv[1] == "filter":
			// tc filter add dev IF parent H protocol P pref 10 handle 800::1 u32 ... flowid F
			if link.root == nil || link.root.kind != "prio" || link.root.handle != argv[6] {
				return supervised.Report{ExitCode: 2, Stderr: []byte("Error: Parent Qdisc doesn't exists.")}, nil
			}
			protocol := argv[8]
			pref, _ := strconv.Atoi(argv[10])
			if link.tables == nil {
				link.tables = map[int]fakeU32Table{}
			}
			tables := maps.Clone(link.tables)
			table, exists := tables[pref]
			switch {
			case !exists:
				// A new preference: the kernel creates its u32 table with
				// the next free number, and drops both again if the filter
				// that created them fails.
				number := 0x800 + len(tables)
				if number > 0x800 {
					number += n.tableGap
				}
				table = fakeU32Table{protocol: protocol, id: fmt.Sprintf("%x:", number)}
				tables[pref] = table
			case table.protocol != protocol:
				return supervised.Report{ExitCode: 2, Stderr: []byte("Error: Filter with specified priority/protocol not found.")}, nil
			}
			// The handle names its table; the kernel puts the key node in that
			// table, whichever preference it belongs to.
			handle := argv[12]
			id, _, _ := strings.Cut(handle, "::")
			owner, found := 0, false
			for p, t := range tables {
				if t.id == id+":" {
					owner, found = p, true
				}
			}
			if !found {
				return supervised.Report{ExitCode: 2, Stderr: []byte("RTNETLINK answers: Invalid argument")}, nil
			}
			for _, filter := range link.filters {
				if options, ok := filter["options"].(map[string]any); ok && options["fh"] == handle {
					return supervised.Report{ExitCode: 2, Stderr: []byte("RTNETLINK answers: File exists")}, nil
				}
			}
			if !exists {
				link.filters = append(link.filters,
					map[string]any{"parent": argv[6], "protocol": protocol, "pref": pref, "kind": "u32", "chain": 0},
					map[string]any{"parent": argv[6], "protocol": protocol, "pref": pref, "kind": "u32", "chain": 0, "options": map[string]any{"fh": table.id, "ht_divisor": 1}})
			}
			link.tables = tables
			leaf := map[string]any{"parent": argv[6], "protocol": tables[owner].protocol, "pref": owner, "kind": "u32", "chain": 0,
				"options": map[string]any{"fh": handle, "order": 1, "key_ht": id, "bkt": "0", "flowid": argv[len(argv)-1], "matches": fakeU32Keys(argv[13:])}}
			link.filters = append(link.filters, leaf)
		case argv[5] == "parent":
			// tc qdisc add dev IF parent H:3 handle C netem delay X [J]
			if link.root == nil || link.root.kind != "prio" || argv[6] != link.root.handle+"3" {
				return supervised.Report{ExitCode: 2, Stderr: []byte("Error: Failed to find specified qdisc.")}, nil
			}
			if n.noRateParse && slices.Contains(argv, "rate") {
				return rateUnparsed, nil
			}
			if n.unparsedFault(argv) {
				return faultUnparsedReport(n.faultUnparsed), nil
			}
			link.child, link.parent = n.netemQdisc(argv[8], argv[10:]), argv[6]
		default:
			// tc qdisc add dev IF root handle H prio bands 3 priomap ...
			if link.root != nil {
				return supervised.Report{ExitCode: 2, Stderr: []byte("Error: Exclusivity flag on, cannot modify.")}, nil
			}
			link.root = &fakeQdisc{kind: "prio", handle: argv[7]}
		}
		return supervised.Report{}, nil
	}
	switch argv[2] {
	case "add":
		apply := func() supervised.Report {
			n.mu.Lock()
			defer n.mu.Unlock()
			switch {
			case !helper.netAdmin:
				return supervised.Report{ExitCode: 2, Stderr: []byte("RTNETLINK answers: Operation not permitted")}
			case !n.kernel:
				return supervised.Report{ExitCode: 2, Stderr: []byte("Error: Specified qdisc kind is unknown.")}
			case link.root != nil:
				return supervised.Report{ExitCode: 2, Stderr: []byte("Error: Exclusivity flag on, cannot modify.")}
			case n.noRateParse && slices.Contains(argv, "rate"):
				return rateUnparsed
			case n.unparsedFault(argv):
				return faultUnparsedReport(n.faultUnparsed)
			}
			link.root = n.netemQdisc(argv[7], argv[9:])
			return supervised.Report{}
		}
		n.mu.Unlock()
		if n.onAdd != nil {
			return n.onAdd(apply)
		}
		return apply(), nil
	case "del":
		defer n.mu.Unlock()
		if !helper.netAdmin {
			return supervised.Report{ExitCode: 2, Stderr: []byte("RTNETLINK answers: Operation not permitted")}, nil
		}
		if link.root == nil || link.root.handle != argv[7] {
			return supervised.Report{ExitCode: 2, Stderr: []byte("Error: Invalid handle.")}, nil
		}
		// Removing a root removes its children and filters with it.
		major, _, _ := strings.Cut(link.root.handle, ":")
		link.extra = slices.DeleteFunc(link.extra, func(extra fakeExtraQdisc) bool { return strings.HasPrefix(extra.parent, major+":") })
		link.root, link.child, link.parent, link.filters, link.tables = nil, nil, "", nil, nil
		return supervised.Report{}, nil
	}
	n.mu.Unlock()
	return supervised.Report{}, fmt.Errorf("unexpected command %v", argv)
}

func (ns *fakeNamespace) link(name string) *fakeLink {
	for _, link := range ns.links {
		if link.name == name {
			return link
		}
	}
	return nil
}

// script answers netemScript from the model; the caller holds n.mu.
func (n *fakeNetem) script(helper *fakeNetemHelper, args []string) (supervised.Report, error) {
	out := map[string]any{"namespace": fmt.Sprintf("net:[%d]", helper.namespace.id), "net_admin": helper.netAdmin}
	if args[0] == "preflight" && n.slowPreflight {
		return supervised.Report{TimedOut: true}, nil
	}
	if args[0] == "preflight" {
		// The script always reports a list, also an empty one.
		out["missing"] = append([]string{}, n.missing...)
		if len(n.missing) == 0 {
			out["broken"] = []string{}
			switch {
			case !helper.netAdmin:
				out["add"] = []any{2, "RTNETLINK answers: Operation not permitted"}
			case !n.kernel:
				out["add"] = []any{2, "Error: Specified qdisc kind is unknown."}
			default:
				out["add"] = []any{0, ""}
				shown, _ := json.Marshal([]map[string]any{{"kind": "netem", "handle": "c7e1:", "root": true}})
				out["shown"] = string(shown)
				out["delete"] = []any{0, ""}
				if slices.Contains(args[1:], "rate") {
					// The script's netem rate on the loopback.
					if n.noRateParse {
						out["rate"] = []any{int(rateUnparsed.ExitCode), string(rateUnparsed.Stderr)}
					} else {
						out["rate"] = []any{0, ""}
						rate := n.netemQdisc("c7e3:", []string{"rate", "1000000bit", "limit", "100"})
						shown, _ := json.Marshal([]map[string]any{{"kind": "netem", "handle": "c7e3:", "root": true, "options": fakeNetemOptions(rate)}})
						out["rate_shown"] = string(shown)
						out["rate_delete"] = []any{0, ""}
					}
				}
				if faults := slices.DeleteFunc(slices.Clone(args[1:]), func(arg string) bool { return arg == "rate" || arg == "peers" }); len(faults) > 0 {
					// The script's netem packet faults on the loopback, each
					// with a correlation, a delay for a reorder, and a delay
					// correlation for a duplicate's.
					argv := []string{}
					switch {
					case slices.Contains(faults, "reorder"):
						argv = append(argv, "delay", "1000us", "0us", "0%")
					case slices.Contains(faults, "duplicate"):
						argv = append(argv, "delay", "0us", "0us", "0%")
					}
					for _, name := range faults {
						fault := preflightFault[name]
						argv = append(argv, name, fmt.Sprintf("%v%%", fault.Percent), fmt.Sprintf("%v%%", fault.Correlation))
					}
					if n.unparsedFault(argv) {
						report := faultUnparsedReport(n.faultUnparsed)
						out["faults"] = []any{report.ExitCode, string(report.Stderr)}
					} else {
						out["faults"] = []any{0, ""}
						qdisc := n.netemQdisc("c7e4:", argv)
						shown, _ := json.Marshal([]map[string]any{{"kind": "netem", "handle": "c7e4:", "root": true, "options": fakeNetemOptions(qdisc)}})
						out["faults_shown"] = string(shown)
						out["faults_delete"] = []any{0, ""}
					}
				}
				if slices.Contains(args[1:], "peers") {
					// The script's prio root and u32 filter on the loopback.
					out["prio"] = []any{0, ""}
					if n.noPrio {
						out["prio"] = []any{2, "Error: Specified qdisc kind is unknown."}
						break
					}
					out["u32"] = []any{0, ""}
					if n.noU32 {
						out["u32"] = []any{2, "Error: TC classifier not found.\nWe have an error talking to the kernel"}
					} else {
						filters, _ := json.Marshal([]map[string]any{{"parent": "c7e2:", "protocol": "ip", "pref": 10, "kind": "u32",
							"options": map[string]any{"fh": "800::1", "flowid": "c7e2:3"}}})
						out["u32_shown"] = string(filters)
					}
					out["prio_delete"] = []any{0, ""}
				}
			}
		}
		if n.editPreflight != nil {
			n.editPreflight(out)
		}
	} else {
		var matches []*fakeLink
		for _, link := range helper.namespace.links {
			if args[0] == "inspect" && link.address.String() == args[1] && link.mac == args[2] ||
				args[0] == "index" && strconv.Itoa(link.index) == args[1] {
				matches = append(matches, link)
			}
		}
		interfaces := []map[string]any{}
		for _, link := range matches {
			interfaces = append(interfaces, map[string]any{"name": link.name, "index": link.index})
		}
		out["interfaces"] = interfaces
		if len(matches) == 1 {
			if matches[0].root != nil {
				matches[0].root.packets += 10
			}
			if matches[0].child != nil {
				matches[0].child.packets += 3
			}
			out["qdiscs"] = qdiscJSON(matches[0])
			out["filters"] = append([]map[string]any{}, matches[0].filters...)
		}
	}
	stdout, err := json.Marshal(out)
	return supervised.Report{Stdout: stdout}, err
}

// delayCluster is a bridge cluster whose OSD osd-0 is attached to tc-public.
func delayCluster(netem *fakeNetem) (*delayTestCluster, *fakeNetemHelper) {
	osd := &fakeNetemHelper{id: "osd-0"}
	cluster := newDelayTestCluster(netem, osd, false)
	return cluster, osd
}

var testDelay = NetworkProfile{HelperImage: "netem-helper:test", Plane: ceph.PublicNetworkPlane, Delay: 200 * time.Millisecond, Jitter: 20 * time.Millisecond}

func TestNetworkDelayValidation(t *testing.T) {
	for name, delay := range map[string]NetworkProfile{
		"no helper":       {Plane: ceph.PublicNetworkPlane, Delay: time.Second},
		"no plane":        {HelperImage: "h", Delay: time.Second},
		"too short":       {HelperImage: "h", Plane: ceph.PublicNetworkPlane, Delay: time.Microsecond},
		"too long":        {HelperImage: "h", Plane: ceph.PublicNetworkPlane, Delay: MaxNetworkDelay + time.Millisecond},
		"jitter > delay":  {HelperImage: "h", Plane: ceph.PublicNetworkPlane, Delay: time.Millisecond, Jitter: 2 * time.Millisecond},
		"negative jitter": {HelperImage: "h", Plane: ceph.PublicNetworkPlane, Delay: time.Millisecond, Jitter: -1},
		"nanoseconds":     {HelperImage: "h", Plane: ceph.PublicNetworkPlane, Delay: time.Millisecond + 1},
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			cluster, osd := delayCluster(netem)
			if lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay); err == nil || lease != nil {
				t.Fatalf("accepted %+v", delay)
			}
			if len(netem.helpers) != 0 {
				t.Fatal("a helper started for an invalid delay")
			}
		})
	}
}

func TestNetworkDelayAppliesAndRestores(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
	if err != nil {
		t.Fatal(err)
	}
	root := netem.root("osd-0")
	if root == nil || root.kind != "netem" || root.delay != testDelay.Delay || root.jitter != testDelay.Jitter || root.handle != lease.handle {
		t.Fatalf("root qdisc %+v", root)
	}
	if lease.Interface != "eth0" || lease.InterfaceIndex == 0 || lease.Namespace == "" || lease.HelperImageID != "sha256:helper" || lease.SandboxID != "1" {
		t.Fatalf("lease identity %+v", lease)
	}
	// The preflight helper is gone; the helper in the target's namespace stays.
	if live := netem.liveHelpers(); live != 1 {
		t.Fatalf("%d helpers run", live)
	}
	status, err := lease.Status(t.Context())
	if err != nil || !status.Applied || status.Gone || status.Delay != testDelay.Delay || status.Jitter != testDelay.Jitter || status.Packets == 0 {
		t.Fatalf("status %+v, %v", status, err)
	}
	if _, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay); err == nil {
		t.Fatal("a second delay of the same endpoint was accepted")
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
		t.Fatalf("root %+v and %d helpers after Restore", netem.root("osd-0"), netem.liveHelpers())
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatalf("a repeated Restore: %v", err)
	}
	if _, err := lease.Status(t.Context()); err == nil {
		t.Fatal("Status of a restored delay")
	}
	again, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
	if err != nil {
		t.Fatalf("a new delay after Restore: %v", err)
	}
	if err := again.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// Each unmet requirement is its own error, found before the target changes.
func TestNetworkDelayRefusesUnsupportedHelpers(t *testing.T) {
	for name, tc := range map[string]struct {
		setup      func(*fakeNetem)
		want, kind error
	}{
		"no tc":        {func(n *fakeNetem) { n.missing = []string{"tc", "ip"} }, ErrNetemImageUnsupported, ceph.ErrImageCapability},
		"no python3":   {func(n *fakeNetem) { n.noPython = true }, ErrNetemImageUnsupported, ceph.ErrImageCapability},
		"no NET_ADMIN": {func(n *fakeNetem) { n.netAdmin = false }, ErrNetemPermissionDenied, ceph.ErrEnvironmentUnavailable},
		"no netem":     {func(n *fakeNetem) { n.kernel = false }, ErrNetemKernelUnsupported, ceph.ErrEnvironmentUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			tc.setup(netem)
			cluster, osd := delayCluster(netem)
			lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
			if !errors.Is(err, tc.want) || !errors.Is(err, tc.kind) || lease != nil {
				t.Fatalf("got %v, lease %v; want %v", err, lease, tc.want)
			}
			for _, other := range categories {
				if other != tc.kind && errors.Is(err, other) {
					t.Fatalf("%v also matches %v", err, other)
				}
			}
			for _, command := range netem.commands {
				if strings.HasPrefix(command, "tc ") {
					t.Fatalf("ran %q on the target", command)
				}
			}
			if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
				t.Fatal("the refusal left a change or a helper")
			}
			// The endpoint is free again.
			if len(cluster.faultsOf(faulthook.NetworkDelay)) != 0 {
				t.Fatalf("reservations left: %v", cluster.faultsOf(faulthook.NetworkDelay))
			}
		})
	}
}

func TestNetworkDelayRefusesTargets(t *testing.T) {
	for name, setup := range map[string]func(*fakeNetemTarget){
		"paused":  func(target *fakeNetemTarget) { target.paused = true },
		"stopped": func(target *fakeNetemTarget) { target.running = false },
		"shared":  func(target *fakeNetemTarget) { target.mode = "container:other" },
		"custom root": func(target *fakeNetemTarget) {
			target.namespace.links[1].root = &fakeQdisc{kind: "tbf", handle: "8001:"}
		},
		"not on the plane": func(target *fakeNetemTarget) { target.namespace.links = target.namespace.links[:1] },
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			setup(netem.targets["osd-0"])
			before := netem.root("osd-0")
			cluster, osd := delayCluster(netem)
			if lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay); !errors.Is(err, ceph.ErrInvalidRequest) || lease != nil {
				t.Fatalf("returned %v, lease %v", err, lease)
			}
			if name != "custom root" && name != "not on the plane" && len(netem.helpers) != 0 {
				t.Fatal("a helper started before the target was admitted")
			}
			if netem.liveHelpers() != 0 || netem.root("osd-0") != before {
				t.Fatal("the refusal left a change or a helper")
			}
			for _, command := range netem.commands {
				if strings.HasPrefix(command, "tc ") {
					t.Fatalf("ran %q on a refused target", command)
				}
			}
		})
	}
	netem := newFakeNetem()
	osd := &fakeNetemHelper{id: "osd-0"}
	cluster := newDelayTestCluster(netem, osd, true)
	if _, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay); !errors.Is(err, ceph.ErrInvalidRequest) || len(netem.helpers) != 0 {
		t.Fatalf("a host-network cluster: %v", err)
	}
}

// An unselected extension, an image that fails its contract, an engine that
// denies what it needs, and a tc command that fails otherwise are told apart.
func TestNetworkDelayClassifiesSelectionAndNativeFailures(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	unselected := testDelay
	unselected.HelperImage = ""
	var missing *ceph.OperationError
	if lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, unselected); !errors.As(err, &missing) || missing.Kind != ceph.ErrImageCapability ||
		missing.Capability != "netem extension" || errors.Is(err, ErrNetemImageUnsupported) || lease != nil || len(netem.helpers) != 0 {
		t.Fatalf("an unselected extension returned %v", err)
	}
	for name, change := range map[string]func(*NetworkProfile){
		"no delay":      func(d *NetworkProfile) { d.Delay = 0 },
		"long jitter":   func(d *NetworkProfile) { d.Jitter = 2 * d.Delay },
		"unknown plane": func(d *NetworkProfile) { d.Plane = "storage" },
	} {
		request := testDelay
		change(&request)
		if _, err := cluster.ApplyNetworkProfile(t.Context(), osd, request); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("%s returned %v", name, err)
		}
	}
	// The kernel refuses the qdisc for another reason: tc's own failure, and
	// nothing changed.
	netem.onAdd = func(func() supervised.Report) (supervised.Report, error) {
		return supervised.Report{ExitCode: 2, Stderr: []byte("RTNETLINK answers: Invalid argument")}, nil
	}
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
	var failure *ceph.NativeFailure
	if !errors.As(err, &failure) || failure.Program != "tc" || failure.ExitCode != 2 || failure.Reason != "RTNETLINK answers: Invalid argument" ||
		errors.Is(err, ceph.ErrImageCapability) || errors.Is(err, ceph.ErrEnvironmentUnavailable) || lease != nil {
		t.Fatalf("a refused tc command returned %v, lease %v", err, lease)
	}
	if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
		t.Fatal("the failed command left a change or a helper")
	}
	// A preflight that runs out of time says nothing about the image.
	netem = newFakeNetem()
	netem.slowPreflight = true
	cluster, osd = delayCluster(netem)
	if lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay); err == nil || classifiedError(err) || !strings.Contains(err.Error(), "timed out") ||
		lease != nil || netem.liveHelpers() != 0 {
		t.Fatalf("a slow preflight returned %v, lease %v", err, lease)
	}
}

// A call canceled while tc runs returns its lease: the change may have been
// applied, and only Restore can tell and undo it.
func TestNetworkDelayCanceledApplyKeepsALease(t *testing.T) {
	netem := newFakeNetem()
	netem.onAdd = func(apply func() supervised.Report) (supervised.Report, error) {
		apply()
		return supervised.Report{}, context.Canceled
	}
	cluster, osd := delayCluster(netem)
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
	if !errors.Is(err, context.Canceled) || classifiedError(err) || lease == nil {
		t.Fatalf("got %v, %v", lease, err)
	}
	if status, err := lease.Status(t.Context()); err != nil || !status.Applied {
		t.Fatalf("status %+v, %v", status, err)
	}
	if err := cluster.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
		t.Fatal("Terminate left the delay")
	}
}

// A tc command whose stop is unconfirmed fences the lease: nothing runs in
// the namespace until the canceler confirms it.
func TestNetworkDelayUnconfirmedApplyIsFenced(t *testing.T) {
	netem := newFakeNetem()
	netem.onAdd = func(apply func() supervised.Report) (supervised.Report, error) {
		return supervised.Report{}, &supervised.NotStoppedError{Marker: "m1", Err: errors.New("cancel failed")}
	}
	netem.cancelErr = errors.New("the helper is frozen")
	cluster, osd := delayCluster(netem)
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
	if lease == nil || err == nil || lease.pending != "m1" {
		t.Fatalf("got %+v, %v", lease, err)
	}
	commands := len(netem.commands)
	if err := lease.Restore(t.Context()); err == nil || len(netem.commands) != commands {
		t.Fatalf("Restore ran %v before the earlier tc was confirmed stopped: %v", netem.commands[commands:], err)
	}
	// The late command lands; once confirmed, Restore removes it.
	netem.setRoot("osd-0", &fakeQdisc{kind: "netem", handle: lease.handle, delay: testDelay.Delay})
	netem.cancelErr = nil
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
		t.Fatal("Restore left the delay")
	}
}

// A root qdisc that someone else installed is refused, not removed.
func TestNetworkDelayRestoreRefusesAReplacedRoot(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
	if err != nil {
		t.Fatal(err)
	}
	foreign := &fakeQdisc{kind: "tbf", handle: "8001:"}
	netem.setRoot("osd-0", foreign)
	if status, err := lease.Status(t.Context()); err != nil || status.Applied || status.RootKind != "tbf" {
		t.Fatalf("status %+v, %v", status, err)
	}
	if err := lease.Restore(t.Context()); err == nil || netem.root("osd-0") != foreign || netem.liveHelpers() != 1 {
		t.Fatalf("Restore replaced a foreign root: %v", err)
	}
	netem.setRoot("osd-0", nil)
	if err := lease.Restore(t.Context()); err != nil || netem.liveHelpers() != 0 {
		t.Fatalf("Restore after the foreign root left: %v", err)
	}
}

// A restart replaces the namespace and ends the delay; Restore must not touch
// the new namespace.
func TestNetworkDelayEndsWithARestart(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
	if err != nil {
		t.Fatal(err)
	}
	netem.restart("osd-0")
	foreign := &fakeQdisc{kind: "tbf", handle: "8001:"}
	netem.setRoot("osd-0", foreign)
	if status, err := lease.Status(t.Context()); err != nil || !status.Gone || status.Applied {
		t.Fatalf("status %+v, %v", status, err)
	}
	commands := len(netem.commands)
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, command := range netem.commands[commands:] {
		if strings.HasPrefix(command, "tc ") {
			t.Fatalf("Restore ran %q after the restart", command)
		}
	}
	if netem.root("osd-0") != foreign || netem.liveHelpers() != 0 {
		t.Fatal("Restore changed the new namespace or kept the helper")
	}
}

// Disconnecting and reconnecting the target replaces its interface: the delay
// ended with the old one, and a qdisc on the new one is not the lease's.
func TestNetworkDelayEndsWithAReconnect(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
	if err != nil {
		t.Fatal(err)
	}
	netem.mu.Lock()
	links := netem.targets["osd-0"].namespace.links
	foreign := &fakeQdisc{kind: "tbf", handle: "8001:"}
	links[1] = &fakeLink{name: "eth0", index: links[1].index + 100, mac: links[1].mac, address: links[1].address, root: foreign}
	netem.mu.Unlock()
	if status, err := lease.Status(t.Context()); err != nil || !status.Gone {
		t.Fatalf("status %+v, %v", status, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if netem.root("osd-0") != foreign || netem.liveHelpers() != 0 {
		t.Fatal("Restore changed the new interface or kept the helper")
	}
}

// A helper that died or was removed is replaced in the same namespace, so
// that the qdisc it leaves behind is still removed.
func TestNetworkDelayRestoreReplacesALostHelper(t *testing.T) {
	for name, lose := range map[string]func(*fakeNetemHelper){
		"exited":  func(helper *fakeNetemHelper) { helper.running = false },
		"removed": func(helper *fakeNetemHelper) { helper.removed = true },
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			cluster, osd := delayCluster(netem)
			lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
			if err != nil {
				t.Fatal(err)
			}
			netem.mu.Lock()
			lose(lease.helper.(*fakeNetemHelper))
			netem.mu.Unlock()
			if err := lease.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
				t.Fatalf("root %+v and %d helpers after Restore", netem.root("osd-0"), netem.liveHelpers())
			}
		})
	}
}

// Terminate restores delays first; one that finishes applying after Terminate
// started is restored by the call itself.
func TestNetworkDelayTerminate(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	if _, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if netem.root("osd-0") != nil || netem.liveHelpers() != 0 || len(cluster.faultsOf(faulthook.NetworkDelay)) != 0 {
		t.Fatal("Terminate left a delay")
	}

	netem = newFakeNetem()
	cluster, osd = delayCluster(netem)
	netem.onAdd = func(apply func() supervised.Report) (supervised.Report, error) {
		if err := cluster.Terminate(context.Background()); err != nil {
			t.Error(err)
		}
		return apply(), nil
	}
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
	if !errors.Is(err, nativeTerminated()) || lease == nil {
		t.Fatalf("got %v, %v", lease, err)
	}
	if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
		t.Fatal("a delay applied during Terminate remained")
	}
}

// Only one call at a time may delay an endpoint.
func TestNetworkDelayReservesTheEndpoint(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	second := make(chan error, 1)
	netem.onAdd = func(apply func() supervised.Report) (supervised.Report, error) {
		_, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
		second <- err
		return apply(), nil
	}
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-second; err == nil || !strings.Contains(err.Error(), "being applied") {
		t.Fatalf("a concurrent delay: %v", err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func (h *fakeNetemHelper) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	return nil
}

// A new delay of an endpoint whose lease is being restored is refused at once;
// it must not wait for the lease while holding the cluster's lock, which
// Restore takes last.
func TestNetworkDelayReserveDuringRestore(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
	if err != nil {
		t.Fatal(err)
	}
	removing, proceed := make(chan struct{}), make(chan struct{})
	netem.onRemove = func() {
		close(removing)
		<-proceed
		netem.onRemove = nil
	}
	restored := make(chan error, 1)
	go func() { restored <- lease.Restore(context.Background()) }()
	<-removing
	second := make(chan error, 1)
	go func() {
		_, err := cluster.ApplyNetworkProfile(context.Background(), osd, testDelay)
		second <- err
	}()
	select {
	case err := <-second:
		if err == nil || !strings.Contains(err.Error(), "restore the existing") {
			t.Fatalf("a delay during Restore: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(proceed)
		select {
		case <-restored:
		case <-time.After(2 * time.Second):
			t.Fatal("ApplyNetworkProfile and Restore deadlocked")
		}
		t.Fatal("ApplyNetworkProfile waited for a lease that is being restored")
	}
	close(proceed)
	if err := <-restored; err != nil {
		t.Fatal(err)
	}
}

// The interface stays the delayed one when its address, MAC or name changes.
func TestNetworkDelayFollowsTheInterfaceIndex(t *testing.T) {
	for name, change := range map[string]func(*fakeLink){
		"address": func(link *fakeLink) { link.address = netip.MustParseAddr("10.88.0.99") },
		"mac":     func(link *fakeLink) { link.mac = "02:42:0a:58:00:99" },
		"name":    func(link *fakeLink) { link.name = "veth9" },
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			cluster, osd := delayCluster(netem)
			lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
			if err != nil {
				t.Fatal(err)
			}
			netem.mu.Lock()
			link := netem.targets["osd-0"].namespace.links[1]
			change(link)
			netem.mu.Unlock()
			if status, err := lease.Status(t.Context()); err != nil || status.Gone || !status.Applied || status.Interface != link.name {
				t.Fatalf("status %+v, %v", status, err)
			}
			if err := lease.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
				t.Fatal("Restore left the delay")
			}
		})
	}
}

// Helpers of a call canceled before it returned a lease are still removed.
func TestNetworkDelayCanceledPreflightRemovesItsHelper(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	ctx, cancel := context.WithCancel(t.Context())
	netem.onStart = func(container.NetworkMode) { cancel() }
	if lease, err := cluster.ApplyNetworkProfile(ctx, osd, testDelay); !errors.Is(err, context.Canceled) || lease != nil {
		t.Fatalf("got %v, %v", lease, err)
	}
	if len(netem.helpers) == 0 || netem.liveHelpers() != 0 {
		t.Fatalf("%d of %d helpers left", netem.liveHelpers(), len(netem.helpers))
	}
}

// A replacement helper runs the image that was checked, by its ID, even when
// the tag now names another image.
func TestNetworkDelayReplacesTheHelperFromTheCheckedImage(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
	if err != nil {
		t.Fatal(err)
	}
	netem.mu.Lock()
	netem.tags[testDelay.HelperImage], netem.broken = "sha256:retagged", "sha256:retagged"
	lease.helper.(*fakeNetemHelper).removed = true
	netem.mu.Unlock()
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
		t.Fatal("Restore left the delay")
	}
	for _, helper := range netem.helpers[2:] {
		if helper.image != "sha256:helper" {
			t.Fatalf("a helper ran %s", helper.image)
		}
	}
}

// A helper that does not run the checked image is removed and refused.
func TestNetworkDelayRefusesAHelperOfAnotherImage(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	netem.onStart = func(mode container.NetworkMode) {
		if mode != "none" {
			netem.tags["sha256:helper"] = "sha256:other"
		}
	}
	if lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay); err == nil || lease != nil {
		t.Fatalf("got %v, %v", lease, err)
	}
	if netem.liveHelpers() != 0 || netem.root("osd-0") != nil {
		t.Fatal("a helper or a change remained")
	}
}

func TestNetemHelperOptions(t *testing.T) {
	var request testcontainers.GenericContainerRequest
	for _, option := range netemHelperOptions("container:osd-0") {
		if err := option.Customize(&request); err != nil {
			t.Fatal(err)
		}
	}
	config, host := &container.Config{User: "nobody"}, &container.HostConfig{}
	request.ConfigModifier(config)
	request.HostConfigModifier(host)
	if config.User != "0:0" || host.NetworkMode != "container:osd-0" || !slices.Equal(host.CapDrop, []string{"ALL"}) ||
		!slices.Equal(host.CapAdd, []string{"NET_ADMIN"}) || host.Init == nil || !*host.Init || host.Privileged {
		t.Fatalf("helper config %+v, host config %+v", config, host)
	}
	if request.Labels["org.testcontainers.ceph.role"] != "netem-helper" || !slices.Equal(request.Entrypoint, []string{"sleep"}) {
		t.Fatalf("helper request %+v", request.ContainerRequest)
	}
}

// A helper whose preflight report is malformed is refused like any other
// unsupported helper: with an error, its helper removed and the endpoint free.
func TestNetworkDelayRefusesMalformedPreflightReports(t *testing.T) {
	for name, edit := range map[string]func(map[string]any){
		"no delete result":      func(out map[string]any) { delete(out, "delete") },
		"empty delete result":   func(out map[string]any) { out["delete"] = []any{} },
		"short delete result":   func(out map[string]any) { out["delete"] = []any{0} },
		"failed delete":         func(out map[string]any) { out["delete"] = []any{2, "RTNETLINK answers: Device or resource busy"} },
		"string add code":       func(out map[string]any) { out["add"] = []any{"0", ""} },
		"fractional add code":   func(out map[string]any) { out["add"] = []any{0.5, ""} },
		"numeric add text":      func(out map[string]any) { out["add"] = []any{0, 7} },
		"no add result":         func(out map[string]any) { delete(out, "add") },
		"no missing list":       func(out map[string]any) { delete(out, "missing") },
		"no broken list":        func(out map[string]any) { delete(out, "broken") },
		"non-netem shown qdisc": func(out map[string]any) { out["shown"] = `[{"kind":"noqueue","root":true}]` },
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			netem.editPreflight = edit
			cluster, osd := delayCluster(netem)
			lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
			if err == nil || lease != nil {
				t.Fatalf("got %v, %v", lease, err)
			}
			if name != "failed delete" && !errors.Is(err, ErrNetemImageUnsupported) {
				t.Fatalf("not reported as an unsupported helper: %v", err)
			}
			for _, command := range netem.commands {
				if strings.HasPrefix(command, "tc ") {
					t.Fatalf("ran %q on the target", command)
				}
			}
			if netem.liveHelpers() != 0 || netem.root("osd-0") != nil || len(cluster.faultsOf(faulthook.NetworkDelay)) != 0 {
				t.Fatalf("%d helpers, root %+v, reservations %v", netem.liveHelpers(), netem.root("osd-0"), cluster.faultsOf(faulthook.NetworkDelay))
			}
		})
	}
}
