//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The types below are the private, test-side implementation of the injected
// effects interface of the MON-step driver (upgradeVolumeMonEffects) for the
// "option 1b" experiment of #64: the retained MON store lives on a Docker
// volume that this test owns and that both the source and the destination MON
// containers mount. Nothing in package ceph knows about the volume.
//
// What is Docker-free here is all decision logic. Every Docker operation goes
// through upgradeMonEngine, which fixtures replace by a fake. The native
// engine is in the optional scenario file, is compiled but never run locally,
// and is not evidence of anything until its owner-triggered run.
//
// How the experiment reaches a volume without a production change. The first
// MON of ceph.Run gets the caller's testcontainers customizers after the
// module's own (ceph/ceph.go runMonitor appends opts after the mon.sh
// options), and testcontainers.WithMounts appends, so a caller-supplied volume
// mount at the MON data directory reaches MON "a". MON "b" and later come from
// AddMonitor, which takes only DaemonOptions values and builds its container
// options itself (ceph/topology.go AddMonitor), so a volume cannot reach them
// from outside the package. The experiment therefore replaces MON "a" only.
//
// Rules the effects keep:
//
//   - Fail closed on the store: the destination entrypoint refuses to run when
//     store.db is absent, so mkfs and a membership change are impossible.
//     mon-join.sh cannot be reused (it is inside ceph/internal/scripts, which
//     this package cannot import), and the retained branch of that script is
//     only the foreground ceph-mon command.
//   - Fail closed on the address: the host ports of the retained monmap are
//     re-bound by an allocator that listens on exactly those ports before the
//     destination exists. A port that is taken makes the lease fail and
//     nothing is created. The lease is released immediately before the
//     destination is created, as ceph.Run releases its own lease before
//     starting a host-mode MON, so a short window remains in which another
//     process could bind a port. The monmap pins the address, so that process
//     can only make ceph-mon fail to bind (the destination exits and Observe
//     reports it); it cannot move the MON to another address.
//   - An attempt is recorded under a deterministic container name before the
//     create call. An unknown create or start outcome keeps that name, is
//     resolved only by inspecting it, and is never replayed.
//   - The volume is released last, only when every container that mounted it
//     is absent, and a volume whose identity changed is never removed.

const (
	upgradeMonStoreOwnerLabel = "org.testcontainers.ceph.upgrade-mon-store-owner"
	upgradeMonRunDir          = "/var/run/ceph"
	upgradeLeasePrefix        = "TC_UPGRADE_EXACT_LEASE "
)

// upgradeMonDataDir is the MON data directory of the monitor called name.
func upgradeMonDataDir(name string) string { return "/var/lib/ceph/mon/ceph-" + name }

// ---- engine seam ----------------------------------------------------------------

type upgradeEngineMount struct {
	Volume   string
	Target   string
	ReadOnly bool
}

type upgradeEngineContainer struct {
	ID          string
	Name        string
	Running     bool
	Status      string // created, running, exited, dead, ...
	ExitCode    int
	HostNetwork bool
	ImageArch   string
	Mounts      []upgradeEngineMount
}

type upgradeEngineVolume struct {
	Name      string
	Driver    string
	CreatedAt string
	Labels    map[string]string
	Options   map[string]string
}

type upgradeLeaseSpec struct {
	Address string
	Ports   []int
}

// upgradeLease holds listening sockets on exactly the requested ports until
// Release succeeds. Bound reports what the allocator itself printed after its
// bind calls, not what was requested.
type upgradeLease interface {
	Bound() (address string, ports []int)
	Release(ctx context.Context) error
}

type upgradeDestSpec struct {
	Name        string
	Image       string
	Env         map[string]string
	Entrypoint  []string
	Files       map[string][]byte
	Mounts      []upgradeEngineMount
	HostNetwork bool
	Labels      map[string]string
}

// upgradeMonEngine is the only way the effects reach Docker. Inspect reports
// ok=false only for a definite not-found.
type upgradeMonEngine interface {
	VolumeCreate(ctx context.Context, name string, labels map[string]string) (upgradeEngineVolume, error)
	VolumeInspect(ctx context.Context, name string) (vol upgradeEngineVolume, ok bool, err error)
	VolumeRemove(ctx context.Context, name string) error
	Inspect(ctx context.Context, ref string) (c upgradeEngineContainer, ok bool, err error)
	Stop(ctx context.Context, ref string, timeout time.Duration) error
	Remove(ctx context.Context, ref string) error
	CopyOut(ctx context.Context, ref, path string) ([]byte, error)
	// ProbeStore reports whether dir/store.db exists on the volume, through a
	// short-lived helper container that the engine removes on every outcome.
	ProbeStore(ctx context.Context, volume, dir string) (bool, error)
	ReserveExact(ctx context.Context, spec upgradeLeaseSpec) (upgradeLease, error)
	Create(ctx context.Context, spec upgradeDestSpec) (id string, err error)
	Start(ctx context.Context, ref string) error
	Exec(ctx context.Context, ref string, argv []string) (stdout string, exit int, err error)
}

// ---- owned store volume ledger ---------------------------------------------------

type upgradeVolumeState int

const (
	upgradeVolumePlanned upgradeVolumeState = iota
	upgradeVolumeCreateAttempted
	upgradeVolumeCreated
	// upgradeVolumeNotCreated: the create call failed and a definite
	// not-found followed. Nothing is owned.
	upgradeVolumeNotCreated
	// upgradeVolumeCreateUnknown: the create call failed and the volume could
	// not be classified. The name stays owned for cleanup.
	upgradeVolumeCreateUnknown
	upgradeVolumeReleaseAttempted
	upgradeVolumeReleased
	// upgradeVolumeReleaseUnknown: the remove call failed. The volume may or
	// may not exist; the release can be retried.
	upgradeVolumeReleaseUnknown
)

func (s upgradeVolumeState) String() string {
	return [...]string{"planned", "create-attempted", "created", "not-created", "create-unknown", "release-attempted", "released", "release-unknown"}[s]
}

// upgradeStoreVolume is the ownership ledger of the one volume that holds the
// retained MON store, modeled on the OSD volume ownership of package ceph: a
// generated name, an owner label, the ordinary local driver without options,
// and a creation time that must not change.
type upgradeStoreVolume struct {
	name, owner, createdAt string
	state                  upgradeVolumeState
	users                  []string
	events                 []string
}

func newUpgradeStoreVolume(runID string) (*upgradeStoreVolume, error) {
	if runID == "" || strings.ContainsAny(runID, " /:\t\n") {
		return nil, fmt.Errorf("run identity %q is not usable in a volume name", runID)
	}
	return &upgradeStoreVolume{name: "tc-ceph-upgrade-mon-" + runID, owner: runID}, nil
}

func (v *upgradeStoreVolume) logf(format string, args ...any) {
	v.events = append(v.events, fmt.Sprintf(format, args...))
}

// identity is the store identity the transition owns: the volume name and its
// creation time. It is empty until the volume was created.
func (v *upgradeStoreVolume) identity() string {
	if v.createdAt == "" {
		return ""
	}
	return "mon-store-volume/" + v.name + "@" + v.createdAt
}

func (v *upgradeStoreVolume) validate(got upgradeEngineVolume) error {
	switch {
	case got.Name != v.name:
		return fmt.Errorf("volume name %q differs from the owned %q", got.Name, v.name)
	case got.Driver != "local":
		return fmt.Errorf("volume %q does not use the ordinary local driver", got.Name)
	case got.Labels[upgradeMonStoreOwnerLabel] != v.owner:
		return fmt.Errorf("volume %q is not owned by this run", got.Name)
	case len(got.Options) != 0:
		return fmt.Errorf("volume %q has driver options", got.Name)
	case got.CreatedAt == "":
		return fmt.Errorf("volume %q reports no creation time", got.Name)
	case v.createdAt != "" && got.CreatedAt != v.createdAt:
		return fmt.Errorf("volume %q creation identity changed", got.Name)
	}
	return nil
}

// create records the attempt before the call. A failed call is classified by
// inspecting the generated name; an unclassifiable failure keeps the name
// owned. The returned error is the creation failure whatever the class.
func (v *upgradeStoreVolume) create(ctx context.Context, eng upgradeMonEngine) error {
	if v.state != upgradeVolumePlanned {
		return fmt.Errorf("volume %s is %s; it is created once", v.name, v.state)
	}
	v.state = upgradeVolumeCreateAttempted
	v.logf("create attempted")
	got, err := eng.VolumeCreate(ctx, v.name, map[string]string{upgradeMonStoreOwnerLabel: v.owner})
	if err == nil {
		if verr := v.validate(got); verr != nil {
			// The volume exists under our name but is not what we asked for:
			// keep it owned for inspection, never use it.
			v.state = upgradeVolumeCreateUnknown
			v.logf("create reply invalid: %v", verr)
			return verr
		}
		v.createdAt, v.state = got.CreatedAt, upgradeVolumeCreated
		v.logf("created %s", v.identity())
		return nil
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	found, ok, ierr := eng.VolumeInspect(cleanup, v.name)
	switch {
	case ierr != nil:
		v.state = upgradeVolumeCreateUnknown
		v.logf("create failed (%v) and the volume could not be classified (%v)", err, ierr)
	case !ok:
		v.state = upgradeVolumeNotCreated
		v.logf("create failed (%v); the volume does not exist", err)
	case v.validate(found) != nil:
		v.state = upgradeVolumeCreateUnknown
		v.logf("create failed (%v); a volume of that name exists but is not ours", err)
	default:
		v.createdAt, v.state = found.CreatedAt, upgradeVolumeCreated
		v.logf("create failed (%v) but the volume exists and is ours", err)
	}
	return fmt.Errorf("create store volume: %w", err)
}

// check re-reads the volume and compares its identity.
func (v *upgradeStoreVolume) Check(ctx context.Context, eng upgradeMonEngine) error {
	if v.state != upgradeVolumeCreated {
		return fmt.Errorf("volume %s is %s, not created", v.name, v.state)
	}
	got, ok, err := eng.VolumeInspect(ctx, v.name)
	switch {
	case err != nil:
		return fmt.Errorf("inspect store volume: %w", err)
	case !ok:
		return fmt.Errorf("store volume %s disappeared", v.name)
	}
	return v.validate(got)
}

// attach records a container that mounts the volume. Release needs every
// recorded user to be absent.
func (v *upgradeStoreVolume) attach(ref string) {
	if !slices.Contains(v.users, ref) {
		v.users = append(v.users, ref)
		v.logf("mounted by %s", ref)
	}
}

// release removes the volume once no recorded user exists. It is a no-op for a
// volume that was never created and does not exist. A refusal keeps the
// ledger as it was; a failed remove is release-unknown and can be repeated.
func (v *upgradeStoreVolume) release(ctx context.Context, eng upgradeMonEngine) error {
	switch v.state {
	case upgradeVolumePlanned, upgradeVolumeNotCreated, upgradeVolumeReleased:
		return nil
	}
	for _, ref := range v.users {
		_, ok, err := eng.Inspect(ctx, ref)
		switch {
		case err != nil:
			return fmt.Errorf("release store volume: cannot tell whether %s still exists: %w", ref, err)
		case ok:
			return fmt.Errorf("release store volume: %s still exists and may mount it", ref)
		}
	}
	got, ok, err := eng.VolumeInspect(ctx, v.name)
	switch {
	case err != nil:
		return fmt.Errorf("release store volume: inspect: %w", err)
	case !ok:
		v.state = upgradeVolumeReleased
		v.logf("release: the volume is already gone")
		return nil
	}
	// A volume of an unknown creation time (create-unknown) is adopted only if
	// it carries our owner label; one that changed identity is never removed.
	if err := v.validate(got); err != nil {
		return fmt.Errorf("release store volume: %w", err)
	}
	v.state = upgradeVolumeReleaseAttempted
	v.logf("release attempted")
	if err := eng.VolumeRemove(ctx, v.name); err != nil {
		v.state = upgradeVolumeReleaseUnknown
		v.logf("release failed: %v", err)
		return fmt.Errorf("remove store volume: %w", err)
	}
	v.state = upgradeVolumeReleased
	v.logf("released")
	return nil
}

// upgradeVolumeReceipt is the volume record emitted on every outcome.
type upgradeVolumeReceipt struct {
	Name, Identity string
	State          string
	Users          []string
	Events         []string
}

func (v *upgradeStoreVolume) Receipt() upgradeVolumeReceipt {
	return upgradeVolumeReceipt{Name: v.name, Identity: v.identity(), State: v.state.String(), Users: slices.Clone(v.users), Events: slices.Clone(v.events)}
}

// ---- exact port lease ------------------------------------------------------------

// upgradeExactLeaseScript listens on exactly the requested ports, on the
// requested address, and prints what it bound. SO_REUSEADDR matches ceph-mon,
// which binds with it: without it a recently closed connection of the source
// MON would make bind fail although ceph-mon could bind. It still fails when
// another process listens on the port. A failed bind exits non-zero before
// printing the line, so a missing line is a failed lease.
const upgradeExactLeaseScript = `import ipaddress, json, signal, socket, sys, time
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
address = sys.argv[1]
ports = [int(p) for p in sys.argv[2:]]
family = socket.AF_INET6 if ipaddress.ip_address(address).version == 6 else socket.AF_INET
sockets = []
for port in ports:
    s = socket.socket(family, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((address, port))
    s.listen(1)
    sockets.append(s)
print('` + upgradeLeasePrefix + `' + json.dumps({'address': address, 'ports': [s.getsockname()[1] for s in sockets]}), flush=True)
while True:
    time.sleep(86400)
`

// parseUpgradeLeaseOutput decodes the allocator's output strictly. A missing,
// null or wrong-typed field, a repeated line, another address or other ports
// is an error, never an empty or matching lease.
func parseUpgradeLeaseOutput(output []byte, spec upgradeLeaseSpec) (string, []int, error) {
	var lines []string
	for _, line := range strings.Split(string(output), "\n") {
		if strings.HasPrefix(line, upgradeLeasePrefix) {
			lines = append(lines, strings.TrimPrefix(line, upgradeLeasePrefix))
		}
	}
	if len(lines) != 1 {
		return "", nil, fmt.Errorf("allocator printed %d lease lines, want exactly one", len(lines))
	}
	var doc struct {
		Address *string `json:"address"`
		Ports   *[]int  `json:"ports"`
	}
	dec := json.NewDecoder(strings.NewReader(lines[0]))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return "", nil, fmt.Errorf("decode lease line: %w", err)
	}
	// More is an iteration check, not an end-of-input check: require EOF.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return "", nil, errors.New("lease line has trailing data")
	}
	switch {
	case doc.Address == nil || *doc.Address == "":
		return "", nil, errors.New("lease line has no address")
	case doc.Ports == nil:
		return "", nil, errors.New("lease line has no ports")
	case *doc.Address != spec.Address:
		return "", nil, fmt.Errorf("lease bound %q, requested %q", *doc.Address, spec.Address)
	case !slices.Equal(*doc.Ports, spec.Ports):
		return "", nil, fmt.Errorf("lease bound ports %v, requested %v", *doc.Ports, spec.Ports)
	}
	return *doc.Address, slices.Clone(*doc.Ports), nil
}

// upgradeLeaseSpecFor builds the exact request from the retained vector: the
// msgr2 port first, then the msgr1 port when the MON has one.
func upgradeLeaseSpecFor(v upgradeMonVector) (upgradeLeaseSpec, error) {
	if err := v.validate(); err != nil {
		return upgradeLeaseSpec{}, err
	}
	if len(v) != 1 || !v[0].IP.Is4() {
		return upgradeLeaseSpec{}, fmt.Errorf("the experiment re-binds one IPv4 endpoint, got %s", v.describe())
	}
	spec := upgradeLeaseSpec{Address: v[0].IP.String(), Ports: []int{int(v[0].V2)}}
	if v[0].V1 != 0 {
		spec.Ports = append(spec.Ports, int(v[0].V1))
	}
	return spec, nil
}

// ---- monmap address vector --------------------------------------------------------

// upgradeMonmapAddr is one entry of a MON's public address vector as the
// quorum status prints it.
type upgradeMonmapAddr struct{ Type, Addr string }

// upgradeVectorFromMonmap builds the MON's address vector from the monmap
// entries. Each host needs exactly one v2 entry and at most one v1 entry; an
// empty list, another type, an unparsable address or a repeated entry is an
// error, never an empty or partial vector.
func upgradeVectorFromMonmap(entries []upgradeMonmapAddr) (upgradeMonVector, error) {
	if len(entries) == 0 {
		return nil, errors.New("monmap entry has no addresses")
	}
	byIP := map[netip.Addr]*upgradeMonAddr{}
	var order []netip.Addr
	for _, entry := range entries {
		ap, err := netip.ParseAddrPort(entry.Addr)
		if err != nil || ap.Port() == 0 {
			return nil, fmt.Errorf("monmap address %q is not host:port", entry.Addr)
		}
		a := byIP[ap.Addr()]
		if a == nil {
			a = &upgradeMonAddr{IP: ap.Addr()}
			byIP[ap.Addr()] = a
			order = append(order, ap.Addr())
		}
		switch entry.Type {
		case "v2":
			if a.V2 != 0 {
				return nil, fmt.Errorf("monmap repeats the v2 address of %s", ap.Addr())
			}
			a.V2 = ap.Port()
		case "v1":
			if a.V1 != 0 {
				return nil, fmt.Errorf("monmap repeats the v1 address of %s", ap.Addr())
			}
			a.V1 = ap.Port()
		default:
			return nil, fmt.Errorf("monmap address type %q is neither v1 nor v2", entry.Type)
		}
	}
	vec := make(upgradeMonVector, 0, len(order))
	for _, ip := range order {
		vec = append(vec, *byIP[ip])
	}
	if err := vec.validate(); err != nil {
		return nil, err
	}
	return vec, nil
}

// ---- destination entrypoint -------------------------------------------------------

// upgradeDestEntrypoint is the whole destination start. It never runs mkfs or a
// monmap update: with the retained store absent it exits before ceph-mon.
// CEPH_RUN_DIR exists so that a Docker-free control can run the script.
const upgradeDestEntrypoint = `set -eu
dir=${CEPH_MON_DATA_DIR}
id=${CEPH_MON_ID}
if [ ! -d "$dir/store.db" ]; then
    echo "tc-upgrade: retained store.db is missing in $dir; refusing to create a store" >&2
    exit 3
fi
mkdir -p "${CEPH_RUN_DIR:-/var/run/ceph}"
exec ceph-mon -f -i "$id"
`

var upgradeCephVersionPattern = regexp.MustCompile(`^ceph version (\d+\.\d+\.\d+)(?: |$)`)

// parseUpgradeCephVersion extracts the full release from `ceph-mon --version`.
// Anything else is an error: an unparsable report is not a version.
func parseUpgradeCephVersion(out string) (string, error) {
	first, _, _ := strings.Cut(strings.TrimSpace(out), "\n")
	m := upgradeCephVersionPattern.FindStringSubmatch(first)
	if m == nil {
		return "", fmt.Errorf("unrecognized version report %q", first)
	}
	return m[1], nil
}

var upgradeConfFSID = regexp.MustCompile(`(?m)^fsid\s*=\s*(\S+)\s*$`)

// ---- effects -----------------------------------------------------------------------

// upgradeMonView is what the cluster's own monmap says about the replaced MON.
type upgradeMonView struct {
	FSID     string
	Addrs    upgradeMonVector
	InQuorum bool
}

type upgradeVolumeMonEffectsConfig struct {
	Engine upgradeMonEngine
	Volume *upgradeStoreVolume
	// RunID makes container names unique to the run.
	RunID string
	// Source is the container reference of the MON that is replaced.
	Source string
	Name   string
	FSID   string
	// Retained is the address vector read from the monmap before the fence.
	Retained  upgradeMonVector
	DestImage string
	// View reads the cluster's monmap entry of the MON and whether it is in
	// quorum. It is called while observing the destination.
	View           func(ctx context.Context) (upgradeMonView, error)
	StopTimeout    time.Duration
	ObserveTimeout time.Duration
	Poll           time.Duration
	// Sleep waits between observations; nil uses a timer.
	Sleep func(ctx context.Context, d time.Duration) error
}

type upgradeLaunchRecord struct {
	Name  string
	State string
}

type upgradeVolumeMonEffects struct {
	cfg upgradeVolumeMonEffectsConfig

	mu       sync.Mutex
	fenced   bool
	conf     []byte
	runVol   string
	arch     string
	lease    upgradeLease
	leased   bool
	vector   upgradeMonVector
	launches map[string]*upgradeLaunchRecord
	order    []string
	events   []string
}

func newUpgradeVolumeMonEffects(cfg upgradeVolumeMonEffectsConfig) (*upgradeVolumeMonEffects, error) {
	switch {
	case isNilUpgradeValue(cfg.Engine):
		return nil, fmt.Errorf("%w: no engine", errUpgradeStepConfig)
	case cfg.Volume == nil:
		return nil, fmt.Errorf("%w: no store volume", errUpgradeStepConfig)
	case cfg.RunID == "" || cfg.Source == "" || cfg.Name == "" || cfg.FSID == "" || cfg.DestImage == "":
		return nil, fmt.Errorf("%w: run, source, MON name, FSID and destination image are required", errUpgradeStepConfig)
	case cfg.View == nil:
		return nil, fmt.Errorf("%w: no monmap view", errUpgradeStepConfig)
	case cfg.StopTimeout <= 0 || cfg.ObserveTimeout <= 0 || cfg.Poll <= 0:
		return nil, fmt.Errorf("%w: timeouts must be positive", errUpgradeStepConfig)
	}
	if _, err := upgradeLeaseSpecFor(cfg.Retained); err != nil {
		return nil, fmt.Errorf("%w: retained vector: %v", errUpgradeStepConfig, err)
	}
	cfg.Retained = slices.Clone(cfg.Retained)
	return &upgradeVolumeMonEffects{cfg: cfg, launches: map[string]*upgradeLaunchRecord{}}, nil
}

func (e *upgradeVolumeMonEffects) logf(format string, args ...any) {
	e.events = append(e.events, fmt.Sprintf(format, args...))
}

func (e *upgradeVolumeMonEffects) containerName(instance string) string {
	return "tc-upgrade-" + e.cfg.RunID + "-" + instance
}

func (e *upgradeVolumeMonEffects) storeDir() string { return upgradeMonDataDir(e.cfg.Name) }

// Fence stops and removes the source MON container, after proving that it is
// the one that mounts the owned volume at the store directory, in host
// networking, with a complete identity. The configuration file is read before
// the stop. A failed stop is classified by inspecting the container and is
// not retried.
func (e *upgradeVolumeMonEffects) Fence(ctx context.Context, daemon upgradeDaemonID) (upgradeDaemonID, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.fenced {
		return upgradeDaemonID{}, errors.New("the source MON was already fenced")
	}
	if daemon.Role != "mon" || daemon.ID != e.cfg.Name {
		return upgradeDaemonID{}, fmt.Errorf("fence asked for %s, this experiment replaces mon.%s", daemon.key(), e.cfg.Name)
	}
	eng := e.cfg.Engine
	src, ok, err := eng.Inspect(ctx, e.cfg.Source)
	switch {
	case err != nil:
		return upgradeDaemonID{}, fmt.Errorf("inspect source: %w", err)
	case !ok:
		return upgradeDaemonID{}, errors.New("source container is already absent; nothing was fenced by this run")
	case !src.HostNetwork:
		return upgradeDaemonID{}, errors.New("source is not on the host network; the bridge address handoff is not implemented")
	case src.ImageArch == "":
		return upgradeDaemonID{}, errors.New("source image architecture is unknown")
	}
	var store, run string
	for _, m := range src.Mounts {
		switch {
		case m.Target == e.storeDir() && m.Volume == e.cfg.Volume.name && !m.ReadOnly:
			store = m.Volume
		case m.Target == upgradeMonRunDir && m.Volume != "":
			run = m.Volume
		}
	}
	if store == "" {
		return upgradeDaemonID{}, fmt.Errorf("source does not mount the owned volume at %s", e.storeDir())
	}
	if run == "" {
		return upgradeDaemonID{}, fmt.Errorf("source has no run volume at %s", upgradeMonRunDir)
	}
	if err := e.cfg.Volume.Check(ctx, eng); err != nil {
		return upgradeDaemonID{}, err
	}
	e.cfg.Volume.attach(e.cfg.Source)
	conf, err := eng.CopyOut(ctx, e.cfg.Source, "/etc/ceph/ceph.conf")
	if err != nil {
		return upgradeDaemonID{}, fmt.Errorf("read source ceph.conf: %w", err)
	}
	if m := upgradeConfFSID.FindSubmatch(conf); m == nil || string(m[1]) != e.cfg.FSID {
		return upgradeDaemonID{}, errors.New("source ceph.conf has no fsid line or another FSID")
	}
	if !strings.Contains(string(conf), e.cfg.Retained[0].IP.String()) {
		return upgradeDaemonID{}, errors.New("source ceph.conf does not mention the retained address")
	}
	e.logf("fence: stopping %s", e.cfg.Source)
	if err := eng.Stop(ctx, e.cfg.Source, e.cfg.StopTimeout); err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		now, still, ierr := eng.Inspect(cleanup, e.cfg.Source)
		e.logf("fence: stop returned %v; running=%v present=%v inspect=%v", err, now.Running, still, ierr)
		return upgradeDaemonID{}, fmt.Errorf("stop source: %w", err)
	}
	after, ok, err := eng.Inspect(ctx, e.cfg.Source)
	switch {
	case err != nil:
		return upgradeDaemonID{}, fmt.Errorf("inspect stopped source: %w", err)
	case ok && after.Running:
		return upgradeDaemonID{}, errors.New("source still runs after the stop")
	case ok:
		e.logf("fence: source exited with code %d", after.ExitCode)
	}
	if ok {
		if err := eng.Remove(ctx, e.cfg.Source); err != nil {
			e.logf("fence: remove failed: %v", err)
			return upgradeDaemonID{}, fmt.Errorf("remove stopped source: %w", err)
		}
	}
	e.fenced, e.conf, e.runVol, e.arch = true, conf, run, src.ImageArch
	e.logf("fence: source stopped and removed")
	return upgradeDaemonID{Role: "mon", ID: e.cfg.Name, FSID: e.cfg.FSID, StoreID: e.cfg.Volume.identity(), Arch: src.ImageArch}, nil
}

// SourcePresent reports whether the source container exists, from Docker's
// definite answer only.
func (e *upgradeVolumeMonEffects) SourcePresent(ctx context.Context, daemon upgradeDaemonID) (bool, error) {
	if daemon.Role != "mon" || daemon.ID != e.cfg.Name {
		return false, fmt.Errorf("source presence asked for %s", daemon.key())
	}
	_, ok, err := e.cfg.Engine.Inspect(ctx, e.cfg.Source)
	return ok, err
}

// Retained reports whether the volume holds store.db and takes the exact port
// lease for the retained endpoint. The lease is taken here, after the source
// is gone and before any destination exists, and only when the store exists.
// A lease that cannot bind the exact ports is an error and holds nothing.
func (e *upgradeVolumeMonEffects) Retained(ctx context.Context, daemon upgradeDaemonID) (bool, upgradeMonVector, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch {
	case daemon.Role != "mon" || daemon.ID != e.cfg.Name:
		return false, nil, fmt.Errorf("retained store asked for %s", daemon.key())
	case !e.fenced:
		return false, nil, errors.New("the source was not fenced by these effects")
	case e.leased:
		return false, nil, errors.New("the endpoint lease was already taken; it is taken once")
	}
	eng := e.cfg.Engine
	if err := e.cfg.Volume.Check(ctx, eng); err != nil {
		return false, nil, err
	}
	has, err := eng.ProbeStore(ctx, e.cfg.Volume.name, e.storeDir())
	if err != nil {
		return false, nil, fmt.Errorf("probe retained store: %w", err)
	}
	if !has {
		e.logf("retained: no store.db on the volume; no lease taken")
		return false, nil, nil
	}
	spec, err := upgradeLeaseSpecFor(e.cfg.Retained)
	if err != nil {
		return true, nil, err
	}
	lease, err := eng.ReserveExact(ctx, spec)
	if lease != nil {
		// The allocator exists whatever the call returned: keep it owned.
		e.lease, e.leased = lease, true
	}
	if err != nil {
		e.logf("retained: exact lease failed: %v", err)
		return true, nil, fmt.Errorf("re-bind retained endpoint %s: %w", e.cfg.Retained.describe(), err)
	}
	addr, ports := lease.Bound()
	ip, perr := netip.ParseAddr(addr)
	if perr != nil || len(ports) != len(spec.Ports) || len(ports) == 0 || ports[0] <= 0 || ports[0] > 65535 {
		return true, nil, fmt.Errorf("lease reported an unusable endpoint %q %v", addr, ports)
	}
	entry := upgradeMonAddr{IP: ip, V2: uint16(ports[0])}
	if len(ports) == 2 {
		if ports[1] <= 0 || ports[1] > 65535 {
			return true, nil, fmt.Errorf("lease reported an unusable msgr1 port %d", ports[1])
		}
		entry.V1 = uint16(ports[1])
	}
	e.vector = upgradeMonVector{entry}
	e.logf("retained: store.db present; lease holds %s", e.vector.describe())
	return true, slices.Clone(e.vector), nil
}

// Launch releases the lease and creates and starts the destination once. The
// plan must be the handoff's: no mkfs, no monmap update, no membership change,
// and the endpoint of the lease. The attempt is recorded under a deterministic
// name before the first engine call. A failed release creates nothing.
func (e *upgradeVolumeMonEffects) Launch(ctx context.Context, instance string, plan upgradeMonPlan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if instance == "" || e.launches[instance] != nil {
		return fmt.Errorf("instance %q is empty or already attempted", instance)
	}
	if plan.Mkfs || plan.MonmapUpdate || plan.MembershipChange {
		return errors.New("the plan would create a store or change membership; refused")
	}
	if !e.leased || e.lease == nil || len(e.vector) != 1 {
		return errors.New("no endpoint lease is held for the destination")
	}
	want := e.vector[0]
	if plan.Env["CEPH_MON_ID"] != e.cfg.Name || plan.Env["CEPH_PUBLIC_ADDRESS"] != want.IP.String() ||
		plan.Env["CEPH_MON_PORT_V2"] != strconv.Itoa(int(want.V2)) ||
		(want.V1 != 0 && plan.Env["CEPH_MON_PORT_V1"] != strconv.Itoa(int(want.V1))) {
		return errors.New("the plan's endpoint is not the one the lease holds; refused")
	}
	rec := &upgradeLaunchRecord{Name: e.containerName(instance), State: "attempted"}
	e.launches[instance] = rec
	e.order = append(e.order, instance)
	e.cfg.Volume.attach(rec.Name)
	e.logf("launch: attempt %s recorded", rec.Name)
	eng := e.cfg.Engine
	if err := e.lease.Release(ctx); err != nil {
		rec.State = "not-created: lease release failed"
		e.logf("launch: %v", err)
		return fmt.Errorf("release endpoint lease: %w", err)
	}
	e.leased, e.lease = false, nil
	env := map[string]string{"CEPH_MON_DATA_DIR": e.storeDir(), "CEPH_RUN_DIR": upgradeMonRunDir}
	for k, v := range plan.Env {
		env[k] = v
	}
	spec := upgradeDestSpec{
		Name: rec.Name, Image: e.cfg.DestImage, Env: env,
		Entrypoint: []string{"/bin/sh", "-c", upgradeDestEntrypoint},
		Files:      map[string][]byte{"/etc/ceph/ceph.conf": slices.Clone(e.conf)},
		Mounts: []upgradeEngineMount{
			{Volume: e.cfg.Volume.name, Target: e.storeDir()},
			{Volume: e.runVol, Target: upgradeMonRunDir},
		},
		HostNetwork: true,
		Labels:      map[string]string{upgradeMonStoreOwnerLabel: e.cfg.Volume.owner},
	}
	if _, err := eng.Create(ctx, spec); err != nil {
		rec.State = "create-unknown"
		e.logf("launch: create returned %v; the name is kept and resolved by inspection", err)
		return fmt.Errorf("create destination: %w", err)
	}
	rec.State = "created"
	if err := eng.Start(ctx, rec.Name); err != nil {
		rec.State = "start-unknown"
		e.logf("launch: start returned %v", err)
		return fmt.Errorf("start destination: %w", err)
	}
	rec.State = "started"
	e.logf("launch: %s started", rec.Name)
	return nil
}

func (e *upgradeVolumeMonEffects) Wait(ctx context.Context) error {
	if e.cfg.Sleep != nil {
		return e.cfg.Sleep(ctx, e.cfg.Poll)
	}
	t := time.NewTimer(e.cfg.Poll)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Observe reports what the recorded destination natively is, within the
// observe deadline. It returns evidence only when the container runs, mounts
// the owned volume at the store directory, reports its own version, and the
// cluster's monmap shows the retained FSID and exactly the retained address
// vector with the MON in quorum. Absence, an exit, a deadline and a monmap
// that differs are separate errors; none is retried by creating anything.
func (e *upgradeVolumeMonEffects) Observe(ctx context.Context, instance string) (upgradeNativeEvidence, error) {
	e.mu.Lock()
	rec := e.launches[instance]
	e.mu.Unlock()
	if rec == nil {
		return upgradeNativeEvidence{}, fmt.Errorf("instance %q was never attempted", instance)
	}
	ctx, cancel := context.WithTimeout(ctx, e.cfg.ObserveTimeout)
	defer cancel()
	eng := e.cfg.Engine
	var last string
	for {
		c, ok, err := eng.Inspect(ctx, rec.Name)
		switch {
		case err != nil:
			return upgradeNativeEvidence{}, fmt.Errorf("inspect destination: %w", err)
		case !ok:
			e.note("observe: %s does not exist (state %s)", rec.Name, rec.State)
			return upgradeNativeEvidence{}, fmt.Errorf("destination %s does not exist; the creation did not take effect", rec.Name)
		case !c.Running && c.Status != "created":
			e.note("observe: %s exited with code %d", rec.Name, c.ExitCode)
			return upgradeNativeEvidence{}, fmt.Errorf("destination %s is %s (exit code %d); a bind or store failure ends here, no second destination is created", rec.Name, c.Status, c.ExitCode)
		case c.Running:
			ev, err := e.probe(ctx, instance, rec, c)
			if err == nil {
				e.note("observe: %s confirmed natively", rec.Name)
				return ev, nil
			}
			last = err.Error()
			if errors.Is(err, errUpgradeObserveFinal) {
				e.note("observe: %v", err)
				return upgradeNativeEvidence{}, err
			}
		}
		if err := e.Wait(ctx); err != nil {
			e.note("observe: deadline: %s", last)
			return upgradeNativeEvidence{}, fmt.Errorf("destination %s was not confirmed in time (last: %s): %w", rec.Name, last, err)
		}
	}
}

// errUpgradeObserveFinal marks an observation that more waiting cannot fix.
var errUpgradeObserveFinal = errors.New("destination observation is final")

func (e *upgradeVolumeMonEffects) note(format string, args ...any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.logf(format, args...)
}

func (e *upgradeVolumeMonEffects) probe(ctx context.Context, instance string, rec *upgradeLaunchRecord, c upgradeEngineContainer) (upgradeNativeEvidence, error) {
	var store string
	for _, m := range c.Mounts {
		if m.Target == e.storeDir() && !m.ReadOnly {
			store = m.Volume
		}
	}
	if store != e.cfg.Volume.name {
		return upgradeNativeEvidence{}, fmt.Errorf("%w: destination mounts %q at the store directory, not the owned volume", errUpgradeObserveFinal, store)
	}
	if err := e.cfg.Volume.Check(ctx, e.cfg.Engine); err != nil {
		return upgradeNativeEvidence{}, fmt.Errorf("%w: %v", errUpgradeObserveFinal, err)
	}
	out, code, err := e.cfg.Engine.Exec(ctx, rec.Name, []string{"ceph-mon", "--version"})
	if err != nil {
		return upgradeNativeEvidence{}, fmt.Errorf("version exec: %w", err)
	}
	if code != 0 {
		return upgradeNativeEvidence{}, fmt.Errorf("version exec exited %d", code)
	}
	version, err := parseUpgradeCephVersion(out)
	if err != nil {
		return upgradeNativeEvidence{}, fmt.Errorf("%w: %v", errUpgradeObserveFinal, err)
	}
	view, err := e.cfg.View(ctx)
	if err != nil {
		return upgradeNativeEvidence{}, fmt.Errorf("monmap view: %w", err)
	}
	if err := view.Addrs.validate(); err != nil {
		return upgradeNativeEvidence{}, fmt.Errorf("monmap address of mon.%s: %w", e.cfg.Name, err)
	}
	if !view.Addrs.sameSet(e.cfg.Retained) {
		return upgradeNativeEvidence{}, fmt.Errorf("%w: monmap address %s differs from the retained %s", errUpgradeObserveFinal, view.Addrs.describe(), e.cfg.Retained.describe())
	}
	if !view.InQuorum {
		return upgradeNativeEvidence{}, fmt.Errorf("mon.%s is not in quorum yet", e.cfg.Name)
	}
	// The destination must still run: a MON that exited after answering the
	// version query is not evidence.
	again, ok, err := e.cfg.Engine.Inspect(ctx, rec.Name)
	if err != nil || !ok || !again.Running {
		return upgradeNativeEvidence{}, errors.New("destination stopped while it was observed")
	}
	return upgradeNativeEvidence{Instance: instance, Version: version, FSID: view.FSID, StoreID: e.cfg.Volume.identity()}, nil
}

// Dispose removes the recorded destination containers by their deterministic
// names, whatever state the creation call left them in, and releases a lease
// that is still held. It removes nothing it did not record, and the volume is
// not touched here: the scenario releases it after the cluster is gone.
func (e *upgradeVolumeMonEffects) Dispose(ctx context.Context, instances []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var errs []error
	if e.lease != nil {
		if err := e.lease.Release(ctx); err != nil {
			errs = append(errs, fmt.Errorf("release endpoint lease: %w", err))
			e.logf("dispose: lease release failed: %v", err)
		} else {
			e.lease, e.leased = nil, false
			e.logf("dispose: lease released")
		}
	}
	eng := e.cfg.Engine
	for _, instance := range instances {
		rec := e.launches[instance]
		if rec == nil {
			errs = append(errs, fmt.Errorf("instance %q was never attempted; not removed", instance))
			continue
		}
		c, ok, err := eng.Inspect(ctx, rec.Name)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("inspect %s: %w", rec.Name, err))
			e.logf("dispose: %s could not be inspected: %v", rec.Name, err)
			continue
		case !ok:
			e.logf("dispose: %s is absent (state %s)", rec.Name, rec.State)
			continue
		}
		if c.Running {
			if err := eng.Stop(ctx, rec.Name, e.cfg.StopTimeout); err != nil {
				errs = append(errs, fmt.Errorf("stop %s: %w", rec.Name, err))
				e.logf("dispose: stop %s failed: %v", rec.Name, err)
				continue
			}
		}
		if err := eng.Remove(ctx, rec.Name); err != nil {
			errs = append(errs, fmt.Errorf("remove %s: %w", rec.Name, err))
			e.logf("dispose: remove %s failed: %v", rec.Name, err)
			continue
		}
		e.logf("dispose: %s removed", rec.Name)
	}
	return errors.Join(errs...)
}

// upgradeVolumeMonEffectsReceipt is what the effects report on every outcome.
type upgradeVolumeMonEffectsReceipt struct {
	Events    []string
	Launches  map[string]string
	Fenced    bool
	LeaseHeld bool
	Vector    upgradeMonVector
}

func (e *upgradeVolumeMonEffects) Receipt() upgradeVolumeMonEffectsReceipt {
	e.mu.Lock()
	defer e.mu.Unlock()
	r := upgradeVolumeMonEffectsReceipt{Events: slices.Clone(e.events), Launches: map[string]string{}, Fenced: e.fenced, LeaseHeld: e.leased, Vector: slices.Clone(e.vector)}
	for _, name := range e.order {
		r.Launches[name] = e.launches[name].Name + ": " + e.launches[name].State
	}
	return r
}

var _ upgradeMonEffects = (*upgradeVolumeMonEffects)(nil)
