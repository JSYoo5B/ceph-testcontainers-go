package cephfs

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Config creates one named filesystem with dedicated pools and MDS
// daemons. Name defaults to tc-cephfs, ActiveMDS to one, and StandbyMDS to zero.
// Omitted pool names become <Name>-metadata and <Name>-data. Metadata and the
// default data pool must be replicated; additional pools may use erasure coding
// with AllowOverwrites. Each pool defaults to application=cephfs. Directory
// layouts select additional pools after filesystem creation. StandbyReplay
// assigns up to one journal follower per active rank; extra standbys stay idle.
type Config struct {
	Name                string
	MetadataPool        ceph.PoolConfig
	DataPool            ceph.PoolConfig
	AdditionalDataPools []ceph.PoolConfig
	ActiveMDS           int
	// NoInitialMDS creates the filesystem/pools without a metadata daemon.
	// ActiveMDS and StandbyMDS must be omitted and StandbyReplay false.
	// The first ScaleMDS must request 1 active / 0 standby; later scale is ordinary.
	NoInitialMDS  bool
	StandbyMDS    int
	StandbyReplay bool
}

// MDS is an owned metadata daemon. ID is its static Ceph daemon name;
// MDSStatus reports its current rank and GID. Stop/Start can inject failures.
// Cluster termination removes stopped and partially initialized daemons too.
type MDS struct {
	testcontainers.Container
	ID             string
	FilesystemName string
	identity       *cephFSMDSIdentity
}

// Filesystem describes one filesystem and embeds its first MDS container
// for compatibility with Start. MDSs exposes every owned metadata daemon.
// The cluster owns their lifetime. Clients use libcephfs through WithClient
// without a kernel or FUSE mount on the host.
type Filesystem struct {
	testcontainers.Container
	FilesystemName      string
	MetadataPool        string
	DataPool            string
	AdditionalDataPools []string
	cluster             *ceph.Container
	config              Config
	roster              mdsRoster
	nativeIdentity      *cephFSNativeIdentity
	coldMDS             *cephFSColdMDS
	pinOverrides        map[string]*PinOverrideHandle
	sessionTimeouts     *cephFSSessionTimeoutsState
	// scheduledPaths holds the directories with a snapshot schedule lease
	// that is not restored yet.
	snapshotScheduleMu sync.Mutex
	scheduledPaths     map[string]*SnapshotScheduleLease
}

// MDSStatus records a metadata daemon's current native FSMap identity. GID
// changes after process restart. Ordinary standbys have rank -1; replay standbys
// follow the rank they would replace. Owned identifies this fixture's daemons.
type MDSStatus struct {
	Name  string `json:"name"`
	Rank  int    `json:"rank"`
	GID   uint64 `json:"gid"`
	State string `json:"state"`
	Owned bool   `json:"owned"`
}

// FilesystemStatus contains active ranks and this fixture's ordinary/replay
// standbys. Standbys belonging to another filesystem are never counted.
type FilesystemStatus struct {
	FilesystemName string
	FilesystemID   int64
	MaxMDS         int
	Active         []MDSStatus
	Standby        []MDSStatus
	StandbyReplay  []MDSStatus
}

// startFilesystem creates dedicated pools and ActiveMDS+StandbyMDS daemons
// with filesystem affinity. Every pool and OSD placement is validated before
// mutations. Existing filesystems are rejected instead of modified. Creating a
// second filesystem enables Ceph's global enable_multiple flag; the existing
// default filesystem and its configuration are preserved.
//
// Customizers apply to each MDS and must preserve its identity, config, keyring,
// entrypoint and networking. A non-nil result with an error identifies attempted
// resources; the cluster owns cleanup, including partial daemon startup.
func startFilesystem(ctx context.Context, c *ceph.Container, config Config, opts ...testcontainers.ContainerCustomizer) (*Filesystem, error) {
	facts, err := ceph.FactsOf(c)
	if err != nil {
		return nil, err
	}
	if facts.Images.CephFS == "" || facts.Images.Client == "" {
		return nil, servicehook.Invalid(fmt.Errorf("cephfs and client images are required before filesystem setup"))
	}

	config = resolveFilesystemDefaults(c, config)
	config, err = normalizeCephFSConfig(config)
	if err != nil {
		return nil, fmt.Errorf("configure cephfs: %w", err)
	}
	if err := clusterutil.LockMutex(ctx, &filesystems(c).setupMu); err != nil {
		return nil, err
	}
	defer filesystems(c).setupMu.Unlock()
	if err := validatePoolPlacement(ctx, c, config); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(c))
	defer cancel()
	data, err := c.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("check existing cephfs filesystems: %w", err)
	}
	var existing cephFSMap
	if err := json.Unmarshal(data, &existing); err != nil || existing.Filesystems == nil {
		return nil, fmt.Errorf("decode existing cephfs filesystems: invalid FSMap (decode error: %v)", err)
	}
	for _, fs := range existing.Filesystems {
		if fs.MDSMap.Name == config.Name {
			return nil, servicehook.Invalid(fmt.Errorf("cephfs filesystem %q already exists", config.Name))
		}
	}
	fs := &Filesystem{
		FilesystemName: config.Name, MetadataPool: config.MetadataPool.Name,
		DataPool: config.DataPool.Name, cluster: c, config: config, roster: newMDSRoster(opts),
	}
	for _, pool := range config.AdditionalDataPools {
		fs.AdditionalDataPools = append(fs.AdditionalDataPools, pool.Name)
	}
	var coldFSID string
	if config.NoInitialMDS {
		coldFSID, err = fs.readColdFSID(ctx, nil)
		if err != nil {
			return nil, err
		}
	}
	if err := servicehook.LockTopology(ctx, c); err != nil {
		return nil, err
	}
	if _, exists := filesystems(c).byName[config.Name]; exists || servicehook.Closed(c) {
		servicehook.Unlock(c)
		return nil, servicehook.Invalid(fmt.Errorf("cephfs filesystem %q setup was already attempted or the cluster is terminated", config.Name))
	}
	filesystems(c).byName[config.Name] = fs
	servicehook.Unlock(c)
	for _, pool := range cephFSConfiguredPools(config) {
		if _, err := c.CreatePool(ctx, pool); err != nil {
			return fs, fmt.Errorf("create cephfs pool: %w", err)
		}
	}
	if len(existing.Filesystems) > 0 && !existing.FeatureFlags.EnableMultiple {
		if _, err := c.Ceph(ctx, "fs", "flag", "set", "enable_multiple", "true"); err != nil {
			return fs, fmt.Errorf("enable multiple cephfs filesystems: %w", err)
		}
	}
	// Set rank counts at creation, avoiding a later max_mds change while the
	// filesystem has no active daemon. Ceph 20.2.4's inline replay/affinity setters
	// look up an FSMap entry before fs-new commits it, so apply those afterward.
	// Ceph 19 has no inline setters; it sets the counts right after fs new,
	// still before any MDS starts.
	settings := [][2]string{
		{"allow_standby_replay", strconv.FormatBool(config.StandbyReplay)},
		{"refuse_standby_for_another_fs", "true"},
	}
	create := []string{"fs", "new", config.Name, fs.MetadataPool, fs.DataPool}
	counts := [][2]string{{"max_mds", strconv.Itoa(config.ActiveMDS)}, {"standby_count_wanted", strconv.Itoa(config.StandbyMDS)}}
	if servicehook.CephBefore(c, 20) {
		settings = append(counts, settings...)
	} else {
		create = append(create, "set", counts[0][0], counts[0][1], counts[1][0], counts[1][1])
	}
	if _, err := c.Ceph(ctx, create...); err != nil {
		return fs, fmt.Errorf("create cephfs: %w", err)
	}
	for _, setting := range settings {
		if _, err := c.Ceph(ctx, "fs", "set", config.Name, setting[0], setting[1]); err != nil {
			return fs, fmt.Errorf("set cephfs %s: %w", setting[0], err)
		}
	}
	if err := fs.captureNativePoolIdentity(ctx); err != nil {
		return fs, fmt.Errorf("capture created cephfs identity: %w", err)
	}
	for _, pool := range fs.AdditionalDataPools {
		if _, err := c.Ceph(ctx, "fs", "add_data_pool", config.Name, pool); err != nil {
			return fs, fmt.Errorf("add cephfs data pool %q: %w", pool, err)
		}
	}
	if config.NoInitialMDS {
		if err := fs.captureColdMDS(ctx, coldFSID); err != nil {
			return fs, err
		}
		return fs, nil
	}
	for i := range config.ActiveMDS + config.StandbyMDS {
		if err := fs.startMDS(ctx); err != nil {
			return fs, fmt.Errorf("run cephfs MDS %d: %w", i, err)
		}
	}
	if err := fs.WaitReady(ctx); err != nil {
		return fs, err
	}
	return fs, nil
}

// MDSs returns a snapshot of this filesystem's owned daemons in creation order.
func (fs *Filesystem) MDSs() []*MDS {
	if fs == nil {
		return nil
	}
	if fs.cluster != nil {
		servicehook.Lock(fs.cluster)
		defer servicehook.Unlock(fs.cluster)
	}
	return fs.roster.snapshot()
}

// mdsSnapshot bounds topology admission for context-taking operations. It keeps
// descriptor identity and creation order, including retained cleanup handles.
func (fs *Filesystem) mdsSnapshot(ctx context.Context) ([]*MDS, error) {
	if fs == nil || fs.cluster == nil {
		return nil, errors.New("cephfs cluster is unavailable")
	}
	if err := servicehook.LockTopology(ctx, fs.cluster); err != nil {
		return nil, err
	}
	defer servicehook.Unlock(fs.cluster)
	return fs.roster.snapshot(), nil
}

// ScaleMDS changes the active/standby daemon counts of this existing filesystem
// without recreating its pools or data. Active must be 1..256 and Standby must
// be non-negative. Additional MDSs preserve the initial customizers and affinity.
// Ceph hands retired ranks back to standbys before this method stops surplus
// owned daemons. Ordinary standbys are removed before replay followers.
//
// The explicit topology request acknowledges Ceph's max_mds health-warning
// confirmation (--yes-i-really-mean-it) in this disposable fixture. Run scaling
// with healthy owned daemons and without concurrent external failure injection
// or FSMap edits. An error retains desired counts and every partial resource for
// cluster cleanup; it never deletes the filesystem, its pools or file data.
// Scaling to zero standby leaves the filesystem's replay preference unchanged.
func (fs *Filesystem) ScaleMDS(ctx context.Context, active, standby int) (returnErr error) {
	if err := validateMDSCounts(active, standby); err != nil {
		return err
	}
	if fs == nil || fs.cluster == nil {
		return errors.New("cephfs cluster is unavailable")
	}
	c := fs.cluster
	release, err := lockSetupThenTopology(ctx, c)
	if err != nil {
		return err
	}
	defer release()
	if fs.Container == nil && fs.coldMDS != nil {
		servicehook.Unlock(c)
		return fs.scaleColdMDS(ctx, active, standby, fs.startMDS)
	}
	if servicehook.Closed(c) || filesystems(c).byName[fs.FilesystemName] != fs || fs.Container == nil || fs.nativeIdentity == nil {
		servicehook.Unlock(c)
		return errors.New("cephfs must be an initialized filesystem owned by a running cluster")
	}
	if err := fs.completedMDSReplacementCohort(); err != nil {
		servicehook.Unlock(c)
		return err
	}
	desired := fs.config
	desired.ActiveMDS, desired.StandbyMDS = active, standby
	servicehook.Unlock(c)
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(c))
	defer cancel()
	if _, err := fs.readNativePools(ctx); err != nil {
		return err
	}
	status, err := fs.MDSStatus(ctx)
	if err != nil {
		return err
	}
	daemons, err := fs.mdsSnapshot(ctx)
	if err != nil {
		return err
	}
	if !cephFSMDSSettled(status, status.MaxMDS, len(daemons)) {
		return errors.New("cephfs owned MDSs must be active or registered standbys before scaling")
	}
	for _, daemon := range daemons {
		state, err := daemon.State(ctx)
		if err != nil {
			return fmt.Errorf("inspect mds.%s before scaling: %w", daemon.ID, err)
		}
		if state == nil || !state.Running {
			return fmt.Errorf("mds.%s is not running; restore it before scaling", daemon.ID)
		}
	}
	if err := servicehook.LockTopology(ctx, c); err != nil {
		return err
	}
	if servicehook.Closed(c) || filesystems(c).byName[fs.FilesystemName] != fs || fs.Container == nil || fs.nativeIdentity == nil {
		servicehook.Unlock(c)
		return errors.New("cephfs must be an initialized filesystem owned by a running cluster")
	}
	if err := fs.completedMDSReplacementCohort(); err != nil {
		servicehook.Unlock(c)
		return err
	}
	fs.config = desired
	servicehook.Unlock(c)
	// A replay follower can replace only the rank it follows. Recycle followers
	// into ordinary standbys while growing ranks, then restore the preference.
	replayPaused := false
	if desired.StandbyReplay && active > status.MaxMDS {
		if _, err := c.Ceph(ctx, "fs", "set", fs.FilesystemName, "allow_standby_replay", "false"); err != nil {
			return fmt.Errorf("prepare replay followers for rank growth: %w", err)
		}
		replayPaused = true
		defer func() {
			if replayPaused {
				restoreCtx, restoreCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
				defer restoreCancel()
				_, err := c.Ceph(restoreCtx, "fs", "set", fs.FilesystemName, "allow_standby_replay", "true")
				if err != nil {
					returnErr = errors.Join(returnErr, fmt.Errorf("restore cephfs standby replay: %w", err))
				}
			}
		}()
		if err := clusterutil.Poll(ctx, func() (bool, error) {
			current, err := fs.MDSStatus(ctx)
			if err != nil {
				return false, err
			}
			daemons, err := fs.mdsSnapshot(ctx)
			if err != nil {
				return false, err
			}
			return len(current.StandbyReplay) == 0 && cephFSMDSSettled(current, status.MaxMDS, len(daemons)), nil
		}); err != nil {
			return fmt.Errorf("wait for replay followers to become standbys: %w", err)
		}
	}
	for {
		daemons, err := fs.mdsSnapshot(ctx)
		if err != nil {
			return err
		}
		if len(daemons) >= active+standby {
			break
		}
		if err := fs.startMDS(ctx); err != nil {
			return fmt.Errorf("add cephfs MDS while scaling: %w", err)
		}
	}
	if status.MaxMDS != active {
		if _, err := c.Ceph(ctx, "fs", "set", fs.FilesystemName, "max_mds", strconv.Itoa(active), "--yes-i-really-mean-it"); err != nil {
			return fmt.Errorf("scale cephfs active ranks: %w", err)
		}
	}
	if _, err := c.Ceph(ctx, "fs", "set", fs.FilesystemName, "standby_count_wanted", strconv.Itoa(standby)); err != nil {
		return fmt.Errorf("scale cephfs standby count: %w", err)
	}
	// A retiring rank passes through up:stopping before it respawns as standby.
	// Active count alone would miss that transition; account for every owned
	// daemon before selecting any surplus candidate for retirement.
	if err := clusterutil.Poll(ctx, func() (bool, error) {
		status, err := fs.MDSStatus(ctx)
		if err != nil {
			return false, err
		}
		daemons, err := fs.mdsSnapshot(ctx)
		if err != nil {
			return false, err
		}
		return cephFSMDSSettled(status, active, len(daemons)), nil
	}); err != nil {
		return fmt.Errorf("wait for cephfs rank handoff: %w", err)
	}
	for {
		daemons, err := fs.mdsSnapshot(ctx)
		if err != nil {
			return err
		}
		if len(daemons) <= active+standby {
			break
		}
		status, err := fs.MDSStatus(ctx)
		if err != nil {
			return err
		}
		candidate := cephFSMDSRetirementCandidate(status, daemons, active)
		if candidate == nil {
			return errors.New("no safely registered owned standby is available for cephfs scale-down")
		}
		if err := fs.retireMDS(ctx, *candidate); err != nil {
			return err
		}
	}
	if replayPaused {
		if _, err := c.Ceph(ctx, "fs", "set", fs.FilesystemName, "allow_standby_replay", "true"); err != nil {
			return fmt.Errorf("restore cephfs standby replay: %w", err)
		}
		replayPaused = false
	}
	return fs.WaitReady(ctx)
}

func (fs *Filesystem) startMDS(ctx context.Context) error {
	return fs.startMDSWithService(ctx, fs.startService)
}

// startService starts an MDS container that the cluster owns.
func (fs *Filesystem) startService(ctx context.Context, name, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	return servicehook.StartService(ctx, fs.cluster, name, image, opts...)
}

// The private service seam preserves real daemon/partial publication while
// allowing lifecycle units to avoid Docker. Production always uses startService.
func (fs *Filesystem) startMDSWithService(ctx context.Context, start func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error)) error {
	_, err := fs.startMDSWithAdmission(ctx, start, nil, nil)
	return err
}

// mdsStartAdmission is what the admission phase of startMDSWithAdmission hands
// to the later phases: the reserved daemon name, the customizers read under the
// cluster mutex, whether this start consumed cold permission, and the control
// container selected for the auth command (cold and replacement starts only).
type mdsStartAdmission struct {
	id      string
	opts    []testcontainers.ContainerCustomizer
	cold    bool
	control testcontainers.Container
}

// Admission/publication hooks belong only to explicit replacement. The legacy
// wrapper above passes nil hooks and preserves ordinary and cold behavior.
// Admission and publication run under c.mu; auth and startService run unlocked.
//
// The function runs three phases: admitMDSStart (under c.mu, released before it
// returns), createMDSCredentials (unlocked) and publishStartedMDS (under c.mu).
// The service start between the last two is unlocked. The caller holds setupMu
// for all of it (R1, cephfs/entry.go:100-102).
func (fs *Filesystem) startMDSWithAdmission(ctx context.Context, start func(context.Context, string, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error), admit func(context.Context, string) (testcontainers.Container, error), published func(*MDS)) (*MDS, error) {
	c := fs.cluster
	a, err := fs.admitMDSStart(ctx, admit)
	if err != nil {
		return nil, err
	}
	id, cold := a.id, a.cold
	keyring, err := fs.createMDSCredentials(ctx, a, admit != nil)
	if err != nil {
		return nil, err
	}
	moduleOpts := []testcontainers.ContainerCustomizer{
		testcontainers.WithEnv(map[string]string{"CEPH_MDS_ID": id, "CEPH_FILESYSTEM": fs.FilesystemName}),
		testcontainers.WithEntrypoint("/bin/sh", "/tc/mds.sh"), testcontainers.WithCmd(),
		testcontainers.WithFiles(servicehook.ScriptFile("mds"), clusterutil.TextFile("/etc/ceph/mds.keyring", keyring, 0o600)),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", "/var/run/ceph/ceph-mds." + id + ".asok"}).WithStartupTimeout(servicehook.StartupTimeout(c))),
	}
	moduleOpts = append(moduleOpts, a.opts...)
	ctr, err := start(ctx, "mds."+id, servicehook.ServiceImage(c, "cephfs"), moduleOpts...)
	// Publish only a real handle: a starter may return a typed nil with its
	// error when no container was created.
	ctr = servicehook.DiagnosticHandle(ctr)
	daemon := fs.publishStartedMDS(ctx, id, ctr, err, published)
	if cold && (err != nil || ctr == nil) {
		return nil, clusterutil.OperationError(ctx, "start initial MDS service", err)
	}
	if admit != nil && (err != nil || ctr == nil || ctx.Err() != nil) {
		return daemon, clusterutil.OperationError(ctx, "start replacement MDS service", err)
	}
	return daemon, err
}

// admitMDSStart is the admission phase: it takes the topology mutex, checks that
// the cluster is open and this handle is the registered filesystem, reads the
// next name and the customizers, runs the cold-start ownership and control
// checks and consumes cold permission, runs the replacement admission hook, and
// reserves the name index. The mutex is released on every return, before any
// auth command. Every refusal text and the order of the checks are unchanged.
func (fs *Filesystem) admitMDSStart(ctx context.Context, admit func(context.Context, string) (testcontainers.Container, error)) (*mdsStartAdmission, error) {
	c := fs.cluster
	if err := servicehook.LockTopology(ctx, c); err != nil {
		return nil, err
	}
	if servicehook.Closed(c) || filesystems(c).byName[fs.FilesystemName] != fs {
		servicehook.Unlock(c)
		return nil, servicehook.Invalid(errors.New("cephfs cluster is terminated or filesystem ownership changed"))
	}
	id := fs.roster.nextID(fs.FilesystemName)
	opts := fs.roster.customizers()
	cold := fs.coldMDS != nil && !fs.coldMDS.attempted
	var control testcontainers.Container
	if cold {
		if fs.nativeIdentity != fs.coldMDS.identity || fs.Container != nil || fs.roster.count() != 0 || fs.roster.issued() != 0 {
			servicehook.Unlock(c)
			return nil, errors.New("original cold MDS ownership changed before startup")
		}
		var err error
		control, err = c.ControlContainer(ctx)
		if err != nil || control == nil {
			servicehook.Unlock(c)
			return nil, clusterutil.OperationError(ctx, "select initial MDS auth control", err)
		}
		if err := fs.checkColdSelectedControl(ctx, control); err != nil {
			servicehook.Unlock(c)
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			servicehook.Unlock(c)
			return nil, err
		}
		// All owner/control admission has succeeded. The next call attempts
		// owned native auth mutation; uncertainty cannot authorize another first.
		fs.coldMDS.attempted = true
	}
	if admit != nil {
		var err error
		control, err = admit(ctx, id)
		if err != nil || control == nil {
			servicehook.Unlock(c)
			return nil, clusterutil.OperationError(ctx, "admit replacement MDS startup", err)
		}
	}
	fs.roster.reserveIndex()
	servicehook.Unlock(c)
	return &mdsStartAdmission{id: id, opts: opts, cold: cold, control: control}, nil
}

// createMDSCredentials runs the auth command for the reserved daemon, without
// any lock. A cold or replacement start sends it through the control container
// selected during admission; an ordinary start uses the cluster CLI. The three
// failure texts are the ones the combined function returned.
func (fs *Filesystem) createMDSCredentials(ctx context.Context, a *mdsStartAdmission, replacement bool) ([]byte, error) {
	args := []string{"auth", "get-or-create", "mds." + a.id,
		"mon", "allow profile mds", "mgr", "allow profile mds",
		"osd", "allow rw tag cephfs *=*", "mds", "allow"}
	if a.cold || replacement {
		keyring, err := servicehook.Command(ctx, a.control, append([]string{"ceph", "--connect-timeout", "5"}, args...)...)
		if err != nil {
			if replacement {
				return nil, clusterutil.OperationError(ctx, "create replacement MDS credentials", err)
			}
			return nil, clusterutil.OperationError(ctx, "create initial MDS credentials", err)
		}
		return keyring, nil
	}
	keyring, err := fs.cluster.Ceph(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("create mds.%s credentials: %w", a.id, err)
	}
	return keyring, nil
}

// publishStartedMDS is the publication phase: for a real handle (nil means the
// starter created no container) it takes the cluster mutex, records the daemon
// with its creation identity in the roster, sets the embedded Container if it
// was unset, runs the published hook and releases. It returns the published
// daemon, or nil when there was no handle. A start error does not stop
// publication: partial handles stay owned.
func (fs *Filesystem) publishStartedMDS(ctx context.Context, id string, ctr testcontainers.Container, startErr error, published func(*MDS)) *MDS {
	if ctr == nil {
		return nil
	}
	c := fs.cluster
	servicehook.Lock(c)
	daemon := &MDS{Container: ctr, ID: id, FilesystemName: fs.FilesystemName}
	daemon.identity = fs.captureMDSIdentity(ctx, daemon, ctr, startErr)
	fs.roster.publish(daemon)
	if fs.Container == nil {
		fs.Container = ctr
	}
	if published != nil {
		published(daemon)
	}
	servicehook.Unlock(c)
	return daemon
}

func (fs *Filesystem) retireMDS(ctx context.Context, candidate MDSStatus) error {
	c := fs.cluster
	var daemon *MDS
	daemons, err := fs.mdsSnapshot(ctx)
	if err != nil {
		return err
	}
	for _, owned := range daemons {
		if owned.ID == candidate.Name {
			daemon = owned
			break
		}
	}
	if daemon == nil || !candidate.Owned || candidate.GID == 0 || (candidate.State != "up:standby" && candidate.State != "up:standby-replay") {
		return servicehook.Invalid(errors.New("refuse to retire an unowned or non-standby MDS"))
	}
	grace := 5 * time.Second
	if err := daemon.Stop(ctx, &grace); err != nil {
		return fmt.Errorf("stop surplus mds.%s: %w", daemon.ID, err)
	}
	if _, err := c.Ceph(ctx, "mds", "fail", strconv.FormatUint(candidate.GID, 10)); err != nil {
		return fmt.Errorf("remove surplus mds.%s generation %d: %w", daemon.ID, candidate.GID, err)
	}
	if err := clusterutil.Poll(ctx, func() (bool, error) {
		data, err := c.Ceph(ctx, "fs", "dump", "--format", "json")
		if err != nil {
			return false, err
		}
		return cephFSMDSAbsent(data, candidate.Name, candidate.GID)
	}); err != nil {
		return fmt.Errorf("wait for surplus mds.%s to leave FSMap: %w", daemon.ID, err)
	}
	if err := daemon.Terminate(ctx); !servicehook.OnlyMissingHostResource(err) {
		return fmt.Errorf("terminate surplus mds.%s: %w", daemon.ID, err)
	}
	servicehook.Lock(c)
	service := "mds." + daemon.ID
	if owned := servicehook.Service(c, service); owned != nil && owned.GetContainerID() == daemon.GetContainerID() {
		servicehook.DeleteService(c, service)
	}
	fs.roster.remove(daemon)
	if fs.Container != nil && fs.Container.GetContainerID() == daemon.GetContainerID() {
		fs.Container = nil
		if fs.roster.count() > 0 {
			fs.Container = fs.roster.at(0).Container
		}
	}
	servicehook.Unlock(c)
	if _, err := c.Ceph(ctx, "auth", "del", "mds."+daemon.ID); err != nil {
		// Container ownership is already removed. A failed auth cleanup leaves
		// only its credential in the disposable cluster, not a phantom daemon.
		return fmt.Errorf("remove surplus mds.%s credentials: %w", daemon.ID, err)
	}
	return nil
}

func cephFSMDSSettled(status *FilesystemStatus, active, owned int) bool {
	if status == nil || active < 1 || status.MaxMDS != active || len(status.Active) != active || len(status.Active)+len(status.Standby)+len(status.StandbyReplay) != owned {
		return false
	}
	for rank, daemon := range status.Active {
		if daemon.Rank != rank || !daemon.Owned || daemon.GID == 0 {
			return false
		}
	}
	return true
}

func cephFSMDSRetirementCandidate(status *FilesystemStatus, daemons []*MDS, active int) *MDSStatus {
	if !cephFSMDSSettled(status, active, len(daemons)) {
		return nil
	}
	for _, candidates := range [][]MDSStatus{status.Standby, status.StandbyReplay} {
		for i := len(daemons) - 1; i >= 0; i-- {
			for _, candidate := range candidates {
				if candidate.Name == daemons[i].ID && candidate.Owned && candidate.GID != 0 {
					return &candidate
				}
			}
		}
	}
	return nil
}

func cephFSMDSAbsent(data []byte, name string, gid uint64) (bool, error) {
	var fsmap cephFSMap
	if err := json.Unmarshal(data, &fsmap); err != nil || fsmap.Filesystems == nil {
		return false, errors.New("decode FSMap while retiring MDS")
	}
	for _, standby := range fsmap.Standbys {
		if standby.Name == name || standby.GID == gid {
			return false, nil
		}
	}
	for _, fs := range fsmap.Filesystems {
		for _, info := range fs.MDSMap.Info {
			if info.Name == name || info.GID == gid {
				return false, nil
			}
		}
	}
	return true, nil
}

// MDSStatus reads one native FSMap, so rank ownership and standby state come
// from the same epoch. Active ranks include external daemons with Owned=false;
// standby lists contain only this fixture's daemons with matching affinity.
func (fs *Filesystem) MDSStatus(ctx context.Context) (*FilesystemStatus, error) {
	if fs == nil || fs.cluster == nil {
		return nil, errors.New("cephfs cluster is unavailable")
	}
	daemons, err := fs.mdsSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot owned CephFS MDSs: %w", err)
	}
	data, err := fs.cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		return nil, fmt.Errorf("read cephfs MDS status: %w", err)
	}
	return parseCephFSMDSStatus(data, fs.FilesystemName, daemons)
}

// WaitReady waits for all configured ranks to be owned and active and all
// configured standbys to register. Replay mode also waits for journal followers.
// After a standby takes over a failed active daemon, use MDSStatus to check
// recovered ranks or restart a daemon before WaitReady; readiness includes the
// requested standby capacity.
func (fs *Filesystem) WaitReady(ctx context.Context) error {
	if fs == nil || fs.cluster == nil {
		return errors.New("cephfs cluster is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, servicehook.StartupTimeout(fs.cluster))
	defer cancel()
	if err := clusterutil.Poll(ctx, func() (bool, error) {
		status, err := fs.MDSStatus(ctx)
		if err != nil {
			return false, err
		}
		if err := servicehook.LockTopology(ctx, fs.cluster); err != nil {
			return false, err
		}
		config := fs.config
		servicehook.Unlock(fs.cluster)
		if !cephFSMDSReady(status, config) {
			return false, fmt.Errorf("cephfs %q is not ready: active=%v standby=%v replay=%v max_mds=%d", fs.FilesystemName, status.Active, status.Standby, status.StandbyReplay, status.MaxMDS)
		}
		return true, nil
	}); err != nil {
		return fmt.Errorf("wait for cephfs MDS topology: %w", err)
	}
	return nil
}

func cephFSMDSReady(status *FilesystemStatus, config Config) bool {
	if status.MaxMDS != config.ActiveMDS || len(status.Active) != config.ActiveMDS || len(status.Standby)+len(status.StandbyReplay) != config.StandbyMDS {
		return false
	}
	for rank, mds := range status.Active {
		if mds.Rank != rank || !mds.Owned || mds.GID == 0 {
			return false
		}
	}
	wantedReplay := 0
	if config.StandbyReplay {
		wantedReplay = min(config.ActiveMDS, config.StandbyMDS)
	}
	seenRanks := make(map[int]bool, len(status.StandbyReplay))
	for _, daemon := range status.StandbyReplay {
		if !daemon.Owned || daemon.GID == 0 || daemon.Rank < 0 || daemon.Rank >= config.ActiveMDS || seenRanks[daemon.Rank] {
			return false
		}
		seenRanks[daemon.Rank] = true
	}
	return len(status.StandbyReplay) == wantedReplay
}

type cephFSMap struct {
	FeatureFlags struct {
		EnableMultiple bool `json:"enable_multiple"`
	} `json:"feature_flags"`
	Standbys    []cephFSMDSInfo `json:"standbys"`
	Filesystems []struct {
		ID     int64 `json:"id"`
		MDSMap struct {
			Name         string                   `json:"fs_name"`
			MaxMDS       int                      `json:"max_mds"`
			Info         map[string]cephFSMDSInfo `json:"info"`
			MetadataPool int64                    `json:"metadata_pool"`
			DataPools    []int64                  `json:"data_pools"`
		} `json:"mdsmap"`
	} `json:"filesystems"`
}

type cephFSMDSInfo struct {
	MDSStatus
	JoinFSCID int64 `json:"join_fscid"`
}

func parseCephFSMDSStatus(data []byte, name string, daemons []*MDS) (*FilesystemStatus, error) {
	var fsmap cephFSMap
	if err := json.Unmarshal(data, &fsmap); err != nil {
		return nil, fmt.Errorf("decode cephfs FSMap: %w", err)
	}
	owned := make(map[string]bool, len(daemons))
	for _, daemon := range daemons {
		if daemon != nil && daemon.FilesystemName == name {
			owned[daemon.ID] = true
		}
	}
	for _, filesystem := range fsmap.Filesystems {
		if filesystem.MDSMap.Name != name {
			continue
		}
		status := &FilesystemStatus{FilesystemName: name, FilesystemID: filesystem.ID, MaxMDS: filesystem.MDSMap.MaxMDS}
		for _, info := range filesystem.MDSMap.Info {
			info.Owned = owned[info.Name]
			switch info.State {
			case "up:active":
				status.Active = append(status.Active, info.MDSStatus)
			case "up:standby-replay":
				if info.Owned && info.JoinFSCID == filesystem.ID {
					status.StandbyReplay = append(status.StandbyReplay, info.MDSStatus)
				}
			}
		}
		for _, info := range fsmap.Standbys {
			if owned[info.Name] && info.State == "up:standby" && info.JoinFSCID == filesystem.ID {
				info.Owned = true
				status.Standby = append(status.Standby, info.MDSStatus)
			}
		}
		less := func(a, b MDSStatus) int {
			if a.Rank != b.Rank {
				return cmp.Compare(a.Rank, b.Rank)
			}
			return cmp.Compare(a.Name, b.Name)
		}
		slices.SortFunc(status.Active, less)
		slices.SortFunc(status.Standby, less)
		slices.SortFunc(status.StandbyReplay, less)
		return status, nil
	}
	return nil, fmt.Errorf("cephfs filesystem %q is absent from FSMap", name)
}

func validatePoolPlacement(ctx context.Context, c *ceph.Container, config Config) error {
	if err := servicehook.LockTopology(ctx, c); err != nil {
		return err
	}
	defer servicehook.Unlock(c)
	if servicehook.Closed(c) {
		return servicehook.Invalid(errors.New("ceph cluster is terminated"))
	}
	if c.Container == nil {
		return errors.New("ceph control container is unavailable")
	}
	for _, pool := range cephFSConfiguredPools(config) {
		count := servicehook.PoolPlacementDomains(c, pool)
		needed := pool.Replicas
		if ec := pool.ErasureCode; ec != nil {
			needed = ec.K + ec.M
		}
		if count < needed {
			return fmt.Errorf("cephfs pool %q needs %d distinct %s domains in root %q with device class %q; cluster owns %d", pool.Name, needed, pool.FailureDomain, pool.CRUSHRoot, pool.DeviceClass, count)
		}
	}
	return nil
}

func cephFSConfiguredPools(config Config) []ceph.PoolConfig {
	return append([]ceph.PoolConfig{config.MetadataPool, config.DataPool}, config.AdditionalDataPools...)
}

func normalizeCephFSConfig(config Config) (Config, error) {
	if config.Name == "" {
		config.Name = "tc-cephfs"
	}
	if len(config.Name) > 110 || !naming.PoolResource.MatchString(config.Name) {
		return config, servicehook.Invalid(errors.New("filesystem name must use letters, digits, underscores, dots or dashes, start with a letter, digit or underscore, and fit generated pool names"))
	}
	if config.NoInitialMDS && (config.ActiveMDS != 0 || config.StandbyMDS != 0 || config.StandbyReplay) {
		return config, servicehook.Invalid(errors.New("NoInitialMDS requires omitted active/standby counts and no standby replay"))
	}
	if config.ActiveMDS == 0 {
		config.ActiveMDS = 1
	}
	if err := validateMDSCounts(config.ActiveMDS, config.StandbyMDS); err != nil {
		return config, err
	}
	if config.StandbyReplay && config.StandbyMDS == 0 {
		return config, servicehook.Invalid(errors.New("standby replay requires at least one standby MDS"))
	}
	if config.MetadataPool.Name == "" {
		config.MetadataPool.Name = config.Name + "-metadata"
	}
	if config.DataPool.Name == "" {
		config.DataPool.Name = config.Name + "-data"
	}
	if config.MetadataPool.ErasureCode != nil || config.DataPool.ErasureCode != nil {
		return config, servicehook.Invalid(errors.New("CephFS metadata and default data pools must be replicated; use additional pools for erasure-coded file data"))
	}
	config.AdditionalDataPools = slices.Clone(config.AdditionalDataPools)
	pools := []*ceph.PoolConfig{&config.MetadataPool, &config.DataPool}
	for i := range config.AdditionalDataPools {
		pools = append(pools, &config.AdditionalDataPools[i])
	}
	names := make(map[string]bool, len(pools))
	for _, pool := range pools {
		if pool.Application != "" && pool.Application != "cephfs" {
			return config, servicehook.Invalid(fmt.Errorf("CephFS pool %q must use application cephfs", pool.Name))
		}
		pool.Application = "cephfs"
		resolved, err := servicehook.NormalizePool(*pool)
		if err != nil {
			return config, fmt.Errorf("configure CephFS pool %q: %w", pool.Name, err)
		}
		*pool = resolved
		if names[pool.Name] {
			return config, servicehook.Invalid(fmt.Errorf("CephFS pool name %q is duplicated", pool.Name))
		}
		names[pool.Name] = true
	}
	return config, nil
}

func validateMDSCounts(active, standby int) error {
	// Ceph's MAX_MDS is 0x100; standby_count_wanted is a signed 32-bit field.
	if active < 1 || active > 256 || standby < 0 || uint64(standby) > 1<<31-1 || active > int(^uint(0)>>1)-standby {
		return servicehook.Invalid(errors.New("active MDS count must be 1..256 and standby count 0..2147483647 without overflowing their total"))
	}
	return nil
}

func cephFSMDSID(name string, index int) string {
	if name == "tc-cephfs" && index == 0 {
		return "a"
	}
	return name + "-" + strconv.Itoa(index)
}
