package ceph

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
)

// Observer groups the read-only live observations of one cluster. Each query
// reads current state rather than retaining a snapshot. It owns no resource
// and needs no cleanup. Queries keep the cluster's normal locking, liveness
// and command-outcome checks.
//
// Configuration, BlocklistEntries, MGRModules, OSDFlags, OSDStates and
// ManagerServices take the topology lock and refuse a terminated cluster
// before running a command; MonitorBootstrapAddresses, HealthDetails and
// FullRatios do the same under ctx and keep their original-FSID checks;
// QuorumStatus takes no lock and fails on a terminated cluster with the error
// of the command path; PGInconsistencies and ObjectWatchers run rados in the
// client container. A refused or failed collection is an error, never an empty
// result.
type Observer struct {
	target *Container
}

// Observe returns an Observer for cluster. A nil cluster returns an error and
// a nil Observer. Nothing is read from the cluster.
//
//purpose:observation
func Observe(cluster *Container) (*Observer, error) {
	if cluster == nil {
		return nil, invalidRequest("observe: the cluster is nil")
	}
	return &Observer{target: cluster}, nil
}

// query resolves the target for one query and returns the CLI contract bound
// to it. The control container is still selected on every command.
func (o *Observer) query() (cliQuery, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	return c.conn().ceph, nil
}

// resolve validates the observer's construction for one query.
func (o *Observer) resolve() (*Container, error) {
	if o == nil || o.target == nil {
		return nil, invalidRequest("observe: the observer is not constructed by Observe")
	}
	return o.target, nil
}

// Status queries the monitor's JSON API through the container-local CLI.
//
//purpose:observation
func (o *Observer) Status(ctx context.Context) (Status, error) {
	run, err := o.query()
	if err != nil {
		return Status{}, err
	}
	return readStatus(ctx, run)
}

// ManagerStatus reads active ownership and registered standbys from the
// native MGR map.
//
//purpose:observation
func (o *Observer) ManagerStatus(ctx context.Context) (ManagerStatus, error) {
	run, err := o.query()
	if err != nil {
		return ManagerStatus{}, err
	}
	return readManagerStatus(ctx, run)
}

// DaemonAddresses reads the names and advertised addresses of the cluster's
// MONs, MGRs and OSDs from the native maps. They are the addresses a client
// learns from the cluster, which can differ from the addresses a container
// has: a host-network cluster advertises its host address, and an OSD that
// is down keeps its last addresses.
//
// In bridge mode the addresses are the daemon containers' addresses on the
// fixture network; in host mode they are ClusterFacts.PublicAddress with the
// allocated ports. Standby MGRs advertise no address, and a MGR that has just
// become active advertises its vector only once it is available, so wait for
// ManagerAvailable after a failover. A down OSD keeps the addresses of its
// last boot; one that never booted has empty vectors. The three maps are read
// one after another, not as one snapshot; compare the epochs when the order
// matters. Each EntityAddress has Type (v2, v1 or any), Addr as Ceph prints it
// and Nonce, which MONs leave at 0.
//
//purpose:observation
func (o *Observer) DaemonAddresses(ctx context.Context) (DaemonAddresses, error) {
	run, err := o.query()
	if err != nil {
		return DaemonAddresses{}, err
	}
	return readDaemonAddresses(ctx, run)
}

// Pools lists the pools that the cluster has, including pools that services or
// Ceph created, as one Pool for each. Every Pool identifies its native pool by
// name and native ID, as the Pool of Container.Pool does, and describes only
// what the listing reports (Name, PGNum, Replicas of a replicated pool,
// MinSize and ErasureCodeProfile); Pool.Status reads the full native state
// and Pool.Usage and Pool.PGs the usage and the PGs. The listing is one read of
// the native pool map, so the pools of one result are consistent with each
// other and later reads through them are separate observations.
//
//purpose:observation
func (o *Observer) Pools(ctx context.Context) ([]*Pool, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	pools, err := c.pools(ctx)
	if err != nil {
		return nil, err
	}
	return pools, nil
}

// The queries below take the topology lock before they read, so they run the
// locked read of the resolved cluster instead of the bare cliQuery. Each keeps
// its own acquisition: Configuration, BlocklistEntries,
// MGRModules, OSDFlags and OSDStates take the topology mutex without a context,
// and ManagerServices waits for it under the caller's context. All of them
// refuse a terminated cluster or one without a control container before they
// run a command.

// Configuration lists exact central database entries, including masked entries.
// Use native config show/daemon config get to inspect a running daemon's effective
// value; this method does not claim a stored setting has overridden local config.
//
// Entries can contain sensitive values.
//
//purpose:observation
func (o *Observer) Configuration(ctx context.Context) ([]ConfigEntry, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	return readConfiguration(ctx, c.conn().ceph)
}

// BlocklistEntries lists exact entries and CIDR ranges without mutating them.
//
// It includes entries made by other tools or by Ceph itself, for example when
// CephFS or RBD evicts a client. Range is true for a CIDR range and Until is
// the MON's absolute expiration time. Listing grants no ownership to remove an
// entry; override.AddBlocklist adds one.
//
//purpose:observation
func (o *Observer) BlocklistEntries(ctx context.Context) ([]BlocklistEntry, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	return readBlocklistEntries(ctx, c.conn().ceph)
}

// MGRModules lists configured and available modules, sorted by name. Enabled
// and CanRun do not prove command readiness; WaitMGRModuleReady probes selected
// built-in modules, and callers must probe their intended commands for others.
//
//purpose:observation
func (o *Observer) MGRModules(ctx context.Context) ([]MGRModuleState, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	return readMGRModules(ctx, c.conn().ceph)
}

// OSDFlags lists exact global OSDMap flag tokens, including native compatibility
// flags that are not among the OSDFlag constants. Per-OSD, CRUSH-node or
// device-class flags are separate native state.
//
//purpose:observation
func (o *Observer) OSDFlags(ctx context.Context) ([]OSDFlag, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	return readOSDFlags(ctx, c.conn().ceph)
}

// OSDStates returns owned OSD states sorted by native ID. It refuses an owned
// ID that is missing, purged or now belongs to a different native UUID.
//
//purpose:observation
func (o *Observer) OSDStates(ctx context.Context) ([]OSDState, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	return c.osdStates(ctx)
}

// ManagerServices returns the service URLs, keyed by module name such as
// prometheus or dashboard, that the active manager advertises in the MGR map.
// A module appears only after it has started its listener, and a failover
// replaces the URLs with the new active manager's. The URLs carry the address
// the module bound: a container address reachable from WithClient containers
// in bridge mode, or the configured address in host mode. They are not mapped
// to Docker host ports. An empty map means no module advertises a service.
//
//purpose:observation
func (o *Observer) ManagerServices(ctx context.Context) (map[string]string, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	return c.managerServices(ctx)
}

// The queries below resolve the control container or the captured bootstrap
// identity themselves, with their own lock acquisition and fences, so they too
// run the read of the resolved cluster.
// QuorumStatus takes no topology lock and selects the control container for each
// read; MonitorBootstrapAddresses, HealthDetails and FullRatios wait for the
// topology lock under the caller's context. HealthDetails and FullRatios choose
// one control handle under that lock and read the original FSID and keyring
// presence under the bootstrap-record lock, so a replacement control container is never used
// mid-query; MonitorBootstrapAddresses compares the same original FSID.

// QuorumStatus reads the monitor quorum and monmap. It takes no topology lock
// and selects the control container for each read.
//
// The query runs under a 5-second process limit inside the control container,
// so it returns an error rather than hanging while quorum is lost.
//
//purpose:observation
func (o *Observer) QuorumStatus(ctx context.Context) (QuorumStatus, error) {
	c, err := o.resolve()
	if err != nil {
		return QuorumStatus{}, err
	}
	return daemonMaps{c.conn()}.quorum(ctx)
}

// MonitorBootstrapAddresses returns the current majority monmap's versioned
// address vectors, suitable for the Ceph mon_host setting. The live cluster
// FSID must match this fixture's original bootstrap configuration. This is a
// bounded native observation, not a network attachment or membership mutation.
//
//purpose:observation
func (o *Observer) MonitorBootstrapAddresses(ctx context.Context) (string, error) {
	c, err := o.resolve()
	if err != nil {
		return "", err
	}
	if c == nil {
		return "", errors.New("ceph cluster is unavailable")
	}
	ctx, cancel := c.monitorClientRefreshContext(ctx)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return "", err
	}
	defer c.mu.Unlock()
	addresses, _, err := c.currentMonitorBootstrapAddresses(ctx)
	return addresses, err
}

// HealthDetails reads the original cluster's health through one captured
// control CLI handle, checking the bootstrap FSID before and after the query.
// It works before storage or MGR readiness, performs no writes and no command
// retries (a MON connection that times out before the command is sent is
// reconnected for up to a minute, as in Container.Ceph) and returns a zero
// snapshot on every failure, including post-query cancellation.
// Native command output is omitted from errors because it may contain secrets.
// External native/configuration writers must not race this observation: the
// three reads are not an atomic transaction. Poll with a caller deadline when
// asserting eventual health after changing a test topology.
//
// Assert on a check's code and its Muted flag, since HEALTH_OK can include
// muted warnings; a sticky mute stays after its check clears. Ceph raises many
// checks only after a delay. Messages can contain pool names, paths or other
// application data.
//
//purpose:observation
func (o *Observer) HealthDetails(ctx context.Context) (HealthSnapshot, error) {
	c, err := o.resolve()
	if err != nil {
		return HealthSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return HealthSnapshot{}, err
	}
	if c == nil {
		return HealthSnapshot{}, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return HealthSnapshot{}, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return HealthSnapshot{}, errClusterTerminated
	}
	control, err := c.conn().controlContainer(ctx)
	if err != nil {
		return HealthSnapshot{}, err
	}
	if control == nil {
		return HealthSnapshot{}, errors.New("ceph control container is unavailable")
	}
	identity, err := c.bootstrap.identity(ctx)
	if err != nil {
		return HealthSnapshot{}, err
	}
	fsid, identityErr := identity.fsid, identity.err
	if identityErr != nil || !identity.confirmed {
		return HealthSnapshot{}, clientOperationError(ctx, "ceph original bootstrap identity is unavailable", identityErr)
	}
	query := func(args ...string) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := command(ctx, control, append([]string{"ceph", "--connect-timeout", "5"}, args...)...)
		if err != nil || ctx.Err() != nil {
			return nil, clientOperationError(ctx, "observe Ceph health: native query failed", err)
		}
		return data, nil
	}
	checkFSID := func() error {
		data, err := query("fsid")
		if err != nil {
			return err
		}
		native := strings.TrimSpace(string(data))
		parsed, err := uuid.Parse(native)
		if err != nil || parsed == uuid.Nil || parsed.String() != native || native != fsid {
			return clientOperationError(ctx, "native health FSID differs from the original cluster", err)
		}
		return ctx.Err()
	}
	if err := checkFSID(); err != nil {
		return HealthSnapshot{}, err
	}
	data, err := query("health", "detail", "--format", "json")
	if err != nil {
		return HealthSnapshot{}, err
	}
	snapshot, err := decodeHealthDetails(data, fsid)
	if err != nil || ctx.Err() != nil {
		return HealthSnapshot{}, clientOperationError(ctx, "decode native Ceph health details failed", err)
	}
	if err := checkFSID(); err != nil {
		return HealthSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return HealthSnapshot{}, err
	}
	return snapshot, nil
}

// FullRatios reads native thresholds through one captured control CLI handle,
// checking the original bootstrap FSID before and after the map query. Native
// zero, equal and misordered thresholds remain observable. It makes no writes,
// command retries or background workers (a MON connection that times out
// before a command is sent is reconnected, as in Container.Ceph) and returns a
// zero snapshot on any failure.
// These separate reads are not atomic; external configuration writers must not
// race the observation. Poll HealthDetails for eventual OSD fullness reports.
//
// The ratios are Ceph's float32 values, so do not compare them with float64
// literals for equality. They are OSDMap policy, separate from central
// configuration and pool quotas.
//
//purpose:observation
func (o *Observer) FullRatios(ctx context.Context) (FullRatioSnapshot, error) {
	c, err := o.resolve()
	if err != nil {
		return FullRatioSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return FullRatioSnapshot{}, err
	}
	if c == nil {
		return FullRatioSnapshot{}, errors.New("ceph cluster is unavailable")
	}
	if err := c.lockTopology(ctx); err != nil {
		return FullRatioSnapshot{}, err
	}
	defer c.mu.Unlock()
	session, err := c.fullRatiosSession(ctx)
	if err != nil {
		return FullRatioSnapshot{}, err
	}
	return session.read(ctx)
}

// The queries below run `rados` in the client container or a supervised scrape
// in the control container. Each one resolves the target and runs the read
// of the resolved cluster, so argument
// validation, topology lock, liveness check, client or control container
// selection, bounds and errors are the existing ones. A failed or refused
// collection is an error; it is never returned as an empty result.

// PGInconsistencies lists the objects that the last deep scrub of a PG found
// inconsistent. An empty result means that scrub found none; it is not a new
// scrub. A PG whose results are no longer available returns an error.
//
//purpose:observation
func (o *Observer) PGInconsistencies(ctx context.Context, pgid PGID) ([]InconsistentObject, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	if err := validatePGID(string(pgid)); err != nil {
		return nil, err
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	return readPGInconsistencies(ctx, c.featureClient().radosLookup(), pgid)
}

// ObjectWatchers lists the clients watching one object, including clients
// outside this fixture. Namespace is empty for the default namespace. It
// reads native state only and grants no ownership.
//
// Each watcher's Address has the IP:port/nonce form of BlocklistEntries. A
// client that stops answering keeps its watch until the OSD's
// osd_client_watch_timeout expires (30 seconds by default); until then a
// notify on the object waits for it and times out.
//
//purpose:observation
func (o *Observer) ObjectWatchers(ctx context.Context, pool, namespace, object string) ([]ObjectWatcher, error) {
	c, err := o.resolve()
	if err != nil {
		return nil, err
	}
	if err := validateExistingPoolName(pool); err != nil {
		return nil, err
	}
	if err := validateRADOSArgument("namespace", namespace, true); err != nil {
		return nil, err
	}
	if err := validateRADOSArgument("object", object, false); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.poolPolicyReady(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	return readObjectWatchers(ctx, c.featureClient().radosLookup(), pool, namespace, object)
}

// ManagerMetrics scrapes the active manager's prometheus module once and
// parses the Prometheus text exposition format. It never enables the module:
// enable it with override.SetMGRModule first, and wait until ManagerServices
// lists it. A module that serves no endpoint is ErrInvalidRequest. The
// scrape runs in the control container, on the cluster's network, at the
// address that ManagerServices advertises; it is bounded to 30 seconds and
// 32 MiB, and a failed fetch or unparsable output is a *NativeFailure or an
// error.
//
// A standby's empty page is an error. The module reports cluster-level metrics
// (pools such as ceph_pool_wr, OSD state, health, the MGRs); since Reef it
// leaves out daemon perf counters unless mgr/prometheus/exclude_perf_counters
// is false, and each daemon's own counters are in its AdminSocket perf dump.
// Pool counters come from the MGR's PG statistics, so poll until an expected
// value appears. In host mode the module first needs its own address and port
// per manager.
//
//purpose:observation
func (o *Observer) ManagerMetrics(ctx context.Context) (ManagerMetrics, error) {
	c, err := o.resolve()
	if err != nil {
		return ManagerMetrics{}, err
	}
	return readManagerMetrics(ctx, c.managerServices, c.conn().supervisedLookup())
}
