package ceph

import "context"

// Observer groups the read-only live observations of one selected target: a
// cluster (Observe) or a tenant lease (ObserveTenant). Both forms have this
// one type and the same queries.
//
// An Observer holds a reference that locates the target, not a result. Each
// query resolves the target again and reads the cluster's actual state at that
// moment, so constructing an Observer captures nothing and a retained Observer
// never returns an earlier answer. A query is not an atomic cluster-wide
// snapshot and is no more consistent than the observation it runs.
//
// A tenant-backed Observer validates its lease on every query. Once the
// tenant is released, or its shared cluster is terminated, every query returns
// ErrInvalidRequest, however long the Observer was kept. A cluster-backed
// Observer has no lease: after Terminate its queries fail with the error of the
// command path, as the command path reports it.
//
// An Observer exposes only its queries. It never exposes the cluster, its
// credentials or an executor, owns no resource and needs no cleanup. It is the
// only way to read these live observations: the Container and Tenant have no
// method of the same names.
//
// The Observer stays in package ceph. Its queries need the captured control
// handle, the topology and configuration locks and the shared-cluster
// admission, which are internal to the package.
type Observer struct {
	// target resolves the cluster for one query and applies the admission of
	// the form: the tenant form checks its lease here.
	target func() (*Container, error)
	// tenant is the lease of the tenant form, or nil. The Pool values that the
	// form lists carry it.
	tenant *Tenant
}

// Observe returns an Observer for cluster. A nil cluster returns an error and
// a nil Observer. Nothing is read from the cluster.
func Observe(cluster *Container) (*Observer, error) {
	if cluster == nil {
		return nil, invalidRequest("observe: the cluster is nil")
	}
	return &Observer{target: func() (*Container, error) { return cluster, nil }}, nil
}

// ObserveTenant returns an Observer for tenant's shared cluster. A nil tenant
// returns an error and a nil Observer. The tenant is not validated or read
// here; every query validates it.
func ObserveTenant(tenant *Tenant) (*Observer, error) {
	if tenant == nil {
		return nil, invalidRequest("observe: the tenant is nil")
	}
	return &Observer{target: tenant.container, tenant: tenant}, nil
}

// query resolves the target for one query and returns the CLI contract bound
// to it. The control container is still selected on every command.
func (o *Observer) query() (cliQuery, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.cephCLI, nil
}

// cluster resolves the target for one query. The tenant form checks its lease
// here, so every query of every group validates it.
func (o *Observer) cluster() (*Container, error) {
	if o == nil || o.target == nil {
		return nil, invalidRequest("observe: the observer is not constructed by Observe or ObserveTenant")
	}
	return o.target()
}

// Status queries the monitor's JSON API through the container-local CLI.
func (o *Observer) Status(ctx context.Context) (Status, error) {
	run, err := o.query()
	if err != nil {
		return Status{}, err
	}
	return readStatus(ctx, run)
}

// ManagerStatus reads active ownership and registered standbys from the
// native MGR map.
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
// A Pool of a tenant-backed Observer remembers the tenant lease that listed it
// and checks that lease on every operation, reads included: once the tenant is
// released or its shared cluster is terminated, every operation returns an
// error that matches ErrInvalidRequest without reaching the cluster. Its
// operations that change the pool are always refused under the tenant's
// shared-cluster policy (ErrSharedCluster), also when the cluster has been
// lent to a later exclusive lease. The usage of SharedPool includes the
// objects of the other tenants.
func (o *Observer) Pools(ctx context.Context) ([]*Pool, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	pools, err := c.pools(ctx)
	if err != nil {
		return nil, err
	}
	if o.tenant != nil {
		lease := &poolLease{tenant: o.tenant}
		for _, pool := range pools {
			pool.lease = lease
		}
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
func (o *Observer) Configuration(ctx context.Context) ([]ConfigEntry, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.observedConfiguration(ctx)
}

// BlocklistEntries lists exact entries and CIDR ranges without mutating them.
func (o *Observer) BlocklistEntries(ctx context.Context) ([]BlocklistEntry, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.observedBlocklistEntries(ctx)
}

// MGRModules lists configured and available modules, sorted by name. Enabled
// and CanRun do not prove command readiness; WaitMGRModuleReady probes selected
// built-in modules, and callers must probe their intended commands for others.
func (o *Observer) MGRModules(ctx context.Context) ([]MGRModuleState, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.observedMGRModules(ctx)
}

// OSDFlags lists exact global OSDMap flag tokens, including native compatibility
// flags that are not among the OSDFlag constants. Per-OSD, CRUSH-node or
// device-class flags are separate native state.
func (o *Observer) OSDFlags(ctx context.Context) ([]OSDFlag, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.osdFlags(ctx)
}

// OSDStates returns owned OSD states sorted by native ID. It refuses an owned
// ID that is missing, purged or now belongs to a different native UUID.
func (o *Observer) OSDStates(ctx context.Context) ([]OSDState, error) {
	c, err := o.cluster()
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
func (o *Observer) ManagerServices(ctx context.Context) (map[string]string, error) {
	c, err := o.cluster()
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
// presence under configMu, so a replacement control container is never used
// mid-query; MonitorBootstrapAddresses compares the same original FSID.

// QuorumStatus reads the monitor quorum and monmap. It takes no topology lock
// and selects the control container for each read.
func (o *Observer) QuorumStatus(ctx context.Context) (QuorumStatus, error) {
	c, err := o.cluster()
	if err != nil {
		return QuorumStatus{}, err
	}
	return c.quorumStatus(ctx)
}

// MonitorBootstrapAddresses returns the current majority monmap's versioned
// address vectors, suitable for the Ceph mon_host setting. The live cluster
// FSID must match this fixture's original bootstrap configuration. This is a
// bounded native observation, not a network attachment or membership mutation.
func (o *Observer) MonitorBootstrapAddresses(ctx context.Context) (string, error) {
	c, err := o.cluster()
	if err != nil {
		return "", err
	}
	return c.monitorBootstrapAddresses(ctx)
}

// HealthDetails reads the original cluster's health through one captured
// control CLI handle, checking the bootstrap FSID before and after the query.
// It works before storage or MGR readiness, performs no writes or retries and
// returns a zero snapshot on every failure, including post-query cancellation.
// Native command output is omitted from errors because it may contain secrets.
// External native/configuration writers must not race this observation: the
// three reads are not an atomic transaction. Poll with a caller deadline when
// asserting eventual health after changing a test topology.
func (o *Observer) HealthDetails(ctx context.Context) (HealthSnapshot, error) {
	c, err := o.cluster()
	if err != nil {
		return HealthSnapshot{}, err
	}
	return c.healthDetails(ctx)
}

// FullRatios reads native thresholds through one captured control CLI handle,
// checking the original bootstrap FSID before and after the map query. Native
// zero, equal and misordered thresholds remain observable. It makes no writes,
// retries or background workers and returns a zero snapshot on any failure.
// These separate reads are not atomic; external configuration writers must not
// race the observation. Poll HealthDetails for eventual OSD fullness reports.
func (o *Observer) FullRatios(ctx context.Context) (FullRatioSnapshot, error) {
	c, err := o.cluster()
	if err != nil {
		return FullRatioSnapshot{}, err
	}
	return c.fullRatios(ctx)
}

// The queries below run `rados` in the client container or a supervised scrape
// in the control container. Each one resolves the target (validating the tenant
// lease) and runs the read of the resolved cluster, so argument
// validation, topology lock, liveness check, client or control container
// selection, bounds and errors are the existing ones. A failed or refused
// collection is an error; it is never returned as an empty result.

// PGInconsistencies lists the objects that the last deep scrub of a PG found
// inconsistent. An empty result means that scrub found none; it is not a new
// scrub. A PG whose results are no longer available returns an error.
func (o *Observer) PGInconsistencies(ctx context.Context, pgid PGID) ([]InconsistentObject, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.pgInconsistencies(ctx, pgid)
}

// ObjectWatchers lists the clients watching one object, including clients
// outside this fixture. Namespace is empty for the default namespace. It
// reads native state only and grants no ownership.
func (o *Observer) ObjectWatchers(ctx context.Context, pool, namespace, object string) ([]ObjectWatcher, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.objectWatchers(ctx, pool, namespace, object)
}

// ManagerMetrics scrapes the active manager's prometheus module once and
// parses the Prometheus text exposition format. It never enables the module:
// enable it with OverrideMGRModule first, and wait until ManagerServices
// lists it. A module that serves no endpoint is ErrInvalidRequest. The
// scrape runs in the control container, on the cluster's network, at the
// address that ManagerServices advertises; it is bounded to 30 seconds and
// 32 MiB, and a failed fetch or unparsable output is a *NativeFailure or an
// error.
func (o *Observer) ManagerMetrics(ctx context.Context) (ManagerMetrics, error) {
	c, err := o.cluster()
	if err != nil {
		return ManagerMetrics{}, err
	}
	return c.managerMetrics(ctx)
}
