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
// command path, exactly like the Container method that it mirrors.
//
// An Observer exposes only its queries. It never exposes the cluster, its
// credentials or an executor, owns no resource and needs no cleanup. Each
// query keeps the execution path, context handling, locks and error behavior
// of the Container method of the same name, which stay available.
type Observer struct {
	// target resolves the cluster for one query and applies the admission of
	// the form: the tenant form checks its lease here.
	target func() (*Container, error)
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
	return &Observer{target: tenant.cluster}, nil
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

// Status reads the monitor's JSON status; see Container.Status.
func (o *Observer) Status(ctx context.Context) (Status, error) {
	run, err := o.query()
	if err != nil {
		return Status{}, err
	}
	return readStatus(ctx, run)
}

// ManagerStatus reads active ownership and registered standbys; see
// Container.ManagerStatus.
func (o *Observer) ManagerStatus(ctx context.Context) (ManagerStatus, error) {
	run, err := o.query()
	if err != nil {
		return ManagerStatus{}, err
	}
	return readManagerStatus(ctx, run)
}

// DaemonAddresses reads the advertised addresses of the MONs, MGRs and OSDs;
// see Container.DaemonAddresses.
func (o *Observer) DaemonAddresses(ctx context.Context) (DaemonAddresses, error) {
	run, err := o.query()
	if err != nil {
		return DaemonAddresses{}, err
	}
	return readDaemonAddresses(ctx, run)
}

// The queries below take the topology lock before they read, so they run the
// Container method of the same name on the resolved cluster instead of the bare
// cliQuery. Each keeps its own acquisition: Configuration, BlocklistEntries,
// MGRModules, OSDFlags and OSDStates take the topology mutex without a context,
// and ManagerServices waits for it under the caller's context. All of them
// refuse a terminated cluster or one without a control container before they
// run a command.

// Configuration lists the central configuration database entries; see
// Container.Configuration.
func (o *Observer) Configuration(ctx context.Context) ([]ConfigEntry, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.Configuration(ctx)
}

// BlocklistEntries lists the exact entries and CIDR ranges of the blocklist;
// see Container.BlocklistEntries.
func (o *Observer) BlocklistEntries(ctx context.Context) ([]BlocklistEntry, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.BlocklistEntries(ctx)
}

// MGRModules lists the configured and available manager modules; see
// Container.MGRModules.
func (o *Observer) MGRModules(ctx context.Context) ([]MGRModuleState, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.MGRModules(ctx)
}

// OSDFlags lists the global OSDMap flags; see Container.OSDFlags.
func (o *Observer) OSDFlags(ctx context.Context) ([]OSDFlag, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.OSDFlags(ctx)
}

// OSDStates returns the states of the owned OSDs; see Container.OSDStates.
func (o *Observer) OSDStates(ctx context.Context) ([]OSDState, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.OSDStates(ctx)
}

// ManagerServices lists the service URLs that manager modules advertise; see
// Container.ManagerServices.
func (o *Observer) ManagerServices(ctx context.Context) (map[string]string, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.ManagerServices(ctx)
}

// The queries below resolve the control container or the captured bootstrap
// identity themselves, with their own lock acquisition and fences, so they too
// run the Container method of the same name on the resolved cluster.
// QuorumStatus takes no topology lock and selects the control container for each
// read; MonitorBootstrapAddresses, HealthDetails and FullRatios wait for the
// topology lock under the caller's context. HealthDetails and FullRatios choose
// one control handle under that lock and read the original FSID and keyring
// presence under configMu, so a replacement control container is never used
// mid-query; MonitorBootstrapAddresses compares the same original FSID.

// QuorumStatus reads the monitor quorum and monmap; see Container.QuorumStatus.
func (o *Observer) QuorumStatus(ctx context.Context) (QuorumStatus, error) {
	c, err := o.cluster()
	if err != nil {
		return QuorumStatus{}, err
	}
	return c.QuorumStatus(ctx)
}

// MonitorBootstrapAddresses returns the current monitor bootstrap addresses of
// the original cluster; see Container.MonitorBootstrapAddresses.
func (o *Observer) MonitorBootstrapAddresses(ctx context.Context) (string, error) {
	c, err := o.cluster()
	if err != nil {
		return "", err
	}
	return c.MonitorBootstrapAddresses(ctx)
}

// HealthDetails reads the health detail of the original cluster; see
// Container.HealthDetails.
func (o *Observer) HealthDetails(ctx context.Context) (HealthSnapshot, error) {
	c, err := o.cluster()
	if err != nil {
		return HealthSnapshot{}, err
	}
	return c.HealthDetails(ctx)
}

// FullRatios reads the three global full thresholds; see Container.FullRatios.
func (o *Observer) FullRatios(ctx context.Context) (FullRatioSnapshot, error) {
	c, err := o.cluster()
	if err != nil {
		return FullRatioSnapshot{}, err
	}
	return c.FullRatios(ctx)
}

// The queries below run `rados` in the client container or a supervised scrape
// in the control container. Each one resolves the target (validating the tenant
// lease) and runs the Container method of the same name, so argument
// validation, topology lock, liveness check, client or control container
// selection, bounds and errors are the existing ones. A failed or refused
// collection is an error; it is never returned as an empty result.

// PGInconsistencies lists the objects that the last deep scrub of a PG found
// inconsistent; see Container.PGInconsistencies.
func (o *Observer) PGInconsistencies(ctx context.Context, pgid PGID) ([]InconsistentObject, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.PGInconsistencies(ctx, pgid)
}

// ObjectWatchers lists the clients watching one object; see
// Container.ObjectWatchers.
func (o *Observer) ObjectWatchers(ctx context.Context, pool, namespace, object string) ([]ObjectWatcher, error) {
	c, err := o.cluster()
	if err != nil {
		return nil, err
	}
	return c.ObjectWatchers(ctx, pool, namespace, object)
}

// ManagerMetrics scrapes the active manager's prometheus module once; see
// Container.ManagerMetrics.
func (o *Observer) ManagerMetrics(ctx context.Context) (ManagerMetrics, error) {
	c, err := o.cluster()
	if err != nil {
		return ManagerMetrics{}, err
	}
	return c.ManagerMetrics(ctx)
}
