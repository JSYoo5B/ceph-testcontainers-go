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
	if o == nil || o.target == nil {
		return nil, invalidRequest("observe: the observer is not constructed by Observe or ObserveTenant")
	}
	c, err := o.target()
	if err != nil {
		return nil, err
	}
	return c.cephCLI, nil
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
