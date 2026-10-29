package harness

import (
	"context"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/harnesshook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/sharedinvariants"
	"github.com/testcontainers/testcontainers-go"
)

// This file is the harness side of the internal contract with package ceph
// (internal/harnesshook). The orchestration keeps its own small interfaces so
// that the model backends of its unit tests need nothing of the core; the
// adapters below turn the contract's Backend and Ops into them.

// invalidRequest, classify and classifyDocker are the core's error
// construction and classification, reached through the contract. The harness
// keeps no copy of them, so the categories, causes and messages cannot drift.
func invalidRequest(format string, args ...any) error {
	return harnesshook.InvalidRequest(format, args...)
}

func classify(kind, err error) error { return harnesshook.Classify(kind, err) }

func classifyDocker(ctx context.Context, err error) error {
	return harnesshook.ClassifyDocker(ctx, err)
}

// sharingMode is how a SharedCluster lends a cluster; the values are the
// core gate's.
type sharingMode = harnesshook.Mode

const (
	notShared         = harnesshook.NotShared
	sharedWithTenants = harnesshook.SharedWithTenants
	sharedExclusively = harnesshook.SharedExclusively
)

// sharedInvariants is the state that a tenant cannot change through its own
// scope; the comparison logic lives in internal/sharedinvariants.
type sharedInvariants = sharedinvariants.Invariants

// sharedPrepare and sharedProbe are a service's callbacks as the cluster
// backend runs them. The set wraps each one so that its error names the
// service.
type (
	sharedPrepare = harnesshook.Prepare
	sharedProbe   = harnesshook.Probe
)

// sharedBackend is what a SharedCluster needs from one cluster. hookBackend
// implements it over the core; unit tests replace it with a model.
type sharedBackend interface {
	createPool(ctx context.Context, name string) error
	// prepareServices runs the services' preparation, given by the
	// SharedCluster's own set, before the baseline.
	prepareServices(ctx context.Context, prepares []sharedPrepare) error
	// ready waits for quorum and active+clean PGs.
	ready(ctx context.Context) error
	// invariants reads the cluster's state except Resources, which
	// resources reads: the services' probes are slower and only the
	// baseline and releases compare them.
	invariants(ctx context.Context, pool string) (sharedInvariants, error)
	resources(ctx context.Context, probes []sharedProbe) (map[string]string, error)
	// daemonsRunning fails when a daemon container of the cluster exited.
	daemonsRunning(ctx context.Context) error
	createClient(ctx context.Context, entity string, caps ceph.ClientCaps) (client *ceph.ClientConfig, created bool, err error)
	deleteClient(ctx context.Context, client *ceph.ClientConfig) error
	// purgeNamespace removes every object of one namespace of pool.
	purgeNamespace(ctx context.Context, pool, namespace string) error
	// setSharing sets the gate of the cluster.
	setSharing(mode sharingMode)
	terminate(ctx context.Context) error
	// ops returns the cluster operations that a Tenant may use, or nil for a
	// model that has no cluster.
	ops() clusterOps
}

type sharedStarter func(ctx context.Context, image string, opts []testcontainers.ContainerCustomizer) (sharedBackend, error)

// clusterOps is what a Tenant and its tenantPort may do in the cluster that a
// sharedBackend holds. It exposes no lock, field or executor: withTopology
// runs a callback under the topology mutex and extension is the
// service-state slot. Every operation goes through the cluster's own gate.
type clusterOps interface {
	startupTimeout() time.Duration
	// command runs a CLI command in the cluster, without a liveness check.
	command(ctx context.Context, args ...string) ([]byte, error)
	withTopology(fn func() error) error
	extension(kind string, create func() any) any
	runNativeCommand(ctx context.Context, target testcontainers.Container, command ceph.NativeCommand) (ceph.NativeCommandResult, error)
	withClientIdentity(client *ceph.ClientConfig, opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption
	// raw returns the gated cluster. Only SharedCluster.Exclusive, which
	// lends the whole cluster deliberately, the tenant Observer and the
	// test-only tenant lookup call it (TestLendHasOnlyTheAllowedCallers).
	raw() *ceph.Container
}

// startSharedContainer starts a cluster through the core.
func startSharedContainer(ctx context.Context, image string, opts []testcontainers.ContainerCustomizer) (sharedBackend, error) {
	backend, err := harnesshook.Start(ctx, image, opts)
	if backend == nil {
		return nil, err
	}
	return hookBackend{b: backend}, err
}

// hookBackend is the sharedBackend of a cluster that the core started.
type hookBackend struct{ b harnesshook.Backend }

func (h hookBackend) createPool(ctx context.Context, name string) error {
	return h.b.CreatePool(ctx, name)
}

func (h hookBackend) prepareServices(ctx context.Context, prepares []sharedPrepare) error {
	return h.b.PrepareServices(ctx, prepares)
}

func (h hookBackend) ready(ctx context.Context) error { return h.b.Ready(ctx) }

func (h hookBackend) invariants(ctx context.Context, pool string) (sharedInvariants, error) {
	return h.b.Invariants(ctx, pool)
}

func (h hookBackend) resources(ctx context.Context, probes []sharedProbe) (map[string]string, error) {
	return h.b.Resources(ctx, probes)
}

func (h hookBackend) daemonsRunning(ctx context.Context) error { return h.b.DaemonsRunning(ctx) }

func (h hookBackend) createClient(ctx context.Context, entity string, caps ceph.ClientCaps) (*ceph.ClientConfig, bool, error) {
	return h.b.CreateClient(ctx, entity, caps)
}

func (h hookBackend) deleteClient(ctx context.Context, client *ceph.ClientConfig) error {
	return h.b.DeleteClient(ctx, client)
}

func (h hookBackend) purgeNamespace(ctx context.Context, pool, namespace string) error {
	return h.b.PurgeNamespace(ctx, pool, namespace)
}

func (h hookBackend) setSharing(mode sharingMode) { h.b.SetSharing(int32(mode)) }

func (h hookBackend) terminate(ctx context.Context) error { return h.b.Terminate(ctx) }

func (h hookBackend) ops() clusterOps {
	ops := harnesshook.AsOps(h.b.Ops())
	if ops == nil {
		return nil
	}
	return hookOps{ops: ops, backend: h.b}
}

// hookOps is the clusterOps of one cluster that the core started.
type hookOps struct {
	ops     harnesshook.Ops
	backend harnesshook.Backend
}

func (o hookOps) startupTimeout() time.Duration { return o.ops.StartupTimeout() }

func (o hookOps) command(ctx context.Context, args ...string) ([]byte, error) {
	return o.ops.Command(ctx, args...)
}

func (o hookOps) withTopology(fn func() error) error { return o.ops.WithTopology(fn) }

func (o hookOps) extension(kind string, create func() any) any { return o.ops.Extension(kind, create) }

func (o hookOps) runNativeCommand(ctx context.Context, target testcontainers.Container, command ceph.NativeCommand) (ceph.NativeCommandResult, error) {
	return o.ops.RunNativeCommand(ctx, target, command)
}

func (o hookOps) withClientIdentity(client *ceph.ClientConfig, opts ...ceph.ClientContainerOption) testcontainers.CustomizeRequestOption {
	return o.ops.WithClientIdentity(client, opts...)
}

func (o hookOps) raw() *ceph.Container { return harnesshook.Lend(o.backend) }
