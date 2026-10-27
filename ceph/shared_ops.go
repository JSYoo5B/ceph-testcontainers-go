package ceph

import (
	"context"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// clusterOps is what a Tenant and its tenantPort may do in the cluster that a
// sharedBackend holds. It replaces the raw *Container that the backend used to
// hand out, so the lease orchestration (shared.go, shared_exclusive.go,
// tenant.go and tenantLease) names no Container field or method. It exposes no
// lock, field or executor: WithTopology runs a callback under the topology
// mutex and Extension is the service-state slot, both as the tenant port
// already offered them. Every operation goes through the cluster's own gate,
// exactly as the forwarders did before.
type clusterOps interface {
	startupTimeout() time.Duration
	// command runs a CLI command in the cluster, without a liveness check.
	command(ctx context.Context, args ...string) ([]byte, error)
	withTopology(fn func() error) error
	extension(kind string, create func() any) any
	runNativeCommand(ctx context.Context, target testcontainers.Container, command NativeCommand) (NativeCommandResult, error)
	withClientIdentity(client *ClientConfig, opts ...ClientContainerOption) testcontainers.CustomizeRequestOption
	// raw returns the gated cluster. Only SharedCluster.Exclusive, which
	// lends the whole cluster deliberately, and the test-only TenantCluster
	// hook call it.
	raw() *Container
}

// containerOps is the clusterOps of one *Container.
type containerOps struct{ c *Container }

func (o containerOps) startupTimeout() time.Duration { return o.c.settings.startupTimeout }

func (o containerOps) command(ctx context.Context, args ...string) ([]byte, error) {
	return serviceHooks{}.ClientCommand(ctx, o.c, args...)
}

func (o containerOps) withTopology(fn func() error) error {
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	return fn()
}

func (o containerOps) extension(kind string, create func() any) any {
	return serviceHooks{}.Extension(o.c, kind, create)
}

func (o containerOps) runNativeCommand(ctx context.Context, target testcontainers.Container, command NativeCommand) (NativeCommandResult, error) {
	return o.c.runNativeCommand(ctx, target, command)
}

func (o containerOps) withClientIdentity(client *ClientConfig, opts ...ClientContainerOption) testcontainers.CustomizeRequestOption {
	return o.c.withClientIdentity(client, opts...)
}

func (o containerOps) raw() *Container { return o.c }
