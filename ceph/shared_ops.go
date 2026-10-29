package ceph

import (
	"context"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// containerOps is the harnesshook.Ops of one *Container: what a Tenant and its
// tenant port may do in the cluster. It exposes no lock, field or executor:
// WithTopology runs a callback under the topology mutex and Extension is the
// service-state slot. Every operation goes through the cluster's own gate.
type containerOps struct{ c *Container }

func (o containerOps) StartupTimeout() time.Duration { return o.c.settings.startupTimeout }

func (o containerOps) Command(ctx context.Context, args ...string) ([]byte, error) {
	return serviceHooks{}.ClientCommand(ctx, o.c, args...)
}

func (o containerOps) WithTopology(fn func() error) error {
	o.c.mu.Lock()
	defer o.c.mu.Unlock()
	return fn()
}

func (o containerOps) Extension(kind string, create func() any) any {
	return serviceHooks{}.Extension(o.c, kind, create)
}

func (o containerOps) RunNativeCommand(ctx context.Context, target testcontainers.Container, command NativeCommand) (NativeCommandResult, error) {
	return o.c.runNativeCommand(ctx, target, command)
}

func (o containerOps) WithClientIdentity(client *ClientConfig, opts ...ClientContainerOption) testcontainers.CustomizeRequestOption {
	return o.c.withClientIdentity(client, opts...)
}
