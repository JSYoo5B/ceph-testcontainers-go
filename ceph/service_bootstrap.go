package ceph

import (
	"context"
	"slices"

	"github.com/testcontainers/testcontainers-go"
)

// serviceBootstrap is the provisioning a service package adds to Run: pools
// created and validated with the initial pools, validation of its own
// configuration against the initial composition, and a start step that runs
// after the initial pools exist. Its method names are exported so that types
// in the service packages can implement it, and it is an alias of an
// unnamed interface so that internal/servicehook can declare the identical
// type for the hook that registers it.
type serviceBootstrap = interface {
	// Kind names the service. Options of one kind replace each other, as
	// repeated With* options do.
	Kind() string
	// Order sorts the start steps: RBD pools, then filesystems, then gateways.
	Order() int
	// InitialPools joins the initial pool list before validation, so every
	// placement and bootstrap rule applies to these pools.
	InitialPools() []PoolConfig
	// NeedsManagers reports that the service cannot start before a manager.
	NeedsManagers() bool
	// MGRModules lists the manager modules the configured service depends on.
	MGRModules() []string
	// Prepare normalizes the service configuration. resolve applies the
	// cluster's pool defaults; validate checks a pool against the initial
	// layout and the names already in use.
	Prepare(resolve func(PoolConfig) PoolConfig, validate func(PoolConfig) error) error
	// Start provisions the service on the running cluster.
	Start(ctx context.Context, c *Container) error
}

// withServiceBootstrap records a service bootstrap built when the option is
// applied. A fallback only applies when no bootstrap of its kind was given.
func withServiceBootstrap(kind string, fallback bool, build func() (serviceBootstrap, error)) Option {
	return func(o *options) error {
		existing := slices.IndexFunc(o.services, func(b serviceBootstrap) bool { return b.Kind() == kind })
		if fallback && existing >= 0 {
			return nil
		}
		b, err := build()
		if err != nil {
			return err
		}
		if existing >= 0 {
			o.services[existing] = b
			return nil
		}
		o.services = append(o.services, b)
		slices.SortStableFunc(o.services, func(a, b serviceBootstrap) int { return a.Order() - b.Order() })
		return nil
	}
}

// requiresMGRModule reports a configured service that depends on module.
func (o options) requiresMGRModule(module string) bool {
	return slices.ContainsFunc(o.services, func(b serviceBootstrap) bool { return slices.Contains(b.MGRModules(), module) })
}

// The cluster reads these parts of a service package's per-cluster state,
// stored with serviceHooks.SetExtension, when the state implements them.
type (
	// serviceSecrets lists credentials that diagnostics must redact.
	serviceSecrets interface{ Secrets() []string }
	// serviceConfigTargets lists service containers, by name, that read the
	// cluster's ceph.conf and need it refreshed after monitor changes.
	serviceConfigTargets interface {
		ConfigTargets() map[string]testcontainers.Container
	}
	// serviceModules reports owned state that depends on a manager module.
	serviceModules interface{ RequiresMGRModule(module string) bool }
)

// extensionsWith returns the per-cluster service states that implement T,
// in kind order. The store takes its own lock; see extensionStore.
func extensionsWith[T any](c *Container) []T {
	var result []T
	for _, state := range c.extensions.inKindOrder() {
		if state, ok := state.(T); ok {
			result = append(result, state)
		}
	}
	return result
}
