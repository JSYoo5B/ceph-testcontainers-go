// Package namedregistry holds the per-cluster state of a service package that
// keeps its owned values by name, such as the rgw gateways. It is generic over
// the value type, so the value type stays in the service package and the
// service package and its harness package can both reach the same registry
// without either naming the other's private declarations
// (docs/design/shared-harness-boundary.md, decision 8). It adds no public
// name and imports no module package.
//
// The registry has no lock. The owner of the cluster state guards it, as the
// cluster mutex guards the rgw gateway registry.
package namedregistry

import (
	"maps"
	"slices"

	"github.com/testcontainers/testcontainers-go"
)

// Registry maps a name to an owned value of type T.
type Registry[T any] struct {
	// ByName holds the owned values by name.
	ByName map[string]T

	secrets func(T) []string
	targets func(name string, v T) (string, testcontainers.Container)
}

// From returns the registry stored in the cluster's extension store under
// kind, creating an empty one on first use. extension is the store accessor,
// as servicehook.Extension and a tenant port provide it.
func From[T any](extension func(kind string, create func() any) any, kind string) *Registry[T] {
	return extension(kind, func() any {
		return &Registry[T]{ByName: make(map[string]T)}
	}).(*Registry[T])
}

// Describe sets how the cluster reads credentials and config targets out of a
// value. It is idempotent and may run on a registry that a reader created
// first. The service package that owns T calls it each time it takes the
// registry for writing, so a registry that a tenant created first still
// reports the owner's secrets and targets.
func (r *Registry[T]) Describe(secrets func(T) []string, targets func(name string, v T) (string, testcontainers.Container)) *Registry[T] {
	r.secrets, r.targets = secrets, targets
	return r
}

// Sorted returns the values ordered by name.
func (r *Registry[T]) Sorted() []T {
	result := make([]T, 0, len(r.ByName))
	for _, name := range slices.Sorted(maps.Keys(r.ByName)) {
		result = append(result, r.ByName[name])
	}
	return result
}

// Secrets lists the values' credentials for diagnostics to redact.
func (r *Registry[T]) Secrets() []string {
	if r.secrets == nil {
		return nil
	}
	var out []string
	for _, v := range r.ByName {
		out = append(out, r.secrets(v)...)
	}
	return out
}

// ConfigTargets lists the containers that read the cluster's ceph.conf.
func (r *Registry[T]) ConfigTargets() map[string]testcontainers.Container {
	targets := make(map[string]testcontainers.Container)
	if r.targets == nil {
		return targets
	}
	for name, v := range r.ByName {
		if key, c := r.targets(name, v); c != nil {
			targets[key] = c
		}
	}
	return targets
}
