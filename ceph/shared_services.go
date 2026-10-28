package ceph

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/sharedinvariants"
)

// sharedService is the support of one service (rbd, cephfs, rgw) for shared
// clusters. A SharedCluster owns the set of them it was constructed with.
type sharedService struct {
	kind string
	// prepare readies a cluster that runs the service before its baseline
	// is taken, while the gate is still open: it creates what the service
	// would otherwise create lazily on a tenant's first use, such as the
	// RGW bucket pools. It does nothing on a cluster without the service.
	prepare func(ctx context.Context, c *Container) error
	// probe lists the service's resources that tenants could create, each
	// mapped to the tenant ID that would own it, or to any other name. A
	// release then reports the tenant's own resources that remain and any
	// resource of no tenant that differs from the baseline.
	probe func(ctx context.Context, c *Container) (map[string]string, error)
}

// sharedPrepare and sharedProbe are a service's callbacks as the cluster
// backend runs them. The set wraps each one so that its error names the
// service.
type (
	sharedPrepare func(ctx context.Context, c *Container) error
	sharedProbe   func(ctx context.Context, c *Container) (map[string]string, error)
)

// sharedServiceSet is the service support one SharedCluster owns. It is built
// once, by NewShared, and never changes afterwards, so it needs no lock and
// two SharedClusters of one process can hold different sets. Its services are
// sorted by kind.
type sharedServiceSet struct {
	services []sharedService
	prepares []sharedPrepare
	probes   []sharedProbe
}

// newSharedServiceSet validates services and returns the set. A service
// without a kind (the zero value is not a constructed service) and a kind
// given twice are errors; neither callback is required.
func newSharedServiceSet(services []sharedService) (sharedServiceSet, error) {
	var errs []error
	seen := map[string]bool{}
	var set sharedServiceSet
	for _, service := range services {
		switch {
		case service.kind == "":
			errs = append(errs, errors.New("a shared service support has no kind"))
			continue
		case seen[service.kind]:
			errs = append(errs, fmt.Errorf("shared service support %q is given twice", service.kind))
			continue
		}
		seen[service.kind] = true
		set.services = append(set.services, service)
	}
	if len(errs) > 0 {
		return sharedServiceSet{}, errors.Join(errs...)
	}
	slices.SortFunc(set.services, func(a, b sharedService) int { return strings.Compare(a.kind, b.kind) })
	for _, service := range set.services {
		kind := service.kind
		if prepare := service.prepare; prepare != nil {
			set.prepares = append(set.prepares, func(ctx context.Context, c *Container) error {
				if err := prepare(ctx, c); err != nil {
					return fmt.Errorf("prepare %s for a shared cluster: %w", kind, err)
				}
				return nil
			})
		}
		if probe := service.probe; probe != nil {
			set.probes = append(set.probes, func(ctx context.Context, c *Container) (map[string]string, error) {
				found, err := probe(ctx, c)
				if err != nil {
					return nil, fmt.Errorf("list the %s resources: %w", kind, err)
				}
				return found, nil
			})
		}
	}
	return set, nil
}

// has reports whether the set holds the support of kind.
func (set sharedServiceSet) has(kind string) bool {
	return slices.ContainsFunc(set.services, func(s sharedService) bool { return s.kind == kind })
}

// withSharedServices gives a SharedCluster exactly these services instead of
// the ones the service packages registered. It is the unexported seam of the
// instance-owned support: the exported option waits for the owner's choice of
// names. Repeated uses add up; a kind given twice is an ErrInvalidRequest at
// the first Lease or Exclusive.
func withSharedServices(services ...sharedService) SharedOption {
	return func(s *sharedSettings) {
		s.serviceList = append(s.serviceList, services...)
		s.serviceListGiven = true
	}
}

// transitionalRegisteredServices is read once, by NewShared, when no
// withSharedServices option was given. It is the last reader of the
// process-global registry, which goes away with the init registration.
func transitionalRegisteredServices() []sharedService { return registeredSharedServices() }

// The process-global registry below is the transitional source of the default
// set. Only NewShared reads it (TestSharedServiceRegistryHasOneReader).
var sharedServices struct {
	mu     sync.Mutex
	byKind map[string]sharedService
}

// registerSharedService records a service's shared-cluster hooks; a second
// registration of a kind replaces the first.
func registerSharedService(service sharedService) {
	sharedServices.mu.Lock()
	defer sharedServices.mu.Unlock()
	if sharedServices.byKind == nil {
		sharedServices.byKind = map[string]sharedService{}
	}
	sharedServices.byKind[service.kind] = service
}

// registeredSharedServices returns the registered services sorted by kind.
func registeredSharedServices() []sharedService {
	sharedServices.mu.Lock()
	defer sharedServices.mu.Unlock()
	services := make([]sharedService, 0, len(sharedServices.byKind))
	for _, kind := range sharedinvariants.SortedKeys(sharedServices.byKind) {
		services = append(services, sharedServices.byKind[kind])
	}
	return services
}

// runPrepares runs the services' preparation on c in order and stops at the
// first failure.
func runPrepares(ctx context.Context, c *Container, prepares []sharedPrepare) error {
	for _, prepare := range prepares {
		if err := prepare(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

// runProbes merges the probes' listings in order; a later service wins a key
// that two list. The first failure stops the run.
func runProbes(ctx context.Context, c *Container, probes []sharedProbe) (map[string]string, error) {
	resources := map[string]string{}
	for _, probe := range probes {
		found, err := probe(ctx, c)
		if err != nil {
			return nil, err
		}
		for key, owner := range found {
			resources[key] = owner
		}
	}
	return resources, nil
}

// releaseScopes releases tenant's service scopes in reverse order of
// attachment. Problems name what remains; errors are releases that could not
// run.
func releaseScopes(ctx context.Context, scopes []tenantScope) ([]string, error) {
	var problems []string
	var errs []error
	for _, scope := range slices.Backward(scopes) {
		remains, err := scope.release(ctx)
		for _, remain := range remains {
			problems = append(problems, scope.key+": "+remain)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("release %s: %w", scope.key, err))
		}
	}
	return problems, errors.Join(errs...)
}
