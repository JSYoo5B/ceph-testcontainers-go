package harness

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// sharedService is the support of one service (rbd, cephfs, rgw) for shared
// clusters. A SharedCluster owns the set of them it was constructed with.
type sharedService struct {
	kind string
	// prepare readies a cluster that runs the service before its baseline
	// is taken, while the gate is still open: it creates what the service
	// would otherwise create lazily on a tenant's first use, such as the
	// RGW bucket pools. It does nothing on a cluster without the service.
	prepare func(ctx context.Context, c *ceph.Container) error
	// probe lists the service's resources that tenants could create, each
	// mapped to the tenant ID that would own it, or to any other name. A
	// release then reports the tenant's own resources that remain and any
	// resource of no tenant that differs from the baseline.
	probe func(ctx context.Context, c *ceph.Container) (map[string]string, error)
}

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
			set.prepares = append(set.prepares, func(ctx context.Context, c *ceph.Container) error {
				if err := prepare(ctx, c); err != nil {
					return fmt.Errorf("prepare %s for a shared cluster: %w", kind, err)
				}
				return nil
			})
		}
		if probe := service.probe; probe != nil {
			set.probes = append(set.probes, func(ctx context.Context, c *ceph.Container) (map[string]string, error) {
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

// ServiceSupport is the shared-cluster support of one service (RBD, CephFS or
// RGW): what a shared cluster prepares before its baseline and lists when it
// verifies a release, so that a tenant's service resources are attributed and
// a leftover is found. Take it from the service's harness package, for
// example rbd/harness.Support, and pass it to NewShared with WithServices.
// The value is opaque: it carries callbacks that receive the cluster, which
// the public API does not show. The zero value is not a support and is
// rejected at construction.
type ServiceSupport struct{ service sharedService }

// WithServices gives the shared cluster the support of the services that its
// tests lease. The cluster owns exactly these services: it prepares and probes
// no other, and a service Lease on a cluster that was not given its support
// fails with ErrInvalidRequest before it creates anything. Repeated uses add
// up. A zero ServiceSupport and a service given twice are an ErrInvalidRequest
// at the first Lease or Exclusive.
func WithServices(supports ...ServiceSupport) SharedOption {
	return func(s *sharedSettings) {
		for _, support := range supports {
			s.serviceList = append(s.serviceList, support.service)
		}
	}
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
