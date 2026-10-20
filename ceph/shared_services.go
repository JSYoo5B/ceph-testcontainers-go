package ceph

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// sharedService is what a service package (rbd, cephfs, rgw) registers for
// shared clusters through internal/servicehook.
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
	for _, kind := range sortedKeys(sharedServices.byKind) {
		services = append(services, sharedServices.byKind[kind])
	}
	return services
}

// resources runs the registered probes on b's cluster.
func (b containerBackend) resources(ctx context.Context) (map[string]string, error) {
	resources := map[string]string{}
	for _, service := range registeredSharedServices() {
		if service.probe == nil {
			continue
		}
		found, err := service.probe(ctx, b.c)
		if err != nil {
			return nil, fmt.Errorf("list the %s resources: %w", service.kind, err)
		}
		for key, owner := range found {
			resources[key] = owner
		}
	}
	return resources, nil
}

// resourceDiff lists the resources of current that differ from baseline:
// those of tenant self, which its release should have removed, and those of
// no live or earlier tenant that were added or removed.
func resourceDiff(baseline, current map[string]string, self string, others map[string]bool) []string {
	var problems []string
	for _, key := range sortedKeys(current) {
		owner := current[key]
		_, known := baseline[key]
		switch {
		case owner == self:
			problems = append(problems, key+" remains")
		case others[owner]:
		case !known:
			problems = append(problems, key+" was added")
		}
	}
	for _, key := range sortedKeys(baseline) {
		if _, ok := current[key]; !ok {
			problems = append(problems, key+" was removed")
		}
	}
	return problems
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
