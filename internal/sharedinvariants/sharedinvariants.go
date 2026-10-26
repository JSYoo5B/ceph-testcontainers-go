// Package sharedinvariants holds the pure comparison logic of a shared
// cluster: the state a tenant cannot change through its own scope and the
// diffs that admission and release derive from it. It has no dependency on
// the cluster, so it is tested without Docker.
package sharedinvariants

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Invariants is the state that a tenant cannot change through its own
// scope. Config values are compared but never reported.
type Invariants struct {
	// Auth maps each entity to its capabilities.
	Auth map[string]string
	// Pools maps each pool name to its policy.
	Pools     map[string]string
	OSDFlags  []string
	Config    map[string]string
	Blocklist []string
	// Health lists the codes of the unmuted health checks.
	Health []string
	// Namespaces counts the objects of each namespace of SharedPool.
	Namespaces map[string]int
	// Resources maps the service resources that the registered probes list
	// to the tenant ID, or other name, that owns them.
	Resources map[string]string
	// Unsettled says why the cluster cannot admit a tenant yet, such as PGs
	// that are not all active+clean; it is empty when it can.
	Unsettled string
}

// TenantOwns reports whether entity is one of the clients that a tenant
// names after its ID.
func TenantOwns(id, entity string) bool {
	return entity == "client."+id || strings.HasPrefix(entity, "client."+id+"-")
}

// AdmissionDiff lists the conditions that admit a tenant and differ from
// the baseline: health, OSD flags and the blocklist.
func (b Invariants) AdmissionDiff(current Invariants) []string {
	var problems []string
	if current.Unsettled != "" {
		problems = append(problems, current.Unsettled)
	}
	if !slices.Equal(b.Health, current.Health) {
		problems = append(problems, fmt.Sprintf("health checks are %v, not %v", current.Health, b.Health))
	}
	if !slices.Equal(b.OSDFlags, current.OSDFlags) {
		problems = append(problems, fmt.Sprintf("OSD flags are %v, not %v", current.OSDFlags, b.OSDFlags))
	}
	if !slices.Equal(b.Blocklist, current.Blocklist) {
		problems = append(problems, fmt.Sprintf("the blocklist has %d entries, not %d", len(current.Blocklist), len(b.Blocklist)))
	}
	return problems
}

// ReleaseDiff lists what differs from the baseline after tenant self was
// cleaned up, ignoring the scopes of the other tenants. lagging reports a
// health difference, which can still settle.
func (b Invariants) ReleaseDiff(current Invariants, pool, self string, others map[string]bool) (problems []string, lagging bool) {
	ownedByOther := func(entity string) bool {
		for id := range others {
			if TenantOwns(id, entity) {
				return true
			}
		}
		return false
	}
	for _, entity := range SortedKeys(current.Auth) {
		caps, known := b.Auth[entity]
		switch {
		case TenantOwns(self, entity):
			problems = append(problems, "tenant client "+entity+" remains")
		case ownedByOther(entity):
		case !known:
			problems = append(problems, "auth entity "+entity+" was added")
		case caps != current.Auth[entity]:
			problems = append(problems, "capabilities of "+entity+" changed")
		}
	}
	for _, entity := range SortedKeys(b.Auth) {
		if _, ok := current.Auth[entity]; !ok {
			problems = append(problems, "auth entity "+entity+" was removed")
		}
	}
	problems = append(problems, mapDiff("pool", b.Pools, current.Pools)...)
	problems = append(problems, mapDiff("config entry", b.Config, current.Config)...)
	if !slices.Equal(b.OSDFlags, current.OSDFlags) {
		problems = append(problems, fmt.Sprintf("OSD flags are %v, not %v", current.OSDFlags, b.OSDFlags))
	}
	if !slices.Equal(b.Blocklist, current.Blocklist) {
		problems = append(problems, fmt.Sprintf("the blocklist has %d entries, not %d", len(current.Blocklist), len(b.Blocklist)))
	}
	for _, namespace := range SortedKeys(current.Namespaces) {
		count := current.Namespaces[namespace]
		switch {
		case namespace == self:
			problems = append(problems, fmt.Sprintf("namespace %s of pool %s still has %d objects", namespace, pool, count))
		case others[namespace]:
		case count != b.Namespaces[namespace]:
			problems = append(problems, fmt.Sprintf("namespace %q of pool %s has %d objects, not %d", namespace, pool, count, b.Namespaces[namespace]))
		}
	}
	for _, namespace := range SortedKeys(b.Namespaces) {
		if _, ok := current.Namespaces[namespace]; !ok {
			problems = append(problems, fmt.Sprintf("namespace %q of pool %s lost its objects", namespace, pool))
		}
	}
	problems = append(problems, ResourceDiff(b.Resources, current.Resources, self, others)...)
	return problems, !slices.Equal(b.Health, current.Health)
}

// mapDiff names the keys added, removed or changed, never the values.
func mapDiff(kind string, baseline, current map[string]string) []string {
	var problems []string
	for _, key := range SortedKeys(current) {
		before, ok := baseline[key]
		switch {
		case !ok:
			problems = append(problems, kind+" "+key+" was added")
		case before != current[key]:
			problems = append(problems, kind+" "+key+" changed")
		}
	}
	for _, key := range SortedKeys(baseline) {
		if _, ok := current[key]; !ok {
			problems = append(problems, kind+" "+key+" was removed")
		}
	}
	return problems
}

// SortedKeys returns the keys of m in order.
func SortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

// ResourceDiff lists the resources of current that differ from baseline:
// those of tenant self, which its release should have removed, and those of
// no live or earlier tenant that were added or removed.
func ResourceDiff(baseline, current map[string]string, self string, others map[string]bool) []string {
	var problems []string
	for _, key := range SortedKeys(current) {
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
	for _, key := range SortedKeys(baseline) {
		if _, ok := current[key]; !ok {
			problems = append(problems, key+" was removed")
		}
	}
	return problems
}
