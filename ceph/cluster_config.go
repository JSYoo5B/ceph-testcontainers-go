package ceph

import (
	"fmt"
	"slices"
)

// clusterField names one setting of ClusterConfig.
type clusterField uint8

const (
	clusterMonitors clusterField = 1 << iota
	clusterManagers
	clusterOSDs
	clusterDefaultRoot
	clusterPoolDefaults
	clusterPools
)

// ClusterConfig is the topology and placement group of the Ceph configuration:
// how many MONs, MGRs and OSDs Run starts, where the initial OSDs sit in the
// CRUSH map, the default CRUSH root and pool size defaults, and the pools Run
// creates after the initial OSDs. It is a plain value with chainable,
// value-returning methods, not a customizer; pass it to Run through its one
// grouped option function, WithClusterConfig. The zero value changes nothing,
// and each field records whether it was set, so an explicit setting is
// different from an unspecified one.
//
//	cluster := ceph.ClusterConfig{}.WithMonitorCount(3).WithOSDCount(3).WithPoolDefaults(3, 2)
//	c, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithClusterConfig(cluster))
//
// The managers (WithManagerCount, WithNoInitialManagers) are one field, and so
// are the initial OSDs (WithOSDCount, WithInitialOSDs, WithNoInitialOSDs).
// Within one value a later call of the same field replaces the earlier one. Run
// refuses a field that two WithClusterConfig options both set, even when they
// agree, so argument order never decides a conflict. Incompatible combinations
// (no initial OSDs with initial pools or services, no initial managers with
// initial CephFS or RGW services, a layout that cannot hold the pools) are
// refused before any resource is created. The composition rules are in
// docs/contributing/configuration-composition.md.
type ClusterConfig struct {
	set         clusterField
	monitors    int
	managers    int
	noManagers  bool
	osds        int
	initialOSDs []OSDConfig
	noOSDs      bool
	crushRoot   string
	replicas    int
	minSize     int
	pools       []PoolConfig
}

// WithMonitorCount selects the initial monitor count (default: 1). Three enables
// quorum failover tests; even counts are allowed for deliberate quorum-loss
// scenarios. A count below one is refused.
func (c ClusterConfig) WithMonitorCount(count int) ClusterConfig {
	c.set |= clusterMonitors
	c.monitors = count
	return c
}

// WithManagerCount starts one active manager and the remaining standbys
// (default: one manager). A count below one is refused; WithNoInitialManagers
// defers the first manager instead.
func (c ClusterConfig) WithManagerCount(count int) ClusterConfig {
	c.set |= clusterManagers
	c.managers, c.noManagers = count, false
	return c
}

// WithNoInitialManagers defers the first MGR until an explicit AddManager.
// Run waits for MON quorum and requested OSDs up/in, without implying PG clean
// or MGR module readiness. Initial CephFS/RGW services are refused before
// allocation; positive-OSD initial pools are allowed. The essential image must
// still contain the MGR and its required dependencies. WaitForClean and
// WaitForPGClean need an available manager, so they cannot succeed before one
// is added, and Ceph may report MGR_DOWN (after mon_mgr_mkfs_grace) or
// HEALTH_ERR while client data I/O still works. To simulate a manager outage in
// a configured cluster, stop the existing manager container instead.
func (c ClusterConfig) WithNoInitialManagers() ClusterConfig {
	c.set |= clusterManagers
	c.managers, c.noManagers = 0, true
	return c
}

// WithOSDCount sets the initial number of OSD containers at default locations
// (default: 2). A count below one is refused; WithNoInitialOSDs starts none.
// Each OSD gets a logical host osd-ID under the default root. Without
// WithPoolDefaults the cluster's default pool size is min(2, count) with
// min_size 1, so a one-OSD cluster gets replica-1 pools that can become clean.
func (c ClusterConfig) WithOSDCount(count int) ClusterConfig {
	c.set |= clusterOSDs
	c.osds, c.initialOSDs, c.noOSDs = count, nil, false
	return c
}

// WithInitialOSDs selects the count and logical CRUSH locations of the initial
// OSDs, one OSDConfig per OSD. An empty layout is refused, and so is a host
// that appears under different racks or roots. The value keeps its own copy of
// configs. Pools that need N distinct failure domains are validated against
// the layout before anything is created.
//
// Run registers the initial OSDs one after another in layout order, so IDs,
// keys and CRUSH host buckets are allocated in that order on every run, and
// every host bucket exists before any OSD boots. It then starts up to four OSD
// containers at a time and waits until all are up and in. If an OSD fails to
// start, no further OSD is started and the error names it; every registered
// OSD and created container stays owned by the returned cluster. AddOSD adds
// one OSD at a time.
func (c ClusterConfig) WithInitialOSDs(configs ...OSDConfig) ClusterConfig {
	c.set |= clusterOSDs
	c.osds, c.initialOSDs, c.noOSDs = len(configs), slices.Clone(configs), false
	if c.initialOSDs == nil {
		// Keep an explicit empty layout apart from WithOSDCount's default one.
		c.initialOSDs = []OSDConfig{}
	}
	return c
}

// WithNoInitialOSDs starts only the selected MON/MGR topology. It keeps the
// ordinary prospective pool defaults (size 2, min_size 1) for later AddOSD
// calls; WithPoolDefaults may override them. Initial pools and CephFS/RGW
// services are refused before allocation. No storage readiness is implied:
// native internal pools may have pending PGs.
func (c ClusterConfig) WithNoInitialOSDs() ClusterConfig {
	c.set |= clusterOSDs
	c.osds, c.initialOSDs, c.noOSDs = 0, nil, true
	return c
}

// WithDefaultCRUSHRoot selects placement for pools that Ceph creates itself,
// including .mgr, and for omitted PoolConfig.CRUSHRoot/OSDConfig.Root fields.
// The initial layout must contain enough OSDs in this root for the default
// replica count. Without this setting the root is default.
func (c ClusterConfig) WithDefaultCRUSHRoot(root string) ClusterConfig {
	c.set |= clusterDefaultRoot
	c.crushRoot = root
	return c
}

// WithPoolDefaults selects size/min_size for pools created by Ceph or callers.
// Without this setting the fast fixture uses size=min(2, OSD count), min_size=1
// (2/1 with WithNoInitialOSDs). It requires 1 <= minSize <= replicas <= 10 and
// sets osd_pool_default_size and osd_pool_default_min_size, which
// WithConfigFile therefore rejects. CreatePool can override these defaults for
// each explicitly configured pool.
func (c ClusterConfig) WithPoolDefaults(replicas, minSize int) ClusterConfig {
	c.set |= clusterPoolDefaults
	c.replicas, c.minSize = replicas, minSize
	return c
}

// WithPools creates the selected pools after initial OSD startup. Configs and
// nested EC settings are copied; cluster defaults are resolved after all Run
// options have been applied.
func (c ClusterConfig) WithPools(configs ...PoolConfig) ClusterConfig {
	c.set |= clusterPools
	c.pools = clonePoolConfigs(configs)
	if c.pools == nil {
		// An explicit empty list replaces the pools with none.
		c.pools = []PoolConfig{}
	}
	return c
}

// WithClusterConfig is the grouped option function that carries a ClusterConfig
// to Run. Like every Option it changes the cluster settings only and never
// reaches a container request.
func WithClusterConfig(cluster ClusterConfig) Option {
	return func(o *options) error { return cluster.apply(o) }
}

// apply validates the fields c set and, only if all are valid, writes them to
// the settings. It refuses a field that an earlier WithClusterConfig already set.
func (c ClusterConfig) apply(o *options) error {
	if clash := c.set & o.clusterSet; clash != 0 {
		return fmt.Errorf("cluster %s set by more than one WithClusterConfig", clusterFieldName(clash))
	}
	if c.set&clusterMonitors != 0 && c.monitors < 1 {
		return fmt.Errorf("monitor count must be at least 1")
	}
	if c.set&clusterManagers != 0 && !c.noManagers && c.managers < 1 {
		return fmt.Errorf("manager count must be at least 1")
	}
	var layout []OSDConfig
	if c.set&clusterOSDs != 0 && !c.noOSDs {
		if c.initialOSDs == nil && c.osds < 1 {
			return fmt.Errorf("OSD count must be at least 1")
		}
		if c.initialOSDs != nil {
			var err error
			if layout, err = normalizeInitialOSDs(c.initialOSDs); err != nil {
				return err
			}
		}
	}
	if c.set&clusterDefaultRoot != 0 && (len(c.crushRoot) > 128 || !crushLocationName.MatchString(c.crushRoot)) {
		return fmt.Errorf("default CRUSH root must use letters, digits, underscores, dots or dashes")
	}
	if c.set&clusterPoolDefaults != 0 && (c.replicas < 1 || c.replicas > 10 || c.minSize < 1 || c.minSize > c.replicas) {
		return fmt.Errorf("pool defaults require 1 <= min_size <= replicas <= 10")
	}
	if c.set&clusterMonitors != 0 {
		o.monitors = c.monitors
	}
	// The no-initial settings keep the prospective counts that pool defaults
	// derive from; prepareInitialComposition suppresses the launches.
	if c.set&clusterManagers != 0 {
		o.noInitialManagers = c.noManagers
		if !c.noManagers {
			o.managers = c.managers
		}
	}
	if c.set&clusterOSDs != 0 {
		o.noInitialOSDs, o.initialOSDs = c.noOSDs, layout
		if !c.noOSDs {
			o.osds = c.osds
		}
	}
	if c.set&clusterDefaultRoot != 0 {
		o.defaultCRUSHRoot = c.crushRoot
	}
	if c.set&clusterPoolDefaults != 0 {
		o.poolReplicas, o.poolMinSize, o.poolDefaultsSet = c.replicas, c.minSize, true
	}
	if c.set&clusterPools != 0 {
		o.pools = clonePoolConfigs(c.pools)
	}
	o.clusterSet |= c.set
	return nil
}

// normalizeInitialOSDs validates an initial layout and returns a normalized copy
// whose omitted roots stay empty until all Run options are applied.
func normalizeInitialOSDs(configs []OSDConfig) ([]OSDConfig, error) {
	if len(configs) == 0 {
		return nil, fmt.Errorf("initial OSD layout must not be empty")
	}
	layout := make([]OSDConfig, len(configs))
	for i, config := range configs {
		var err error
		layout[i], err = normalizeOSDConfig(config)
		if err != nil {
			return nil, fmt.Errorf("initial OSD %d: %w", i, err)
		}
		layout[i].Root = config.Root
		for j := 0; j < i; j++ {
			if layout[i].Host != "" && layout[i].Host == layout[j].Host && (layout[i].Rack != layout[j].Rack || (layout[i].Root != "" && layout[j].Root != "" && layout[i].Root != layout[j].Root)) {
				return nil, fmt.Errorf("host %q has conflicting CRUSH locations", layout[i].Host)
			}
		}
	}
	return layout, nil
}

func clusterFieldName(f clusterField) string {
	switch {
	case f&clusterMonitors != 0:
		return "monitor count"
	case f&clusterManagers != 0:
		return "managers"
	case f&clusterOSDs != 0:
		return "initial OSDs"
	case f&clusterDefaultRoot != 0:
		return "default CRUSH root"
	case f&clusterPoolDefaults != 0:
		return "pool defaults"
	default:
		return "pools"
	}
}
