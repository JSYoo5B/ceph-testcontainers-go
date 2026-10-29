package ceph

import (
	"fmt"
)

// clonePoolConfigs copies configs and their nested EC settings. A non-nil empty
// list stays non-nil.
func clonePoolConfigs(configs []PoolConfig) []PoolConfig {
	if configs == nil {
		return nil
	}
	pools := make([]PoolConfig, len(configs))
	for i, config := range configs {
		pools[i] = clonePoolConfig(config)
	}
	return pools
}

func clonePoolConfig(config PoolConfig) PoolConfig {
	if config.ErasureCode != nil {
		ec := *config.ErasureCode
		config.ErasureCode = &ec
	}
	return config
}

func resolvePoolDefaults(settings options, config PoolConfig) PoolConfig {
	config = clonePoolConfig(config)
	if config.CRUSHRoot == "" {
		config.CRUSHRoot = clusterCRUSHRoot(settings)
	}
	if config.ErasureCode == nil {
		if config.Replicas == 0 && settings.poolReplicas > 0 {
			config.Replicas = settings.poolReplicas
		}
		if config.MinSize == 0 && settings.poolMinSize > 0 {
			config.MinSize = settings.poolMinSize
		}
	}
	return config
}

// Validate every initial pool against the requested placement before allocating
// a network or daemon. Runtime IDs are unknown here; ordinal IDs suffice to
// model the distinct OSD/host/rack domains of the initial layout.
func prepareInitialComposition(settings *options) error {
	needsManagers := false
	for _, service := range settings.services {
		for _, pool := range service.InitialPools() {
			settings.pools = append(settings.pools, clonePoolConfig(pool))
		}
		needsManagers = needsManagers || service.NeedsManagers()
	}
	if settings.noInitialOSDs {
		if len(settings.pools) != 0 || len(settings.services) != 0 {
			return fmt.Errorf("ClusterConfig.WithNoInitialOSDs requires no initial user pools, CephFS filesystems or RGW gateways; add OSDs before provisioning them")
		}
		// Run resolves positive prospective pool defaults before this suppresses
		// initial launches. Later AddOSD uses the same placement and defaults.
		settings.osds = 0
	}
	if settings.noInitialManagers {
		if needsManagers {
			return fmt.Errorf("ClusterConfig.WithNoInitialManagers requires no initial CephFS filesystems or RGW gateways; add a manager before provisioning them")
		}
		settings.managers = 0
	}
	settings.defaultCRUSHRoot = clusterCRUSHRoot(*settings)
	planned := &Container{}
	for i := range settings.osds {
		var config OSDConfig
		if len(settings.initialOSDs) != 0 {
			config = settings.initialOSDs[i]
		}
		config, err := normalizeOSDConfig(resolveOSDDefaults(*settings, config))
		if err != nil {
			return err
		}
		if config.Host == "" {
			config.Host = fmt.Sprintf("osd-%d", i)
		}
		planned.osdHandles.register(i, &OSDContainer{ID: i, placement: config})
		if len(settings.initialOSDs) != 0 {
			// Keep host omission until a real OSD ID is assigned at startup.
			config.Host = settings.initialOSDs[i].Host
			settings.initialOSDs[i] = config
		}
	}
	locations := make([]OSDConfig, 0, planned.osdHandles.count())
	for _, osd := range planned.osdHandles.each() {
		locations = append(locations, osd.Placement())
	}
	if err := validateCRUSHHierarchy(settings.defaultCRUSHRoot, locations); err != nil {
		return err
	}
	defaults := PoolConfig{CRUSHRoot: settings.defaultCRUSHRoot, FailureDomain: "osd"}
	eligible := planned.poolPlacementDomains(defaults)
	needed := settings.poolReplicas
	if needed == 0 {
		needed = min(2, settings.osds)
	}
	if !settings.noInitialOSDs && (eligible < needed || eligible == 0) {
		return fmt.Errorf("default CRUSH root %q needs %d initial OSDs for automatically created pools, requested layout has %d; select a populated root with ClusterConfig.WithDefaultCRUSHRoot", settings.defaultCRUSHRoot, needed, eligible)
	}
	names := make(map[string]bool)
	validatePool := func(config PoolConfig) error {
		if names[config.Name] {
			return fmt.Errorf("initial pool %q is duplicated", config.Name)
		}
		names[config.Name] = true
		needed := config.Replicas
		if ec := config.ErasureCode; ec != nil {
			needed = ec.K + ec.M
		}
		if domains := planned.poolPlacementDomains(config); domains < needed {
			return fmt.Errorf("initial pool %q needs %d distinct %s domains, requested layout has %d", config.Name, needed, config.FailureDomain, domains)
		}
		return nil
	}
	for i, config := range settings.pools {
		config, err := normalizePoolConfig(resolvePoolDefaults(*settings, config))
		if err != nil {
			return err
		}
		if err := validatePool(config); err != nil {
			return err
		}
		settings.pools[i] = config
	}
	for _, service := range settings.services {
		if err := service.Prepare(func(pool PoolConfig) PoolConfig { return resolvePoolDefaults(*settings, pool) }, validatePool); err != nil {
			return err
		}
	}
	return nil
}
