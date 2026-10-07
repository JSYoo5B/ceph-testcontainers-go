package ceph

import (
	"fmt"
	"slices"
	"strings"
)

// WithPools creates the selected pools after initial OSD startup. Repeated
// options replace the list. Configs and nested EC settings are copied; cluster
// defaults are resolved after all Run options have been applied.
func WithPools(configs ...PoolConfig) Option {
	return func(o *options) error {
		pools := make([]PoolConfig, len(configs))
		for i, config := range configs {
			pools[i] = clonePoolConfig(config)
		}
		o.pools = pools
		return nil
	}
}

// WithCephFS creates named filesystems and their MDS topologies during Run.
// No arguments selects the default filesystem. Repeated options replace the
// list; StartCephFSWithConfig can add further filesystems after Run returns.
func WithCephFS(configs ...CephFSConfig) Option {
	if len(configs) == 0 {
		configs = []CephFSConfig{{}}
	}
	return func(o *options) error {
		filesystems := make([]CephFSConfig, len(configs))
		for i, config := range configs {
			filesystems[i] = cloneCephFSConfig(config)
		}
		o.filesystems = filesystems
		return nil
	}
}

// WithRGW starts named gateways during Run. No arguments starts one default
// gateway. Repeated options replace the list. All gateways share this cluster's
// backing zone; multisite scope is configured separately by multicluster APIs.
func WithRGW(configs ...RGWConfig) Option {
	if len(configs) == 0 {
		configs = []RGWConfig{{}}
	}
	return func(o *options) error {
		o.gateways = slices.Clone(configs)
		return nil
	}
}

// Gateways returns named gateway descriptors sorted by gateway name, including
// partially initialized gateways. The cluster owns their containers.
func (c *Container) Gateways() []*RGWContainer {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]*RGWContainer, 0, len(c.gateways))
	for _, gateway := range c.gateways {
		result = append(result, gateway)
	}
	slices.SortFunc(result, func(a, b *RGWContainer) int { return strings.Compare(a.GatewayName, b.GatewayName) })
	return result
}

// Filesystems returns owned filesystem descriptors sorted by filesystem name,
// including a descriptor whose setup partially failed. The cluster owns MDSs.
func (c *Container) Filesystems() []*CephFSContainer {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]*CephFSContainer, 0, len(c.filesystems))
	for _, fs := range c.filesystems {
		result = append(result, fs)
	}
	slices.SortFunc(result, func(a, b *CephFSContainer) int { return strings.Compare(a.FilesystemName, b.FilesystemName) })
	return result
}

func clonePoolConfig(config PoolConfig) PoolConfig {
	if config.ErasureCode != nil {
		ec := *config.ErasureCode
		config.ErasureCode = &ec
	}
	return config
}

func cloneCephFSConfig(config CephFSConfig) CephFSConfig {
	config.MetadataPool = clonePoolConfig(config.MetadataPool)
	config.DataPool = clonePoolConfig(config.DataPool)
	config.AdditionalDataPools = slices.Clone(config.AdditionalDataPools)
	for i := range config.AdditionalDataPools {
		config.AdditionalDataPools[i] = clonePoolConfig(config.AdditionalDataPools[i])
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

func resolveFilesystemDefaults(settings options, config CephFSConfig) CephFSConfig {
	config = cloneCephFSConfig(config)
	config.MetadataPool = resolvePoolDefaults(settings, config.MetadataPool)
	config.DataPool = resolvePoolDefaults(settings, config.DataPool)
	for i := range config.AdditionalDataPools {
		config.AdditionalDataPools[i] = resolvePoolDefaults(settings, config.AdditionalDataPools[i])
	}
	return config
}

// Validate every initial pool against the requested placement before allocating
// a network or daemon. Runtime IDs are unknown here; ordinal IDs suffice to
// model the distinct OSD/host/rack domains of the initial layout.
func prepareInitialComposition(settings *options) error {
	if settings.noInitialOSDs {
		if settings.initialOSDsExplicit || len(settings.initialOSDs) != 0 {
			return fmt.Errorf("WithNoInitialOSDs cannot be combined with explicit initial OSD count or layout")
		}
		if len(settings.pools) != 0 || len(settings.filesystems) != 0 || len(settings.gateways) != 0 {
			return fmt.Errorf("WithNoInitialOSDs requires no initial user pools, CephFS filesystems or RGW gateways; add OSDs before provisioning them")
		}
		// Run resolves positive prospective pool defaults before this suppresses
		// initial launches. Later AddOSD uses the same placement and defaults.
		settings.osds = 0
	}
	gatewayNames := make(map[string]bool)
	for i, config := range settings.gateways {
		config, err := normalizeRGWConfig(config)
		if err != nil {
			return err
		}
		if gatewayNames[config.Name] {
			return fmt.Errorf("initial RGW gateway %q is duplicated", config.Name)
		}
		gatewayNames[config.Name] = true
		settings.gateways[i] = config
	}
	settings.defaultCRUSHRoot = clusterCRUSHRoot(*settings)
	planned := &Container{osds: make(map[int]*OSDContainer)}
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
		planned.osds[i] = &OSDContainer{ID: i, placement: config}
		if len(settings.initialOSDs) != 0 {
			// Keep host omission until a real OSD ID is assigned at startup.
			config.Host = settings.initialOSDs[i].Host
			settings.initialOSDs[i] = config
		}
	}
	locations := make([]OSDConfig, 0, len(planned.osds))
	for _, osd := range planned.osds {
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
		return fmt.Errorf("default CRUSH root %q needs %d initial OSDs for automatically created pools, requested layout has %d; select a populated root with WithDefaultCRUSHRoot", settings.defaultCRUSHRoot, needed, eligible)
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
	fsNames := make(map[string]bool)
	for i, config := range settings.filesystems {
		config, err := normalizeCephFSConfig(resolveFilesystemDefaults(*settings, config))
		if err != nil {
			return err
		}
		if fsNames[config.Name] {
			return fmt.Errorf("initial filesystem %q is duplicated", config.Name)
		}
		fsNames[config.Name] = true
		for _, pool := range cephFSConfiguredPools(config) {
			if err := validatePool(pool); err != nil {
				return err
			}
		}
		settings.filesystems[i] = config
	}
	return nil
}
