package ceph

import (
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

type options struct {
	imageSet             *ImageSet
	osds                 int
	initialOSDs          []OSDConfig
	initialOSDsExplicit  bool
	noInitialOSDs        bool
	poolReplicas         int
	poolMinSize          int
	poolDefaultsSet      bool
	defaultCRUSHRoot     string
	pools                []PoolConfig
	services             []serviceBootstrap
	monitors             int
	managers             int
	managerCountExplicit bool
	noInitialManagers    bool
	blockSize            int64
	osdMemorySize        int64
	osdVolumeStorage     bool
	// osdStorageSet records which OSDStorage fields a carrier already set, so a
	// second carrier setting the same field is refused.
	osdStorageSet  osdStorageField
	auth           AuthConfig
	osdObjectStore OSDObjectStore
	startupTimeout time.Duration
	// essentialImage is the Run image. It starts every MON, MGR and OSD and the
	// control-plane CLI executor. Role images come from imageSet.
	essentialImage string
	// cephfsOverride and rgwOverride hold the images of cephfs.WithMDSImage and
	// rgw.WithImage. They replace the role of imageSet whatever the option order.
	cephfsOverride, rgwOverride string
	hostNetwork                 bool
	separateClusterNetwork      bool
	publicAddress               string
	hostAddressSet              bool
	messengerMode               MessengerMode
	messengerPolicy             MessengerPolicy
	addressFamily               AddressFamily
	userConfig                  []userConfigSection
	// networkSet records which NetworkConfig fields a carrier already set, so a
	// second carrier setting the same field is refused.
	networkSet networkField
}

// WithMonitorCount selects the initial monitor count. Three enables quorum
// failover tests; even counts are allowed for deliberate quorum-loss scenarios.
func WithMonitorCount(count int) Option {
	return func(o *options) error {
		if count < 1 {
			return fmt.Errorf("monitor count must be at least 1")
		}
		o.monitors = count
		return nil
	}
}

// WithManagerCount starts one active manager and the remaining standbys.
func WithManagerCount(count int) Option {
	return func(o *options) error {
		if count < 1 {
			return fmt.Errorf("manager count must be at least 1")
		}
		o.managers = count
		o.managerCountExplicit = true
		return nil
	}
}

// WithNoInitialManagers defers the first MGR until an explicit AddManager.
// Run waits for MON quorum and requested OSDs up/in, without implying PG clean
// or MGR module readiness. Initial CephFS/RGW services and an explicit manager
// count are rejected before allocation; positive-OSD initial pools are allowed.
// The ordinary default is one MGR and WithManagerCount(0) remains invalid.
// The essential image must still contain the MGR and its required dependencies.
func WithNoInitialManagers() Option {
	return func(o *options) error {
		o.noInitialManagers = true
		return nil
	}
}

const hostPortAttempts = 5

// Option transfers cluster settings outside the monitor's container request.
type Option func(*options) error

// Customize implements testcontainers.ContainerCustomizer.
func (Option) Customize(*testcontainers.GenericContainerRequest) error { return nil }

// WithHostNetwork places every cluster daemon and WithClient container in the
// Docker daemon's host network. MON and RGW ports are selected automatically.
// The default advertised address is 127.0.0.1 for clients on that same host.
// Run fails with ErrHostNetworkUnavailable unless the engine runs Linux
// containers and the test process reaches the first MON's advertised endpoint.
func WithHostNetwork() Option {
	return func(o *options) error {
		o.hostNetwork = true
		return nil
	}
}

// WithNoInitialOSDs starts only the selected MON/MGR topology. It keeps the
// ordinary prospective pool defaults (size 2, min_size 1) for later AddOSD calls;
// WithPoolDefaults may override them. Initial pools, CephFS/RGW services and
// explicit WithOSDCount/WithInitialOSDs options are rejected before allocation.
// No storage readiness is implied: native internal pools may have pending PGs.
// WithOSDCount(0) and WithInitialOSDs() keep their existing validation errors.
func WithNoInitialOSDs() Option {
	return func(o *options) error {
		o.noInitialOSDs = true
		return nil
	}
}

// WithOSDCount sets the initial number of OSD containers (default: 2).
func WithOSDCount(count int) Option {
	return func(o *options) error {
		if count < 1 {
			return fmt.Errorf("OSD count must be at least 1")
		}
		o.osds = count
		o.initialOSDs = nil
		o.initialOSDsExplicit = true
		return nil
	}
}

// WithInitialOSDs selects the count and logical CRUSH locations of initial OSDs.
// A subsequent WithOSDCount replaces this layout with default OSD locations.
func WithInitialOSDs(configs ...OSDConfig) Option {
	return func(o *options) error {
		if len(configs) == 0 {
			return fmt.Errorf("initial OSD layout must not be empty")
		}
		layout := make([]OSDConfig, len(configs))
		for i, config := range configs {
			var err error
			layout[i], err = normalizeOSDConfig(config)
			if err != nil {
				return fmt.Errorf("initial OSD %d: %w", i, err)
			}
			// Resolve omitted roots only after all Run options are applied.
			layout[i].Root = config.Root
			for j := 0; j < i; j++ {
				if layout[i].Host != "" && layout[i].Host == layout[j].Host && (layout[i].Rack != layout[j].Rack || (layout[i].Root != "" && layout[j].Root != "" && layout[i].Root != layout[j].Root)) {
					return fmt.Errorf("host %q has conflicting CRUSH locations", layout[i].Host)
				}
			}
		}
		o.initialOSDs, o.osds = layout, len(layout)
		o.initialOSDsExplicit = true
		return nil
	}
}

// WithDefaultCRUSHRoot selects placement for pools that Ceph creates itself,
// including .mgr, and for omitted PoolConfig.CRUSHRoot/OSDConfig.Root fields.
// The initial layout must contain enough OSDs in this root for the default
// replica count. Without this option the root is default.
func WithDefaultCRUSHRoot(root string) Option {
	return func(o *options) error {
		if len(root) > 128 || !crushLocationName.MatchString(root) {
			return fmt.Errorf("default CRUSH root must use letters, digits, underscores, dots or dashes")
		}
		o.defaultCRUSHRoot = root
		return nil
	}
}

// WithPoolDefaults selects size/min_size for pools created by Ceph or callers.
// Without this option the fast fixture uses size=min(2, OSD count), min_size=1.
// CreatePool can override these defaults for each explicitly configured pool.
func WithPoolDefaults(replicas, minSize int) Option {
	return func(o *options) error {
		if replicas < 1 || replicas > 10 || minSize < 1 || minSize > replicas {
			return fmt.Errorf("pool defaults require 1 <= min_size <= replicas <= 10")
		}
		o.poolReplicas, o.poolMinSize, o.poolDefaultsSet = replicas, minSize, true
		return nil
	}
}

// OSDObjectStore names the object store that OSDs keep their data in.
type OSDObjectStore string

const (
	// OSDBlueStore keeps each OSD's data in a sparse BlueStore file. It is the
	// default.
	OSDBlueStore OSDObjectStore = "bluestore"
	// OSDMemStore keeps each OSD's data in the OSD process's memory, with
	// OSDStorage.WithBlockSize as its capacity. A clean stop saves the store to the
	// OSD's data directory and Start loads it again; a killed OSD loses it.
	// It is Ceph's test backend and needs no block file.
	OSDMemStore OSDObjectStore = "memstore"
)

// WithStartupTimeout bounds bootstrap, readiness and topology operations.
// Each operation is also bounded by the caller's context. Default: 3 minutes.
func WithStartupTimeout(timeout time.Duration) Option {
	return func(o *options) error {
		if timeout <= 0 {
			return fmt.Errorf("startup timeout must be positive")
		}
		o.startupTimeout = timeout
		return nil
	}
}

// objectStore returns the selected OSD object store, BlueStore by default.
func (o options) objectStore() OSDObjectStore {
	if o.osdObjectStore == "" {
		return OSDBlueStore
	}
	return o.osdObjectStore
}
