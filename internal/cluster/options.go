package cluster

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

type options struct {
	osds                   int
	initialOSDs            []OSDConfig
	initialOSDsExplicit    bool
	noInitialOSDs          bool
	poolReplicas           int
	poolMinSize            int
	poolDefaultsSet        bool
	defaultCRUSHRoot       string
	pools                  []PoolConfig
	rbdPools               []PoolConfig
	rbdPoolNames           []string
	filesystems            []CephFSConfig
	gateways               []RGWConfig
	monitors               int
	managers               int
	managerCountExplicit   bool
	noInitialManagers      bool
	blockSize              int64
	osdMemorySize          int64
	startupTimeout         time.Duration
	osdImage               string
	rgwImage               string
	mdsImage               string
	controlImage           string
	hostNetwork            bool
	separateClusterNetwork bool
	publicAddress          string
	hostAddressSet         bool
	messengerMode          MessengerMode
}

// WithMessengerMode selects the Messenger policy at bootstrap. MessengerDefault
// retains Ceph's image defaults and v1/v2 addresses. MessengerV2Secure requires
// encrypted v2 connections without CRC-mode or v1 fallback, including later
// daemons and WithClient containers. This option does not enable RGW HTTP TLS.
func WithMessengerMode(mode MessengerMode) Option {
	return func(o *options) error {
		if mode != MessengerDefault && mode != MessengerV2Secure {
			return fmt.Errorf("unsupported Messenger mode %d", mode)
		}
		o.messengerMode = mode
		return nil
	}
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
// The control image must still contain the MGR and its required dependencies.
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

// WithSeparateClusterNetwork creates a second owned bridge for OSD replication,
// recovery and heartbeat traffic. MON/MGR/MDS/RGW and WithClient use only the
// public bridge. Docker selects both subnets. This option requires bridge mode.
func WithSeparateClusterNetwork() Option {
	return func(o *options) error {
		o.separateClusterNetwork = true
		return nil
	}
}

// WithHostAddress selects a local IPv4 address to bind and advertise in host
// mode, typically the Docker host's address when Docker is remote. The address
// must be assigned on the Docker daemon host and reachable from the test
// process; Run verifies both. Requires WithHostNetwork.
func WithHostAddress(address string) Option {
	return func(o *options) error {
		ip := net.ParseIP(address)
		if ip == nil || ip.To4() == nil || ip.IsUnspecified() || ip.IsMulticast() || ip.Equal(net.IPv4bcast) {
			return fmt.Errorf("host address must be a specific IPv4 address")
		}
		o.publicAddress = ip.String()
		o.hostAddressSet = true
		return nil
	}
}

// WithOSDImage selects the image for initial and subsequently added OSDs.
// If omitted, OSDs use the image passed to Run. Use matching Ceph versions.
func WithOSDImage(image string) Option {
	return func(o *options) error {
		if strings.TrimSpace(image) == "" {
			return fmt.Errorf("OSD image must not be blank")
		}
		o.osdImage = image
		return nil
	}
}

// WithRGWImage selects the image used by gateways started with StartRGW.
// If omitted, RGW uses the image passed to Run. Use matching Ceph versions.
func WithRGWImage(image string) Option {
	return func(o *options) error {
		if strings.TrimSpace(image) == "" {
			return fmt.Errorf("RGW image must not be blank")
		}
		o.rgwImage = image
		return nil
	}
}

// WithMDSImage selects the metadata server image used by StartCephFS.
// If omitted, MDS uses the image passed to Run. Use matching Ceph versions.
func WithMDSImage(image string) Option {
	return func(o *options) error {
		if strings.TrimSpace(image) == "" {
			return fmt.Errorf("MDS image must not be blank")
		}
		o.mdsImage = image
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

// WithOSDBlockSize sets each sparse BlueStore file's logical size in bytes.
// The minimum is 64 MiB and the default remains 1 GiB. These are disposable
// files, not real disks. Small functional fixtures can use WithNoInitialOSDs
// and TemporaryConfig to skip mClock's startup capacity benchmark before
// AddOSD; the benchmark can exhaust a small file even without client data.
func WithOSDBlockSize(size int64) Option {
	return func(o *options) error {
		if size < 64<<20 {
			return fmt.Errorf("OSD block size must be at least 64 MiB")
		}
		o.blockSize = size
		return nil
	}
}

// WithOSDInMemoryStorage keeps all OSD stores in one Docker-managed tmpfs
// volume. maxBytes is a positive cluster-wide filesystem allocation ceiling,
// independent of each sparse file's logical WithOSDBlockSize. It does not
// reserve RAM or limit daemon RSS; tmpfs may swap, and a full volume fails I/O.
// An owned helper using the control image preserves data across OSD Stop/Start.
// Cluster termination discards the volume. Helper loss or Docker host/VM
// restart can lose data. Requires a Linux Docker engine with local tmpfs
// volume support, including compatible Docker Desktop engines. No host mount,
// privileged container or additional image is required. The default remains
// container-local sparse files. NoInitialOSDs defers allocation until AddOSD.
func WithOSDInMemoryStorage(maxBytes int64) Option {
	return func(o *options) error {
		if maxBytes <= 0 {
			return fmt.Errorf("OSD in-memory storage size must be positive")
		}
		o.osdMemorySize = maxBytes
		return nil
	}
}

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
