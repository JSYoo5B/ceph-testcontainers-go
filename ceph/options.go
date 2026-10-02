package ceph

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

type options struct {
	osds             int
	initialOSDs      []OSDConfig
	poolReplicas     int
	poolMinSize      int
	poolDefaultsSet  bool
	defaultCRUSHRoot string
	pools            []PoolConfig
	filesystems      []CephFSConfig
	gateways         []RGWConfig
	monitors         int
	managers         int
	blockSize        int64
	startupTimeout   time.Duration
	osdImage         string
	rgwImage         string
	mdsImage         string
	controlImage     string
	hostNetwork      bool
	publicAddress    string
	hostAddressSet   bool
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
func WithHostNetwork() Option {
	return func(o *options) error {
		o.hostNetwork = true
		return nil
	}
}

// WithHostAddress selects a local IPv4 address to bind and advertise in host
// mode. Use an address reachable by remote clients when Docker is remote.
// The address must exist on the Docker daemon host. Requires WithHostNetwork.
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

// WithRGWImage selects the image used by StartRGW.
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

// WithOSDCount sets the initial number of OSD containers (default: 2).
func WithOSDCount(count int) Option {
	return func(o *options) error {
		if count < 1 {
			return fmt.Errorf("OSD count must be at least 1")
		}
		o.osds = count
		o.initialOSDs = nil
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
// The default is 1 GiB; these are disposable files, not real disks.
func WithOSDBlockSize(size int64) Option {
	return func(o *options) error {
		if size < 1<<30 {
			return fmt.Errorf("OSD block size must be at least 1 GiB")
		}
		o.blockSize = size
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
