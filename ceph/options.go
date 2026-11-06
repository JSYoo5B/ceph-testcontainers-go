package ceph

import (
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

type options struct {
	// monitorVolume keeps every MON store in its own cluster-owned volume
	// (WithMonitorStorage).
	monitorVolume     bool
	imageSet          *ImageSet
	osds              int
	initialOSDs       []OSDConfig
	noInitialOSDs     bool
	poolReplicas      int
	poolMinSize       int
	poolDefaultsSet   bool
	defaultCRUSHRoot  string
	pools             []PoolConfig
	services          []serviceBootstrap
	monitors          int
	managers          int
	noInitialManagers bool
	blockSize         int64
	osdMemorySize     int64
	osdVolumeStorage  bool
	osdBenchmark      OSDBenchmarkPolicy
	// osdStorageSet records which OSDStorage fields a carrier already set, so a
	// second carrier setting the same field is refused.
	osdStorageSet  osdStorageField
	auth           AuthConfig
	osdObjectStore OSDObjectStore
	startupTimeout time.Duration
	// essentialImage is the Run image. It starts every MON and OSD and the
	// control-plane CLI executor, and the MGRs unless imageSet.MGR selects
	// another image. Role images come from imageSet.
	essentialImage string
	// cephfsOverride and rgwOverride hold the images of
	// ImageOptions.WithCephFSImage and WithRGWImage. They replace the role of
	// imageSet.
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
	// clusterSet records which ClusterConfig fields a carrier already set, so a
	// second carrier setting the same field is refused.
	clusterSet clusterField
	// zones is the ZoneConfig group; see zoneSettings.
	zones zoneSettings
	// managerPorts are the TCP ports every bridge-mode MGR container
	// publishes (WithManagerPorts); nil publishes none.
	managerPorts []int
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
// MON port reservation is retried up to five times when another process takes
// a port, and the reachability probe is retried for up to 5 seconds; later
// MON, MGR, OSD, MDS and RGW endpoints are not probed. MGR, OSD and MDS use
// Ceph's dynamic ports. Docker Desktop needs host networking enabled (4.34 or
// later), and a test process that itself runs in a bridge-network container
// cannot use host mode. It cannot be combined with
// NetworkConfig.WithSeparateClusterNetwork, and fault.InterruptNetwork and
// fault.ApplyNetworkProfile are unavailable in host mode.
//
//purpose:construction
func WithHostNetwork() Option {
	return func(o *options) error {
		o.hostNetwork = true
		return nil
	}
}

// WithManagerPorts publishes the listed TCP ports of every MGR container,
// the initial ones and those of AddManager, on ports the Docker engine
// selects, so that a process on the Docker host reaches a MGR module listener
// such as the Dashboard's (dashboard.Dashboard.HostEndpoint). Without it a
// bridge-mode MGR publishes no port and only containers on the cluster network
// reach its listeners. Docker cannot publish a port of a created container,
// so the ports are fixed for the cluster's lifetime. Read a manager's mapped
// port with its handle's MappedPort; a stopped manager has none and a started
// one may get another. The ports must be the ones the module listens on in
// the container: WithManagerPorts does not configure a module.
//
// A port outside 1 to 65535, a repeated port, an empty list, a second
// WithManagerPorts and the combination with WithHostNetwork, where the
// managers share the host's ports, are refused.
//
//purpose:construction,handoff
func WithManagerPorts(ports ...int) Option {
	return func(o *options) error {
		if o.managerPorts != nil {
			return fmt.Errorf("manager ports set by more than one WithManagerPorts")
		}
		if len(ports) == 0 {
			return fmt.Errorf("WithManagerPorts needs at least one port")
		}
		for i, port := range ports {
			if port < 1 || port > 65535 {
				return fmt.Errorf("manager port %d is outside 1 to 65535", port)
			}
			if slices.Contains(ports[:i], port) {
				return fmt.Errorf("manager port %d is listed twice", port)
			}
		}
		o.managerPorts = slices.Clone(ports)
		return nil
	}
}

// managerExposedPorts returns the container ports every MGR container
// publishes: those of WithManagerPorts, in bridge mode.
func (o options) managerExposedPorts() []string {
	if o.hostNetwork {
		return nil
	}
	ports := make([]string, 0, len(o.managerPorts))
	for _, port := range o.managerPorts {
		ports = append(ports, strconv.Itoa(port)+"/tcp")
	}
	return ports
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
// Run's whole bootstrap, from validation to the initial services, shares it.
// Each operation is also bounded by the caller's context. Default: 3 minutes.
//
// It bounds Run as a whole and each later topology, readiness and wait
// operation, such as AddOSD, WaitForClean, Pool.WaitForPGCount and
// DeepScrubPG, so a wait never outlives the shorter of the two limits.
// Increase it for slow engines, large pools or long recovery waits.
//
//purpose:construction
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
