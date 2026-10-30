package ceph

import (
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

type options struct {
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
	// osdStorageSet records which OSDStorage fields a carrier already set, so a
	// second carrier setting the same field is refused.
	osdStorageSet  osdStorageField
	auth           AuthConfig
	osdObjectStore OSDObjectStore
	startupTimeout time.Duration
	// essentialImage is the Run image. It starts every MON, MGR and OSD and the
	// control-plane CLI executor. Role images come from imageSet.
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
