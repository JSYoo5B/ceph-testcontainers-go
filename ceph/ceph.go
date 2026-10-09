package ceph

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jsyoo5b/ceph-testcontainers-go/internal/dockerbridge"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// DefaultImage pins the multi-architecture manifest tested by this PoC.
const DefaultImage = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"

//go:embed internal/scripts/*.sh
var scripts embed.FS

// Container embeds the MON/control container and owns the MGR, OSDs, optional
// RGW/MDS services and network.
// Use Terminate to clean up the entire cluster. Generic Run options customize
// the MON; module options configure the cluster. Use WithHostNetwork to change
// cluster networking, rather than changing only the MON's network mode.
type Container struct {
	testcontainers.Container
	mu                    sync.Mutex
	cephfsSetupMu         sync.Mutex
	controlMu             sync.RWMutex
	configMu              sync.RWMutex
	configOverrides       map[string]*ConfigOverride
	flagOverrides         map[string]*OSDFlagOverride
	fullRatiosOverride    *FullRatiosOverride
	blocklistOverrides    map[string]*BlocklistOverride
	moduleOverrides       map[string]*MGRModuleOverride
	controlPlane          testcontainers.Container
	settings              options
	network               *testcontainers.DockerNetwork
	clusterNetwork        *testcontainers.DockerNetwork
	publicSubnet          string
	clusterSubnet         string
	clusterNetworkRemoved bool
	interruptions         map[string]*NetworkInterruption
	pauses                map[string]*ContainerPause
	manager               testcontainers.Container
	monitors              map[string]*MonitorContainer
	managers              map[string]*ManagerContainer
	services              map[string]testcontainers.Container
	filesystems           map[string]*CephFSContainer
	gateways              map[string]*RGWContainer
	osds                  map[int]*OSDContainer
	osdMemory             *osdMemoryStorage
	config                []byte
	keyring               []byte
	portLeases            []*hostPortLease
	closed                bool
	monitorTerminated     bool
	networkRemoved        bool
}

// OSDContainer is one storage daemon backed by a sparse BlueStore file.
// The file is container-local unless WithOSDInMemoryStorage is selected.
// Stop/Start can be used for failure injection; RemoveOSD drains and purges it.
type OSDContainer struct {
	testcontainers.Container
	ID          int
	nativeUUID  string
	placement   OSDConfig
	purged      bool
	purgeIssued bool
}

// Run creates configurable MON, MGR and OSD containers for a disposable cluster.
// img must satisfy the control role: MON/MGR, CLI, Python clients and mirrors.
// Other roles use img unless overridden by WithOSDImage, WithRGWImage or
// WithMDSImage. Images contain runtime tools; this module supplies cluster setup.
// A non-nil Container returned with an error must still be terminated.
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	settings := options{
		osds: 2, monitors: 1, managers: 1, blockSize: 1 << 30, startupTimeout: 3 * time.Minute,
		osdImage: img, rgwImage: img, mdsImage: img, controlImage: img, publicAddress: "127.0.0.1",
	}
	for _, opt := range opts {
		if opt, ok := opt.(Option); ok {
			if err := opt(&settings); err != nil {
				return nil, fmt.Errorf("configure ceph: %w", err)
			}
		}
	}
	if settings.hostAddressSet && !settings.hostNetwork {
		return nil, errors.New("WithHostAddress requires WithHostNetwork")
	}
	if settings.hostNetwork && settings.separateClusterNetwork {
		return nil, errors.New("WithSeparateClusterNetwork requires bridge mode")
	}
	if !settings.poolDefaultsSet {
		settings.poolReplicas, settings.poolMinSize = min(2, settings.osds), 1
	}
	if err := prepareInitialComposition(&settings); err != nil {
		return nil, fmt.Errorf("configure initial ceph topology: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, settings.startupTimeout)
	defer cancel()
	var nw *testcontainers.DockerNetwork
	var err error
	if settings.hostNetwork {
		if err := checkHostNetworkEngine(ctx); err != nil {
			return nil, err
		}
	} else {
		nw, err = dockerbridge.New(ctx)
	}
	c := &Container{settings: settings, network: nw, osds: make(map[int]*OSDContainer), services: make(map[string]testcontainers.Container),
		monitors: make(map[string]*MonitorContainer), managers: make(map[string]*ManagerContainer), filesystems: make(map[string]*CephFSContainer), gateways: make(map[string]*RGWContainer)}
	if err != nil {
		if nw == nil {
			return nil, fmt.Errorf("create ceph network: %w", err)
		}
		return c, fmt.Errorf("create ceph network: %w", err)
	}
	if settings.separateClusterNetwork {
		if c.clusterNetwork, err = dockerbridge.New(ctx); err != nil {
			return c, fmt.Errorf("create Ceph replication network: %w", err)
		}
		if c.publicSubnet, c.clusterSubnet, err = inspectNetworkSubnets(ctx, nw.Name, c.clusterNetwork.Name); err != nil {
			return c, err
		}
	}
	mon, err := c.runMonitor(ctx, img, uuid.NewString(), opts...)
	if mon != nil {
		c.Container = mon
	}
	if err != nil {
		return c, fmt.Errorf("run ceph monitor: %w", err)
	}
	if c.config, err = readFile(ctx, mon, "/etc/ceph/ceph.conf"); err != nil {
		return c, err
	}
	if c.keyring, err = readFile(ctx, mon, "/etc/ceph/ceph.client.admin.keyring"); err != nil {
		return c, err
	}
	if err := c.configureDefaultCRUSHRoot(ctx); err != nil {
		return c, fmt.Errorf("configure default CRUSH placement: %w", err)
	}
	for i := 1; i < settings.monitors; i++ {
		if _, err := c.AddMonitor(ctx, daemonName(i)); err != nil {
			return c, err
		}
	}
	for i := range settings.managers {
		if _, err := c.AddManager(ctx, daemonName(i)); err != nil {
			return c, err
		}
	}
	for i := range settings.osds {
		var placement OSDConfig
		if len(settings.initialOSDs) != 0 {
			placement = settings.initialOSDs[i]
		}
		if _, err := c.AddOSDWithConfig(ctx, placement); err != nil {
			return c, err
		}
	}
	if err := c.waitForInitialDaemons(ctx); err != nil {
		return c, err
	}
	for _, pool := range settings.pools {
		if _, err := c.CreatePool(ctx, pool); err != nil {
			return c, err
		}
	}
	for _, fs := range settings.filesystems {
		if _, err := c.StartCephFSWithConfig(ctx, fs); err != nil {
			return c, err
		}
	}
	for _, gateway := range settings.gateways {
		if _, err := c.StartRGWWithConfig(ctx, gateway); err != nil {
			return c, err
		}
	}
	return c, nil
}

func (c *Container) waitForInitialDaemons(ctx context.Context) error {
	if c.settings.noInitialManagers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.WaitForQuorum(ctx); err != nil {
			return fmt.Errorf("wait for ceph quorum without initial managers: %w", err)
		}
		// MON OSDMap state is available before the first MGR. Its captured UUIDs
		// and up/in flags do not rely on MGR-produced PG or health statistics.
		if err := c.poll(ctx, func() (bool, error) {
			states, err := c.OSDStates(ctx)
			if err != nil {
				return false, err
			}
			for _, state := range states {
				if !state.Up || !state.In {
					return false, nil
				}
			}
			return true, nil
		}); err != nil {
			return fmt.Errorf("wait for initial OSDs without managers: %w", err)
		}
		return ctx.Err()
	}
	if err := c.poll(ctx, func() (bool, error) {
		s, err := c.Status(ctx)
		return s.MgrMap.Available, err
	}); err != nil {
		return fmt.Errorf("wait for ceph manager: %w", err)
	}
	return nil
}

// ControlImage returns the image selected for MON/MGR and auxiliary CLI/client
// containers. It is fixed by Run and remains available after termination.
// Multicluster setup clients use this image instead of a daemon-only RGW image.
func (c *Container) ControlImage() string {
	if c == nil {
		return ""
	}
	return c.settings.controlImage
}

func (c *Container) runMonitor(ctx context.Context, image, fsid string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	attempts := 1
	if c.settings.hostNetwork {
		attempts = hostPortAttempts
	}
	for attempt := range attempts {
		var lease *hostPortLease
		var err error
		if c.settings.hostNetwork {
			lease, err = reserveHostPorts(ctx, image, c.settings.publicAddress, c.settings.monitorPortCount(), c.settings.startupTimeout)
			c.trackHostPortLease(lease)
			if err != nil {
				return nil, err
			}
		}
		moduleOpts := []testcontainers.ContainerCustomizer{
			testcontainers.WithEntrypoint("/bin/sh", "/tc/mon.sh"),
			testcontainers.WithCmd(),
			testcontainers.WithEnv(map[string]string{
				"CEPH_FSID": fsid, "CEPH_OSD_BLOCK_SIZE": strconv.FormatInt(c.settings.blockSize, 10),
				"CEPH_POOL_SIZE": strconv.Itoa(c.settings.poolReplicas), "CEPH_POOL_MIN_SIZE": strconv.Itoa(c.settings.poolMinSize),
				"CEPH_PUBLIC_NETWORK": c.publicSubnet, "CEPH_CLUSTER_NETWORK": c.clusterSubnet,
			}),
			testcontainers.WithFiles(scriptFile("mon")),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status", "--format", "json"}).WithStartupTimeout(c.settings.startupTimeout)),
		}
		if lease == nil {
			moduleOpts = append(moduleOpts, testcontainers.WithExposedPorts(c.settings.monitorExposedPorts()...), network.WithNetwork([]string{"ceph-mon"}, c.network))
		} else {
			monEnv := c.settings.monitorPortEnvironment(lease.Ports)
			monEnv["CEPH_PUBLIC_ADDRESS"] = c.settings.publicAddress
			moduleOpts = append(moduleOpts, hostContainerCustomizer(c.settings.publicAddress), testcontainers.WithNoStart(),
				testcontainers.WithEnv(monEnv))
		}
		moduleOpts = append(moduleOpts, opts...)
		if c.settings.messengerMode == MessengerV2Secure {
			moduleOpts = append(moduleOpts, testcontainers.WithEnv(map[string]string{messengerV2SecureEnvironment: "true"}))
		}
		if lease != nil {
			moduleOpts = append(moduleOpts, hostContainerCustomizer(c.settings.publicAddress), testcontainers.WithNoStart())
		}
		mon, err := testcontainers.Run(ctx, image, moduleOpts...)
		if lease != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			releaseErr := lease.Release(cleanupCtx)
			cancel()
			if err != nil || releaseErr != nil {
				return mon, errors.Join(err, releaseErr)
			}
			if err = mon.Start(ctx); err == nil {
				// Validate the environment before MGR/OSD creation. A probe failure
				// is not a port conflict, so it bypasses the retry below.
				return mon, verifyHostMonitor(ctx, c.settings.publicAddress, lease.Ports[0])
			}
		}
		if err == nil {
			return mon, nil
		}
		if lease == nil || attempt == attempts-1 || ctx.Err() != nil {
			return mon, err
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		if !isPortConflict(cleanupCtx, mon) {
			cancel()
			return mon, err
		}
		cleanupErr := mon.Terminate(cleanupCtx)
		cancel()
		if !onlyMissingHostResource(cleanupErr) {
			return mon, errors.Join(err, cleanupErr)
		}
	}
	return nil, errors.New("monitor port attempts exhausted")
}

// NetworkName returns the cluster's bridge name, or "host" in host mode.
func (c *Container) NetworkName() string {
	if c.settings.hostNetwork {
		return "host"
	}
	if c.network == nil {
		return ""
	}
	return c.network.Name
}

// UsesHostNetwork reports whether the cluster shares Docker's host network.
func (c *Container) UsesHostNetwork() bool { return c.settings.hostNetwork }

// PublicAddress returns the advertised host-mode IPv4 address; empty in bridge mode.
func (c *Container) PublicAddress() string {
	if !c.settings.hostNetwork {
		return ""
	}
	return c.settings.publicAddress
}

// ConnectionConfig returns independent copies of ceph.conf and the ephemeral
// admin keyring for a native client. In host mode the MON ports are final.
func (c *Container) ConnectionConfig() ([]byte, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, nil, errors.New("ceph cluster is terminated")
	}
	c.configMu.RLock()
	defer c.configMu.RUnlock()
	if len(c.config) == 0 || len(c.keyring) == 0 {
		return nil, nil, errors.New("ceph cluster bootstrap is incomplete")
	}
	return bytes.Clone(c.config), bytes.Clone(c.keyring), nil
}

func (c *Container) trackHostPortLease(lease *hostPortLease) {
	if lease == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.portLeases = append(c.portLeases, lease)
}

// ManagerContainer returns the initial MGR a for compatibility and inspection.
// It may be nil after a is removed while another MGR remains active. Use
// Managers and ManagerStatus for the current candidates and active identity.
// The cluster owns its cleanup.
func (c *Container) ManagerContainer() testcontainers.Container {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.manager
}

// WithClient connects another container to Ceph and copies its admin credentials.
// These privileged, ephemeral credentials are for trusted test containers only.
// Caller owns and terminates that container before terminating the cluster.
func (c *Container) WithClient() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		c.configMu.RLock()
		config, keyring := bytes.Clone(c.config), bytes.Clone(c.keyring)
		c.configMu.RUnlock()
		if c.settings.hostNetwork {
			if err := hostContainerCustomizer(c.settings.publicAddress).Customize(req); err != nil {
				return err
			}
		} else {
			if err := network.WithNetworkName(nil, c.NetworkName())(req); err != nil {
				return err
			}
		}
		return testcontainers.WithFiles(
			textFile("/etc/ceph/ceph.conf", config, 0o644),
			textFile("/etc/ceph/ceph.client.admin.keyring", keyring, 0o600),
		)(req)
	}
}

// Ceph runs the Ceph CLI in the control container. Arguments are passed directly,
// without shell interpolation. Use --format json for machine-readable responses.
func (c *Container) Ceph(ctx context.Context, args ...string) ([]byte, error) {
	control, err := c.ControlContainerContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("select Ceph CLI container: %w", err)
	}
	return command(ctx, control, append([]string{"ceph", "--connect-timeout", "5"}, args...)...)
}

// AddOSD registers, formats and starts a new OSD, then waits for it to be up/in.
// Topology operations are serialized. An OSD returned with an error is tracked
// for cleanup and may be inspected; no data is silently purged on failure.
func (c *Container) AddOSD(ctx context.Context) (*OSDContainer, error) {
	return c.AddOSDWithConfig(ctx, OSDConfig{})
}

// AddOSDWithConfig starts an OSD in an explicit logical CRUSH location.
// Omitted host names resolve to osd-ID; logical hosts/racks are simulated
// placement domains on the same Docker engine, not physical failure domains.
func (c *Container) AddOSDWithConfig(ctx context.Context, config OSDConfig) (*OSDContainer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config, err := normalizeOSDConfig(resolveOSDDefaults(c.settings, config))
	if err != nil {
		return nil, err
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("ceph cluster is terminated")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, osd := range c.osds {
		if osd.purged || osd.purgeIssued {
			return nil, fmt.Errorf("finish removing osd.%d before adding another OSD", osd.ID)
		}
	}
	if err := c.validateOSDPlacement(config); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.ensureOSDMemory(ctx); err != nil {
		return nil, err
	}
	osdUUID := uuid.NewString()
	control := c.cliContainer()
	secret, err := command(ctx, control, "ceph-authtool", "--gen-print-key")
	if err != nil {
		return nil, err
	}
	keys, err := json.Marshal(map[string]string{"cephx_secret": strings.TrimSpace(string(secret))})
	if err != nil {
		return nil, err
	}
	keyPath := "/tmp/osd-" + osdUUID + ".json"
	if err := control.CopyToContainer(ctx, keys, keyPath, 0o600); err != nil {
		return nil, fmt.Errorf("copy osd registration key: %w", err)
	}
	result, err := c.Ceph(ctx, "osd", "new", osdUUID, "-i", keyPath)
	if err != nil {
		return nil, err
	}
	id, err := strconv.Atoi(strings.TrimSpace(string(result)))
	if err != nil {
		return nil, fmt.Errorf("parse osd ID %q: %w", result, err)
	}
	if config.Host == "" {
		config.Host = fmt.Sprintf("osd-%d", id)
	}
	osd := &OSDContainer{ID: id, nativeUUID: osdUUID, placement: config}
	c.osds[id] = osd
	// A generated host name is known only after registration. Preserve the
	// partial OSD descriptor if it conflicts, without moving any CRUSH bucket.
	if err := c.validateOSDPlacement(config); err != nil {
		return osd, fmt.Errorf("place registered osd.%d: %w", id, err)
	}
	keyring := []byte(fmt.Sprintf("[osd.%d]\n\tkey = %s\n", id, strings.TrimSpace(string(secret))))
	osdOptions := []testcontainers.ContainerCustomizer{
		c.WithClient(), testcontainers.WithEntrypoint("/bin/sh", "/tc/osd.sh"), testcontainers.WithCmd(),
		testcontainers.WithEnv(map[string]string{
			"CEPH_OSD_ID": strconv.Itoa(id), "CEPH_OSD_UUID": osdUUID, "CEPH_OSD_HOST": config.Host,
			"CEPH_OSD_ROOT": config.Root, "CEPH_OSD_RACK": config.Rack, "CEPH_OSD_DEVICE_CLASS": config.DeviceClass,
		}),
		testcontainers.WithFiles(scriptFile("osd"), textFile("/etc/ceph/osd.keyring", keyring, 0o600)),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", fmt.Sprintf("/var/run/ceph/ceph-osd.%d.asok", id)}).WithStartupTimeout(c.settings.startupTimeout)),
	}
	if c.osdMemory != nil {
		if err := c.osdMemory.requireFreshOSD(ctx, id); err != nil {
			return osd, err
		}
		osdOptions = append(osdOptions, c.osdMemory.mount("/var/lib/ceph/osd"))
	}
	if c.clusterNetwork != nil {
		osdOptions = append(osdOptions, network.WithNetworkName(nil, c.clusterNetwork.Name))
	}
	ctr, err := testcontainers.Run(ctx, c.settings.osdImage, osdOptions...)
	if ctr != nil {
		osd.Container = ctr
	}
	if err != nil {
		return osd, fmt.Errorf("run ceph osd.%d: %w", id, err)
	}
	if err := c.waitOSD(ctx, id, true); err != nil {
		return osd, fmt.Errorf("wait for osd.%d up/in: %w", id, err)
	}
	if config.DeviceClass != "" {
		// Ceph assigns a class from the sparse backing device during startup.
		// A different explicit class must be applied through the CRUSH CLI.
		for _, args := range [][]string{
			{"osd", "crush", "rm-device-class", fmt.Sprintf("osd.%d", id)},
			{"osd", "crush", "set-device-class", config.DeviceClass, fmt.Sprintf("osd.%d", id)},
		} {
			if _, err := c.Ceph(ctx, args...); err != nil {
				return osd, fmt.Errorf("assign osd.%d device class: %w", id, err)
			}
		}
	}
	return osd, nil
}

// RemoveOSD drains an owned OSD, waits for safe-to-destroy, stops it, waits for
// down, purges it from Ceph and removes its container. It refuses the last OSD.
// Every removal phase verifies the captured registration UUID. External OSD
// replacement must not race removal: Ceph has no UUID compare-and-swap purge.
// An uncertain purge response remains tracked; a retry reconciles fresh native
// absence only after this handle issued purge. Completed purge retries only
// Docker cleanup. On error retry with a fresh context or terminate the cluster.
func (c *Container) RemoveOSD(ctx context.Context, id int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("ceph cluster is terminated")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	osd, ok := c.osds[id]
	if !ok || osd == nil {
		return fmt.Errorf("osd.%d is not owned by this cluster", id)
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	name := fmt.Sprintf("osd.%d", id)
	if !osd.purged {
		expected, err := uuid.Parse(osd.nativeUUID)
		if err != nil || expected == uuid.Nil {
			return fmt.Errorf("osd.%d registration UUID is unavailable", id)
		}
		dump, err := c.removalOSDDump(ctx)
		if err != nil {
			return err
		}
		if _, exists := dump.osds[id]; !exists && osd.purgeIssued {
			// The previous purge may have committed despite a lost response.
			// A fresh, strict absence can settle only this handle's own request.
			osd.purged = true
		} else {
			if _, err := c.ownedOSDState(dump, id); err != nil {
				return err
			}
			remaining := 0
			for otherID, other := range c.osds {
				if other == nil || other.purged {
					continue
				}
				if _, err := c.ownedOSDState(dump, otherID); err == nil {
					remaining++
				}
			}
			if remaining <= 1 {
				return errors.New("cannot remove the last OSD; terminate the cluster instead")
			}
		}
	}
	if !osd.purged {
		// Reweight avoids the small-cluster active+remapped case documented by Ceph.
		if err := c.removalOSDMutation(ctx, id, "osd", "crush", "reweight", name, "0"); err != nil {
			return err
		}
		if err := c.removalOSDMutation(ctx, id, "osd", "out", strconv.Itoa(id)); err != nil {
			return err
		}
		if err := c.waitOSDRemoval(ctx, id, true); err != nil {
			return fmt.Errorf("drain %s: %w", name, err)
		}
		if _, err := c.removalOSDState(ctx, id); err != nil {
			return err
		}
		if osd.Container != nil {
			stopTimeout := 10 * time.Second
			if err := osd.Stop(ctx, &stopTimeout); err != nil {
				return fmt.Errorf("stop %s: %w", name, err)
			}
		}
		if err := c.waitOSDRemoval(ctx, id, false); err != nil {
			return fmt.Errorf("wait for %s down: %w", name, err)
		}
		if _, err := c.removalOSDState(ctx, id); err != nil {
			return err
		}
		// Mark before the request because cancellation/transport failure does
		// not establish whether the MON committed the purge.
		osd.purgeIssued = true
		if _, err := c.Ceph(ctx, "osd", "purge", strconv.Itoa(id), "--yes-i-really-mean-it"); err != nil {
			return err
		}
		osd.purged = true
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if osd.Container != nil {
		if err := osd.Terminate(ctx); !onlyMissingHostResource(err) {
			return fmt.Errorf("terminate %s: %w", name, err)
		}
	}
	if c.osdMemory != nil {
		if err := c.osdMemory.discardOSD(ctx, id); err != nil {
			return err
		}
	}
	delete(c.osds, id)
	return nil
}

// Native reads and each mutation remain inside the serialized removal. A
// malformed/missing/replaced identity stops the operation immediately rather
// than being polled until timeout and then mistaken for successful draining.
func (c *Container) removalOSDDump(ctx context.Context) (osdPolicySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return osdPolicySnapshot{}, err
	}
	dump, err := c.osdPolicyDump(ctx)
	if err != nil {
		return dump, err
	}
	return dump, ctx.Err()
}

func (c *Container) removalOSDState(ctx context.Context, id int) (OSDState, error) {
	dump, err := c.removalOSDDump(ctx)
	if err != nil {
		return OSDState{}, err
	}
	return c.ownedOSDState(dump, id)
}

func (c *Container) removalOSDMutation(ctx context.Context, id int, args ...string) error {
	if _, err := c.removalOSDState(ctx, id); err != nil {
		return err
	}
	_, err := c.Ceph(ctx, args...)
	return err
}

func (c *Container) waitOSDRemoval(ctx context.Context, id int, drain bool) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		state, err := c.removalOSDState(ctx, id)
		if err != nil {
			return errors.Join(err, last)
		}
		if !drain && !state.Up {
			return nil
		}
		if drain {
			if _, err := c.Ceph(ctx, "osd", "safe-to-destroy", strconv.Itoa(id)); err == nil {
				return nil
			} else {
				last = err
			}
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

// OSDs returns a snapshot of owned OSD containers sorted by ID.
func (c *Container) OSDs() []*OSDContainer {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]*OSDContainer, 0, len(c.osds))
	for _, osd := range c.osds {
		result = append(result, osd)
	}
	slices.SortFunc(result, func(a, b *OSDContainer) int { return a.ID - b.ID })
	return result
}

// ServiceContainers returns owned optional services (such as RGW or MDS),
// sorted by service name. The cluster owns their cleanup.
func (c *Container) ServiceContainers() []testcontainers.Container {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.services))
	for name := range c.services {
		names = append(names, name)
	}
	slices.Sort(names)
	result := make([]testcontainers.Container, 0, len(names))
	for _, name := range names {
		result = append(result, c.services[name])
	}
	return result
}

// startService registers partial failures too, so callers can always terminate
// the cluster after a failed RGW/MDS bootstrap.
func (c *Container) startService(ctx context.Context, name, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("ceph cluster is terminated")
	}
	if _, exists := c.services[name]; exists {
		return nil, fmt.Errorf("ceph service %s already started", name)
	}
	if len(c.config) == 0 || len(c.keyring) == 0 {
		return nil, errors.New("ceph cluster bootstrap is incomplete")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	moduleOpts := append([]testcontainers.ContainerCustomizer{c.WithClient()}, opts...)
	ctr, err := testcontainers.Run(ctx, image, moduleOpts...)
	if ctr != nil {
		c.services[name] = ctr
	}
	if err != nil {
		return ctr, fmt.Errorf("run ceph %s: %w", name, err)
	}
	return ctr, nil
}

// Terminate removes all owned daemons before removing the isolated network.
// It also works on a partially initialized cluster returned by Run.
func (c *Container) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	c.closed = true
	var errs []error
	blockedNetworks := make(map[string]bool)
	// Restore caller-owned clients as well as owned daemons before removing any
	// network. A failed restore remains retryable through its interruption handle.
	for _, interruption := range c.interruptions {
		if err := interruption.Restore(ctx); err != nil {
			errs = append(errs, fmt.Errorf("restore interrupted network: %w", err))
			blockedNetworks[interruption.NetworkName] = true
		}
	}
	// A frozen process cannot handle the stop signal until it is thawed.
	for _, pause := range c.pauses {
		if err := pause.Resume(ctx); err != nil {
			errs = append(errs, fmt.Errorf("resume paused container: %w", err))
		}
	}
	for _, lease := range c.portLeases {
		if err := lease.Release(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	// Stop gateways and metadata servers while their backing OSDs are alive.
	for name, ctr := range c.services {
		if err := ctr.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
			errs = append(errs, fmt.Errorf("terminate service %s: %w", name, err))
		} else {
			delete(c.services, name)
		}
	}
	for id, osd := range c.osds {
		if osd.Container != nil {
			if err := osd.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
				errs = append(errs, err)
				continue
			}
		}
		delete(c.osds, id)
	}
	if c.osdMemory != nil && len(c.osds) == 0 {
		if err := c.osdMemory.release(ctx); err != nil {
			errs = append(errs, err)
		} else {
			c.osdMemory = nil
		}
	}
	if c.manager != nil {
		if err := c.manager.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
			errs = append(errs, err)
		} else {
			c.manager = nil
		}
	}
	for name, mgr := range c.managers {
		if name == "a" {
			if c.manager == nil {
				delete(c.managers, name)
			}
			continue
		}
		if mgr.Container != nil {
			if err := mgr.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
				errs = append(errs, err)
				continue
			}
		}
		delete(c.managers, name)
	}
	for name, mon := range c.monitors {
		if mon.Container != nil {
			if err := mon.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
				errs = append(errs, err)
				continue
			}
		}
		delete(c.monitors, name)
	}
	if c.Container != nil && !c.monitorTerminated {
		if err := c.Container.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
			errs = append(errs, err)
		} else {
			c.monitorTerminated = true
		}
	}
	c.controlMu.Lock()
	if c.controlPlane != nil {
		if err := c.controlPlane.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
			errs = append(errs, err)
		} else {
			c.controlPlane = nil
		}
	}
	c.controlMu.Unlock()
	if c.clusterNetwork != nil && !c.clusterNetworkRemoved && !blockedNetworks[c.clusterNetwork.Name] {
		if err := c.clusterNetwork.Remove(ctx); !onlyMissingHostResource(err) {
			errs = append(errs, err)
		} else {
			c.clusterNetworkRemoved = true
		}
	}
	if c.network != nil && !c.networkRemoved && !blockedNetworks[c.network.Name] {
		if err := c.network.Remove(ctx); !onlyMissingHostResource(err) {
			errs = append(errs, err)
		} else {
			c.networkRemoved = true
		}
	}
	return errors.Join(errs...)
}

func (c *Container) waitOSD(ctx context.Context, id int, up bool) error {
	return c.poll(ctx, func() (bool, error) {
		result, err := c.Ceph(ctx, "osd", "dump", "--format", "json")
		if err != nil {
			return false, err
		}
		var dump struct {
			OSDs []struct {
				ID int `json:"osd"`
				Up int `json:"up"`
				In int `json:"in"`
			} `json:"osds"`
		}
		if err := json.Unmarshal(result, &dump); err != nil {
			return false, err
		}
		for _, osd := range dump.OSDs {
			if osd.ID == id {
				return (up && osd.Up == 1 && osd.In == 1) || (!up && osd.Up == 0), nil
			}
		}
		return !up, nil
	})
}

func (c *Container) poll(ctx context.Context, check func() (bool, error)) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, last)
		}
		ok, err := check()
		if err != nil {
			last = err
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(err, last)
		}
		if ok && err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

func scriptFile(name string) testcontainers.ContainerFile {
	data, err := scripts.ReadFile("internal/scripts/" + name + ".sh")
	if err != nil {
		panic(err)
	} // Only embedded, compile-time-known paths are used.
	return textFile("/tc/"+name+".sh", data, 0o755)
}

func textFile(path string, data []byte, mode int64) testcontainers.ContainerFile {
	return testcontainers.ContainerFile{Reader: bytes.NewReader(data), ContainerFilePath: path, FileMode: mode}
}

func readFile(ctx context.Context, ctr testcontainers.Container, path string) ([]byte, error) {
	r, err := ctr.CopyFileFromContainer(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

func command(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
	if ctr == nil {
		return nil, errors.New("ceph control container is unavailable")
	}
	exitCode, reader, err := ctr.Exec(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("exec %s: %w", args[0], err)
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
		return nil, fmt.Errorf("read %s output: %w", args[0], err)
	}
	if exitCode != 0 {
		return stdout.Bytes(), fmt.Errorf("%s exited %d: %s", args[0], exitCode, strings.TrimSpace(stdout.String()+stderr.String()))
	}
	return stdout.Bytes(), nil
}
