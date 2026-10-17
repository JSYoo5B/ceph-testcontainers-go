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

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/dockerbridge"
	"github.com/containerd/errdefs"
	"github.com/google/uuid"
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
	mu                 sync.Mutex
	controlMu          sync.RWMutex
	configMu           sync.RWMutex
	configOverrides    map[string]*ConfigOverride
	flagOverrides      map[string]*OSDFlagOverride
	fullRatiosOverride *FullRatiosOverride
	blocklistOverrides map[string]*BlocklistOverride
	moduleOverrides    map[string]*MGRModuleOverride
	controlPlane       testcontainers.Container
	cephVersion        string
	// authFences records, per auth entity, what an earlier RotateDaemonKey
	// or client key change left uncertain; see authFence.
	authFences map[string]*authFence
	// clientSecrets holds every client key that the module generated or
	// confirmed, old and attempted ones included, for diagnostics to redact.
	clientSecrets map[string]bool
	// clientKeys holds, per client identity that this cluster created, the
	// key that the module last confirmed for it. A descriptor with another
	// key, or of a deleted identity, is stale, also when it is a copy.
	clientKeys map[string]string
	// strayAuthInputs holds files with keys in the control container that
	// earlier auth calls could not remove for sure; later ones remove them.
	// A file whose copy failed stays listed, as true, because Docker may
	// still write it.
	strayAuthInputs map[string]bool
	// clientRevocations holds, per client entity, the state of the temporary
	// revocation that has removed it until its Restore.
	clientRevocations map[string]*clientRevocationState
	// keyRotationDocker replaces the Docker client of RotateDaemonKey in tests.
	keyRotationDocker func(context.Context) (keyRotationExecClient, error)
	// owner labels every container of the cluster; see clusterOwnerLabel.
	owner string
	// ownedContainerDocker replaces the Docker client of Terminate's removal
	// of labeled containers in tests.
	ownedContainerDocker func(context.Context) (ownedContainerClient, error)
	// keyTypeOption records whether the control image's ceph-authtool has
	// --key-type; see generateKeyArgs.
	keyTypeOption         bool
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
	extensionsMu          sync.Mutex
	extensions            map[string]any
	osds                  map[int]*OSDContainer
	osdMemory             *osdMemoryStorage
	run                   *runVolume
	config                []byte
	keyring               []byte
	portLeases            []*hostPortLease
	closed                bool
	monitorTerminated     bool
	networkRemoved        bool

	// nativeMu guards the RunNativeCommand calls that run or whose command
	// is not confirmed as stopped, and whether Terminate refuses new ones.
	nativeMu          sync.Mutex
	nativeInvocations map[*nativeInvocation]bool
	nativeClosed      bool
	// delays holds network delays by network and container; a nil value
	// reserves an endpoint while its delay is being applied.
	delays map[string]*NetworkDelayLease
	// newNetemBackend replaces the Docker netem backend in tests.
	newNetemBackend func() netemBackend
	// controlImageID is the ID of the first MON's image, which errors about
	// the control image's capabilities name.
	controlImageID string
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
// Other roles use img unless overridden by WithOSDImage, WithMDSImage or
// rgw.WithImage. Images contain runtime tools; this module supplies cluster setup.
// A non-nil Container returned with an error must still be terminated.
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	settings := options{
		osds: 2, monitors: 1, managers: 1, blockSize: 1 << 30, startupTimeout: 3 * time.Minute,
		osdImage: img, rgwImage: img, mdsImage: img, controlImage: img, publicAddress: "127.0.0.1",
	}
	for _, opt := range opts {
		if opt, ok := opt.(Option); ok {
			if err := opt(&settings); err != nil {
				return nil, classify(ErrInvalidRequest, fmt.Errorf("configure ceph: %w", err))
			}
		}
	}
	if settings.hostAddressSet && !settings.hostNetwork {
		return nil, invalidRequest("WithHostAddress requires WithHostNetwork")
	}
	if settings.hostNetwork && settings.separateClusterNetwork {
		return nil, invalidRequest("WithSeparateClusterNetwork requires bridge mode")
	}
	if settings.messengerMode == MessengerV2Secure && (settings.messengerPolicy.Monitors != nil || settings.messengerPolicy.Managers != nil || settings.messengerPolicy.OSDs != nil) {
		return nil, invalidRequest("WithMessengerPolicy cannot be combined with MessengerV2Secure, which fixes every connection mode")
	}
	if settings.addressFamily != AddressIPv4 && settings.hostNetwork {
		return nil, invalidRequest("WithAddressFamily(%s) requires bridge mode", settings.addressFamily)
	}
	if settings.addressFamily == AddressIPv4MappedIPv6 && settings.separateClusterNetwork {
		return nil, invalidRequest("WithAddressFamily(%s) cannot be combined with WithSeparateClusterNetwork", settings.addressFamily)
	}
	if settings.osdObjectStore == OSDMemStore && settings.osdMemorySize != 0 {
		return nil, invalidRequest("WithOSDInMemoryStorage keeps BlueStore files in memory and cannot be combined with OSDMemStore")
	}
	if !settings.poolDefaultsSet {
		settings.poolReplicas, settings.poolMinSize = min(2, settings.osds), 1
	}
	if err := prepareInitialComposition(&settings); err != nil {
		return nil, classify(ErrInvalidRequest, fmt.Errorf("configure initial ceph topology: %w", err))
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
		nw, err = dockerbridge.NewFamily(ctx, settings.bridgeFamily())
	}
	c := &Container{settings: settings, network: nw, osds: make(map[int]*OSDContainer), services: make(map[string]testcontainers.Container),
		monitors: make(map[string]*MonitorContainer), managers: make(map[string]*ManagerContainer), owner: uuid.NewString()}
	if err != nil {
		if nw == nil {
			return nil, classifyDocker(ctx, fmt.Errorf("create ceph network: %w", err))
		}
		return c, classifyDocker(ctx, fmt.Errorf("create ceph network: %w", err))
	}
	if settings.separateClusterNetwork {
		if c.clusterNetwork, err = dockerbridge.NewFamily(ctx, settings.bridgeFamily()); err != nil {
			return c, classifyDocker(ctx, fmt.Errorf("create Ceph replication network: %w", err))
		}
		if c.publicSubnet, c.clusterSubnet, err = inspectNetworkSubnets(ctx, nw.Name, c.clusterNetwork.Name, settings.addressFamily); err != nil {
			return c, err
		}
	}
	// Publish before creation: an interrupted create may have committed, and the
	// generated volume name stays owned for removal by Terminate.
	c.run = newRunVolume()
	if err := c.run.create(ctx); err != nil {
		return c, err
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
	if err := c.recordCephVersion(ctx); err != nil {
		return c, err
	}
	if c.keyTypeOption, err = detectKeyTypeOption(ctx, mon); err != nil {
		return c, err
	}
	c.controlImageID = containerImageID(ctx, mon)
	if c.keyring, err = readFile(ctx, mon, "/etc/ceph/ceph.client.admin.keyring"); err != nil {
		return c, err
	}
	if err := c.configureDefaultCRUSHRoot(ctx); err != nil {
		return c, fmt.Errorf("configure default CRUSH placement: %w", err)
	}
	if err := c.applyMessengerPolicy(ctx); err != nil {
		return c, fmt.Errorf("apply messenger policy: %w", err)
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
	for _, service := range settings.services {
		if err := service.Start(ctx, c); err != nil {
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

// runContainer is testcontainers.Run returning only a real handle. Run
// returns a nil *DockerContainer when it fails before a container exists;
// as a testcontainers.Container that would be a non-nil interface holding a
// nil pointer, whose Logs and GetContainerID dereference it.
func runContainer(ctx context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	created, err := testcontainers.Run(ctx, image, opts...)
	if created == nil {
		return nil, err
	}
	return created, err
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
			lease, err = reserveHostPorts(ctx, image, c.settings.publicAddress, c.settings.monitorPortCount(), c.settings.startupTimeout, c.ownedLabel())
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
				"CEPH_OSD_OBJECTSTORE": string(c.settings.objectStore()),
				"CEPH_POOL_SIZE":       strconv.Itoa(c.settings.poolReplicas), "CEPH_POOL_MIN_SIZE": strconv.Itoa(c.settings.poolMinSize),
				"CEPH_PUBLIC_NETWORK": c.publicSubnet, "CEPH_CLUSTER_NETWORK": c.clusterSubnet,
				addressFamilyEnvironment: c.settings.addressFamily.String(),
			}),
			testcontainers.WithEnv(c.settings.auth.environment()),
			testcontainers.WithFiles(scriptFile("mon")),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status", "--format", "json"}).WithStartupTimeout(c.settings.startupTimeout)),
		}
		moduleOpts = append(moduleOpts, c.runMount()...)
		if lease == nil {
			moduleOpts = append(moduleOpts, testcontainers.WithExposedPorts(c.settings.monitorExposedPorts()...), network.WithNetwork([]string{"ceph-mon"}, c.network),
				keepMonitorAddress(c.network.Name))
		} else {
			monEnv := c.settings.monitorPortEnvironment(lease.Ports)
			monEnv["CEPH_PUBLIC_ADDRESS"] = c.settings.publicAddress
			moduleOpts = append(moduleOpts, hostContainerCustomizer(c.settings.publicAddress), testcontainers.WithNoStart(),
				testcontainers.WithEnv(monEnv))
		}
		moduleOpts = append(moduleOpts, opts...)
		if env := c.settings.messengerEnvironment(); env != nil {
			moduleOpts = append(moduleOpts, testcontainers.WithEnv(env))
		}
		if len(c.settings.userConfig) > 0 {
			moduleOpts = append(moduleOpts, testcontainers.WithFiles(textFile(userConfigPath, renderUserConfig(c.settings.userConfig), 0o644)))
		}
		if lease != nil {
			moduleOpts = append(moduleOpts, hostContainerCustomizer(c.settings.publicAddress), testcontainers.WithNoStart())
		}
		mon, err := runContainer(ctx, image, append(moduleOpts, c.ownedLabel())...)
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
		if refusal := bootstrapRefusal(ctx, mon); refusal != nil {
			return mon, refusal.with(ctx, err)
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
	return nil, classify(ErrEnvironmentUnavailable, errors.New("monitor port attempts exhausted"))
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
		return nil, nil, errClusterTerminated
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
// The options customize the container's ceph.conf.
func (c *Container) WithClient(opts ...ClientContainerOption) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		c.configMu.RLock()
		config, keyring := bytes.Clone(c.config), bytes.Clone(c.keyring)
		c.configMu.RUnlock()
		config, err := clientContainerConfig(config, opts)
		if err != nil {
			return err
		}
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
		return nil, errClusterTerminated
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
	// Refuse an unsupported key type before allocating anything.
	generate, err := c.generateKeyArgs(c.settings.auth.KeyType)
	if err != nil {
		return nil, err
	}
	if err := c.ensureOSDMemory(ctx); err != nil {
		return nil, err
	}
	osdUUID := uuid.NewString()
	control := c.cliContainer()
	secret, err := command(ctx, control, generate...)
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
	if err := c.clearAdminSocket(ctx, fmt.Sprintf("osd.%d", id)); err != nil {
		return osd, err
	}
	osdOptions = append(osdOptions, c.runMount()...)
	if c.clusterNetwork != nil {
		osdOptions = append(osdOptions, network.WithNetworkName(nil, c.clusterNetwork.Name))
	}
	ctr, err := testcontainers.Run(ctx, c.settings.osdImage, append(osdOptions, c.ownedLabel())...)
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
		return errClusterTerminated
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	osd, ok := c.osds[id]
	if !ok || osd == nil {
		return invalid(fmt.Errorf("osd.%d is not owned by this cluster", id))
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
		return nil, errClusterTerminated
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
	moduleOpts = append(moduleOpts, c.ownedLabel())
	ctr, err := runContainer(ctx, image, moduleOpts...)
	if ctr != nil {
		c.services[name] = ctr
	}
	if err != nil {
		return ctr, fmt.Errorf("run ceph %s: %w", name, err)
	}
	return ctr, nil
}

// Terminate removes all owned daemons before removing the isolated network.
// It also works on a partially initialized cluster returned by Run. The whole
// cluster is discarded, so each container gets terminationStopTimeout instead
// of Docker's 10 seconds to stop before it is killed; a StopTimeout option
// passed by the caller still takes precedence.
func (c *Container) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	opts = append([]testcontainers.TerminateOption{testcontainers.StopTimeout(terminationStopTimeout)}, opts...)
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	c.closed = true
	var errs []error
	blockedNetworks := make(map[string]bool)
	// Remove network delays while their helpers and targets still run: removing
	// a helper leaves its qdisc in a caller-owned target. A failed restore
	// remains retryable through its lease.
	for _, delay := range c.delays {
		if delay == nil {
			continue
		}
		c.mu.Unlock()
		err := delay.Restore(ctx)
		c.mu.Lock()
		if err != nil {
			errs = append(errs, fmt.Errorf("restore network delay: %w", err))
		}
	}
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
	// Stop native commands while their containers still run. Those that
	// stay unconfirmed are retried after the teardown, which ends the ones in
	// the cluster's own containers.
	_ = c.stopNativeInvocations(ctx)
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
	var memoryErr error
	if c.osdMemory != nil && len(c.osds) == 0 {
		if memoryErr = c.osdMemory.release(ctx); memoryErr == nil {
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
	errs = append(errs, c.removeLabeledContainers(ctx))
	// The removed containers can include one that held the OSD memory volume,
	// such as an OSD or keeper whose create reply was lost.
	if memoryErr != nil && c.osdMemory != nil && len(c.osds) == 0 {
		if memoryErr = c.osdMemory.release(ctx); memoryErr == nil {
			c.osdMemory = nil
		}
	}
	errs = append(errs, memoryErr)
	if c.run != nil {
		if err := c.run.remove(ctx); err != nil {
			errs = append(errs, err)
		}
	}
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
	errs = append(errs, c.stopNativeInvocations(ctx)...)
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

// A gracefully stopping MDS otherwise waits about five seconds; data in a
// terminated cluster is never read again.
const terminationStopTimeout = 2 * time.Second

func (c *Container) poll(ctx context.Context, check func() (bool, error)) error {
	return clusterutil.Poll(ctx, check)
}

func scriptFile(name string) testcontainers.ContainerFile {
	data, err := scripts.ReadFile("internal/scripts/" + name + ".sh")
	if err != nil {
		panic(err)
	} // Only embedded, compile-time-known paths are used.
	return textFile("/tc/"+name+".sh", data, 0o755)
}

func textFile(path string, data []byte, mode int64) testcontainers.ContainerFile {
	return clusterutil.TextFile(path, data, mode)
}

func readFile(ctx context.Context, ctr testcontainers.Container, path string) ([]byte, error) {
	r, err := ctr.CopyFileFromContainer(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	defer func() { _ = r.Close() }()
	return io.ReadAll(r)
}

// command runs args in ctr and returns its standard output. A nonzero exit
// returns the output with a *NativeFailure; a program that the image lacks
// is ErrImageCapability, and a failure of Docker is ErrEnvironmentUnavailable
// unless ctx ended or the container is gone or stopped. A ceph CLI that no
// MON answered within its --connect-timeout connects again; see
// reconnectCeph.
func command(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
	if ctr == nil {
		return nil, invalidRequest("ceph control container is unavailable")
	}
	return reconnectCeph(ctx, args, func() ([]byte, error) { return commandOnce(ctx, ctr, args...) })
}

func commandOnce(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
	exitCode, reader, err := ctr.Exec(ctx, args)
	if err != nil {
		return nil, classifyDispatched(ctx, fmt.Errorf("exec %s: %w", args[0], err))
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
		return nil, classifyDispatched(ctx, fmt.Errorf("read %s output: %w", args[0], err))
	}
	if exitCode != 0 {
		if (exitCode == 126 || exitCode == 127) && strings.HasPrefix(strings.TrimSpace(stdout.String()+stderr.String()), dockerExecFailed) {
			return nil, &OperationError{Kind: ErrImageCapability, Capability: args[0],
				Err: fmt.Errorf("the container cannot run %s: it is missing or not executable", args[0])}
		}
		return stdout.Bytes(), nativeFailure(args[0], exitCode, stdout.Bytes(), stderr.Bytes())
	}
	return stdout.Bytes(), nil
}

// The ceph CLI connects again for at most cephConnectWait, pausing
// cephConnectPause between attempts; tests shorten both.
var (
	cephConnectWait  = time.Minute
	cephConnectPause = time.Second
)

// reconnectCeph runs a ceph CLI command with run, and runs it again while no
// MON answered its connection within --connect-timeout. The CLI then exits
// before it sends the command, so nothing was sent: a MON that stalls, as on
// a runner whose storage pauses for longer than that timeout, or an election
// delays the command instead of failing it. The attempts end with ctx, which
// a call that ends this way matches, or after cephConnectWait, which returns
// the last failure.
func reconnectCeph(ctx context.Context, args []string, run func() ([]byte, error)) ([]byte, error) {
	reconnect := len(args) > 0 && args[0] == "ceph" && slices.Contains(args, "--connect-timeout") &&
		!slices.Contains(args, "-h") && !slices.Contains(args, "--help")
	until := time.Now().Add(cephConnectWait)
	for {
		out, err := run()
		var failure *NativeFailure
		if !reconnect || !errors.As(err, &failure) || !cephConnectTimedOut(failure) || !time.Now().Before(until) {
			return out, err
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%s: no MON answered the connection before the context ended: %w", args[0], ctx.Err())
		case <-time.After(cephConnectPause):
		}
	}
}

// cephConnectTimeouts are the whole error outputs of a ceph CLI whose
// connection to the MONs timed out before it sent its command: the expiry of
// --connect-timeout, which bounds only run_in_thread(cluster_handle.connect),
// a connection that librados reports as interrupted, and librados's own
// connection timeout.
var cephConnectTimeouts = []string{
	"timed out",
	"Cluster connection interrupted or timed out",
	"[errno 110] RADOS timed out (error connecting to the cluster)",
}

// cephConnectTimedOut reports a ceph CLI that gave up connecting to the MONs
// and exited 1 without sending its command. Only those exact outputs count:
// a reply to a command that the CLI sent, such as "Error EPERM: ...", names
// an errno, and another connection error, such as a missing keyring, does
// not pass by waiting.
func cephConnectTimedOut(failure *NativeFailure) bool {
	return failure.Program == "ceph" && failure.ExitCode == 1 && failure.Errno == "" &&
		slices.Contains(cephConnectTimeouts, failure.Reason)
}

// dockerExecFailed starts the output of an exec whose program the runtime
// could not start.
const dockerExecFailed = "OCI runtime exec failed"

// classifyDispatched classifies a failed Docker call that may have started a
// native command, such as an exec whose final inspection failed after the
// command ran. Its effect is unknown, so a container that is gone or stopped
// is no refused request: that error stays unclassified. A call that ctx
// ended matches ctx's error, and any other failure is the engine's.
func classifyDispatched(ctx context.Context, err error) error {
	if err != nil && ctx.Err() == nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) &&
		(errdefs.IsNotFound(err) || errdefs.IsConflict(err)) {
		return err
	}
	return classifyDocker(ctx, err)
}

// classifyDocker classifies a failed Docker call: a call that ctx ended stays
// unclassified and matches ctx's error, a container that is gone or not
// running is a refused request, and anything else is the engine's failure.
func classifyDocker(ctx context.Context, err error) error {
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil && !errors.Is(err, ctx.Err()):
		// A transport that ctx closed may fail without wrapping its error.
		return fmt.Errorf("%w: %w", err, ctx.Err())
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		return err
	case errdefs.IsNotFound(err) || errdefs.IsConflict(err):
		return classify(ErrInvalidRequest, err)
	}
	return classify(ErrEnvironmentUnavailable, err)
}
