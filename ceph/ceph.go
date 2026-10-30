package ceph

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
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

// DefaultImage pins the published, pruned dependency-role aggregate on both
// native architectures. Explicit ImageSet selections avoid unused feature roles.
const DefaultImage = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:104315166259499ea6855fc163ac052c37fd7261faab638f13a78be4a2004a80"

//go:embed internal/scripts/*.sh
var scripts embed.FS

// Container embeds the MON/control container and owns the MGR, OSDs, optional
// RGW/MDS services and network.
// Use Terminate to clean up the entire cluster. Generic Run options customize
// the MON; module options configure the cluster. Use WithHostNetwork to change
// cluster networking, rather than changing only the MON's network mode.
type Container struct {
	testcontainers.Container
	mu sync.Mutex
	// controlPlane is the separate control container and the lock that guards
	// it; see controlPlaneState.
	controlPlane controlPlaneState
	// toolkit is the feature-client cache; see featureClientState for what
	// guards each part.
	toolkit featureClientState
	// bootstrap is the first MON's configuration, admin keyring and Ceph
	// version and the lock that guards the first two; see bootstrapRecord.
	bootstrap bootstrapRecord
	// startup is the owner label ID, the first MON's key-type support and
	// image ID, and the test seam of the labeled-container removal; see
	// startupIdentity. It has no lock: it is written before the cluster is
	// shared.
	startup  startupIdentity
	settings options
	// ledger is the auth records and the daemon exec seam, owned by
	// authLedger; see authLedgerState.
	ledger authLedgerState
	// members is the MON and MGR membership, owned by monitorTopology; see
	// membershipState.
	members membershipState
	// fleet is the OSD registry, storage and device claims, owned by
	// osdFleet; see osdFleetState.
	fleet osdFleetState
	// extensions is the per-cluster state of the service packages, by kind,
	// and the lock that guards it; see extensionStore.
	extensions extensionStore
	// nets is the bridge state; see networkState for what guards each part.
	nets networkState
	// monStores is the owned MON store volumes, by MON name; see
	// monStoreState. It has no lock of its own: mu guards it. Nothing publishes
	// a record yet.
	monStores monStoreState
	// runDir is the shared admin-socket run directory; see runDirState for
	// what guards each part.
	runDir runDirState
	// portLeases is the host-port reservations of host network mode; see
	// portLeaseState. It has no lock of its own: mu guards it.
	portLeases portLeaseState
	closed     bool

	// native is the native-command state and the cancellation and fencing of
	// its calls; see nativeCommands. Its state has its own leaf lock.
	native nativeCommands
	// faults holds the network delays, interruptions and pauses, and the
	// bookkeeping entries of the blocklist, full ratios, configuration, OSD
	// flag and MGR module overrides, reserved or held; see faultRegistry. It
	// is guarded by mu, and terminate runs its undo through faults.undo.
	faults faultRegistry
	// debugLevels admits one override.SetDebugLevel window per daemon and
	// option; it has its own leaf lock and is handed out only by
	// overrideHooks.DebugWindows. Terminate does not touch it.
	debugLevels debugLevelRegistry
	// daemonRunner replaces runContainer for AddMonitor and AddManager containers in tests.
	daemonRunner func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error)
}

// OSDContainer is one storage daemon backed by a sparse BlueStore file.
// The file is container-local unless OSDStorage.WithInMemory or
// OSDStorage.WithVolume is selected.
// Stop/Start can be used for failure injection; OSDContainer.Remove drains and purges it.
type OSDContainer struct {
	testcontainers.Container
	ID          int
	owner       *Container
	nativeUUID  string
	placement   OSDConfig
	purged      bool
	purgeIssued bool
	// detaching marks an OSD that OSDContainer.Detach fenced; detachRemoved marks
	// one whose container it removed. Both keep the OSD owned for retry.
	detaching, detachRemoved bool
	// image is the image the container was started from; it never changes, and
	// DetachedOSD.Reopen reuses it.
	image string
}

// resolveOptions applies the Ceph options among opts over the defaults for
// img and refuses a combination that Run cannot build. It runs before Run
// touches Docker; every refusal is an ErrInvalidRequest.
func resolveOptions(img string, opts []testcontainers.ContainerCustomizer) (options, error) {
	settings := options{
		osds: 2, monitors: 1, managers: 1, blockSize: 1 << 30, startupTimeout: 3 * time.Minute,
		essentialImage: img, publicAddress: "127.0.0.1",
	}
	for _, opt := range opts {
		if opt, ok := opt.(Option); ok {
			if err := opt(&settings); err != nil {
				return options{}, classify(ErrInvalidRequest, fmt.Errorf("configure ceph: %w", err))
			}
		}
	}
	if err := configureImages(&settings); err != nil {
		return options{}, invalidRequest("image set: %v", err)
	}
	if settings.hostAddressSet && !settings.hostNetwork {
		return options{}, invalidRequest("NetworkConfig.WithHostAddress requires WithHostNetwork")
	}
	if settings.hostNetwork && settings.separateClusterNetwork {
		return options{}, invalidRequest("NetworkConfig.WithSeparateClusterNetwork requires bridge mode")
	}
	if settings.messengerMode == MessengerV2Secure && (settings.messengerPolicy.Monitors != nil || settings.messengerPolicy.Managers != nil || settings.messengerPolicy.OSDs != nil) {
		return options{}, invalidRequest("NetworkConfig.WithMessengerPolicy cannot be combined with MessengerV2Secure, which fixes every connection mode")
	}
	if settings.addressFamily != AddressIPv4 && settings.hostNetwork {
		return options{}, invalidRequest("NetworkConfig.WithAddressFamily(%s) requires bridge mode", settings.addressFamily)
	}
	if settings.addressFamily != AddressDualStack && userConfigSets(settings.userConfig, dualStackOrderKey) {
		return options{}, invalidRequest("WithConfigFile sets %s, which requires NetworkConfig.WithAddressFamily(%s)", dualStackOrderKey, AddressDualStack)
	}
	if settings.addressFamily == AddressIPv4MappedIPv6 && settings.separateClusterNetwork {
		return options{}, invalidRequest("NetworkConfig.WithAddressFamily(%s) cannot be combined with NetworkConfig.WithSeparateClusterNetwork", settings.addressFamily)
	}
	if settings.osdObjectStore == OSDMemStore && settings.osdMemorySize != 0 {
		return options{}, invalidRequest("OSDStorage.WithInMemory keeps BlueStore files in memory and cannot be combined with OSDMemStore")
	}
	if settings.osdVolumeStorage && (settings.osdMemorySize != 0 || settings.osdObjectStore == OSDMemStore) {
		return options{}, invalidRequest("OSDStorage.WithVolume requires classic BlueStore and cannot be combined with OSDStorage.WithInMemory")
	}
	if !settings.poolDefaultsSet {
		settings.poolReplicas, settings.poolMinSize = min(2, settings.osds), 1
	}
	if err := prepareInitialComposition(&settings); err != nil {
		return options{}, classify(ErrInvalidRequest, fmt.Errorf("configure initial ceph topology: %w", err))
	}
	return settings, nil
}

// Run creates configurable MON, MGR and OSD containers for a disposable cluster.
// img supplies all roles unless WithImageOptions selects explicit role images
// with ImageOptions.WithImageSet, in which case img must be empty or the set's
// essential image. Run recognises the Option values among opts (every Ceph
// setting, including each grouped configuration value carried by its option
// function), applies and validates them before it creates anything, and passes
// all of opts, in argument order, to the first MON's container request after
// the module's own request options and before the messenger environment, the
// WithConfigFile file and the ownership label. An Option has no effect on that
// request, so a setting passed as one is never lost and never reaches a
// container.
// Images contain runtime tools; this module supplies cluster setup.
// A non-nil Container returned with an error must still be terminated.
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	settings, err := resolveOptions(img, opts)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, settings.startupTimeout)
	defer cancel()
	var nw *testcontainers.DockerNetwork
	if settings.hostNetwork {
		if err := checkHostNetworkEngine(ctx); err != nil {
			return nil, err
		}
	} else {
		nw, err = dockerbridge.NewFamily(ctx, settings.bridgeFamily())
	}
	c := &Container{settings: settings, nets: networkState{public: nw}, startup: newStartupIdentity(uuid.NewString())}
	if err != nil {
		if nw == nil {
			return nil, classifyDocker(ctx, fmt.Errorf("create ceph network: %w", err))
		}
		return c, classifyDocker(ctx, fmt.Errorf("create ceph network: %w", err))
	}
	if settings.separateClusterNetwork {
		if c.nets.cluster, err = dockerbridge.NewFamily(ctx, settings.bridgeFamily()); err != nil {
			return c, classifyDocker(ctx, fmt.Errorf("create Ceph replication network: %w", err))
		}
		if c.nets.publicSubnet, c.nets.clusterSubnet, err = inspectNetworkSubnets(ctx, nw.Name, c.nets.cluster.Name, settings.addressFamily); err != nil {
			return c, err
		}
	} else if settings.addressFamily == AddressDualStack {
		// Without public_network, a dual-stack daemon cannot pick an address
		// of each family. The single bridge serves both planes.
		if c.nets.publicSubnet, c.nets.clusterSubnet, err = inspectNetworkSubnets(ctx, nw.Name, nw.Name, settings.addressFamily); err != nil {
			return c, err
		}
	}
	// create publishes before creating: an interrupted create may have committed,
	// and the generated volume name stays owned for removal by Terminate.
	if err := c.runDir.create(ctx); err != nil {
		return c, err
	}
	mon, err := c.runMonitor(ctx, settings.essentialImage, uuid.NewString(), opts...)
	if mon != nil {
		c.Container = mon
	}
	if err != nil {
		return c, fmt.Errorf("run ceph monitor: %w", err)
	}
	config, err := readFile(ctx, mon, "/etc/ceph/ceph.conf")
	c.bootstrap.recordConfig(config)
	if err != nil {
		return c, err
	}
	if err := c.recordCephVersion(ctx); err != nil {
		return c, err
	}
	keyTypeOption, err := detectKeyTypeOption(ctx, mon)
	c.startup.recordKeyTypeOption(keyTypeOption)
	if err != nil {
		return c, err
	}
	c.startup.recordFirstMonitorImage(containerImageID(ctx, mon))
	keyring, err := readFile(ctx, mon, "/etc/ceph/ceph.client.admin.keyring")
	c.bootstrap.recordKeyring(keyring)
	if err != nil {
		return c, err
	}
	if err := c.configureDefaultCRUSHRoot(ctx); err != nil {
		return c, fmt.Errorf("configure default CRUSH placement: %w", err)
	}
	if err := c.applyMessengerPolicy(ctx); err != nil {
		return c, fmt.Errorf("apply messenger policy: %w", err)
	}
	if err := c.startInitialTopology(ctx); err != nil {
		return c, err
	}
	return c, nil
}

// startInitialTopology brings a cluster whose first MON is configured to the
// composition Run was asked for: the other MONs, the MGRs and OSDs, their
// readiness, the requested pools and the registered services, in that order.
// A failure leaves what started owned for Terminate.
func (c *Container) startInitialTopology(ctx context.Context) error {
	for i := 1; i < c.settings.monitors; i++ {
		if _, err := c.AddMonitor(ctx, daemonName(i)); err != nil {
			return err
		}
	}
	if err := c.startInitialManagers(ctx); err != nil {
		return err
	}
	if err := c.startInitialOSDs(ctx); err != nil {
		return err
	}
	if err := c.finishInitialManagers(ctx); err != nil {
		return err
	}
	if err := c.waitForInitialDaemons(ctx); err != nil {
		return err
	}
	if err := c.checkDualStackAddresses(ctx); err != nil {
		return err
	}
	for _, pool := range c.settings.pools {
		if _, err := c.CreatePool(ctx, pool); err != nil {
			return err
		}
	}
	for _, service := range c.settings.services {
		if err := service.Start(ctx, c); err != nil {
			return err
		}
	}
	return nil
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
		if err := clusterutil.Poll(ctx, func() (bool, error) {
			states, err := c.osdStates(ctx)
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
	if err := clusterutil.Poll(ctx, func() (bool, error) {
		s, err := readStatus(ctx, c.conn().ceph)
		return s.MgrMap.Available, err
	}); err != nil {
		return fmt.Errorf("wait for ceph manager: %w", err)
	}
	return nil
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
			lease, err = reserveHostPorts(ctx, image, c.settings.publicAddress, c.settings.monitorPortCount(), c.settings.startupTimeout, c.startup.ownedLabel())
			c.trackHostPortLease(lease)
			if err != nil {
				return nil, err
			}
		}
		moduleOpts := []testcontainers.ContainerCustomizer{
			testcontainers.WithEntrypoint("/bin/sh", "/tc/mon.sh"),
			testcontainers.WithCmd(),
			testcontainers.WithEnv(c.monitorEnvironment(fsid)),
			testcontainers.WithFiles(scriptFile("mon")),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status", "--format", "json"}).WithStartupTimeout(c.settings.startupTimeout)),
		}
		moduleOpts = append(moduleOpts, c.runDir.mounts()...)
		if lease == nil {
			moduleOpts = append(moduleOpts, testcontainers.WithExposedPorts(c.settings.monitorExposedPorts()...), network.WithNetwork([]string{"ceph-mon"}, c.nets.public),
				keepMonitorAddress(c.nets.public.Name))
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
		mon, err := runContainer(ctx, image, append(moduleOpts, c.startup.ownedLabel())...)
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

// monitorEnvironment returns the mon.sh variables of the first MON:
// bootstrap settings, address family, networks and Cephx. Run adds the
// Messenger mode's variables after the caller's options.
func (c *Container) monitorEnvironment(fsid string) map[string]string {
	env := map[string]string{
		"CEPH_FSID": fsid, "CEPH_OSD_BLOCK_SIZE": strconv.FormatInt(c.settings.blockSize, 10),
		"CEPH_OSD_OBJECTSTORE": string(c.settings.objectStore()),
		"CEPH_POOL_SIZE":       strconv.Itoa(c.settings.poolReplicas), "CEPH_POOL_MIN_SIZE": strconv.Itoa(c.settings.poolMinSize),
		"CEPH_PUBLIC_NETWORK": c.nets.publicSubnet, "CEPH_CLUSTER_NETWORK": c.nets.clusterSubnet,
		addressFamilyEnvironment: c.settings.addressFamily.String(),
	}
	maps.Copy(env, c.settings.auth.environment())
	return env
}

// monitorJoinEnvironment returns the mon-join.sh variables of a later MON.
func (o options) monitorJoinEnvironment(name string) map[string]string {
	env := map[string]string{"CEPH_MON_ID": name, addressFamilyEnvironment: o.addressFamily.String()}
	maps.Copy(env, o.messengerEnvironment())
	return env
}

func (c *Container) trackHostPortLease(lease *hostPortLease) {
	if lease == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.portLeases.track(lease)
}

// ManagerContainer returns the initial MGR a for compatibility and inspection.
// It may be nil after a is removed while another MGR remains active. Use
// Managers and ManagerStatus for the current candidates and active identity.
// The cluster owns its cleanup.
func (c *Container) ManagerContainer() testcontainers.Container {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.members.roster.initialManager()
}

// WithClient connects another container to Ceph and copies its admin credentials.
// These privileged, ephemeral credentials are for trusted test containers only.
// Caller owns and terminates that container before terminating the cluster.
// The options customize the container's ceph.conf.
func (c *Container) WithClient(opts ...ClientContainerOptions) testcontainers.CustomizeRequestOption {
	return c.adminClient(opts...)
}

// adminClient attaches a container of the module's own with the admin keyring.
func (c *Container) adminClient(opts ...ClientContainerOptions) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		config, keyring := c.bootstrap.clones()
		config, err := clientContainerConfig(config, opts)
		if err != nil {
			return err
		}
		if c.settings.hostNetwork {
			if err := hostContainerCustomizer(c.settings.publicAddress).Customize(req); err != nil {
				return err
			}
		} else {
			if err := network.WithNetworkName(nil, c.staticFacts().NetworkName)(req); err != nil {
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
	return c.conn().ceph(ctx, args...)
}

// AddOSD registers, formats and starts a new OSD, then waits for it to be up/in.
// With no argument the OSD takes the cluster defaults: host osd-ID under the
// default root and the essential image. OSDOptions values select an explicit
// logical CRUSH location (WithPlacement) or start this one OSD from another
// image (WithImage); several values compose as described at OSDOptions. Omitted
// host names resolve to osd-ID; logical hosts/racks are simulated placement
// domains on the same Docker engine, not physical failure domains.
//
// The context is checked first, then the assembled configuration is validated
// once before the topology lock is taken, before any
// OSD is registered and before any container exists. Topology operations are
// serialized. An OSD returned with an error is tracked for cleanup and may be
// inspected; no data is silently purged on failure. The image override is kept
// in the OSD's recipe for DetachedOSD.Reopen.
func (c *Container) AddOSD(ctx context.Context, configs ...OSDOptions) (*OSDContainer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	plan, err := composeOSDOptions(configs)
	if err != nil {
		return nil, err
	}
	config, err := normalizeOSDConfig(resolveOSDDefaults(c.settings, plan.placement))
	if err != nil {
		return nil, err
	}
	image := c.settings.essentialImage
	if plan.imageSet {
		image = plan.image
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
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	launch, err := c.osdFleet().registerOSD(ctx, config, image, nil)
	if err != nil {
		return launch.osd, err
	}
	if err := c.osdFleet().startOSD(ctx, launch); err != nil {
		return launch.osd, err
	}
	if err := (osdMap{c.conn()}).waitUp(ctx, []int{launch.osd.ID}); err != nil {
		return launch.osd, err
	}
	return launch.osd, osdMap{c.conn()}.assignDeviceClass(ctx, launch.osd)
}

// osdLaunch is a registered, owned OSD and its immutable creation recipe.
// Copies share the recipe's single-attempt guard; they do not authorize replay.
type osdLaunch struct {
	osd    *OSDContainer
	recipe *osdLaunchRecipe
}

// registerOSD allocates the next OSD ID with a new key and records the OSD in
// c.osdHandles before its container exists, so that Terminate owns it from then on.
// The caller holds c.mu and has normalized config. A nonnil buckets is the
// set of CRUSH host buckets that the caller created; registerOSD then also
// creates the OSD's host bucket in its location, so that the bucket IDs
// follow the registration order although the daemons boot concurrently. The
// returned launch.osd is nil when the error came before the registration.
func (f osdFleet) registerOSD(ctx context.Context, config OSDConfig, image string, buckets map[string]bool) (osdLaunch, error) {
	for _, osd := range f.osdHandles.each() {
		if osd.purged || osd.purgeIssued {
			return osdLaunch{}, fmt.Errorf("finish removing osd.%d before adding another OSD", osd.ID)
		}
	}
	if err := f.validateOSDPlacement(config); err != nil {
		return osdLaunch{}, err
	}
	if err := ctx.Err(); err != nil {
		return osdLaunch{}, err
	}
	// Refuse an unsupported key type before allocating anything.
	generate, err := f.keyArgs(f.settings.auth.KeyType)
	if err != nil {
		return osdLaunch{}, err
	}
	if err := f.ensureOSDVolume(ctx); err != nil {
		return osdLaunch{}, err
	}
	if err := f.ensureOSDMemory(ctx); err != nil {
		return osdLaunch{}, err
	}
	osdUUID := uuid.NewString()
	control := f.conn.cliContainer()
	secret, err := command(ctx, control, generate...)
	if err != nil {
		return osdLaunch{}, err
	}
	keys, err := json.Marshal(map[string]string{"cephx_secret": strings.TrimSpace(string(secret))})
	if err != nil {
		return osdLaunch{}, err
	}
	keyPath := "/tmp/osd-" + osdUUID + ".json"
	if err := control.CopyToContainer(ctx, keys, keyPath, 0o600); err != nil {
		return osdLaunch{}, fmt.Errorf("copy osd registration key: %w", err)
	}
	result, err := f.conn.ceph(ctx, "osd", "new", osdUUID, "-i", keyPath)
	if err != nil {
		return osdLaunch{}, err
	}
	id, err := strconv.Atoi(strings.TrimSpace(string(result)))
	if err != nil {
		return osdLaunch{}, fmt.Errorf("parse osd ID %q: %w", result, err)
	}
	if config.Host == "" {
		config.Host = fmt.Sprintf("osd-%d", id)
	}
	osd := &OSDContainer{ID: id, owner: f.owner, nativeUUID: osdUUID, placement: config, image: image}
	f.osdHandles.register(id, osd)
	launch := osdLaunch{osd: osd}
	// A generated host name is known only after registration. Preserve the
	// partial OSD descriptor if it conflicts, without moving any CRUSH bucket.
	if err := f.validateOSDPlacement(config); err != nil {
		return launch, fmt.Errorf("place registered osd.%d: %w", id, err)
	}
	if buckets != nil && !buckets[config.Host] {
		// Creating the bucket here leaves the booting OSD's create-or-move
		// only its own insertion; a missing rack is created with the host.
		args := []string{"osd", "crush", "add-bucket", config.Host, "host", "root=" + config.Root}
		if config.Rack != "" {
			args = append(args, "rack="+config.Rack)
		}
		if _, err := f.conn.ceph(ctx, args...); err != nil {
			return launch, fmt.Errorf("create CRUSH host %s of osd.%d: %w", config.Host, id, err)
		}
		buckets[config.Host] = true
	}
	keyring := []byte(fmt.Sprintf("[osd.%d]\n\tkey = %s\n", id, strings.TrimSpace(string(secret))))
	if err := f.stores.requireFresh(ctx, id); err != nil {
		return launch, err
	}
	if err := f.clearSocket(ctx, fmt.Sprintf("osd.%d", id)); err != nil {
		return launch, err
	}
	launch.recipe = f.launchRecipe(osd, keyring)
	return launch, nil
}

// startOSD creates and starts the container of a registered OSD and records
// it in the OSD's handle, also when the start then fails. It does not use
// c.mu, so that Run can start several OSDs at once. The topology caller holds
// c.mu until every start joins; a launch admits only one creation attempt.
func (f osdFleet) startOSD(ctx context.Context, launch osdLaunch) error {
	if launch.osd == nil || launch.recipe == nil {
		return errors.New("OSD launch preparation is incomplete")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !launch.recipe.started.CompareAndSwap(false, true) {
		return fmt.Errorf("osd.%d creation was already attempted; do not replay its launch", launch.osd.ID)
	}
	if launch.osd.Container != nil {
		return fmt.Errorf("osd.%d already has a container; do not rebind its handle", launch.osd.ID)
	}
	ctr, err := f.stores.runOSD(ctx, launch.recipe.image, launch.recipe)
	if ctr != nil {
		launch.osd.Container = ctr
	}
	if err != nil {
		return fmt.Errorf("run ceph osd.%d: %w", launch.osd.ID, err)
	}
	return nil
}

// assignDeviceClass applies the configured class of an OSD that is up.
func (om osdMap) assignDeviceClass(ctx context.Context, osd *OSDContainer) error {
	if osd.placement.DeviceClass == "" {
		return nil
	}
	// Ceph assigns a class from the sparse backing device during startup.
	// A different explicit class must be applied through the CRUSH CLI.
	for _, args := range [][]string{
		{"osd", "crush", "rm-device-class", fmt.Sprintf("osd.%d", osd.ID)},
		{"osd", "crush", "set-device-class", osd.placement.DeviceClass, fmt.Sprintf("osd.%d", osd.ID)},
	} {
		if _, err := om.conn.ceph(ctx, args...); err != nil {
			return fmt.Errorf("assign osd.%d device class: %w", osd.ID, err)
		}
	}
	return nil
}

// removeOSD implements OSDContainer.Remove, which documents the behavior. The caller
// checks the handle under the topology lock.
func (c *Container) removeOSD(ctx context.Context, handle *OSDContainer) error {
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
	id := handle.ID
	if err := c.fleet.osdHandles.current(handle); err != nil {
		return err
	}
	osd := handle
	if osd.detaching {
		return fmt.Errorf("osd.%d is being detached; retry OSDContainer.Detach", id)
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	name := fmt.Sprintf("osd.%d", id)
	if err := c.osdFleet().checkOSDRemovable(ctx, osd); err != nil {
		return err
	}
	if osd.Container != nil {
		if err := c.native.fence(ctx, osd.GetContainerID()); err != nil {
			return fmt.Errorf("stop native commands for %s: %w", name, err)
		}
	}
	if err := c.osdFleet().drainAndPurgeOSD(ctx, osd); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if osd.Container != nil {
		if err := osd.Terminate(ctx); !onlyMissingHostResource(err) {
			return fmt.Errorf("terminate %s: %w", name, err)
		}
	}
	if err := c.fleet.stores.discard(ctx, id); err != nil {
		return err
	}
	// The container is gone when it was terminated above; a launch that never
	// created one keeps no claim, and an unconfirmed lost create reply keeps it
	// for Terminate.
	c.fleet.deviceClaims.release(id, osd.Container != nil)
	c.fleet.osdHandles.forget(id)
	return nil
}

// checkOSDRemovable verifies, before removeOSD fences or changes anything,
// that osd is the registered native OSD this handle created and that another
// owned OSD remains. A purge that may have committed with a lost response is
// settled here by a fresh, strict absence. The caller holds c.mu.
func (f osdFleet) checkOSDRemovable(ctx context.Context, osd *OSDContainer) error {
	id := osd.ID
	if osd.purged {
		return nil
	}
	expected, err := uuid.Parse(osd.nativeUUID)
	if err != nil || expected == uuid.Nil {
		return fmt.Errorf("osd.%d registration UUID is unavailable", id)
	}
	dump, err := osdMap{f.conn}.removalDump(ctx)
	if err != nil {
		return err
	}
	if _, exists := dump.osds[id]; !exists && osd.purgeIssued {
		// The previous purge may have committed despite a lost response.
		// A fresh, strict absence can settle only this handle's own request.
		osd.purged = true
	} else {
		if _, err := f.osdHandles.ownedState(dump, id); err != nil {
			return err
		}
		remaining := 0
		for otherID, other := range f.osdHandles.each() {
			if other == nil || other.purged {
				continue
			}
			if _, err := f.osdHandles.ownedState(dump, otherID); err == nil {
				remaining++
			}
		}
		if remaining <= 1 {
			return errors.New("cannot remove the last OSD; terminate the cluster instead")
		}
	}
	return nil
}

// drainAndPurgeOSD takes osd out of the data placement, waits for the drain,
// stops its container, waits until the MONs report it down and purges it,
// rereading the owned native state between the steps. It marks purgeIssued
// before the purge request, whose lost response does not tell whether the MON
// committed it. An already purged OSD is left alone. The caller holds c.mu and
// has fenced the OSD's native commands.
func (f osdFleet) drainAndPurgeOSD(ctx context.Context, osd *OSDContainer) error {
	id := osd.ID
	name := fmt.Sprintf("osd.%d", id)
	if osd.purged {
		return nil
	}
	// Reweight avoids the small-cluster active+remapped case documented by Ceph.
	if err := f.removalOSDMutation(ctx, id, "osd", "crush", "reweight", name, "0"); err != nil {
		return err
	}
	if err := f.removalOSDMutation(ctx, id, "osd", "out", strconv.Itoa(id)); err != nil {
		return err
	}
	if err := f.waitOSDRemoval(ctx, id, true); err != nil {
		return fmt.Errorf("drain %s: %w", name, err)
	}
	if _, err := f.removalOSDState(ctx, id); err != nil {
		return err
	}
	if osd.Container != nil {
		stopTimeout := 10 * time.Second
		if err := osd.Stop(ctx, &stopTimeout); err != nil {
			return fmt.Errorf("stop %s: %w", name, err)
		}
	}
	if err := f.waitOSDRemoval(ctx, id, false); err != nil {
		return fmt.Errorf("wait for %s down: %w", name, err)
	}
	if _, err := f.removalOSDState(ctx, id); err != nil {
		return err
	}
	// Mark before the request because cancellation/transport failure does
	// not establish whether the MON committed the purge.
	osd.purgeIssued = true
	if _, err := f.conn.ceph(ctx, "osd", "purge", strconv.Itoa(id), "--yes-i-really-mean-it"); err != nil {
		return err
	}
	osd.purged = true
	return nil
}

// Native reads and each mutation remain inside the serialized removal. A
// malformed/missing/replaced identity stops the operation immediately rather
// than being polled until timeout and then mistaken for successful draining.
func (om osdMap) removalDump(ctx context.Context) (osdPolicySnapshot, error) {
	if err := ctx.Err(); err != nil {
		return osdPolicySnapshot{}, err
	}
	dump, err := om.policyDump(ctx)
	if err != nil {
		return dump, err
	}
	return dump, ctx.Err()
}

// removalOSDState reads the native state of one OSD of an ongoing removal.
// The caller holds c.mu.
func (f osdFleet) removalOSDState(ctx context.Context, id int) (OSDState, error) {
	dump, err := osdMap{f.conn}.removalDump(ctx)
	if err != nil {
		return OSDState{}, err
	}
	return f.osdHandles.ownedState(dump, id)
}

// removalOSDMutation runs one mutation of a removal after re-checking the OSD.
// The caller holds c.mu.
func (f osdFleet) removalOSDMutation(ctx context.Context, id int, args ...string) error {
	if _, err := f.removalOSDState(ctx, id); err != nil {
		return err
	}
	_, err := f.conn.ceph(ctx, args...)
	return err
}

// waitOSDRemoval waits for the drain or the stop of an OSD in removal. The
// caller holds c.mu.
func (f osdFleet) waitOSDRemoval(ctx context.Context, id int, drain bool) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		state, err := f.removalOSDState(ctx, id)
		if err != nil {
			return errors.Join(err, last)
		}
		if !drain && !state.Up {
			return nil
		}
		if drain {
			if _, err := f.conn.ceph(ctx, "osd", "safe-to-destroy", strconv.Itoa(id)); err == nil {
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

// ServiceContainers returns owned optional services (such as RGW or MDS),
// sorted by service name. The cluster owns their cleanup.
func (c *Container) ServiceContainers() []testcontainers.Container {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.members.roster.sortedServices()
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
	if strings.TrimSpace(image) == "" {
		return nil, invalidRequest("image is required for service %s", name)
	}
	if c.members.roster.hasService(name) {
		return nil, fmt.Errorf("ceph service %s already started", name)
	}
	if !c.bootstrap.isComplete() {
		return nil, errors.New("ceph cluster bootstrap is incomplete")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	moduleOpts := append([]testcontainers.ContainerCustomizer{c.adminClient()}, opts...)
	moduleOpts = append(moduleOpts, c.startup.ownedLabel())
	ctr, err := runContainer(ctx, image, moduleOpts...)
	if ctr != nil {
		c.members.roster.setService(name, ctr)
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
// passed by the caller still takes precedence. From its start, also when it
// then fails, Terminate refuses new native commands and cancels the running
// ones, including the command of an rgw.Gateway.Admin call.
func (c *Container) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	return c.terminate(ctx, opts...)
}

func (c *Container) terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	opts = append([]testcontainers.TerminateOption{testcontainers.StopTimeout(terminationStopTimeout)}, opts...)
	// Refuse and cancel native commands before waiting for the mutex: an
	// rgw.Gateway.Admin call holds it while its command runs, and returns
	// once that command is stopped.
	c.native.cancelAll()
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	c.closed = true
	// Undo faults in their fixed phases: network delays first, with c.mu
	// released around each Restore, while their helpers and targets still
	// run; then interruptions of caller-owned clients and owned daemons
	// before any network is removed; then pauses, because a frozen process
	// cannot handle the stop signal until it is thawed. A failed undo
	// remains retryable through its handle.
	errs, blockedNetworks := c.faults.undo(ctx, &c.mu)
	// Stop native commands while their containers still run. Those that
	// stay unconfirmed are retried after the teardown, which ends the ones in
	// the cluster's own containers.
	_ = c.native.stopAll(ctx)
	errs = append(errs, c.portLeases.release(ctx)...)
	// Stop gateways and metadata servers while their backing OSDs are alive.
	for name, ctr := range c.members.roster.eachService() {
		if err := ctr.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
			errs = append(errs, fmt.Errorf("terminate service %s: %w", name, err))
		} else {
			c.members.roster.dropService(name)
		}
	}
	for id, osd := range c.fleet.osdHandles.each() {
		if osd.Container != nil {
			if err := osd.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
				errs = append(errs, err)
				continue
			}
		}
		c.fleet.deviceClaims.release(id, osd.Container != nil)
		c.fleet.osdHandles.forget(id)
	}
	memoryErr := c.fleet.stores.releaseMemory(ctx, c.fleet.osdHandles.count() == 0)
	volumeErr := c.fleet.stores.releaseVolume(ctx, c.fleet.osdHandles.count() == 0)
	if first := c.members.roster.initialManager(); first != nil {
		if err := first.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
			errs = append(errs, err)
		} else {
			c.members.roster.clearInitialManager()
		}
	}
	for name, mgr := range c.members.roster.eachManager() {
		if name == "a" {
			if c.members.roster.initialManager() == nil {
				c.members.roster.dropManager(name)
			}
			continue
		}
		if mgr.Container != nil {
			if err := mgr.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
				errs = append(errs, err)
				continue
			}
		}
		c.members.roster.dropManager(name)
	}
	for name, mon := range c.members.roster.eachMonitor() {
		if mon.Container != nil {
			if err := mon.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
				errs = append(errs, err)
				continue
			}
		}
		c.members.roster.dropMonitor(name)
	}
	if topology := c.monitorTopology(); c.Container != nil && !topology.firstMonitorTerminated() {
		if err := c.Container.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
			errs = append(errs, err)
		} else {
			topology.markFirstMonitorTerminated()
		}
	}
	// The added and the primary MON containers are the users of the MON store
	// volumes; release the stores whose attachment is gone.
	monStoreErr := c.monStores.releaseStores(ctx)
	if err := c.controlPlane.terminate(ctx, opts...); err != nil {
		errs = append(errs, err)
	}
	if err := c.toolkit.lock(ctx); err != nil {
		errs = append(errs, err)
	} else {
		if client := c.toolkit.current(); client != nil {
			if err := client.Terminate(ctx, opts...); !onlyMissingHostResource(err) {
				errs = append(errs, err)
			} else {
				c.toolkit.clear()
			}
		}
		c.toolkit.unlock()
	}

	scanErr := c.startup.removeLabeledContainers(ctx)
	errs = append(errs, scanErr)
	// The removed containers can include a storage user whose create reply
	// was lost, such as an OSD or an owned storage helper.
	memoryErr = c.fleet.stores.retryMemory(ctx, memoryErr, c.fleet.osdHandles.count() == 0)
	volumeErr = c.fleet.stores.retryVolume(ctx, volumeErr, c.fleet.osdHandles.count() == 0)
	monStoreErr = c.monStores.retryStores(ctx, monStoreErr)
	errs = append(errs, memoryErr, volumeErr, monStoreErr,
		c.fleet.deviceClaims.settle(scanErr == nil && c.startup.ownerID() != ""))
	if err := c.runDir.release(ctx); err != nil {
		errs = append(errs, err)
	}
	errs = append(errs, c.nets.removeBridges(ctx, blockedNetworks)...)
	errs = append(errs, c.native.stopAll(ctx)...)
	return errors.Join(errs...)
}

// waitUp waits until every OSD of ids is up and in. Its error names the
// OSDs that were not.
func (om osdMap) waitUp(ctx context.Context, ids []int) error {
	pending := slices.Clone(ids)
	err := clusterutil.Poll(ctx, func() (bool, error) {
		flags, err := om.upIn(ctx)
		if err != nil {
			return false, err
		}
		pending = pending[:0]
		for _, id := range ids {
			if state := flags[id]; !state.up || !state.in {
				pending = append(pending, id)
			}
		}
		return len(pending) == 0, nil
	})
	if err == nil {
		return nil
	}
	names := make([]string, len(pending))
	for i, id := range pending {
		names[i] = fmt.Sprintf("osd.%d", id)
	}
	return fmt.Errorf("wait for %s up/in: %w", strings.Join(names, ", "), err)
}

type osdFlags struct{ up, in bool }

// upIn returns the up and in flags of every OSD in the OSDMap.
func (om osdMap) upIn(ctx context.Context) (map[int]osdFlags, error) {
	result, err := om.conn.ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return nil, err
	}
	var dump struct {
		OSDs []struct {
			ID int `json:"osd"`
			Up int `json:"up"`
			In int `json:"in"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(result, &dump); err != nil {
		return nil, err
	}
	flags := make(map[int]osdFlags, len(dump.OSDs))
	for _, osd := range dump.OSDs {
		flags[osd.ID] = osdFlags{up: osd.Up == 1, in: osd.In == 1}
	}
	return flags, nil
}

// A gracefully stopping MDS otherwise waits about five seconds; data in a
// terminated cluster is never read again.
const terminationStopTimeout = 2 * time.Second

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
