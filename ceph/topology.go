package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// daemonMaps is the cluster's native MON quorum and MGR map view: the quorum
// status, the removal of one MON from the monmap and the wait for a manager to
// register. It holds no state and takes no lock; the roster stays on Container
// under Container.mu.
type daemonMaps struct {
	conn cephConn
}

// MonitorContainer is an owned quorum member. Stop/Start preserves its store;
// MonitorContainer.Remove explicitly removes membership and deletes the container.
type MonitorContainer struct {
	testcontainers.Container
	DaemonName string
	owner      *Container
	image      string
}

// ManagerContainer is an owned active or standby manager.
type ManagerContainer struct {
	testcontainers.Container
	DaemonName string
	owner      *Container
	authOwned  bool
	terminated bool
	image      string
}

var daemonNamePattern = naming.DaemonName

func daemonName(index int) string {
	if index < 26 {
		return string(rune('a' + index))
	}
	return fmt.Sprintf("node-%d", index)
}

// ensureControlPlane is called while topology holds c.mu. Separating CLI from
// MON a keeps topology operations available when that quorum member is stopped.
func (c *Container) ensureControlPlane(ctx context.Context) error {
	if c.controlPlane.current() != nil {
		return nil
	}
	ctr, err := testcontainers.Run(ctx, c.settings.essentialImage, append([]testcontainers.ContainerCustomizer{c.adminClient(), c.startup.ownedLabel(),
		WithIdleEntrypoint(),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"}))},
		c.runDir.mounts()...)...)
	if ctr != nil {
		c.controlPlane.publish(ctr)
	}
	return err
}

// monitorsLocked lists the owned MONs. The caller holds c.mu.
func (t monitorTopology) monitorsLocked() []*MonitorContainer {
	result := make([]*MonitorContainer, 0, t.roster.monitorCount()+1)
	if t.firstMonitorStands() {
		result = append(result, &MonitorContainer{Container: t.first, DaemonName: "a", owner: t.owner, image: t.settings.essentialImage})
	}
	for _, mon := range t.roster.eachMonitor() {
		result = append(result, mon)
	}
	slices.SortFunc(result, func(a, b *MonitorContainer) int { return strings.Compare(a.DaemonName, b.DaemonName) })
	return result
}

// QuorumStatus contains the actual quorum and advertised monitor addresses.
type QuorumStatus struct {
	QuorumNames []string `json:"quorum_names"`
	MonMap      struct {
		FSID string `json:"fsid"`
		Mons []struct {
			Name        string `json:"name"`
			PublicAddrs struct {
				Addrvec []struct {
					Type string `json:"type"`
					Addr string `json:"addr"`
				} `json:"addrvec"`
			} `json:"public_addrs"`
		} `json:"mons"`
	} `json:"monmap"`
}

// quorum implements Observer.QuorumStatus.
func (dm daemonMaps) quorum(ctx context.Context) (QuorumStatus, error) {
	control, err := dm.conn.controlContainer(ctx)
	if err != nil {
		return QuorumStatus{}, fmt.Errorf("select monitor quorum control: %w", err)
	}
	return queryMonitorQuorum(ctx, control)
}

// WaitForQuorum waits for a majority of the current monmap, including fixtures
// with a stopped monitor. It does not require HEALTH_OK or every MON to run.
//
//purpose:observation
func (c *Container) WaitForQuorum(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	return clusterutil.Poll(ctx, func() (bool, error) {
		status, err := daemonMaps{c.conn()}.quorum(ctx)
		return len(status.MonMap.Mons) > 0 && len(status.QuorumNames) > len(status.MonMap.Mons)/2, err
	})
}

// AddMonitor joins a new named MON from the live quorum's map and shared key.
// Existing daemon data is retained; a partial new member remains owned on error.
// DaemonOptions.WithImage starts this one MON from another image; the default is the
// essential image. Options are validated before the topology lock is taken or
// anything is created.
//
// Names already in the monmap are refused, and a can never be reused. The
// first call also starts the independent control container. After joining, it
// copies the new quorum's addresses into the mon_host of every owned daemon's
// ceph.conf, including stopped daemons; if only that copy failed, retry
// RefreshMonitorConfig. Caller-owned client containers are not updated (see
// RefreshClientMonitorConfig). A partially started member stays owned on
// error.
//
//purpose:construction,condition
func (c *Container) AddMonitor(ctx context.Context, name string, opts ...DaemonOptions) (*MonitorContainer, error) {
	if !daemonNamePattern.MatchString(name) {
		return nil, errors.New("invalid monitor name")
	}
	daemon, err := resolveDaemonOptions(opts)
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
	if (name == "a" && !c.monitorTopology().firstMonitorTerminated()) || c.members.roster.monitor(name) != nil {
		return nil, fmt.Errorf("monitor %s is already owned", name)
	}
	if name == "a" {
		return nil, errors.New("primary monitor name a cannot be reused")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	key, monmap, err := c.monitorJoinMaterial(ctx, name)
	if err != nil {
		return nil, err
	}
	mon := &MonitorContainer{DaemonName: name, owner: c, image: c.settings.daemonImage(daemon)}
	c.members.roster.addMonitor(name, mon)
	join := monitorJoin{name: name, config: []byte(strings.ReplaceAll(string(c.bootstrap.rawConfig()), "mon initial members = a\n", "")), key: key, monmap: monmap}
	if err := c.startJoiningMonitor(ctx, mon, join); err != nil {
		return mon, err
	}
	if err := clusterutil.Poll(ctx, func() (bool, error) {
		status, err := daemonMaps{c.conn()}.quorum(ctx)
		return slices.Contains(status.QuorumNames, name), err
	}); err != nil {
		return mon, fmt.Errorf("join monitor %s to quorum: %w", name, err)
	}
	return mon, c.monitorTopology().refreshMonitorConfig(ctx)
}

// monitorJoin is what a joining MON starts from: the cluster configuration
// without the initial-members line, the shared mon. key and the live monmap.
type monitorJoin struct {
	name                string
	config, key, monmap []byte
}

// monitorJoinMaterial refuses a name the live monmap already has, starts the
// independent CLI control and reads the shared mon. key and the live monmap.
// The caller holds c.mu.
func (c *Container) monitorJoinMaterial(ctx context.Context, name string) (key, monmap []byte, err error) {
	status, err := daemonMaps{c.conn()}.quorum(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect monitor membership: %w", err)
	}
	for _, member := range status.MonMap.Mons {
		if member.Name == name {
			return nil, nil, fmt.Errorf("monitor %s is already registered", name)
		}
	}
	return c.monitorReplacementMaterial(ctx)
}

// startJoiningMonitor starts mon's container. On the host network it reserves
// the MON ports first and retries a lost port race with a fresh reservation;
// any other failure, or the last attempt's, returns with mon still owned. The
// caller holds c.mu.
func (c *Container) startJoiningMonitor(ctx context.Context, mon *MonitorContainer, join monitorJoin) error {
	if c.settings.monitorVolume {
		// The first generation of the MON's own store volume. The admin
		// socket is cleared before every creation, so that a socket a MON
		// that lost its host ports left cannot pass the next one's ready check.
		gens := c.monitorGenerations(join.name, func(ports []int) []testcontainers.ContainerCustomizer {
			return c.monitorJoinOptions(join, ports)
		}, monitorSocketReady(join.name))
		gens.beforeCreate = func(ctx context.Context) error { return c.clearAdminSocket(ctx, "mon."+join.name) }
		ctr, err := c.startMonitorStore(ctx, join.name, mon.image, gens)
		if ctr != nil {
			mon.Container = ctr
		}
		return err
	}
	attempts := 1
	if c.settings.hostNetwork {
		attempts = hostPortAttempts
	}
	for attempt := range attempts {
		var lease *hostPortLease
		var err error
		if c.settings.hostNetwork {
			lease, err = reserveHostPorts(ctx, c.settings.essentialImage, c.staticFacts().PublicAddress, c.settings.monitorPortCount(), c.settings.startupTimeout, c.startup.ownedLabel())
			// c.mu is already held, so register ownership without re-locking it.
			c.portLeases.track(lease)
			if err != nil {
				return err
			}
		}
		if err := c.clearAdminSocket(ctx, "mon."+join.name); err != nil {
			return err
		}
		var ports []int
		if lease != nil {
			ports = lease.Ports
		}
		mon.Container, err = c.runDaemon(ctx, mon.image, c.monitorJoinOptions(join, ports)...)
		if lease != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			releaseErr := lease.Release(cleanupCtx)
			cancel()
			if err != nil || releaseErr != nil {
				return errors.Join(err, releaseErr)
			}
			err = mon.Start(ctx)
		}
		if err == nil {
			return nil
		}
		if lease == nil || attempt+1 == attempts || ctx.Err() != nil {
			return err
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		conflict := isPortConflict(cleanupCtx, mon.Container)
		if conflict {
			err = mon.Terminate(cleanupCtx)
		}
		cancel()
		if !conflict || !onlyMissingHostResource(err) {
			return err
		}
	}
	return nil
}

// monitorSocketReady is the ready check of a joining MON: its admin socket.
func monitorSocketReady(name string) []string {
	return []string{"test", "-S", "/var/run/ceph/ceph-mon." + name + ".asok"}
}

// monitorJoinOptions is the container recipe of a joining MON: the join
// script with the configuration, key and monmap, its socket wait, and either
// the reserved host ports (created, not started) or the kept bridge address.
// ports is nil on a bridge.
func (c *Container) monitorJoinOptions(join monitorJoin, ports []int) []testcontainers.ContainerCustomizer {
	opts := []testcontainers.ContainerCustomizer{c.adminClient(),
		testcontainers.WithEntrypoint("/bin/sh", "/tc/mon-join.sh"), testcontainers.WithCmd(),
		testcontainers.WithEnv(c.settings.monitorJoinEnvironment(join.name)),
		testcontainers.WithFiles(scriptFile("mon-join"), textFile("/etc/ceph/ceph.conf", join.config, 0o644),
			textFile("/etc/ceph/mon.keyring", join.key, 0o600), textFile("/tc/monmap", join.monmap, 0o600)),
		testcontainers.WithWaitStrategy(wait.ForExec(monitorSocketReady(join.name)).WithStartupTimeout(c.settings.startupTimeout))}
	if ports != nil {
		portEnv := c.settings.monitorPortEnvironment(ports)
		portEnv["CEPH_PUBLIC_ADDRESS"] = c.staticFacts().PublicAddress
		opts = append(opts, testcontainers.WithNoStart(), testcontainers.WithEnv(portEnv))
	} else {
		opts = append(opts, keepMonitorAddress(c.staticFacts().NetworkName))
	}
	opts = append(opts, c.runDir.mounts()...)
	return append(opts, c.startup.ownedLabel())
}

// removeMonitor implements MonitorContainer.Remove, which documents the behavior. The caller
// checks the handle under the topology lock.
func (c *Container) removeMonitor(ctx context.Context, handle *MonitorContainer) error {
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.closed {
		return errClusterTerminated
	}
	return c.monitorTopology().retireMonitor(ctx, handle)
}

// retireMonitor kills the MON of handle while a majority quorum survives
// without it, removes it from the native membership, terminates its container,
// refreshes the monitor configuration of the survivors and drops it from the
// membership. The caller holds c.mu and has admitted the call.
func (t monitorTopology) retireMonitor(ctx context.Context, handle *MonitorContainer) error {
	name := handle.DaemonName
	mon, err := t.currentMonitor(handle)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, t.settings.startupTimeout)
	defer cancel()
	var status QuorumStatus
	if err := clusterutil.Poll(ctx, func() (bool, error) {
		current, err := daemonMaps{t.conn}.quorum(ctx)
		if err != nil {
			return false, err
		}
		if len(current.MonMap.Mons) == 0 {
			return false, errors.New("native monitor membership map is empty")
		}
		status = current
		return true, nil
	}); err != nil {
		return fmt.Errorf("inspect monitor %s membership before removal: %w", name, err)
	}
	member := false
	for _, current := range status.MonMap.Mons {
		if current.Name == name {
			member = true
			break
		}
	}
	if member {
		remaining := len(status.QuorumNames)
		if slices.Contains(status.QuorumNames, name) {
			remaining--
		}
		if remaining <= len(status.MonMap.Mons)/2 || len(status.MonMap.Mons) <= 1 {
			return errors.New("removing this monitor would lose quorum; add a replacement first")
		}
	}
	// Stop the MON before it leaves the membership, as Ceph's manual removal
	// procedure does. Committing the removal renumbers the survivors' ranks,
	// and each of them then drops every client session. A warm client hunts
	// again from a monmap that still lists the removed MON. If that MON still
	// runs, it can authenticate the client and go away before the session is
	// ready: the client keeps it as its session, its messenger retries the
	// gone address forever, and the keepalive timeout, which needs the
	// session's features, never fires. A killed MON authenticates nobody, so
	// the client settles on a survivor. The surviving majority was checked
	// above and elects without it.
	if mon.Container != nil {
		if err := t.killMonitor(ctx, mon.Container); err != nil {
			return fmt.Errorf("stop monitor %s before its removal: %w", name, err)
		}
	}
	if member {
		// The removed MON no longer runs; the CLI hunts only the survivors.
		survivors := status
		survivors.MonMap.Mons = slices.DeleteFunc(slices.Clone(status.MonMap.Mons), func(current struct {
			Name        string `json:"name"`
			PublicAddrs struct {
				Addrvec []struct {
					Type string `json:"type"`
					Addr string `json:"addr"`
				} `json:"addrvec"`
			} `json:"public_addrs"`
		}) bool {
			return current.Name == name
		})
		survivors.QuorumNames = slices.DeleteFunc(slices.Clone(status.QuorumNames), func(current string) bool { return current == name })
		hosts, err := monitorBootstrapAddresses(survivors)
		if err != nil {
			return fmt.Errorf("select surviving monitors before removing %s: %w", name, err)
		}
		if err := (daemonMaps{t.conn}).removeMonitorMember(ctx, hosts, name); err != nil {
			return fmt.Errorf("remove monitor %s from native membership: %w", name, err)
		}
	}
	// A failed AddMonitor can own a descriptor before any container or map
	// member exists. Membership can also be gone after a previous removal
	// succeeded but Docker cleanup failed. Neither case changes quorum.
	if mon.Container != nil {
		if err := mon.Terminate(ctx); !onlyMissingHostResource(err) {
			return fmt.Errorf("terminate removed monitor %s container: %w", name, err)
		}
	}
	// Removing a MON can elect a new leader after the membership command has
	// committed. Keep cleanup ownership until a fresh native view confirms the
	// remaining quorum and the control client's bootstrap addresses are copied.
	if err := t.refreshMonitorConfigAfterRemoval(ctx, name); err != nil {
		return fmt.Errorf("refresh control configuration after removing monitor %s: %w", name, err)
	}
	// A removed MON never comes back on its store: the record is released
	// once nothing mounts the volume, and kept with the handle for a retry
	// while that is uncertain. A later MON of the same name gets a new store.
	if err := t.monStores.Retire(ctx, name); err != nil {
		return fmt.Errorf("release the store of removed monitor %s: %w", name, err)
	}
	// MON a is the embedded first container until a replacement moved it
	// into the roster (replaceMonitor).
	if t.roster.monitor(name) == mon {
		t.roster.dropMonitor(name)
	} else if name == "a" {
		t.markFirstMonitorTerminated()
	}
	return nil
}

// refreshMonitorConfig refreshes the control configuration after a monitor
// change. The caller holds c.mu.
func (t monitorTopology) refreshMonitorConfig(ctx context.Context) error {
	return t.refreshMonitorConfigAfterRemoval(ctx, "")
}

// RefreshMonitorConfig copies the live quorum's bootstrap addresses into the
// global mon_host entry of every owned daemon's ceph.conf, including stopped
// daemons, without starting or restarting them. Other entries and comments are
// retained. Caller-owned clients and multicluster daemon configs are not copied.
//
// AddMonitor and MonitorContainer.Remove already refresh these files. Retry this method
// after an AddMonitor that joined native membership but could not copy every
// config; retry MonitorContainer.Remove itself after a partial removal. Successful copies
// and the future-client template are retained on partial failure. Repeating a
// refresh reads each current file and skips copies that are already up to date.
// External config writers must not race this operation: Docker has no file CAS.
//
//purpose:condition,handoff
func (c *Container) RefreshMonitorConfig(ctx context.Context) error {
	if c == nil {
		return errors.New("ceph cluster is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.closed {
		return errClusterTerminated
	}
	return c.monitorTopology().refreshMonitorConfig(ctx)
}

// Unlike Mutex.Lock, waiting for serialized topology work must respect the
// caller's deadline. The caller owns Unlock after this helper succeeds.
func (c *Container) lockTopology(ctx context.Context) error {
	return lockTopologyMutex(ctx, &c.mu)
}

// The caller holds c.mu throughout. The topology caller supplies its existing operation deadline. Only native
// reads are polled here; membership is never mutated or daemons restarted. Each
// config is read through Docker's archive API, which also works when stopped.
func (t monitorTopology) refreshMonitorConfigAfterRemoval(ctx context.Context, removedName string) error {
	var status QuorumStatus
	if err := clusterutil.Poll(ctx, func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		current, err := daemonMaps{t.conn}.quorum(ctx)
		if err != nil {
			return false, err
		}
		if len(current.MonMap.Mons) == 0 || len(current.QuorumNames) <= len(current.MonMap.Mons)/2 {
			return false, errors.New("monitor membership has no majority quorum")
		}
		if _, err := monitorBootstrapAddresses(current); err != nil {
			return false, err
		}
		if removedName != "" {
			if slices.Contains(current.QuorumNames, removedName) {
				return false, fmt.Errorf("removed monitor %s remains in native quorum", removedName)
			}
			for _, member := range current.MonMap.Mons {
				if member.Name == removedName {
					return false, fmt.Errorf("removed monitor %s remains in native monmap", removedName)
				}
			}
		}
		status = current
		return true, nil
	}); err != nil {
		return err
	}
	addresses, err := monitorBootstrapAddresses(status)
	if err != nil {
		return err
	}
	template := t.bootstrap.template()
	template, err = replaceGlobalMonitorHost(template, addresses)
	if err != nil {
		return fmt.Errorf("refresh future client configuration: %w", err)
	}
	control := t.conn.cliContainer()
	if control == nil {
		return errors.New("ceph control container is unavailable")
	}
	if err := copyMonitorBootstrapConfig(ctx, control, addresses); err != nil {
		return fmt.Errorf("refresh control configuration: %w", err)
	}
	// Publish after the control's current file confirms the new addresses. A
	// later daemon copy failure must not keep new clients on removed monitors.
	t.bootstrap.publishConfig(template)
	var errs []error
	targets, err := t.monitorConfigTargets(ctx, control, removedName)
	if err != nil {
		return err
	}
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if err := copyMonitorBootstrapConfig(ctx, target.container, addresses); err != nil {
			errs = append(errs, fmt.Errorf("refresh %s configuration: %w", target.name, err))
		}
	}
	return errors.Join(errs...)
}

func monitorBootstrapAddresses(status QuorumStatus) (string, error) {
	names := make(map[string]bool)
	var endpoints []string
	for _, mon := range status.MonMap.Mons {
		if mon.Name == "" || names[mon.Name] || len(mon.PublicAddrs.Addrvec) == 0 {
			return "", errors.New("native monmap has empty/duplicate members or missing addresses")
		}
		names[mon.Name] = true
		var addresses []string
		seen := make(map[string]bool)
		for _, addr := range mon.PublicAddrs.Addrvec {
			endpoint, nonce, hasNonce := strings.Cut(addr.Addr, "/")
			host, port, err := net.SplitHostPort(endpoint)
			ip, ipErr := netip.ParseAddr(host)
			number, portErr := strconv.Atoi(port)
			if err != nil || ipErr != nil || ip.IsUnspecified() || portErr != nil || number < 1 || number > 65535 || (addr.Type != "v1" && addr.Type != "v2") {
				return "", errors.New("native monmap contains an invalid bootstrap address")
			}
			if hasNonce {
				if _, err := strconv.ParseUint(nonce, 10, 64); err != nil {
					return "", errors.New("native monmap contains an invalid address nonce")
				}
			}
			// Ceph prints IPv4-mapped addresses in a form it cannot parse.
			address := addr.Type + ":" + net.JoinHostPort(cephAddress(ip), port)
			if seen[address] {
				return "", errors.New("native monmap contains a duplicate member address")
			}
			seen[address] = true
			addresses = append(addresses, address)
		}
		endpoints = append(endpoints, "["+strings.Join(addresses, ",")+"]")
	}
	quorum := make(map[string]bool)
	for _, name := range status.QuorumNames {
		if !names[name] || quorum[name] {
			return "", errors.New("native quorum has unknown or duplicate members")
		}
		quorum[name] = true
	}
	if len(names) == 0 || len(quorum) <= len(names)/2 {
		return "", errors.New("monitor membership has no majority quorum")
	}
	return strings.Join(endpoints, " "), nil
}

type monitorConfigTarget struct {
	name      string
	container testcontainers.Container
}

// The topology caller holds c.mu. Deduplicate the generic service handles and
// filesystem/gateway descriptors without calling accessors that re-lock c.mu.
func (t monitorTopology) monitorConfigTargets(ctx context.Context, control testcontainers.Container, removedName string) ([]monitorConfigTarget, error) {
	var candidates []monitorConfigTarget
	client, err := t.client(ctx)
	if err != nil {
		return nil, err
	}
	add := func(name string, ctr testcontainers.Container) {
		if ctr != nil {
			candidates = append(candidates, monitorConfigTarget{name: name, container: ctr})
		}
	}
	add("client", client)
	if t.firstMonitorStands() && removedName != "a" {
		add("mon.a", t.first)
	}
	for name, mon := range t.roster.eachMonitor() {
		if mon != nil && name != removedName {
			add("mon."+name, mon.Container)
		}
	}
	for name, mgr := range t.roster.eachManager() {
		if mgr != nil && !mgr.terminated {
			add("mgr."+name, mgr.Container)
		}
	}
	if initial := t.roster.manager("a"); initial == nil || !initial.terminated {
		add("mgr.a", t.roster.initialManager())
	}
	for id, osd := range t.osds.osdHandles.each() {
		if osd != nil && !osd.purged {
			add(fmt.Sprintf("osd.%d", id), osd.Container)
		}
	}
	for name, ctr := range t.roster.eachService() {
		add(name, ctr)
	}
	for _, targets := range t.serviceTargets() {
		for _, name := range slices.Sorted(maps.Keys(targets)) {
			add(name, targets[name])
		}
	}
	slices.SortFunc(candidates, func(a, b monitorConfigTarget) int { return strings.Compare(a.name, b.name) })
	seen := map[string]bool{control.GetContainerID(): true}
	var targets []monitorConfigTarget
	for _, target := range candidates {
		id := target.container.GetContainerID()
		if id != "" && seen[id] {
			continue
		}
		seen[id] = true
		targets = append(targets, target)
	}
	return targets, nil
}

const monitorConfigMaxBytes = 1 << 20

func copyMonitorBootstrapConfig(ctx context.Context, ctr testcontainers.Container, addresses string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	reader, err := ctr.CopyFileFromContainer(ctx, "/etc/ceph/ceph.conf")
	if err != nil {
		return err
	}
	config, readErr := io.ReadAll(io.LimitReader(reader, monitorConfigMaxBytes+1))
	if err := errors.Join(readErr, reader.Close()); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	updated, err := replaceGlobalMonitorHost(config, addresses)
	if err != nil {
		return err
	}
	if bytes.Equal(config, updated) {
		return nil
	}
	return ctr.CopyToContainer(ctx, updated, "/etc/ceph/ceph.conf", 0o644)
}

// Accept the fixture's single explicit global setting, including native key
// aliases, whitespace and comments. Ambiguous sections, duplicates, includes,
// continuations and malformed quoted values are refused without rewriting.
func replaceGlobalMonitorHost(config []byte, addresses string) ([]byte, error) {
	if len(config) > monitorConfigMaxBytes || !utf8.Valid(config) || bytes.IndexByte(config, 0) >= 0 || strings.TrimSpace(addresses) == "" || strings.ContainsAny(addresses, "\r\n\x00#;") {
		return nil, errors.New("invalid or oversized monitor bootstrap configuration")
	}
	lines := strings.SplitAfter(string(config), "\n")
	var scan monitorConfigScan
	for index, original := range lines {
		line, err := splitMonitorConfigLine(original)
		if err != nil {
			return nil, err
		}
		host, err := scan.next(line)
		if err != nil {
			return nil, err
		}
		if host {
			lines[index] = line.withValue(addresses)
		}
	}
	if scan.globals != 1 || scan.hosts != 1 {
		return nil, errors.New("configuration needs exactly one explicit global mon_host entry")
	}
	return []byte(strings.Join(lines, "")), nil
}

// monitorConfigLine is one configuration line: its text without the line
// ending, the part before any comment, that part trimmed, and the ending.
type monitorConfigLine struct {
	text, content, trimmed, ending string
}

func splitMonitorConfigLine(original string) (monitorConfigLine, error) {
	text := strings.TrimSuffix(strings.TrimSuffix(original, "\n"), "\r")
	comment, err := monitorConfigComment(text)
	if err != nil {
		return monitorConfigLine{}, err
	}
	content := text
	if comment >= 0 {
		content = text[:comment]
	}
	return monitorConfigLine{text: text, content: content, trimmed: strings.TrimSpace(content), ending: original[len(text):]}, nil
}

// withValue is the line with its value replaced by value, keeping the key,
// the blanks around the value, any comment and the line ending.
func (l monitorConfigLine) withValue(value string) string {
	start := strings.IndexByte(l.text, '=') + 1
	for start < len(l.text) && (l.text[start] == ' ' || l.text[start] == '\t') {
		start++
	}
	end := len(l.content)
	for end > start && (l.text[end-1] == ' ' || l.text[end-1] == '\t') {
		end--
	}
	return l.text[:start] + value + l.text[end:] + l.ending
}

// monitorConfigScan follows the sections of a configuration and refuses any
// line whose meaning would be ambiguous to Ceph's parser.
type monitorConfigScan struct {
	section, global bool
	globals, hosts  int
}

// next reads one line and reports whether it is the global mon_host entry.
func (s *monitorConfigScan) next(line monitorConfigLine) (bool, error) {
	switch {
	case line.trimmed == "":
		return false, nil
	case strings.HasPrefix(line.trimmed, "["):
		return false, s.enter(line.trimmed)
	}
	key, _, hasValue := strings.Cut(line.content, "=")
	if !s.section || !hasValue || strings.TrimSpace(key) == "" || strings.HasPrefix(line.trimmed, "!") || strings.HasSuffix(line.trimmed, "\\") {
		return false, errors.New("ambiguous configuration entry or include/continuation")
	}
	normalized := strings.Join(strings.Fields(strings.NewReplacer("_", " ", "-", " ").Replace(key)), "_")
	if normalized == "include" || normalized == "includedir" {
		return false, errors.New("configuration include cannot be refreshed unambiguously")
	}
	if !s.global || normalized != "mon_host" {
		return false, nil
	}
	s.hosts++
	if s.hosts != 1 {
		return false, errors.New("duplicate global mon_host configuration")
	}
	return true, nil
}

// enter starts the section of a header line.
func (s *monitorConfigScan) enter(header string) error {
	if !strings.HasSuffix(header, "]") || strings.Count(header, "[") != 1 || strings.Count(header, "]") != 1 || strings.TrimSpace(header[1:len(header)-1]) == "" {
		return errors.New("ambiguous configuration section")
	}
	s.section = true
	s.global = strings.TrimSpace(header[1:len(header)-1]) == "global"
	if s.global {
		s.globals++
		if s.globals != 1 {
			return errors.New("duplicate global configuration section")
		}
	}
	return nil
}

func monitorConfigComment(line string) (int, error) {
	var quoted byte
	escaped := false
	for index := range len(line) {
		char := line[index]
		if escaped {
			escaped = false
			continue
		}
		if char == '\\' {
			escaped = true
			continue
		}
		if quoted != 0 {
			if char == quoted {
				quoted = 0
			}
			continue
		}
		switch char {
		case '\'', '"':
			quoted = char
		case '#', ';':
			return index, nil
		}
	}
	if quoted != 0 || escaped {
		return -1, errors.New("ambiguous configuration quoting or continuation")
	}
	return -1, nil
}

// AddManager creates a named active/standby candidate and waits for its mgrmap
// registration. Existing daemon identities are rejected; it does not force
// active ownership away from an existing MGR.
// With AddressDualStack it then checks the active MGR's advertised addresses
// and fails with ErrUnsupported when one family has no concrete address.
// DaemonOptions.WithImage starts this one MGR from another image; the default is the
// essential image. Options are validated before the topology lock is taken or
// any identity is created.
//
// It never forces the active role away from a running manager, and success
// does not mean every MGR module is ready; use WaitMGRModuleReady or
// WaitForClean. On an AddressDualStack cluster it then checks the active MGR's
// advertised addresses.
//
//purpose:construction,condition
func (c *Container) AddManager(ctx context.Context, name string, opts ...DaemonOptions) (*ManagerContainer, error) {
	if !daemonNamePattern.MatchString(name) {
		return nil, errors.New("invalid manager name")
	}
	daemon, err := resolveDaemonOptions(opts)
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
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	mgr, err := c.monitorTopology().startManager(ctx, name, c.settings.daemonImage(daemon))
	if err != nil {
		return mgr, err
	}
	if err := (daemonMaps{c.conn()}).waitManagerRegistered(ctx, name); err != nil {
		return mgr, err
	}
	if err := c.checkDualStackAddresses(ctx); err != nil {
		return mgr, err
	}
	return mgr, nil
}

// startManager creates the identity of a new MGR and starts its container,
// but does not wait for the MGR to register. The caller holds c.mu. The
// returned MGR is nil when the error came before it was owned.
func (t monitorTopology) startManager(ctx context.Context, name, image string) (*ManagerContainer, error) {
	if t.roster.manager(name) != nil {
		return nil, fmt.Errorf("manager %s is already owned", name)
	}
	data, err := authStore{t.conn}.clientCommand(ctx, "check manager identity", "auth", "ls", "--format", "json")
	if err != nil {
		return nil, err
	}
	var identities struct {
		AuthDump []struct {
			Entity string `json:"entity"`
		} `json:"auth_dump"`
	}
	if err := json.Unmarshal(data, &identities); err != nil || identities.AuthDump == nil {
		return nil, errors.New("decode manager identity listing")
	}
	for _, identity := range identities.AuthDump {
		if identity.Entity == "mgr."+name {
			return nil, invalid(fmt.Errorf("manager identity mgr.%s already exists", name))
		}
	}
	status, err := readManagerStatus(ctx, t.conn.ceph)
	if err != nil {
		return nil, err
	}
	if _, registered := managerMapGID(status, name); registered {
		return nil, fmt.Errorf("manager %s is already registered", name)
	}
	key, err := authStore{t.conn}.clientCommand(ctx, "create manager identity", "auth", "get-or-create", "mgr."+name, "mon", "allow profile mgr", "osd", "allow *", "mds", "allow *")
	if err != nil {
		return nil, err
	}
	mgr := &ManagerContainer{DaemonName: name, owner: t.owner, authOwned: true, image: image}
	t.roster.addManager(name, mgr)
	if err := t.osds.clearSocket(ctx, "mgr."+name); err != nil {
		return mgr, err
	}
	mgr.Container, err = t.runDaemon(ctx, image, append([]testcontainers.ContainerCustomizer{t.adminClient(), t.osds.startup.ownedLabel(),
		testcontainers.WithEntrypoint("/bin/sh", "/tc/mgr.sh"), testcontainers.WithCmd(),
		testcontainers.WithEnv(map[string]string{"CEPH_MGR_ID": name}),
		testcontainers.WithFiles(scriptFile("mgr"), textFile("/etc/ceph/mgr.keyring", key, 0o600)),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", "/var/run/ceph/ceph-mgr." + name + ".asok"}).WithStartupTimeout(t.settings.startupTimeout))},
		t.osds.runDir.mounts()...)...)
	if name == "a" {
		t.roster.recordInitialManager(mgr.Container)
	}
	return mgr, err
}

// waitManagerRegistered waits until the MGR is the available active MGR or a
// standby.
func (dm daemonMaps) waitManagerRegistered(ctx context.Context, name string) error {
	if err := clusterutil.Poll(ctx, func() (bool, error) {
		status, err := readManagerStatus(ctx, dm.conn.ceph)
		if status.ActiveName == name && status.Available {
			return true, err
		}
		for _, standby := range status.Standbys {
			if standby.Name == name {
				return true, err
			}
		}
		return false, err
	}); err != nil {
		return fmt.Errorf("register manager %s: %w", name, err)
	}
	return nil
}

// removeManager implements ManagerContainer.Remove, which documents the behavior. The caller
// checks the handle under the topology lock.
func (c *Container) removeManager(ctx context.Context, handle *ManagerContainer) error {
	name := handle.DaemonName
	if !daemonNamePattern.MatchString(name) {
		return errors.New("invalid manager name")
	}
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
	defer c.mu.Unlock()
	if c.closed {
		return errClusterTerminated
	}
	return c.monitorTopology().retireManager(ctx, handle)
}

// retireManager removes the owned MGR of handle once another running,
// registered owned manager can take over: it terminates the container,
// fails the daemon out of the manager map, waits for the replacement,
// deletes the identity and drops it from the roster. The caller holds c.mu
// and has admitted the call.
func (t monitorTopology) retireManager(ctx context.Context, handle *ManagerContainer) error {
	name := handle.DaemonName
	mgr, err := t.roster.currentManager(handle)
	if err != nil {
		return err
	}
	if !mgr.authOwned {
		return invalid(fmt.Errorf("manager %s is not owned", name))
	}
	if t.roster.managerCount() <= 1 {
		return errors.New("cannot remove the last manager candidate; add a replacement first")
	}
	ctx, cancel := context.WithTimeout(ctx, t.settings.startupTimeout)
	defer cancel()
	status, err := readManagerStatus(ctx, t.conn.ceph)
	if err != nil {
		return err
	}
	gid, registered := managerMapGID(status, name)
	if registered && gid == 0 {
		return fmt.Errorf("manager %s has no daemon ID in the manager map", name)
	}
	candidates := make(map[string]bool)
	for otherName, other := range t.roster.eachManager() {
		if otherName == name || other.Container == nil || other.terminated || !other.authOwned {
			continue
		}
		if otherGID, present := managerMapGID(status, otherName); !present || otherGID == 0 {
			continue
		}
		state, err := other.State(ctx)
		if err != nil {
			return fmt.Errorf("inspect replacement manager %s: %w", otherName, err)
		}
		if state != nil && state.Running && !state.Paused && !state.Restarting {
			candidates[otherName] = true
		}
	}
	if len(candidates) == 0 {
		return errors.New("no other running, registered owned manager; add or restart a replacement first")
	}
	// A failed manager that is still running can immediately beacon back into
	// the map. Delete its container first, then remove any remaining map entry.
	if mgr.Container != nil && !mgr.terminated {
		if err := mgr.Terminate(ctx); !onlyMissingHostResource(err) {
			return fmt.Errorf("terminate manager %s: %w", name, err)
		}
	}
	mgr.terminated = true
	if registered {
		// Ceph interprets a numeric argument as a daemon GID, even when a
		// daemon's name consists entirely of digits. Use the observed GID.
		if _, err := t.conn.ceph(ctx, "mgr", "fail", strconv.FormatUint(gid, 10)); err != nil {
			return fmt.Errorf("remove manager %s from the manager map: %w", name, err)
		}
	}
	if err := clusterutil.Poll(ctx, func() (bool, error) {
		status, err := readManagerStatus(ctx, t.conn.ceph)
		_, present := managerMapGID(status, name)
		return !present && status.Available && candidates[status.ActiveName], err
	}); err != nil {
		return fmt.Errorf("wait for manager replacement after removing %s: %w", name, err)
	}
	if _, err := (authStore{t.conn}).clientCommand(ctx, "delete manager identity", "auth", "del", "mgr."+name); err != nil {
		return err
	}
	t.roster.dropManager(name)
	// The removal is complete; a replacement that advertises an unreachable
	// address is reported, not undone.
	return t.checkAddresses(ctx)
}

func managerMapGID(status ManagerStatus, name string) (uint64, bool) {
	if status.ActiveName == name {
		return status.ActiveGID, true
	}
	for _, standby := range status.Standbys {
		if standby.Name == name {
			return standby.GID, true
		}
	}
	return 0, false
}

// ManagerStatus reports active ownership and registered standbys.
type ManagerStatus struct {
	Available  bool   `json:"available"`
	ActiveName string `json:"active_name"`
	ActiveGID  uint64 `json:"active_gid"`
	Standbys   []struct {
		Name string `json:"name"`
		GID  uint64 `json:"gid"`
	} `json:"standbys"`
}

// monitorRemovalObserveInterval spaces membership reads while a removal runs.
const monitorRemovalObserveInterval = 500 * time.Millisecond

// removeMonitorMember sends one removal of name to the monitors in hosts. The
// commit renumbers the survivors' ranks, and each drops its sessions, the
// CLI's included, so the reply can be lost and the CLI then waits for its
// deadline. While the command runs, fresh membership reads that no longer list
// name count as the commit and end the wait. The command is never resent.
func (dm daemonMaps) removeMonitorMember(ctx context.Context, hosts, name string) error {
	command, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := dm.conn.ceph(command, "-m", hosts, "mon", "remove", name)
		done <- err
	}()
	ticker := time.NewTicker(monitorRemovalObserveInterval)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-ticker.C:
			status, err := dm.quorum(command)
			if err != nil || len(status.MonMap.Mons) == 0 {
				continue
			}
			member := false
			for _, current := range status.MonMap.Mons {
				member = member || current.Name == name
			}
			if !member {
				cancel()
				// The membership read observed the commit; the canceled
				// command's own result, normally the lost reply, does not change it.
				_ = <-done
				return nil
			}
		}
	}
}
