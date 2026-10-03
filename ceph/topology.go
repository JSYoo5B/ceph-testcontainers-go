package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// MonitorContainer is an owned quorum member. Stop/Start preserves its store;
// RemoveMonitor explicitly removes membership and deletes the container.
type MonitorContainer struct {
	testcontainers.Container
	DaemonName string
}

// ManagerContainer is an owned active or standby manager.
type ManagerContainer struct {
	testcontainers.Container
	DaemonName string
	authOwned  bool
	terminated bool
}

var daemonNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

func daemonName(index int) string {
	if index < 26 {
		return string(rune('a' + index))
	}
	return fmt.Sprintf("node-%d", index)
}

func (c *Container) cliContainer() testcontainers.Container {
	c.controlMu.RLock()
	defer c.controlMu.RUnlock()
	if c.controlPlane != nil {
		return c.controlPlane
	}
	return c.Container
}

// ControlContainer returns the stable CLI container for multi-monitor clusters,
// or the primary MON for the default single-monitor fixture. The cluster owns it.
func (c *Container) ControlContainer() testcontainers.Container { return c.cliContainer() }

// ensureControlPlane is called while topology holds c.mu. Separating CLI from
// MON a keeps topology operations available when that quorum member is stopped.
func (c *Container) ensureControlPlane(ctx context.Context) error {
	c.controlMu.RLock()
	exists := c.controlPlane != nil
	c.controlMu.RUnlock()
	if exists {
		return nil
	}
	ctr, err := testcontainers.Run(ctx, c.settings.controlImage, c.WithClient(),
		testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})))
	if ctr != nil {
		c.controlMu.Lock()
		c.controlPlane = ctr
		c.controlMu.Unlock()
	}
	return err
}

// Monitors returns membership owned by this fixture, sorted by daemon name.
func (c *Container) Monitors() []*MonitorContainer {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]*MonitorContainer, 0, len(c.monitors)+1)
	if c.Container != nil && !c.monitorTerminated {
		result = append(result, &MonitorContainer{Container: c.Container, DaemonName: "a"})
	}
	for _, mon := range c.monitors {
		result = append(result, mon)
	}
	slices.SortFunc(result, func(a, b *MonitorContainer) int { return strings.Compare(a.DaemonName, b.DaemonName) })
	return result
}

// Managers returns active/standby candidates owned by this fixture.
func (c *Container) Managers() []*ManagerContainer {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]*ManagerContainer, 0, len(c.managers))
	for _, mgr := range c.managers {
		result = append(result, mgr)
	}
	slices.SortFunc(result, func(a, b *ManagerContainer) int { return strings.Compare(a.DaemonName, b.DaemonName) })
	return result
}

// QuorumStatus contains the actual quorum and advertised monitor addresses.
type QuorumStatus struct {
	QuorumNames []string `json:"quorum_names"`
	MonMap      struct {
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

func (c *Container) QuorumStatus(ctx context.Context) (QuorumStatus, error) {
	var status QuorumStatus
	data, err := c.Ceph(ctx, "quorum_status", "--format", "json")
	if err == nil {
		err = json.Unmarshal(data, &status)
	}
	return status, err
}

// WaitForQuorum waits for a majority of the current monmap, including fixtures
// with a stopped monitor. It does not require HEALTH_OK or every MON to run.
func (c *Container) WaitForQuorum(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	return c.poll(ctx, func() (bool, error) {
		status, err := c.QuorumStatus(ctx)
		return len(status.MonMap.Mons) > 0 && len(status.QuorumNames) > len(status.MonMap.Mons)/2, err
	})
}

// AddMonitor joins a new named MON from the live quorum's map and shared key.
// Existing daemon data is retained; a partial new member remains owned on error.
func (c *Container) AddMonitor(ctx context.Context, name string) (*MonitorContainer, error) {
	if !daemonNamePattern.MatchString(name) {
		return nil, errors.New("invalid monitor name")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("ceph cluster is terminated")
	}
	if (name == "a" && !c.monitorTerminated) || c.monitors[name] != nil {
		return nil, fmt.Errorf("monitor %s is already owned", name)
	}
	if name == "a" {
		return nil, errors.New("primary monitor name a cannot be reused")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	status, err := c.QuorumStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect monitor membership: %w", err)
	}
	for _, member := range status.MonMap.Mons {
		if member.Name == name {
			return nil, fmt.Errorf("monitor %s is already registered", name)
		}
	}
	if err := c.ensureControlPlane(ctx); err != nil {
		return nil, fmt.Errorf("start independent CLI control: %w", err)
	}
	key, err := c.Ceph(ctx, "auth", "get", "mon.")
	if err != nil {
		return nil, err
	}
	mapPath := "/tmp/tc-monmap-" + uuid.NewString()
	if _, err := c.Ceph(ctx, "mon", "getmap", "-o", mapPath); err != nil {
		return nil, err
	}
	monmap, err := readFile(ctx, c.cliContainer(), mapPath)
	if err != nil {
		return nil, err
	}
	mon := &MonitorContainer{DaemonName: name}
	if c.monitors == nil {
		c.monitors = make(map[string]*MonitorContainer)
	}
	c.monitors[name] = mon
	attempts := 1
	if c.settings.hostNetwork {
		attempts = hostPortAttempts
	}
	for attempt := range attempts {
		var lease *hostPortLease
		if c.settings.hostNetwork {
			lease, err = reserveHostPorts(ctx, c.settings.controlImage, c.PublicAddress(), 2, c.settings.startupTimeout)
			// c.mu is already held, so register ownership without re-locking it.
			if lease != nil {
				c.portLeases = append(c.portLeases, lease)
			}
			if err != nil {
				return mon, err
			}
		}
		config := []byte(strings.ReplaceAll(string(c.config), "mon initial members = a\n", ""))
		opts := []testcontainers.ContainerCustomizer{c.WithClient(),
			testcontainers.WithEntrypoint("/bin/sh", "/tc/mon-join.sh"), testcontainers.WithCmd(),
			testcontainers.WithEnv(map[string]string{"CEPH_MON_ID": name}),
			testcontainers.WithFiles(scriptFile("mon-join"), textFile("/etc/ceph/ceph.conf", config, 0o644),
				textFile("/etc/ceph/mon.keyring", key, 0o600), textFile("/tc/monmap", monmap, 0o600)),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", "/var/run/ceph/ceph-mon." + name + ".asok"}).WithStartupTimeout(c.settings.startupTimeout))}
		if lease != nil {
			opts = append(opts, testcontainers.WithNoStart(), testcontainers.WithEnv(map[string]string{
				"CEPH_PUBLIC_ADDRESS": c.PublicAddress(),
				"CEPH_MON_PORT_V2":    fmt.Sprint(lease.Ports[0]), "CEPH_MON_PORT_V1": fmt.Sprint(lease.Ports[1]),
			}))
		}
		mon.Container, err = testcontainers.Run(ctx, c.settings.controlImage, opts...)
		if lease != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			releaseErr := lease.Release(cleanupCtx)
			cancel()
			if err != nil || releaseErr != nil {
				return mon, errors.Join(err, releaseErr)
			}
			err = mon.Start(ctx)
		}
		if err == nil {
			break
		}
		if lease == nil || attempt+1 == attempts || ctx.Err() != nil {
			return mon, err
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		conflict := isPortConflict(cleanupCtx, mon.Container)
		if conflict {
			err = mon.Terminate(cleanupCtx)
		}
		cancel()
		if !conflict || !onlyMissingHostResource(err) {
			return mon, err
		}
	}
	if err := c.poll(ctx, func() (bool, error) {
		status, err := c.QuorumStatus(ctx)
		return slices.Contains(status.QuorumNames, name), err
	}); err != nil {
		return mon, fmt.Errorf("join monitor %s to quorum: %w", name, err)
	}
	return mon, c.refreshMonitorConfig(ctx)
}

// RemoveMonitor preserves a working quorum; add a replacement first when a
// stop would leave fewer than a majority of the current map running.
func (c *Container) RemoveMonitor(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("ceph cluster is terminated")
	}
	mon := c.monitors[name]
	if name == "a" && !c.monitorTerminated {
		mon = &MonitorContainer{Container: c.Container, DaemonName: "a"}
	}
	if mon == nil {
		return fmt.Errorf("monitor %s is not owned", name)
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	status, err := c.QuorumStatus(ctx)
	if err != nil {
		return err
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
		if _, err := c.Ceph(ctx, "mon", "remove", name); err != nil {
			return err
		}
	}
	// A failed AddMonitor can own a descriptor before any container or map
	// member exists. Membership can also be gone after a previous removal
	// succeeded but Docker cleanup failed. Neither case changes quorum.
	if mon.Container != nil {
		if err := mon.Terminate(ctx); !onlyMissingHostResource(err) {
			return err
		}
	}
	// Removing a MON can elect a new leader after the membership command has
	// committed. Keep cleanup ownership until a fresh native view confirms the
	// remaining quorum and the control client's bootstrap addresses are copied.
	if err := c.refreshMonitorConfigAfterRemoval(ctx, name); err != nil {
		return fmt.Errorf("refresh control configuration after removing monitor %s: %w", name, err)
	}
	if name == "a" {
		c.monitorTerminated = true
	} else {
		delete(c.monitors, name)
	}
	return nil
}

func (c *Container) refreshMonitorConfig(ctx context.Context) error {
	return c.refreshMonitorConfigAfterRemoval(ctx, "")
}

// The topology caller supplies its existing operation deadline. Only native
// reads are retried here; membership mutation and config copying are not.
func (c *Container) refreshMonitorConfigAfterRemoval(ctx context.Context, removedName string) error {
	var status QuorumStatus
	if err := c.poll(ctx, func() (bool, error) {
		current, err := c.QuorumStatus(ctx)
		if err != nil {
			return false, err
		}
		if len(current.MonMap.Mons) == 0 || len(current.QuorumNames) <= len(current.MonMap.Mons)/2 {
			return false, errors.New("monitor membership has no majority quorum")
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
	var endpoints []string
	for _, mon := range status.MonMap.Mons {
		var addresses []string
		for _, addr := range mon.PublicAddrs.Addrvec {
			endpoint, _, _ := strings.Cut(addr.Addr, "/")
			addresses = append(addresses, addr.Type+":"+endpoint)
		}
		endpoints = append(endpoints, "["+strings.Join(addresses, ",")+"]")
	}
	c.configMu.Lock()
	defer c.configMu.Unlock()
	lines := strings.Split(string(c.config), "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "mon host = ") {
			lines[i] = "mon host = " + strings.Join(endpoints, " ")
		}
	}
	config := []byte(strings.Join(lines, "\n"))
	if err := c.cliContainer().CopyToContainer(ctx, config, "/etc/ceph/ceph.conf", 0o644); err != nil {
		return err
	}
	c.config = config
	return nil
}

// AddManager creates a named active/standby candidate and waits for its mgrmap
// registration. Existing daemon identities are rejected; it does not force
// active ownership away from an existing MGR.
func (c *Container) AddManager(ctx context.Context, name string) (*ManagerContainer, error) {
	if !daemonNamePattern.MatchString(name) {
		return nil, errors.New("invalid manager name")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("ceph cluster is terminated")
	}
	if c.managers[name] != nil {
		return nil, fmt.Errorf("manager %s is already owned", name)
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	data, err := c.clientAuthCommand(ctx, "check manager identity", "auth", "ls", "--format", "json")
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
			return nil, fmt.Errorf("manager identity mgr.%s already exists", name)
		}
	}
	status, err := c.ManagerStatus(ctx)
	if err != nil {
		return nil, err
	}
	if _, registered := managerMapGID(status, name); registered {
		return nil, fmt.Errorf("manager %s is already registered", name)
	}
	key, err := c.clientAuthCommand(ctx, "create manager identity", "auth", "get-or-create", "mgr."+name, "mon", "allow profile mgr", "osd", "allow *", "mds", "allow *")
	if err != nil {
		return nil, err
	}
	mgr := &ManagerContainer{DaemonName: name, authOwned: true}
	if c.managers == nil {
		c.managers = make(map[string]*ManagerContainer)
	}
	c.managers[name] = mgr
	mgr.Container, err = testcontainers.Run(ctx, c.settings.controlImage, c.WithClient(),
		testcontainers.WithEntrypoint("/bin/sh", "/tc/mgr.sh"), testcontainers.WithCmd(),
		testcontainers.WithEnv(map[string]string{"CEPH_MGR_ID": name}),
		testcontainers.WithFiles(scriptFile("mgr"), textFile("/etc/ceph/mgr.keyring", key, 0o600)),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", "/var/run/ceph/ceph-mgr." + name + ".asok"}).WithStartupTimeout(c.settings.startupTimeout)))
	if name == "a" {
		c.manager = mgr.Container
	}
	if err != nil {
		return mgr, err
	}
	if err := c.poll(ctx, func() (bool, error) {
		status, err := c.ManagerStatus(ctx)
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
		return mgr, fmt.Errorf("register manager %s: %w", name, err)
	}
	return mgr, nil
}

// RemoveManager deletes an owned manager after another running, registered
// candidate is available. Add a replacement before removing the last candidate.
// On failure the handle remains owned for retry or cluster cleanup. Do not edit
// these manager identities externally while a topology operation is in progress.
func (c *Container) RemoveManager(ctx context.Context, name string) error {
	if !daemonNamePattern.MatchString(name) {
		return errors.New("invalid manager name")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errors.New("ceph cluster is terminated")
	}
	mgr := c.managers[name]
	if mgr == nil || !mgr.authOwned {
		return fmt.Errorf("manager %s is not owned", name)
	}
	if len(c.managers) <= 1 {
		return errors.New("cannot remove the last manager candidate; add a replacement first")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	status, err := c.ManagerStatus(ctx)
	if err != nil {
		return err
	}
	gid, registered := managerMapGID(status, name)
	if registered && gid == 0 {
		return fmt.Errorf("manager %s has no daemon ID in the manager map", name)
	}
	candidates := make(map[string]bool)
	for otherName, other := range c.managers {
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
		if _, err := c.Ceph(ctx, "mgr", "fail", strconv.FormatUint(gid, 10)); err != nil {
			return fmt.Errorf("remove manager %s from the manager map: %w", name, err)
		}
	}
	if err := c.poll(ctx, func() (bool, error) {
		status, err := c.ManagerStatus(ctx)
		_, present := managerMapGID(status, name)
		return !present && status.Available && candidates[status.ActiveName], err
	}); err != nil {
		return fmt.Errorf("wait for manager replacement after removing %s: %w", name, err)
	}
	if _, err := c.clientAuthCommand(ctx, "delete manager identity", "auth", "del", "mgr."+name); err != nil {
		return err
	}
	delete(c.managers, name)
	if name == "a" {
		c.manager = nil
	}
	return nil
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

func (c *Container) ManagerStatus(ctx context.Context) (ManagerStatus, error) {
	var status ManagerStatus
	data, err := c.Ceph(ctx, "mgr", "dump", "--format", "json")
	if err == nil {
		err = json.Unmarshal(data, &status)
	}
	return status, err
}
