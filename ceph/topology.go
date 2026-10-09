package ceph

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
		WithIdleEntrypoint(),
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

func (c *Container) QuorumStatus(ctx context.Context) (QuorumStatus, error) {
	control, err := c.ControlContainerContext(ctx)
	if err != nil {
		return QuorumStatus{}, fmt.Errorf("select monitor quorum control: %w", err)
	}
	return queryMonitorQuorum(ctx, control)
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
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
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
			lease, err = reserveHostPorts(ctx, c.settings.controlImage, c.PublicAddress(), c.settings.monitorPortCount(), c.settings.startupTimeout)
			// c.mu is already held, so register ownership without re-locking it.
			if lease != nil {
				c.portLeases = append(c.portLeases, lease)
			}
			if err != nil {
				return mon, err
			}
		}
		config := []byte(strings.ReplaceAll(string(c.config), "mon initial members = a\n", ""))
		monEnv := map[string]string{"CEPH_MON_ID": name}
		if c.settings.messengerMode == MessengerV2Secure {
			monEnv[messengerV2SecureEnvironment] = "true"
		}
		opts := []testcontainers.ContainerCustomizer{c.WithClient(),
			testcontainers.WithEntrypoint("/bin/sh", "/tc/mon-join.sh"), testcontainers.WithCmd(),
			testcontainers.WithEnv(monEnv),
			testcontainers.WithFiles(scriptFile("mon-join"), textFile("/etc/ceph/ceph.conf", config, 0o644),
				textFile("/etc/ceph/mon.keyring", key, 0o600), textFile("/tc/monmap", monmap, 0o600)),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", "/var/run/ceph/ceph-mon." + name + ".asok"}).WithStartupTimeout(c.settings.startupTimeout))}
		if lease != nil {
			portEnv := c.settings.monitorPortEnvironment(lease.Ports)
			portEnv["CEPH_PUBLIC_ADDRESS"] = c.PublicAddress()
			opts = append(opts, testcontainers.WithNoStart(), testcontainers.WithEnv(portEnv))
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
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
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
	var status QuorumStatus
	if err := c.poll(ctx, func() (bool, error) {
		current, err := c.QuorumStatus(ctx)
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
		// A CLI session on the monitor being removed can lose the reply and
		// wait until its deadline, so send the command only to survivors.
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
		if _, err := c.Ceph(ctx, "-m", hosts, "mon", "remove", name); err != nil {
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

// RefreshMonitorConfig copies the live quorum's bootstrap addresses into the
// global mon_host entry of every owned daemon's ceph.conf, including stopped
// daemons, without starting or restarting them. Other entries and comments are
// retained. Caller-owned clients and multicluster daemon configs are not copied.
//
// AddMonitor and RemoveMonitor already refresh these files. Retry this method
// after an AddMonitor that joined native membership but could not copy every
// config; retry RemoveMonitor itself after a partial removal. Successful copies
// and the future-client template are retained on partial failure. Repeating a
// refresh reads each current file and skips copies that are already up to date.
// External config writers must not race this operation: Docker has no file CAS.
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
		return errors.New("ceph cluster is terminated")
	}
	return c.refreshMonitorConfig(ctx)
}

// Unlike Mutex.Lock, waiting for serialized topology work must respect the
// caller's deadline. The caller owns Unlock after this helper succeeds.
func (c *Container) lockTopology(ctx context.Context) error {
	return lockTopologyMutex(ctx, &c.mu)
}

// The topology caller supplies its existing operation deadline. Only native
// reads are polled here; membership is never mutated or daemons restarted. Each
// config is read through Docker's archive API, which also works when stopped.
func (c *Container) refreshMonitorConfigAfterRemoval(ctx context.Context, removedName string) error {
	var status QuorumStatus
	if err := c.poll(ctx, func() (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		current, err := c.QuorumStatus(ctx)
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
	c.configMu.RLock()
	template := bytes.Clone(c.config)
	c.configMu.RUnlock()
	template, err = replaceGlobalMonitorHost(template, addresses)
	if err != nil {
		return fmt.Errorf("refresh future client configuration: %w", err)
	}
	control := c.cliContainer()
	if control == nil {
		return errors.New("ceph control container is unavailable")
	}
	if err := copyMonitorBootstrapConfig(ctx, control, addresses); err != nil {
		return fmt.Errorf("refresh control configuration: %w", err)
	}
	// Publish after the control's current file confirms the new addresses. A
	// later daemon copy failure must not keep new clients on removed monitors.
	c.configMu.Lock()
	c.config = template
	c.configMu.Unlock()
	var errs []error
	for _, target := range c.monitorConfigTargets(control, removedName) {
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
			address := addr.Type + ":" + endpoint
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
func (c *Container) monitorConfigTargets(control testcontainers.Container, removedName string) []monitorConfigTarget {
	var candidates []monitorConfigTarget
	add := func(name string, ctr testcontainers.Container) {
		if ctr != nil {
			candidates = append(candidates, monitorConfigTarget{name: name, container: ctr})
		}
	}
	if !c.monitorTerminated && removedName != "a" {
		add("mon.a", c.Container)
	}
	for name, mon := range c.monitors {
		if mon != nil && name != removedName {
			add("mon."+name, mon.Container)
		}
	}
	for name, mgr := range c.managers {
		if mgr != nil && !mgr.terminated {
			add("mgr."+name, mgr.Container)
		}
	}
	if initial := c.managers["a"]; initial == nil || !initial.terminated {
		add("mgr.a", c.manager)
	}
	for id, osd := range c.osds {
		if osd != nil && !osd.purged {
			add(fmt.Sprintf("osd.%d", id), osd.Container)
		}
	}
	for name, ctr := range c.services {
		add(name, ctr)
	}
	for name, gateway := range c.gateways {
		if gateway != nil {
			add("rgw."+name, gateway.Container)
		}
	}
	for _, fs := range c.filesystems {
		if fs == nil {
			continue
		}
		for _, mds := range fs.mdss {
			if mds != nil {
				add("mds."+mds.ID, mds.Container)
			}
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
	return targets
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
	global, section, globals, hosts := false, false, 0, 0
	for index, original := range lines {
		line := strings.TrimSuffix(original, "\n")
		line = strings.TrimSuffix(line, "\r")
		ending := original[len(line):]
		comment, err := monitorConfigComment(line)
		if err != nil {
			return nil, err
		}
		content := line
		if comment >= 0 {
			content = line[:comment]
		}
		trimmed := strings.TrimSpace(content)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "[") {
			if !strings.HasSuffix(trimmed, "]") || strings.Count(trimmed, "[") != 1 || strings.Count(trimmed, "]") != 1 || strings.TrimSpace(trimmed[1:len(trimmed)-1]) == "" {
				return nil, errors.New("ambiguous configuration section")
			}
			section = true
			global = strings.TrimSpace(trimmed[1:len(trimmed)-1]) == "global"
			if global {
				globals++
				if globals != 1 {
					return nil, errors.New("duplicate global configuration section")
				}
			}
			continue
		}
		key, _, hasValue := strings.Cut(content, "=")
		if !section || !hasValue || strings.TrimSpace(key) == "" || strings.HasPrefix(trimmed, "!") || strings.HasSuffix(trimmed, "\\") {
			return nil, errors.New("ambiguous configuration entry or include/continuation")
		}
		normalized := strings.Join(strings.Fields(strings.NewReplacer("_", " ", "-", " ").Replace(key)), "_")
		if normalized == "include" || normalized == "includedir" {
			return nil, errors.New("configuration include cannot be refreshed unambiguously")
		}
		if !global || normalized != "mon_host" {
			continue
		}
		hosts++
		if hosts != 1 {
			return nil, errors.New("duplicate global mon_host configuration")
		}
		equals := strings.IndexByte(line, '=')
		start := equals + 1
		for start < len(line) && (line[start] == ' ' || line[start] == '\t') {
			start++
		}
		end := len(content)
		for end > start && (line[end-1] == ' ' || line[end-1] == '\t') {
			end--
		}
		lines[index] = line[:start] + addresses + line[end:] + ending
	}
	if globals != 1 || hosts != 1 {
		return nil, errors.New("configuration needs exactly one explicit global mon_host entry")
	}
	return []byte(strings.Join(lines, "")), nil
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
		if char == '\'' || char == '"' {
			quoted = char
		} else if char == '#' || char == ';' {
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
func (c *Container) AddManager(ctx context.Context, name string) (*ManagerContainer, error) {
	if !daemonNamePattern.MatchString(name) {
		return nil, errors.New("invalid manager name")
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
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
	if err := c.lockTopology(ctx); err != nil {
		return err
	}
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
