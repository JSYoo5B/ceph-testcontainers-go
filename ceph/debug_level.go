package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// debugLevelRefusal is a set that was not issued because the target was not
// the window's running generation; changed marks a terminal mismatch.
type debugLevelRefusal struct {
	changed bool
	err     error
}

func (e *debugLevelRefusal) Error() string { return e.err.Error() }

func (e *debugLevelRefusal) Unwrap() error { return e.err }

// Changed reports a terminal mismatch. The override package reads it through
// an interface, because it cannot name this type.
func (e *debugLevelRefusal) Changed() bool { return e.changed }

type debugLevelGeneration struct {
	// owned is false when the cluster no longer owns the same daemon
	// descriptor, or has terminated.
	owned bool
	// identity is the container ID and start time, followed for an MDS or
	// MGR by its process incarnation; see debugTarget.
	identity        string
	running, paused bool
}

// checkDebugLevelGeneration compares a reported generation with identity.
// A paused daemon cannot report its process incarnation, so only its
// container part is compared, and the full comparison waits for a later
// check.
func checkDebugLevelGeneration(gen debugLevelGeneration, err error, identity, daemon string) (changed bool, _ error) {
	containerPart := func(identity string) string {
		part, _, _ := strings.Cut(identity, "/")
		return part
	}
	switch {
	case err != nil:
		return false, fmt.Errorf("inspect %s: %w", daemon, err)
	case !gen.owned:
		return true, fmt.Errorf("%s is no longer owned by this cluster", daemon)
	case gen.identity == "" || containerPart(gen.identity) != containerPart(identity):
		return true, fmt.Errorf("%s runs in another container generation", daemon)
	case !gen.running:
		return true, fmt.Errorf("%s is stopped; its runtime level is gone", daemon)
	case gen.paused:
		return false, fmt.Errorf("%s is paused", daemon)
	case gen.identity != identity:
		return true, fmt.Errorf("%s runs another daemon process: its native GID or admin socket changed", daemon)
	}
	return false, nil
}

// debugLevelRegistry admits one window per daemon and option. The guards in
// fault_registry_state_test.go pin its leaf contract. It has its own
// leaf lock on purpose: releasing a window must not wait behind a long
// topology operation that holds the cluster mutex. The owner of a slot is
// opaque; the override package passes its window state.
type debugLevelRegistry struct {
	mu     sync.Mutex
	active map[string]any
}

// Reserve admits the window owner for key, or refuses an open window.
func (r *debugLevelRegistry) Reserve(key string, owner any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[key] != nil {
		return invalid(errors.New("a debug level window for this daemon and option is open; restore it first"))
	}
	if r.active == nil {
		r.active = make(map[string]any)
	}
	r.active[key] = owner
	return nil
}

// Release frees key while owner still holds it.
func (r *debugLevelRegistry) Release(key string, owner any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active[key] == owner {
		delete(r.active, key)
	}
}

// openDebugTarget checks that daemon is an OSD, MGR or MDS that this cluster
// owns and can run, and returns the target of its admin socket. It waits for
// the topology lock under ctx and releases it before it returns.
func (c *Container) openDebugTarget(ctx context.Context, daemon string) (*debugTarget, error) {
	kind, name, err := parseDebugDaemon(daemon)
	if err != nil {
		return nil, invalid(err)
	}
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	t := &debugTarget{c: c, kind: kind, name: name, daemon: daemon}
	switch kind {
	case "osd":
		n, _ := strconv.Atoi(name)
		t.osd = c.fleet.osdHandles.get(n)
	case "mgr":
		t.mgr = c.members.roster.manager(name)
	case "mds":
		t.mds = c.members.roster.service(daemon)
	}
	closed, owns := c.closed, t.ownedLocked()
	c.mu.Unlock()
	switch {
	case closed:
		return nil, errClusterTerminated
	case !owns:
		return nil, invalid(fmt.Errorf("daemon %s is not a running-capable OSD, MGR or MDS owned by this cluster", daemon))
	}
	return t, nil
}

// parseDebugDaemon accepts osd.<id>, mgr.<name> and mds.<name>.
func parseDebugDaemon(daemon string) (kind, name string, err error) {
	refusal := fmt.Errorf("debug level daemon %q must be an OSD, MGR or MDS such as osd.0, mgr.a or mds.<name>", daemon)
	kind, name, _ = strings.Cut(daemon, ".")
	switch kind {
	case "osd", "mgr":
		if k, id, err := parseAdminSocketDaemon(daemon); err == nil && k == kind {
			return kind, id, nil
		}
	case "mds":
		if naming.DaemonName.MatchString(name) && len(name) <= 64 {
			return kind, name, nil
		}
	}
	return "", "", refusal
}

// debugTarget reaches the admin socket of one OSD, MGR or MDS descriptor. An
// OSD's and a MGR's socket is reached from the control container through
// the shared run directory; an MDS's socket is reached in the MDS's own
// container, which does not mount it.
//
// A window is bound to the daemon's incarnation: the container generation
// and, for an MDS or MGR, the daemon process inside it. Both respawn in place
// (ceph mds fail, ceph mgr fail, a changed set of MGR modules), which keeps
// the container but drops every runtime value. An MDS reports its native GID
// on its own admin socket. A MGR does not, so its incarnation is its GID in
// the MgrMap together with the inode and change time of its admin socket,
// which a respawned process binds anew before the MgrMap shows it.
type debugTarget struct {
	c      *Container
	kind   string
	name   string
	daemon string
	osd    *OSDContainer
	mgr    *ManagerContainer
	mds    testcontainers.Container
}

// ownedLocked reports whether the cluster still owns the descriptor that the
// target was opened on. The caller holds c.mu.
func (t *debugTarget) ownedLocked() bool {
	switch t.kind {
	case "osd":
		return t.osd.ownedBy(t.c)
	case "mgr":
		return t.mgr.ownedBy(t.c)
	case "mds":
		return t.mds != nil && t.mds.GetContainerID() != "" && t.c.members.roster.service(t.daemon) == t.mds
	}
	return false
}

// container is the daemon's own container.
func (t *debugTarget) container() testcontainers.Container {
	switch t.kind {
	case "osd":
		return t.osd.Container
	case "mgr":
		return t.mgr.Container
	}
	return t.mds
}

// execTarget selects the container that runs the target's admin socket
// commands. The caller holds c.mu.
func (t *debugTarget) execTarget(ctx context.Context) (testcontainers.Container, error) {
	if t.kind == "mds" {
		return t.mds, nil
	}
	if !t.c.runDir.shared() {
		return nil, errors.New("this cluster has no shared run directory for admin sockets")
	}
	control, err := t.c.conn().controlContainer(ctx)
	if err != nil {
		return nil, fmt.Errorf("select admin socket control: %w", err)
	}
	return control, nil
}

func (t *debugTarget) generation(ctx context.Context) (debugLevelGeneration, error) {
	if err := t.c.lockTopology(ctx); err != nil {
		return debugLevelGeneration{}, err
	}
	defer t.c.mu.Unlock()
	return t.generationLocked(ctx)
}

// generationLocked is generation for a caller that holds c.mu. The
// incarnation of a stopped or paused MDS or MGR is not read: it cannot
// answer.
func (t *debugTarget) generationLocked(ctx context.Context) (debugLevelGeneration, error) {
	c := t.c
	if c.closed || !t.ownedLocked() {
		return debugLevelGeneration{}, nil
	}
	ctr := t.container()
	state, err := ctr.State(ctx)
	if err != nil {
		return debugLevelGeneration{}, err
	}
	gen := debugLevelGeneration{owned: true, identity: daemonIdentity(ctr, state), running: state.Running, paused: state.Paused}
	if t.kind == "osd" || gen.identity == "" || !gen.running || gen.paused {
		return gen, nil
	}
	incarnation, err := t.incarnationLocked(ctx)
	if err != nil {
		return debugLevelGeneration{}, err
	}
	gen.identity += incarnation
	return gen, nil
}

// incarnationLocked reads the process part of an MDS's or MGR's identity,
// "/gid=<GID>" and, for a MGR, "/asok=<inode>@<change time>". The caller
// holds c.mu.
func (t *debugTarget) incarnationLocked(ctx context.Context) (string, error) {
	c := t.c
	if t.kind == "mds" {
		out, err := c.adminSocketCommand(ctx, t.mds, t.daemon, "status")
		if err != nil {
			return "", err
		}
		var status struct {
			ID *uint64 `json:"id"`
		}
		if err := json.Unmarshal(out, &status); err != nil || status.ID == nil || *status.ID == 0 {
			return "", fmt.Errorf("%s reports no native GID in its status", t.daemon)
		}
		return "/gid=" + strconv.FormatUint(*status.ID, 10), nil
	}
	control, err := t.execTarget(ctx)
	if err != nil {
		return "", err
	}
	socket, err := command(ctx, control, "stat", "-c", "%i@%z", adminSocketDir+"/ceph-"+t.daemon+".asok")
	if err != nil {
		return "", fmt.Errorf("stat the admin socket of %s: %w", t.daemon, err)
	}
	dump, err := c.conn().ceph(ctx, "mgr", "dump", "--format", "json")
	if err != nil {
		return "", fmt.Errorf("read the MgrMap: %w", err)
	}
	gid, err := managerGID(dump, t.name)
	if err != nil {
		return "", err
	}
	return "/gid=" + strconv.FormatUint(gid, 10) + "/asok=" + strings.ReplaceAll(strings.TrimSpace(string(socket)), " ", "_"), nil
}

// managerGID finds the GID of the active or standby MGR name in a MgrMap.
func managerGID(dump []byte, name string) (uint64, error) {
	var status ManagerStatus
	if err := json.Unmarshal(dump, &status); err != nil {
		return 0, fmt.Errorf("decode the MgrMap: %w", err)
	}
	if status.ActiveName == name && status.ActiveGID != 0 {
		return status.ActiveGID, nil
	}
	for _, standby := range status.Standbys {
		if standby.Name == name && standby.GID != 0 {
			return standby.GID, nil
		}
	}
	return 0, fmt.Errorf("mgr.%s is not in the MgrMap", name)
}

// query runs one read on the target's admin socket.
func (t *debugTarget) query(ctx context.Context, args ...string) ([]byte, error) {
	switch t.kind {
	case "osd":
		return t.c.adminSocket(ctx, t.osd, args...)
	case "mgr":
		return t.c.adminSocket(ctx, t.mgr, args...)
	}
	c := t.c
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	closed, owned := c.closed, t.ownedLocked()
	c.mu.Unlock()
	switch {
	case closed:
		return nil, errClusterTerminated
	case !owned:
		return nil, invalid(fmt.Errorf("daemon %s is not owned by this cluster, or this handle is stale", t.daemon))
	}
	return c.adminSocketCommand(ctx, t.mds, t.daemon, args...)
}

func (t *debugTarget) get(ctx context.Context, name string) (string, error) {
	out, err := t.query(ctx, "config", "get", name)
	if err != nil {
		return "", err
	}
	return decodeDebugLevel(out, name)
}

// set holds c.mu, which the cluster's topology operations hold, from the
// generation check until the command has ended, so that the cluster cannot
// replace the daemon in between. The command is registered as a native
// invocation of the daemon's container instance, as RunNativeCommand's are,
// and one whose end is unconfirmed stays registered until settle confirms it:
// the fence of OSDContainer.Remove and OSDContainer.Detach drains it first or
// refuses, and Terminate stops it.
func (t *debugTarget) set(ctx context.Context, identity, name, value string) (func(context.Context) error, error) {
	c := t.c
	if err := c.lockTopology(ctx); err != nil {
		return nil, &debugLevelRefusal{err: err}
	}
	defer c.mu.Unlock()
	gen, err := t.generationLocked(ctx)
	if changed, err := checkDebugLevelGeneration(gen, err, identity, t.daemon); err != nil {
		return nil, &debugLevelRefusal{changed: changed, err: err}
	}
	exec, err := t.execTarget(ctx)
	if err != nil {
		return nil, &debugLevelRefusal{err: err}
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	// The invocation may run in control but addresses the daemon instance,
	// which its fence names.
	inv := &nativeInvocation{owner: c, target: exec, targetID: t.container().GetContainerID(), label: "admin socket",
		cancel: cancel, stopCommand: supervised.Cancel, done: make(chan struct{})}
	switch c.native.calls.admit(inv, nil) {
	case nativeRefusedClosed:
		return nil, &debugLevelRefusal{changed: true, err: errClusterTerminated}
	case nativeRefusedFenced:
		return nil, &debugLevelRefusal{changed: true, err: fmt.Errorf("%s is fenced for removal", t.daemon)}
	}

	_, err = c.adminSocketCommand(runCtx, exec, t.daemon, "config", "set", name, value)
	marker, unconfirmed := supervised.Unconfirmed(err)
	c.native.calls.finish(inv, marker, unconfirmed)
	close(inv.done)
	if !unconfirmed {
		return nil, err
	}
	return func(ctx context.Context) error {
		if err := inv.stop(ctx); err != nil {
			return err
		}
		c.native.calls.forget(inv)
		return nil
	}, err
}

// Generation reports the target's current incarnation as plain values, for
// the override package.
func (t *debugTarget) Generation(ctx context.Context) (owned bool, identity string, running, paused bool, err error) {
	gen, err := t.generation(ctx)
	return gen.owned, gen.identity, gen.running, gen.paused, err
}

// Get reads the daemon's effective level of option name.
func (t *debugTarget) Get(ctx context.Context, name string) (string, error) {
	return t.get(ctx, name)
}

// Set issues the set of option name only while the daemon runs incarnation
// identity. A refusal reports Changed() and issued nothing.
func (t *debugTarget) Set(ctx context.Context, identity, name, value string) (func(context.Context) error, error) {
	return t.set(ctx, identity, name, value)
}

// decodeDebugLevel reads the reply of config get, {"<name>": "<value>"}.
func decodeDebugLevel(out []byte, name string) (string, error) {
	var reply map[string]string
	if err := json.Unmarshal(out, &reply); err != nil {
		return "", fmt.Errorf("decode config get %s", name)
	}
	value, ok := reply[name]
	if !ok || value == "" {
		return "", fmt.Errorf("config get %s returned no value", name)
	}
	return value, nil
}
