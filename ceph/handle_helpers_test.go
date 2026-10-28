package ceph

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

// The helpers below return the descriptor that the cluster fixture holds for
// an ID or a name, so that a unit test can drive an operation that is a
// method of the handle. When the fixture holds none, they return a descriptor
// that the cluster does not own, so that the operation reports the same
// "not owned" refusal as a stale handle does. A test that holds the topology
// lock on purpose reads the fixture without it.

func osdHandle(t testing.TB, _ context.Context, c *Container, id int) *OSDContainer {
	t.Helper()
	if c.mu.TryLock() {
		defer c.mu.Unlock()
	}
	if osd := c.osdHandles.get(id); osd != nil {
		if osd.owner == nil {
			osd.owner = c
		}
		return osd
	}
	return &OSDContainer{ID: id, owner: c}
}

func monitorHandle(t testing.TB, _ context.Context, c *Container, name string) *MonitorContainer {
	t.Helper()
	if c.mu.TryLock() {
		defer c.mu.Unlock()
	}
	if mon := c.roster.monitor(name); mon != nil {
		if mon.owner == nil {
			mon.owner = c
		}
		return mon
	}
	if name == "a" && c.Container != nil && !c.monitorTerminated {
		return &MonitorContainer{Container: c.Container, DaemonName: "a", owner: c}
	}
	return &MonitorContainer{DaemonName: name, owner: c}
}

func managerHandle(t testing.TB, _ context.Context, c *Container, name string) *ManagerContainer {
	t.Helper()
	if c.mu.TryLock() {
		defer c.mu.Unlock()
	}
	if mgr := c.roster.manager(name); mgr != nil {
		if mgr.owner == nil {
			mgr.owner = c
		}
		return mgr
	}
	return &ManagerContainer{DaemonName: name, owner: c}
}

// poolHandle returns a handle that identifies the pool by name alone: its
// native ID binds on the first operation, as a pool from a failed CreatePool
// does.
func poolHandle(t testing.TB, _ context.Context, c *Container, name string) *Pool {
	t.Helper()
	return &Pool{PoolConfig: PoolConfig{Name: name}, owner: c, ident: &poolIdentity{}}
}

// adminSocketOf runs an admin socket command through the handle that the
// fixture holds for the daemon name, such as "osd.0" or "mon.a".
func adminSocketOf(t testing.TB, c *Container, daemon string, args ...string) ([]byte, error) {
	t.Helper()
	ctx := t.Context()
	kind, id, _ := strings.Cut(daemon, ".")
	switch kind {
	case "mon":
		return monitorHandle(t, ctx, c, id).AdminSocket(ctx, args...)
	case "mgr":
		return managerHandle(t, ctx, c, id).AdminSocket(ctx, args...)
	case "osd":
		n, err := strconv.Atoi(id)
		if err != nil {
			t.Fatalf("daemon %q is not an osd.<id>", daemon)
		}
		return osdHandle(t, ctx, c, n).AdminSocket(ctx, args...)
	}
	t.Fatalf("daemon %q is not a mon.<name>, mgr.<name> or osd.<id>", daemon)
	return nil, nil
}

// rotateKeyOf rotates the key through the handle that the fixture holds for
// the MGR or OSD that daemon names.
func rotateKeyOf(t testing.TB, c *Container, daemon string) error {
	t.Helper()
	ctx := t.Context()
	kind, id, _ := strings.Cut(daemon, ".")
	switch kind {
	case "mgr":
		return managerHandle(t, ctx, c, id).RotateKey(ctx)
	case "osd":
		n, err := strconv.Atoi(id)
		if err != nil {
			t.Fatalf("daemon %q is not an osd.<id>", daemon)
		}
		return osdHandle(t, ctx, c, n).RotateKey(ctx)
	}
	t.Fatalf("daemon %q is not a mgr.<name> or osd.<id>", daemon)
	return nil
}
