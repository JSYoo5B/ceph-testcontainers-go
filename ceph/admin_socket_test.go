package ceph

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/volume"
	mobycl "github.com/moby/moby/client"
)

func TestAdminSocketDaemonNames(t *testing.T) {
	for _, name := range []string{"mon.a", "mon.mon-1", "mgr.b", "osd.0", "osd.12"} {
		if _, _, err := parseAdminSocketDaemon(name); err != nil {
			t.Errorf("%s rejected: %v", name, err)
		}
	}
	for _, name := range []string{"", "osd", "osd.", "osd.-1", "osd.01", "osd.a", "mds.a", "client.admin", "mon.-a", "mon.a b", "mon.a/b"} {
		if _, _, err := parseAdminSocketDaemon(name); err == nil {
			t.Errorf("%q accepted", name)
		}
	}
}

// adminSocketCluster owns mon.a, mgr.a and osd.0, with osd.1 purged and
// mgr.b terminated. Its control fake records every command it runs.
func adminSocketCluster() (*Container, *poolFixtureContainer) {
	control := &poolFixtureContainer{Output: map[string]string{}}
	c := &Container{
		Container: control,
		settings:  options{startupTimeout: time.Second},
		runDir:    runDirState{volume: &runVolume{name: "run"}},
		fleet:     osdFleetState{osdHandles: osdRegistry{owned: map[int]*OSDContainer{0: {ID: 0, Container: control}, 1: {ID: 1, Container: control, purged: true}}}},
		members:   membershipState{roster: memberRoster{managers: map[string]*ManagerContainer{"a": {Container: control, DaemonName: "a"}, "b": {Container: control, DaemonName: "b", terminated: true}}}},
	}
	return c, control
}

func TestAdminSocketRefusesBeforeRunningCommands(t *testing.T) {
	for name, test := range map[string]struct {
		edit   func(*Container)
		daemon string
		args   []string
		want   string
	}{
		"empty command":      {nil, "osd.0", nil, "empty"},
		"empty word":         {nil, "osd.0", []string{"status", ""}, "non-empty"},
		"NUL in word":        {nil, "osd.0", []string{"config\x00get"}, "NUL"},
		"unknown OSD":        {nil, "osd.7", []string{"status"}, "not owned"},
		"purged OSD":         {nil, "osd.1", []string{"status"}, "not owned"},
		"terminated MGR":     {nil, "mgr.b", []string{"status"}, "not owned"},
		"terminated MON a":   {func(c *Container) { c.members.monitorTerminated = true }, "mon.a", []string{"version"}, "not owned"},
		"closed cluster":     {func(c *Container) { c.closed = true }, "osd.0", []string{"status"}, "terminated"},
		"no run volume":      {func(c *Container) { c.runDir.volume = nil }, "osd.0", []string{"status"}, "shared run directory"},
		"removed run volume": {func(c *Container) { c.runDir.volume.removed = true }, "osd.0", []string{"status"}, "shared run directory"},
	} {
		t.Run(name, func(t *testing.T) {
			c, control := adminSocketCluster()
			if test.edit != nil {
				test.edit(c)
			}
			_, err := adminSocketOf(t, c, test.daemon, test.args...)
			if err == nil || !strings.Contains(err.Error(), test.want) || len(control.Calls) != 0 {
				t.Fatalf("err=%v calls=%v", err, control.Calls)
			}
		})
	}
}

// TestOSDDaemonAccessSharesOneOwnershipRule pins that AdminSocket, RotateKey
// and the debug-level target refuse the same OSD states before any command:
// an OSD whose purge was issued or that Detach fenced is still registered,
// but none of them reaches its daemon.
func TestOSDDaemonAccessSharesOneOwnershipRule(t *testing.T) {
	for name, mark := range map[string]func(*OSDContainer){
		"purged":       func(o *OSDContainer) { o.purged = true },
		"purge issued": func(o *OSDContainer) { o.purgeIssued = true },
		"detaching":    func(o *OSDContainer) { o.detaching = true },
		"no container": func(o *OSDContainer) { o.Container = nil },
	} {
		t.Run(name, func(t *testing.T) {
			c, control := adminSocketCluster()
			osd := osdHandle(t, t.Context(), c, 0)
			mark(osd)
			if err := osd.SetIn(t.Context(), false); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("SetIn = %v", err)
			}
			if _, err := osd.AdminSocket(t.Context(), "status"); err == nil || !strings.Contains(err.Error(), "not owned") {
				t.Errorf("AdminSocket = %v", err)
			}
			if err := osd.RotateKey(t.Context()); err == nil || !strings.Contains(err.Error(), "not owned") {
				t.Errorf("RotateKey = %v", err)
			}
			if _, err := c.openDebugTarget(t.Context(), "osd.0"); err == nil || !strings.Contains(err.Error(), "not a running-capable OSD") {
				t.Errorf("openDebugTarget = %v", err)
			}
			if len(control.Calls) != 0 {
				t.Fatalf("a refused OSD ran commands: %v", control.Calls)
			}
		})
	}
	c, _ := adminSocketCluster()
	target, err := c.openDebugTarget(t.Context(), "osd.0")
	if err != nil {
		t.Fatal(err)
	}
	c.fleet.osdHandles.get(0).detaching = true
	if gen, err := target.generation(t.Context()); err != nil || gen.owned {
		t.Fatalf("debug generation of a detaching OSD = %+v, %v", gen, err)
	}
}

func TestAdminSocketBusyClusterHonorsDeadline(t *testing.T) {
	c, control := adminSocketCluster()
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := osdHandle(t, ctx, c, 0).AdminSocket(ctx, "status"); err == nil || len(control.Calls) != 0 {
		t.Fatalf("busy cluster ran a command: %v %v", err, control.Calls)
	}
}

// runVolumeClient keeps one volume and answers the calls the run volume makes.
type runVolumeClient struct {
	volumes map[string]volume.Volume
}

func (f *runVolumeClient) VolumeCreate(_ context.Context, options mobycl.VolumeCreateOptions) (mobycl.VolumeCreateResult, error) {
	created := volume.Volume{Name: options.Name, Driver: options.Driver, Labels: options.Labels}
	f.volumes[options.Name] = created
	return mobycl.VolumeCreateResult{Volume: created}, nil
}

func (f *runVolumeClient) VolumeInspect(_ context.Context, name string, _ mobycl.VolumeInspectOptions) (mobycl.VolumeInspectResult, error) {
	existing, ok := f.volumes[name]
	if !ok {
		return mobycl.VolumeInspectResult{}, notFoundError{}
	}
	return mobycl.VolumeInspectResult{Volume: existing}, nil
}

func (f *runVolumeClient) VolumeRemove(_ context.Context, name string, _ mobycl.VolumeRemoveOptions) (mobycl.VolumeRemoveResult, error) {
	delete(f.volumes, name)
	return mobycl.VolumeRemoveResult{}, nil
}

func (f *runVolumeClient) Close() error { return nil }

type notFoundError struct{}

func (notFoundError) Error() string { return "no such volume" }
func (notFoundError) NotFound()     {}

func TestRunVolumeRemovesOnlyItsOwnVolume(t *testing.T) {
	docker := &runVolumeClient{volumes: map[string]volume.Volume{}}
	v := newRunVolume()
	v.newClient = func(context.Context) (osdMemoryVolumeClient, error) { return docker, nil }
	if err := v.create(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A volume of the same name that another owner created is not removed.
	foreign := docker.volumes[v.name]
	foreign.Labels = map[string]string{runVolumeOwnerLabel: "someone-else"}
	docker.volumes[v.name] = foreign
	if err := v.remove(t.Context()); err == nil || v.removed {
		t.Fatal("a foreign volume was removed")
	}
	foreign.Labels = map[string]string{runVolumeOwnerLabel: v.owner}
	docker.volumes[v.name] = foreign
	if err := v.remove(t.Context()); err != nil || !v.removed || len(docker.volumes) != 0 {
		t.Fatalf("own volume not removed: %v", err)
	}
	// Removal is idempotent, including after the volume disappeared.
	v.removed = false
	if err := v.remove(t.Context()); err != nil || !v.removed {
		t.Fatalf("missing volume not treated as removed: %v", err)
	}
}
