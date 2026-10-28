package ceph

import (
	"context"
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
	c := &Container{Container: control, settings: options{startupTimeout: time.Second}, runDir: runDirState{volume: &runVolume{name: "run"}},
		monitors:   map[string]*MonitorContainer{},
		managers:   map[string]*ManagerContainer{"a": {Container: control, DaemonName: "a"}, "b": {Container: control, DaemonName: "b", terminated: true}},
		osdHandles: osdRegistry{owned: map[int]*OSDContainer{0: {ID: 0, Container: control}, 1: {ID: 1, Container: control, purged: true}}}}
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
		"terminated MON a":   {func(c *Container) { c.monitorTerminated = true }, "mon.a", []string{"version"}, "not owned"},
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
