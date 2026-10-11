package ceph

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/volume"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
)

const (
	runVolumeOwnerLabel = "org.testcontainers.ceph.run-owner"
	// adminSocketDir is the run directory that every MON, MGR, OSD and control
	// container shares through the run volume.
	adminSocketDir         = "/var/run/ceph"
	adminSocketStdoutLimit = 16 << 20
	adminSocketStderrLimit = 64 << 10
	adminSocketErrorLimit  = 512
)

// runVolume is the Docker volume mounted at /var/run/ceph in every MON, MGR,
// OSD and control container of one cluster. Daemons create their admin
// sockets there, so a container with the Ceph CLI can reach the socket of a
// daemon whose own image has no CLI, such as an OSD role image.
type runVolume struct {
	name      string
	owner     string
	removed   bool
	newClient func(context.Context) (osdMemoryVolumeClient, error)
}

func newRunVolume() *runVolume {
	owner := uuid.NewString()
	return &runVolume{name: "tc-ceph-run-" + owner, owner: owner}
}

func (v *runVolume) client(ctx context.Context) (osdMemoryVolumeClient, error) {
	if v.newClient != nil {
		return v.newClient(ctx)
	}
	return testcontainers.NewDockerClientWithOpts(ctx)
}

func (v *runVolume) create(ctx context.Context) (err error) {
	docker, err := v.client(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	labels := testcontainers.GenericLabels()
	labels[runVolumeOwnerLabel] = v.owner
	created, err := docker.VolumeCreate(ctx, mobycl.VolumeCreateOptions{Name: v.name, Driver: "local", Labels: labels})
	if err != nil {
		return fmt.Errorf("create Ceph run volume: %w", err)
	}
	return v.validate(created.Volume)
}

func (v *runVolume) validate(created volume.Volume) error {
	if created.Name != v.name || created.Driver != "local" || created.Labels[runVolumeOwnerLabel] != v.owner {
		return errors.New("ceph run volume identity or ownership differs")
	}
	return nil
}

// mount shares the run volume as the container's /var/run/ceph.
func (v *runVolume) mount() testcontainers.CustomizeRequestOption {
	return testcontainers.WithMounts(testcontainers.ContainerMount{
		Source: testcontainers.DockerVolumeMountSource{Name: v.name, VolumeOptions: &mount.VolumeOptions{
			Labels: maps.Clone(testcontainers.GenericLabels()),
		}},
		Target: testcontainers.ContainerMountTarget(adminSocketDir),
	})
}

// remove deletes the volume once no container uses it. A volume that is
// already gone counts as removed.
func (v *runVolume) remove(ctx context.Context) (err error) {
	if v.removed {
		return nil
	}
	docker, err := v.client(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, docker.Close()) }()
	inspected, err := docker.VolumeInspect(ctx, v.name, mobycl.VolumeInspectOptions{})
	if errdefs.IsNotFound(err) {
		v.removed = true
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect Ceph run volume: %w", err)
	}
	if err := v.validate(inspected.Volume); err != nil {
		return err
	}
	if _, err := docker.VolumeRemove(ctx, v.name, mobycl.VolumeRemoveOptions{}); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("remove Ceph run volume: %w", err)
	}
	v.removed = true
	return nil
}

// runMount returns the run volume mount for a new daemon or control
// container, or nothing for a cluster value without a run volume.
func (c *Container) runMount() []testcontainers.ContainerCustomizer {
	if c.run == nil {
		return nil
	}
	return []testcontainers.ContainerCustomizer{c.run.mount()}
}

// AdminSocket runs a command on the admin socket of one MON, MGR or OSD that
// this cluster owns and returns the command's output. daemon is the Ceph
// daemon name, such as "mon.a", "mgr.b" or "osd.0", and args are the command
// words, such as "status", "version" or "config", "get", "osd_heartbeat_grace";
// "help" lists the daemon's commands.
//
// The command does not use the Messenger or Cephx: it reaches the daemon
// through its local socket, so it works while the daemon cannot reach a
// monitor and reports what the daemon itself has applied, for example the
// newest OSDMap epoch in an OSD's "status". It runs in the control container,
// which shares the daemons' run directory, so it also works with OSD images
// that have no Ceph CLI. The daemon must be running; a stopped or removed
// daemon returns an error. A paused daemon accepts the connection but does not
// answer, so the call waits for ctx or the cluster's startup timeout, which
// also bounds every call. When the call ends early, its CLI process in the
// control container is killed before AdminSocket returns. A nonzero exit
// returns the output with an error.
func (c *Container) AdminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error) {
	kind, id, err := parseAdminSocketDaemon(daemon)
	if err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return nil, errors.New("admin socket command is empty")
	}
	for _, arg := range args {
		if arg == "" || strings.ContainsRune(arg, 0) {
			return nil, errors.New("admin socket command words must be non-empty and contain no NUL")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	if err := c.lockTopology(ctx); err != nil {
		return nil, err
	}
	closed, shared, owned := c.closed, c.run != nil && !c.run.removed, c.ownsDaemon(kind, id)
	c.mu.Unlock()
	switch {
	case closed:
		return nil, errors.New("ceph cluster is terminated")
	case !shared:
		return nil, errors.New("this cluster has no shared run directory for admin sockets")
	case !owned:
		return nil, fmt.Errorf("daemon %s is not owned by this cluster", daemon)
	}
	control, err := c.ControlContainerContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("select admin socket control: %w", err)
	}
	socket := adminSocketDir + "/ceph-" + daemon + ".asok"
	result, err := supervisedCommand(ctx, control, "admin socket", c.settings.startupTimeout, adminSocketStdoutLimit, adminSocketStderrLimit,
		append([]string{"ceph", "--admin-daemon", socket}, args...)...)
	if err != nil {
		return nil, fmt.Errorf("admin socket of %s: %w", daemon, err)
	}
	if result.exitCode != 0 {
		message := strings.TrimSpace(string(result.stderr))
		if len(message) > adminSocketErrorLimit {
			message = message[:adminSocketErrorLimit]
		}
		return result.stdout, fmt.Errorf("admin socket of %s exited %d: %s", daemon, result.exitCode, message)
	}
	return result.stdout, nil
}

func parseAdminSocketDaemon(daemon string) (kind, id string, err error) {
	kind, id, ok := strings.Cut(daemon, ".")
	if !ok || !naming.DaemonName.MatchString(id) || len(id) > 64 {
		return "", "", fmt.Errorf("admin socket daemon %q must be mon.<name>, mgr.<name> or osd.<id>", daemon)
	}
	switch kind {
	case "mon", "mgr":
		return kind, id, nil
	case "osd":
		if n, err := strconv.Atoi(id); err == nil && n >= 0 && strconv.Itoa(n) == id {
			return kind, id, nil
		}
	}
	return "", "", fmt.Errorf("admin socket daemon %q must be mon.<name>, mgr.<name> or osd.<id>", daemon)
}

// ownsDaemon reports a running-capable daemon descriptor that this cluster
// owns. The caller holds c.mu.
func (c *Container) ownsDaemon(kind, id string) bool {
	switch kind {
	case "mon":
		if id == "a" && c.Container != nil && !c.monitorTerminated {
			return true
		}
		mon := c.monitors[id]
		return mon != nil && mon.Container != nil
	case "mgr":
		mgr := c.managers[id]
		return mgr != nil && mgr.Container != nil && !mgr.terminated
	case "osd":
		n, err := strconv.Atoi(id)
		if err != nil {
			return false
		}
		osd := c.osds[n]
		return osd != nil && osd.Container != nil && !osd.purged
	}
	return false
}

// clearAdminSocket removes a socket file that a removed daemon of the same
// name left in the shared run directory, so the readiness check of a new
// daemon cannot pass on the old file. The caller holds c.mu.
func (c *Container) clearAdminSocket(ctx context.Context, daemon string) error {
	if c.run == nil {
		return nil
	}
	control := c.cliContainer()
	if control == nil {
		return errors.New("ceph control container is unavailable")
	}
	if _, err := command(ctx, control, "rm", "-f", adminSocketDir+"/ceph-"+daemon+".asok"); err != nil {
		return fmt.Errorf("clear admin socket of %s: %w", daemon, err)
	}
	return nil
}
