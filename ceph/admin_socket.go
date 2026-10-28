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
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

const (
	runVolumeOwnerLabel = "org.testcontainers.ceph.run-owner"
	// adminSocketDir is the run directory that every MON, MGR, OSD and control
	// container shares through the run volume.
	adminSocketDir         = "/var/run/ceph"
	adminSocketStdoutLimit = 16 << 20
	adminSocketStderrLimit = 64 << 10
)

// runVolume is the Docker volume mounted at /var/run/ceph in every MON, MGR,
// OSD and control-plane container of one cluster. Daemons create their admin
// sockets there, so a container with the Ceph CLI can reach the socket of a
// daemon whatever its own image contains.
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

// adminSocket implements the AdminSocket method of the daemon handles.
// handle must be owned by c now; the checks run under the topology lock.
func (c *Container) adminSocket(ctx context.Context, handle daemonHandle, args ...string) ([]byte, error) {
	kind, id := handle.daemonRef()
	daemon := kind + "." + id
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
	closed, shared, owned := c.closed, c.runDir.shared(), handle.ownedBy(c)
	c.mu.Unlock()
	switch {
	case closed:
		return nil, errClusterTerminated
	case !owned:
		return nil, invalid(fmt.Errorf("daemon %s is not owned by this cluster, or this handle is stale", daemon))
	case !shared:
		return nil, errors.New("this cluster has no shared run directory for admin sockets")
	}
	control, err := c.controlContainerContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("select admin socket control: %w", err)
	}
	return c.adminSocketCommand(ctx, control, daemon, args...)
}

// adminSocketCommand runs args on the admin socket of daemon, which the
// caller checked, in control: an error that supervised.Unconfirmed reports
// leaves the command possibly running there.
func (c *Container) adminSocketCommand(ctx context.Context, control testcontainers.Container, daemon string, args ...string) ([]byte, error) {
	socket := adminSocketDir + "/ceph-" + daemon + ".asok"
	result, err := supervised.Command(ctx, control, "admin socket", c.settings.startupTimeout, adminSocketStdoutLimit, adminSocketStderrLimit,
		append([]string{"ceph", "--admin-daemon", socket}, args...)...)
	if err != nil {
		return nil, fmt.Errorf("admin socket of %s: %w", daemon, err)
	}
	if result.ExitCode != 0 {
		return result.Stdout, fmt.Errorf("admin socket of %s: %w", daemon, nativeFailure("ceph", result.ExitCode, nil, result.Stderr))
	}
	return result.Stdout, nil
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
		mon := c.roster.monitor(id)
		return mon != nil && mon.Container != nil
	case "mgr":
		mgr := c.roster.manager(id)
		return mgr != nil && mgr.Container != nil && !mgr.terminated
	case "osd":
		n, err := strconv.Atoi(id)
		if err != nil {
			return false
		}
		osd := c.osdHandles.get(n)
		return osd != nil && osd.Container != nil && !osd.purged
	}
	return false
}

// clearAdminSocket removes a socket file that a removed daemon of the same
// name left in the shared run directory, so the readiness check of a new
// daemon cannot pass on the old file. The caller holds c.mu.
func (c *Container) clearAdminSocket(ctx context.Context, daemon string) error {
	if !c.runDir.published() {
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
