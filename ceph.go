package ceph

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// DefaultImage pins the multi-architecture manifest tested by this PoC.
const DefaultImage = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"

//go:embed internal/scripts/*.sh
var scripts embed.FS

// Container embeds the MON/control container and owns the MGR, OSDs, optional
// RGW/MDS services and network.
// Use Terminate to clean up the entire cluster. Generic Run options customize
// the MON; module options configure the cluster. Do not change MON networking.
type Container struct {
	testcontainers.Container
	mu                sync.Mutex
	settings          options
	network           *testcontainers.DockerNetwork
	manager           testcontainers.Container
	services          map[string]testcontainers.Container
	osds              map[int]*OSDContainer
	config            []byte
	keyring           []byte
	closed            bool
	monitorTerminated bool
	networkRemoved    bool
}

// OSDContainer is one storage daemon backed by a container-local sparse file.
// Stop/Start can be used for failure injection; RemoveOSD drains and purges it.
type OSDContainer struct {
	testcontainers.Container
	ID     int
	purged bool
}

// Run creates one MON, one MGR and a configurable number of OSD containers.
// img supplies MON/MGR and all other roles unless overridden by image options.
// A non-nil Container returned with an error must still be terminated.
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	settings := options{
		osds: 2, blockSize: 1 << 30, startupTimeout: 3 * time.Minute,
		osdImage: img, rgwImage: img, mdsImage: img,
	}
	for _, opt := range opts {
		if opt, ok := opt.(Option); ok {
			if err := opt(&settings); err != nil {
				return nil, fmt.Errorf("configure ceph: %w", err)
			}
		}
	}
	ctx, cancel := context.WithTimeout(ctx, settings.startupTimeout)
	defer cancel()
	nw, err := network.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create ceph network: %w", err)
	}
	c := &Container{settings: settings, network: nw, osds: make(map[int]*OSDContainer), services: make(map[string]testcontainers.Container)}
	moduleOpts := []testcontainers.ContainerCustomizer{
		testcontainers.WithEntrypoint("/bin/sh", "/tc/mon.sh"),
		testcontainers.WithCmd(),
		testcontainers.WithEnv(map[string]string{
			"CEPH_FSID": uuid.NewString(), "CEPH_OSD_BLOCK_SIZE": strconv.FormatInt(settings.blockSize, 10),
		}),
		testcontainers.WithExposedPorts("3300/tcp", "6789/tcp"),
		network.WithNetwork([]string{"ceph-mon"}, nw),
		testcontainers.WithFiles(scriptFile("mon")),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status", "--format", "json"}).WithStartupTimeout(settings.startupTimeout)),
	}
	moduleOpts = append(moduleOpts, opts...)
	mon, err := testcontainers.Run(ctx, img, moduleOpts...)
	if mon != nil {
		c.Container = mon
	}
	if err != nil {
		return c, fmt.Errorf("run ceph monitor: %w", err)
	}
	if c.config, err = readFile(ctx, mon, "/etc/ceph/ceph.conf"); err != nil {
		return c, err
	}
	if c.keyring, err = readFile(ctx, mon, "/etc/ceph/ceph.client.admin.keyring"); err != nil {
		return c, err
	}
	mgrKey, err := c.Ceph(ctx, "auth", "get-or-create", "mgr.a", "mon", "allow profile mgr", "osd", "allow *", "mds", "allow *")
	if err != nil {
		return c, err
	}
	mgr, err := testcontainers.Run(ctx, img,
		c.WithClient(), testcontainers.WithEntrypoint("/bin/sh", "/tc/mgr.sh"), testcontainers.WithCmd(),
		testcontainers.WithFiles(scriptFile("mgr"), textFile("/etc/ceph/mgr.keyring", mgrKey, 0o600)),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status", "--format", "json"}).WithStartupTimeout(settings.startupTimeout)),
	)
	if mgr != nil {
		c.manager = mgr
	}
	if err != nil {
		return c, fmt.Errorf("run ceph manager: %w", err)
	}
	for range settings.osds {
		if _, err := c.AddOSD(ctx); err != nil {
			return c, err
		}
	}
	if err := c.poll(ctx, func() (bool, error) {
		s, err := c.Status(ctx)
		return s.MgrMap.Available, err
	}); err != nil {
		return c, fmt.Errorf("wait for ceph manager: %w", err)
	}
	return c, nil
}

// NetworkName returns the isolated network to which application containers connect.
func (c *Container) NetworkName() string { return c.network.Name }

// WithClient connects another container to Ceph and copies its admin credentials.
// These privileged, ephemeral credentials are for trusted test containers only.
// Caller owns and terminates that container before terminating the cluster.
func (c *Container) WithClient() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		if err := network.WithNetworkName(nil, c.NetworkName())(req); err != nil {
			return err
		}
		return testcontainers.WithFiles(
			textFile("/etc/ceph/ceph.conf", c.config, 0o644),
			textFile("/etc/ceph/ceph.client.admin.keyring", c.keyring, 0o600),
		)(req)
	}
}

// Ceph runs the Ceph CLI in the control container. Arguments are passed directly,
// without shell interpolation. Use --format json for machine-readable responses.
func (c *Container) Ceph(ctx context.Context, args ...string) ([]byte, error) {
	return command(ctx, c.Container, append([]string{"ceph", "--connect-timeout", "5"}, args...)...)
}

// AddOSD registers, formats and starts a new OSD, then waits for it to be up/in.
// Topology operations are serialized. An OSD returned with an error is tracked
// for cleanup and may be inspected; no data is silently purged on failure.
func (c *Container) AddOSD(ctx context.Context) (*OSDContainer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("ceph cluster is terminated")
	}
	for _, osd := range c.osds {
		if osd.purged {
			return nil, fmt.Errorf("finish removing osd.%d before adding another OSD", osd.ID)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	osdUUID := uuid.NewString()
	secret, err := command(ctx, c.Container, "ceph-authtool", "--gen-print-key")
	if err != nil {
		return nil, err
	}
	keys, err := json.Marshal(map[string]string{"cephx_secret": strings.TrimSpace(string(secret))})
	if err != nil {
		return nil, err
	}
	keyPath := "/tmp/osd-" + osdUUID + ".json"
	if err := c.CopyToContainer(ctx, keys, keyPath, 0o600); err != nil {
		return nil, fmt.Errorf("copy osd registration key: %w", err)
	}
	result, err := c.Ceph(ctx, "osd", "new", osdUUID, "-i", keyPath)
	if err != nil {
		return nil, err
	}
	id, err := strconv.Atoi(strings.TrimSpace(string(result)))
	if err != nil {
		return nil, fmt.Errorf("parse osd ID %q: %w", result, err)
	}
	osd := &OSDContainer{ID: id}
	c.osds[id] = osd
	keyring := []byte(fmt.Sprintf("[osd.%d]\n\tkey = %s\n", id, strings.TrimSpace(string(secret))))
	ctr, err := testcontainers.Run(ctx, c.settings.osdImage,
		c.WithClient(), testcontainers.WithEntrypoint("/bin/sh", "/tc/osd.sh"), testcontainers.WithCmd(),
		testcontainers.WithEnv(map[string]string{
			"CEPH_OSD_ID": strconv.Itoa(id), "CEPH_OSD_UUID": osdUUID, "CEPH_OSD_HOST": fmt.Sprintf("osd-%d", id),
		}),
		testcontainers.WithFiles(scriptFile("osd"), textFile("/etc/ceph/osd.keyring", keyring, 0o600)),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", fmt.Sprintf("/var/run/ceph/ceph-osd.%d.asok", id)}).WithStartupTimeout(c.settings.startupTimeout)),
	)
	if ctr != nil {
		osd.Container = ctr
	}
	if err != nil {
		return osd, fmt.Errorf("run ceph osd.%d: %w", id, err)
	}
	if err := c.waitOSD(ctx, id, true); err != nil {
		return osd, fmt.Errorf("wait for osd.%d up/in: %w", id, err)
	}
	return osd, nil
}

// RemoveOSD drains an owned OSD, waits for safe-to-destroy, stops it, waits for
// down, purges it from Ceph and removes its container. It refuses the last OSD.
// On timeout the OSD remains tracked; inspect the cluster and retry or terminate.
func (c *Container) RemoveOSD(ctx context.Context, id int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	osd, ok := c.osds[id]
	if !ok {
		return fmt.Errorf("osd.%d is not owned by this cluster", id)
	}
	if len(c.osds) <= 1 && !osd.purged {
		return errors.New("cannot remove the last OSD; terminate the cluster instead")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	name := fmt.Sprintf("osd.%d", id)
	if !osd.purged {
		// Reweight avoids the small-cluster active+remapped case documented by Ceph.
		if _, err := c.Ceph(ctx, "osd", "crush", "reweight", name, "0"); err != nil {
			return err
		}
		if _, err := c.Ceph(ctx, "osd", "out", strconv.Itoa(id)); err != nil {
			return err
		}
		if err := c.poll(ctx, func() (bool, error) {
			_, err := c.Ceph(ctx, "osd", "safe-to-destroy", strconv.Itoa(id))
			return err == nil, err
		}); err != nil {
			return fmt.Errorf("drain %s: %w", name, err)
		}
		if osd.Container != nil {
			stopTimeout := 10 * time.Second
			if err := osd.Stop(ctx, &stopTimeout); err != nil {
				return fmt.Errorf("stop %s: %w", name, err)
			}
		}
		if err := c.waitOSD(ctx, id, false); err != nil {
			return fmt.Errorf("wait for %s down: %w", name, err)
		}
		if _, err := c.Ceph(ctx, "osd", "purge", strconv.Itoa(id), "--yes-i-really-mean-it"); err != nil {
			return err
		}
		osd.purged = true
	}
	if osd.Container != nil {
		if err := osd.Terminate(ctx); err != nil {
			return fmt.Errorf("terminate %s: %w", name, err)
		}
	}
	delete(c.osds, id)
	return nil
}

// OSDs returns a snapshot of owned OSD containers sorted by ID.
func (c *Container) OSDs() []*OSDContainer {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]*OSDContainer, 0, len(c.osds))
	for _, osd := range c.osds {
		result = append(result, osd)
	}
	slices.SortFunc(result, func(a, b *OSDContainer) int { return a.ID - b.ID })
	return result
}

// ServiceContainers returns owned optional services (such as RGW or MDS),
// sorted by service name. The cluster owns their cleanup.
func (c *Container) ServiceContainers() []testcontainers.Container {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.services))
	for name := range c.services {
		names = append(names, name)
	}
	slices.Sort(names)
	result := make([]testcontainers.Container, 0, len(names))
	for _, name := range names {
		result = append(result, c.services[name])
	}
	return result
}

// startService registers partial failures too, so callers can always terminate
// the cluster after a failed RGW/MDS bootstrap.
func (c *Container) startService(ctx context.Context, name, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("ceph cluster is terminated")
	}
	if _, exists := c.services[name]; exists {
		return nil, fmt.Errorf("ceph service %s already started", name)
	}
	if len(c.config) == 0 || len(c.keyring) == 0 {
		return nil, errors.New("ceph cluster bootstrap is incomplete")
	}
	ctx, cancel := context.WithTimeout(ctx, c.settings.startupTimeout)
	defer cancel()
	moduleOpts := append([]testcontainers.ContainerCustomizer{c.WithClient()}, opts...)
	ctr, err := testcontainers.Run(ctx, image, moduleOpts...)
	if ctr != nil {
		c.services[name] = ctr
	}
	if err != nil {
		return ctr, fmt.Errorf("run ceph %s: %w", name, err)
	}
	return ctr, nil
}

// Terminate removes all owned daemons before removing the isolated network.
// It also works on a partially initialized cluster returned by Run.
func (c *Container) Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var errs []error
	// Stop gateways and metadata servers while their backing OSDs are alive.
	for name, ctr := range c.services {
		if err := ctr.Terminate(ctx, opts...); err != nil {
			errs = append(errs, fmt.Errorf("terminate service %s: %w", name, err))
		} else {
			delete(c.services, name)
		}
	}
	for id, osd := range c.osds {
		if osd.Container != nil {
			if err := osd.Terminate(ctx, opts...); err != nil {
				errs = append(errs, err)
				continue
			}
		}
		delete(c.osds, id)
	}
	if c.manager != nil {
		if err := c.manager.Terminate(ctx, opts...); err != nil {
			errs = append(errs, err)
		} else {
			c.manager = nil
		}
	}
	if c.Container != nil && !c.monitorTerminated {
		if err := c.Container.Terminate(ctx, opts...); err != nil {
			errs = append(errs, err)
		} else {
			c.monitorTerminated = true
		}
	}
	if c.network != nil && !c.networkRemoved {
		if err := c.network.Remove(ctx); err != nil {
			errs = append(errs, err)
		} else {
			c.networkRemoved = true
		}
	}
	return errors.Join(errs...)
}

func (c *Container) waitOSD(ctx context.Context, id int, up bool) error {
	return c.poll(ctx, func() (bool, error) {
		result, err := c.Ceph(ctx, "osd", "dump", "--format", "json")
		if err != nil {
			return false, err
		}
		var dump struct {
			OSDs []struct {
				ID int `json:"osd"`
				Up int `json:"up"`
				In int `json:"in"`
			} `json:"osds"`
		}
		if err := json.Unmarshal(result, &dump); err != nil {
			return false, err
		}
		for _, osd := range dump.OSDs {
			if osd.ID == id {
				return (up && osd.Up == 1 && osd.In == 1) || (!up && osd.Up == 0), nil
			}
		}
		return !up, nil
	})
}

func (c *Container) poll(ctx context.Context, check func() (bool, error)) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		if ok, err := check(); ok && err == nil {
			return nil
		} else if err != nil {
			last = err
		}
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), last)
		case <-ticker.C:
		}
	}
}

func scriptFile(name string) testcontainers.ContainerFile {
	data, err := scripts.ReadFile("internal/scripts/" + name + ".sh")
	if err != nil {
		panic(err)
	} // Only embedded, compile-time-known paths are used.
	return textFile("/tc/"+name+".sh", data, 0o755)
}

func textFile(path string, data []byte, mode int64) testcontainers.ContainerFile {
	return testcontainers.ContainerFile{Reader: bytes.NewReader(data), ContainerFilePath: path, FileMode: mode}
}

func readFile(ctx context.Context, ctr testcontainers.Container, path string) ([]byte, error) {
	r, err := ctr.CopyFileFromContainer(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

func command(ctx context.Context, ctr testcontainers.Container, args ...string) ([]byte, error) {
	if ctr == nil {
		return nil, errors.New("ceph control container is unavailable")
	}
	exitCode, reader, err := ctr.Exec(ctx, args)
	if err != nil {
		return nil, fmt.Errorf("exec %s: %w", args[0], err)
	}
	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, reader); err != nil {
		return nil, fmt.Errorf("read %s output: %w", args[0], err)
	}
	if exitCode != 0 {
		return stdout.Bytes(), fmt.Errorf("%s exited %d: %s", args[0], exitCode, strings.TrimSpace(stdout.String()+stderr.String()))
	}
	return stdout.Bytes(), nil
}
