//go:build integration && ci_optional && ci_batch_mon_volume_retention

//ci: timeout=40m job-timeout=50 image-roles=essential,client

package integration_test

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

// This is the owner-triggered native scenario of the retained-volume
// experiment (option 1b of #64). It replaces the first MON of a host-network
// cluster with a container that mounts the same Docker volume, keeping its
// address, and writes and reads back exact objects around the replacement.
//
// What it is: the retention mechanism (volume ownership, fence, exact port
// re-bind, destination start without mkfs, native identity and address
// checks) run once on one runner.
//
// What it is not: without CEPH_TEST_UPGRADE_MON_DEST_IMAGE the destination
// image is the cluster's own image, a same-release pair, and the run is not
// upgrade evidence of any kind. With the variable set to another image the
// destination is that image, and the run still proves nothing about a 19.2.5
// to 20.2.4 transition until the source cluster and that image are selected
// and the full MON-step driver, MGR and OSD steps are run. Only MON "a" can be
// replaced: it is the one MON whose container options reach ceph.Run's
// caller. Bridge networking is not implemented.

const (
	upgradeNativePool       = "tc-upgrade-volume"
	upgradeNativeDestEnvVar = "CEPH_TEST_UPGRADE_MON_DEST_IMAGE"
)

// TestMonitorStoreVolumeRetention is optional: it is started by name from a
// pull request comment and is in no default selector.
func TestMonitorStoreVolumeRetention(t *testing.T) {
	for _, mode := range networkModes(t, "host") {
		if !t.Run(mode.Name, monitorStoreVolumeRetention) {
			return
		}
	}
}

func monitorStoreVolumeRetention(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	dest := os.Getenv(upgradeNativeDestEnvVar)
	label := "retention mechanism only, same-release destination, NOT upgrade evidence"
	if dest == "" {
		dest = image
	} else if dest != image {
		label = "retention mechanism with a destination image parameter, NOT upgrade evidence"
	}
	t.Logf("scenario kind: %s (source image %s, destination image %s)", label, image, dest)

	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	eng := &nativeUpgradeEngine{docker: docker, helperImage: image, ctrs: map[string]testcontainers.Container{}}

	runID := uuid.NewString()
	vol, err := newUpgradeStoreVolume(runID)
	if err != nil {
		t.Fatal(err)
	}
	// Registered before the cluster, so it runs after the cluster and every
	// destination are gone. It reports the ledger on every outcome.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Minute)
		defer stop()
		err := vol.release(cleanup, eng)
		t.Logf("volume receipt: %+v", vol.receipt())
		if err != nil {
			t.Errorf("release store volume: %v", err)
		}
	})
	if err := vol.create(ctx, eng); err != nil {
		t.Fatal(err)
	}

	storeDir := upgradeMonDataDir("a")
	opts := append(slices.Clone(roles), ceph.WithHostNetwork(), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(3).WithOSDCount(2)),
		// ceph.Run hands this customizer to the first MON after the module's own options.
		testcontainers.WithMounts(testcontainers.ContainerMount{
			Source: testcontainers.DockerVolumeMountSource{Name: vol.name},
			Target: testcontainers.ContainerMountTarget(storeDir),
		}))
	cluster, _ := newServiceClusterWithOptions(t, image, opts...)
	waitQuorumNames(t, ctx, cluster, "a", "b", "c")

	view := func(ctx context.Context) (upgradeMonView, error) {
		observer, err := ceph.Observe(cluster)
		if err != nil {
			return upgradeMonView{}, err
		}
		status, err := observer.QuorumStatus(ctx)
		if err != nil {
			return upgradeMonView{}, err
		}
		for _, mon := range status.MonMap.Mons {
			if mon.Name != "a" {
				continue
			}
			entries := make([]upgradeMonmapAddr, 0, len(mon.PublicAddrs.Addrvec))
			for _, a := range mon.PublicAddrs.Addrvec {
				entries = append(entries, upgradeMonmapAddr{Type: a.Type, Addr: a.Addr})
			}
			vec, err := upgradeVectorFromMonmap(entries)
			if err != nil {
				return upgradeMonView{}, fmt.Errorf("mon.a address vector: %w", err)
			}
			return upgradeMonView{FSID: status.MonMap.FSID, Addrs: vec, InQuorum: slices.Contains(status.QuorumNames, "a")}, nil
		}
		return upgradeMonView{}, errors.New("mon.a is not in the monmap")
	}
	before, err := view(ctx)
	if err != nil {
		t.Fatal(err)
	}
	epoch := func() int {
		out, err := cluster.Ceph(ctx, "mon", "dump", "--format", "json")
		if err != nil {
			t.Fatalf("mon dump: %v", err)
		}
		dump, err := parseUpgradeMonDump(out)
		if err != nil {
			t.Fatalf("mon dump: %v", err)
		}
		return dump.Epoch
	}
	epochBefore := epoch()

	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: upgradeNativePool, PGNum: 8, Application: "rados"})
	if err != nil || pool == nil {
		t.Fatalf("create pool: %v", err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	client, err := cluster.ClientContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	workload := &nativeUpgradeWorkload{client: client, pool: upgradeNativePool}
	var ledger upgradeWorkloadLedger
	for range 2 {
		if rec := ledger.write(ctx, workload, "baseline"); rec.Outcome != upgradeWriteAcked {
			t.Fatalf("baseline write %s was %s: %v", rec.Object, rec.Outcome, rec.Err)
		}
	}
	if v := ledger.verify(ctx, workload); !v.OK() || len(v.Unknown) != 0 {
		t.Fatalf("baseline readback is not exact: %+v", v)
	}

	eff, err := newUpgradeVolumeMonEffects(upgradeVolumeMonEffectsConfig{
		Engine: eng, Volume: vol, RunID: runID, Source: cluster.GetContainerID(), Name: "a", FSID: before.FSID,
		Retained: before.Addrs, DestImage: dest, View: view,
		StopTimeout: 30 * time.Second, ObserveTimeout: 3 * time.Minute, Poll: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The receipts are emitted on every outcome, before the destination is removed.
	defer func() { t.Logf("effects receipt: %+v", eff.receipt()) }()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Minute)
		defer stop()
		if err := eff.Dispose(cleanup, eff.attempted()); err != nil {
			t.Errorf("dispose destination: %v", err)
		}
	})

	handoff, err := newUpgradeMonHandoff(upgradeMonRetained{Name: "a", FSID: before.FSID, StoreID: vol.identity(), Addrs: before.Addrs})
	if err != nil {
		t.Fatal(err)
	}
	daemon := upgradeDaemonID{Role: "mon", ID: "a"}
	fenced, err := eff.Fence(ctx, daemon)
	if err != nil {
		t.Fatalf("fence: %v", err)
	}
	if present, err := eff.SourcePresent(ctx, daemon); err != nil || present {
		t.Fatalf("source present = %v, %v", present, err)
	}
	hasStore, reserved, err := eff.Retained(ctx, fenced)
	if err != nil {
		t.Fatalf("retained: %v", err)
	}
	const instance = "dst-mon-a"
	plan, err := handoff.admit(upgradeMonLaunch{Transition: instance, Name: "a", FSID: fenced.FSID, StoreID: fenced.StoreID, HasStore: hasStore, Reserved: reserved})
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}
	if err := eff.Launch(ctx, instance, plan); err != nil {
		t.Fatalf("launch (the attempt is observed, not repeated): %v", err)
	}
	ev, err := eff.Observe(ctx, instance)
	if err != nil {
		t.Fatalf("observe: %v", err)
	}
	t.Logf("destination evidence: version %s, FSID %s, store %s", ev.Version, ev.FSID, ev.StoreID)
	if ev.FSID != fenced.FSID || ev.StoreID != fenced.StoreID {
		t.Fatalf("destination evidence %+v differs from the fenced identity %+v", ev, fenced)
	}

	waitQuorumNames(t, ctx, cluster, "a", "b", "c")
	// The epoch is reported, not asserted: Ceph may bump it for reasons other
	// than membership. Membership and the address are asserted by Observe.
	t.Logf("monmap epoch before the replacement %d, after %d", epochBefore, epoch())
	for range 2 {
		rec := ledger.write(ctx, workload, "mixed")
		t.Logf("mixed write %s: %s", rec.Object, rec.Outcome)
	}
	verdict := ledger.verify(ctx, workload)
	t.Logf("workload classes: %v, unknown %v", verdict.Classes, verdict.Unknown)
	if !verdict.OK() {
		t.Fatalf("workload check: violations %v, inconclusive %v", verdict.Violations, verdict.Inconclusive)
	}
	if out, err := cluster.Ceph(ctx, "versions"); err == nil {
		t.Logf("ceph versions after the replacement: %s", strings.TrimSpace(string(out)))
	}
}

// attempted lists the destination instances that were recorded, in order.
func (e *upgradeVolumeMonEffects) attempted() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.order)
}

// ---- native engine -----------------------------------------------------------------

// nativeUpgradeEngine is the Docker implementation of upgradeMonEngine. It is
// compiled by the tag checks and runs only in the owner-triggered batch.
type nativeUpgradeEngine struct {
	docker      *testcontainers.DockerClient
	helperImage string
	mu          sync.Mutex
	ctrs        map[string]testcontainers.Container
}

func (e *nativeUpgradeEngine) VolumeCreate(ctx context.Context, name string, labels map[string]string) (upgradeEngineVolume, error) {
	all := testcontainers.GenericLabels()
	maps.Copy(all, labels)
	res, err := e.docker.VolumeCreate(ctx, mobycl.VolumeCreateOptions{Name: name, Driver: "local", Labels: all})
	if err != nil {
		return upgradeEngineVolume{}, err
	}
	v := res.Volume
	return upgradeEngineVolume{Name: v.Name, Driver: v.Driver, CreatedAt: v.CreatedAt, Labels: v.Labels, Options: v.Options}, nil
}

func (e *nativeUpgradeEngine) VolumeInspect(ctx context.Context, name string) (upgradeEngineVolume, bool, error) {
	res, err := e.docker.VolumeInspect(ctx, name, mobycl.VolumeInspectOptions{})
	if errdefs.IsNotFound(err) {
		return upgradeEngineVolume{}, false, nil
	}
	if err != nil {
		return upgradeEngineVolume{}, false, err
	}
	v := res.Volume
	return upgradeEngineVolume{Name: v.Name, Driver: v.Driver, CreatedAt: v.CreatedAt, Labels: v.Labels, Options: v.Options}, true, nil
}

func (e *nativeUpgradeEngine) VolumeRemove(ctx context.Context, name string) error {
	_, err := e.docker.VolumeRemove(ctx, name, mobycl.VolumeRemoveOptions{})
	return err
}

func (e *nativeUpgradeEngine) Inspect(ctx context.Context, ref string) (upgradeEngineContainer, bool, error) {
	res, err := e.docker.ContainerInspect(ctx, ref, mobycl.ContainerInspectOptions{})
	if errdefs.IsNotFound(err) {
		return upgradeEngineContainer{}, false, nil
	}
	if err != nil {
		return upgradeEngineContainer{}, false, err
	}
	c := res.Container
	out := upgradeEngineContainer{ID: c.ID, Name: strings.TrimPrefix(c.Name, "/")}
	if c.State != nil {
		out.Running, out.Status, out.ExitCode = c.State.Running, string(c.State.Status), c.State.ExitCode
	}
	if c.HostConfig != nil {
		out.HostNetwork = c.HostConfig.NetworkMode.IsHost()
	}
	image, err := e.docker.ImageInspect(ctx, c.Image)
	if err != nil {
		return upgradeEngineContainer{}, false, fmt.Errorf("inspect image of %s: %w", ref, err)
	}
	out.ImageArch = image.Architecture
	for _, m := range c.Mounts {
		if m.Type == mount.TypeVolume {
			out.Mounts = append(out.Mounts, upgradeEngineMount{Volume: m.Name, Target: m.Destination, ReadOnly: !m.RW})
		}
	}
	return out, true, nil
}

func (e *nativeUpgradeEngine) Stop(ctx context.Context, ref string, timeout time.Duration) error {
	seconds := int(timeout.Seconds())
	_, err := e.docker.ContainerStop(ctx, ref, mobycl.ContainerStopOptions{Timeout: &seconds})
	return err
}

// Remove never forces and never removes volumes.
func (e *nativeUpgradeEngine) Remove(ctx context.Context, ref string) error {
	_, err := e.docker.ContainerRemove(ctx, ref, mobycl.ContainerRemoveOptions{})
	if errdefs.IsNotFound(err) {
		return nil
	}
	return err
}

func (e *nativeUpgradeEngine) CopyOut(ctx context.Context, ref, path string) ([]byte, error) {
	res, err := e.docker.CopyFromContainer(ctx, ref, mobycl.CopyFromContainerOptions{SourcePath: path})
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Content.Close() }()
	tr := tar.NewReader(res.Content)
	hdr, err := tr.Next()
	if err != nil {
		return nil, err
	}
	if hdr.Typeflag != tar.TypeReg {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	return io.ReadAll(io.LimitReader(tr, 1<<20))
}

func nativeHostNetwork() testcontainers.CustomizeRequestOption {
	return testcontainers.WithHostConfigModifier(func(h *container.HostConfig) { h.NetworkMode = "host" })
}

// ProbeStore runs a short-lived helper with the volume mounted read-only and
// no network, and removes it on every outcome.
func (e *nativeUpgradeEngine) ProbeStore(ctx context.Context, volume, dir string) (bool, error) {
	ctr, err := testcontainers.Run(ctx, e.helperImage,
		testcontainers.WithEntrypoint("/bin/sh", "-c", `test -d "$1/store.db"`, "probe", dir), testcontainers.WithCmd(),
		testcontainers.WithHostConfigModifier(func(h *container.HostConfig) { h.NetworkMode = "none" }),
		testcontainers.WithMounts(testcontainers.ContainerMount{Source: testcontainers.DockerVolumeMountSource{Name: volume}, Target: testcontainers.ContainerMountTarget(dir), ReadOnly: true}),
		testcontainers.WithWaitStrategy(wait.ForExit()))
	if ctr == nil {
		return false, err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer stop()
		_ = ctr.Terminate(cleanup)
	}()
	if err != nil {
		return false, err
	}
	state, err := ctr.State(ctx)
	if err != nil {
		return false, err
	}
	switch state.ExitCode {
	case 0:
		return true, nil
	case 1:
		return false, nil
	}
	return false, fmt.Errorf("store probe exited %d", state.ExitCode)
}

type nativeUpgradeLease struct {
	ctr     testcontainers.Container
	address string
	ports   []int
}

func (l *nativeUpgradeLease) Bound() (string, []int) { return l.address, slices.Clone(l.ports) }

func (l *nativeUpgradeLease) Release(ctx context.Context) error {
	if l.ctr == nil {
		return nil
	}
	if err := l.ctr.Terminate(ctx); err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	l.ctr = nil
	return nil
}

func (e *nativeUpgradeEngine) ReserveExact(ctx context.Context, spec upgradeLeaseSpec) (upgradeLease, error) {
	args := []string{"-u", "-c", upgradeExactLeaseScript, spec.Address}
	for _, p := range spec.Ports {
		args = append(args, strconv.Itoa(p))
	}
	ctr, err := testcontainers.Run(ctx, e.helperImage, nativeHostNetwork(),
		testcontainers.WithEntrypoint("python3"), testcontainers.WithCmd(args...),
		testcontainers.WithWaitStrategy(wait.ForLog(upgradeLeasePrefix).WithStartupTimeout(time.Minute)))
	lease := &nativeUpgradeLease{ctr: ctr}
	fail := func(cause error) (upgradeLease, error) {
		if ctr == nil {
			return nil, cause
		}
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer stop()
		if rerr := lease.Release(cleanup); rerr != nil {
			// The allocator may still hold the ports: keep it owned.
			return lease, errors.Join(cause, rerr)
		}
		return nil, cause
	}
	if err != nil {
		return fail(err)
	}
	logs, err := ctr.Logs(ctx)
	if err != nil {
		return fail(err)
	}
	out, err := io.ReadAll(logs)
	_ = logs.Close()
	if err != nil {
		return fail(err)
	}
	lease.address, lease.ports, err = parseUpgradeLeaseOutput(out, spec)
	if err != nil {
		return fail(err)
	}
	return lease, nil
}

func (e *nativeUpgradeEngine) Create(ctx context.Context, spec upgradeDestSpec) (string, error) {
	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithName(spec.Name), testcontainers.WithNoStart(),
		testcontainers.WithEntrypoint(spec.Entrypoint...), testcontainers.WithCmd(),
		testcontainers.WithEnv(spec.Env), testcontainers.WithLabels(spec.Labels),
	}
	if spec.HostNetwork {
		opts = append(opts, nativeHostNetwork())
	}
	for path, data := range spec.Files {
		opts = append(opts, testcontainers.WithFiles(testcontainers.ContainerFile{Reader: bytes.NewReader(data), ContainerFilePath: path, FileMode: 0o644}))
	}
	for _, m := range spec.Mounts {
		opts = append(opts, testcontainers.WithMounts(testcontainers.ContainerMount{
			Source: testcontainers.DockerVolumeMountSource{Name: m.Volume}, Target: testcontainers.ContainerMountTarget(m.Target), ReadOnly: m.ReadOnly,
		}))
	}
	ctr, err := testcontainers.Run(ctx, spec.Image, opts...)
	if ctr != nil {
		e.mu.Lock()
		e.ctrs[spec.Name] = ctr
		e.mu.Unlock()
		return ctr.GetContainerID(), err
	}
	return "", err
}

func (e *nativeUpgradeEngine) Start(ctx context.Context, ref string) error {
	_, err := e.docker.ContainerStart(ctx, ref, mobycl.ContainerStartOptions{})
	return err
}

func (e *nativeUpgradeEngine) Exec(ctx context.Context, ref string, argv []string) (string, int, error) {
	e.mu.Lock()
	ctr := e.ctrs[ref]
	e.mu.Unlock()
	if ctr == nil {
		return "", 0, fmt.Errorf("container %s is not known to the engine", ref)
	}
	code, r, err := ctr.Exec(ctx, argv, tcexec.Multiplexed())
	if err != nil {
		return "", code, err
	}
	out, err := io.ReadAll(r)
	return string(out), code, err
}

// ---- native workload --------------------------------------------------------------

// nativeUpgradeWorkload writes and reads whole objects with the rados CLI in
// the client container. Every call is a new process, so every read uses a
// connection that never wrote.
type nativeUpgradeWorkload struct {
	client testcontainers.Container
	pool   string
}

func (w *nativeUpgradeWorkload) run(ctx context.Context, argv ...string) (string, error) {
	code, r, err := w.client.Exec(ctx, append([]string{"timeout", "120"}, argv...), tcexec.Multiplexed())
	if err != nil {
		return "", err
	}
	out, rerr := io.ReadAll(r)
	if rerr != nil {
		return "", rerr
	}
	if code != 0 {
		return string(out), fmt.Errorf("%s exited %d: %s", argv[0], code, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func (w *nativeUpgradeWorkload) Write(ctx context.Context, object string, data []byte) error {
	path := "/tmp/tc-upgrade-put-" + object
	if err := w.client.CopyToContainer(ctx, data, path, 0o600); err != nil {
		// Nothing was submitted to the cluster.
		return fmt.Errorf("%w: stage %s: %v", errUpgradeWriteRejected, object, err)
	}
	_, err := w.run(ctx, "rados", "-p", w.pool, "put", object, path)
	return err
}

func (w *nativeUpgradeWorkload) ReadFresh(ctx context.Context, object string) ([]byte, bool, error) {
	listing, err := w.run(ctx, "rados", "-p", w.pool, "ls")
	if err != nil {
		return nil, false, err
	}
	if !slices.Contains(strings.Split(listing, "\n"), object) {
		return nil, false, nil
	}
	path := "/tmp/tc-upgrade-get-" + object
	if _, err := w.run(ctx, "rados", "-p", w.pool, "get", object, path); err != nil {
		return nil, false, err
	}
	rc, err := w.client.CopyFileFromContainer(ctx, path)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, false, err
	}
	if data == nil {
		data = []byte{}
	}
	return data, true, nil
}
