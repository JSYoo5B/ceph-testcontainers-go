//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_osd_retained_reopen))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// TestOSDRetainedReopen detaches the only OSD of a single-replica, one-PG
// pool and reopens its retained store in a different container. No other
// replica exists, so the known bytes read afterwards can come only from the
// retained store, not from backfill.
//
// Before the positive path it proves three refusals without mutation: detach
// of a paused OSD, reopen of a store whose keyring an observer changed, and
// RemoveOSD and a second ReopenOSD of a detached or consumed handle.
func TestOSDRetainedReopen(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	image, opts := integrationImages(t)
	opts = opts.WithCustomizers(ceph.WithNoInitialOSDs(), ceph.WithOSDVolumeStorage())
	cluster, err := ceph.Run(ctx, opts)
	terminated := false
	if cluster != nil {
		t.Cleanup(func() {
			if terminated {
				return
			}
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error("retained reopen fixture cleanup", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	original, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "retained-data", Application: "rados", Replicas: 1, MinSize: 1, PGNum: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	osdVolumeData(t, ctx, cluster, pool.Name, "seed")
	volume, _ := osdVolumeBacking(t, ctx, docker.Client, cluster)
	quorum, err := observeCluster(cluster).QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	registered := osdMemoryIdentities(t, ctx, cluster)
	oldContainer := original.GetContainerID()
	noout, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
	if err != nil {
		t.Fatal(err)
	}

	// A held pause needs the container for its undo, so detach refuses it
	// before the fence and the stop.
	pause, err := fault.Pause(ctx, cluster, original)
	if pause != nil {
		defer func() { _ = pause.Resume(context.WithoutCancel(ctx)) }()
	}
	if err != nil {
		t.Fatal(err)
	}
	pausedRegistration := noInitialOSDNativeDump(t, ctx, cluster)
	if refused, err := original.Detach(ctx); err == nil || refused != nil || !strings.Contains(err.Error(), "container pause") {
		t.Fatal("DetachOSD of a paused OSD was not refused", err)
	}
	if raw := osdMemoryInspect(t, ctx, docker.Client, oldContainer); !raw.State.Running || !raw.State.Paused {
		t.Fatalf("pause refusal changed the container state %+v", raw.State)
	}
	if owned := clusteraccess.OSDs(t, cluster); len(owned) != 1 || owned[0] != original || original.GetContainerID() != oldContainer {
		t.Fatal("pause refusal changed the owned OSD handle")
	}
	if !slices.Equal(pausedRegistration, noInitialOSDNativeDump(t, ctx, cluster)) {
		t.Fatal("pause refusal changed the registration")
	}
	if err := pause.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	// A fence would refuse this command for the instance permanently.
	probe, err := cluster.RunNativeCommand(ctx, original, ceph.NativeCommand{Argv: []string{"test", "-f", "/var/lib/ceph/osd/ceph-" + strconv.Itoa(original.ID) + "/ready"}, Timeout: 30 * time.Second, MaxStdout: 1 << 10, MaxStderr: 1 << 10, Label: "unfenced probe"})
	if err != nil || probe.ExitCode != 0 {
		t.Fatal("pause refusal fenced the OSD instance", probe.ExitCode, err)
	}

	detached, err := original.Detach(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if detached.ID() != original.ID || detached.UUID() != registered[original.ID] || detached.ClusterFSID() != quorum.MonMap.FSID {
		t.Fatalf("detached identity osd.%d %s %s, registered %v fsid %s", detached.ID(), detached.UUID(), detached.ClusterFSID(), registered, quorum.MonMap.FSID)
	}
	if _, err := docker.Client.ContainerInspect(ctx, oldContainer, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("detached container remains", err)
	}
	if len(clusteraccess.OSDs(t, cluster)) != 0 {
		t.Fatal("a detached OSD is still listed")
	}
	native := noInitialOSDNativeDump(t, ctx, cluster)
	if len(native) != 1 || native[0].ID != original.ID || native[0].UUID != detached.UUID() || native[0].Up != 0 || native[0].In != 1 {
		t.Fatalf("detached registration %+v", native)
	}
	if err := original.Remove(ctx); err == nil {
		t.Fatal("RemoveOSD purged a detached OSD")
	}
	if !slices.Equal(native, noInitialOSDNativeDump(t, ctx, cluster)) {
		t.Fatal("RemoveOSD refusal changed the detached registration")
	}

	// An independent read/write observer changes the retained keyring.
	// Reopen must refuse before any write and start no container.
	observer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: image, Entrypoint: []string{"sleep"}, Cmd: []string{"infinity"},
		HostConfigModifier: func(h *container.HostConfig) { h.NetworkMode = "none" },
		Mounts:             testcontainers.ContainerMounts{{Source: testcontainers.DockerVolumeMountSource{Name: volume, VolumeOptions: &mount.VolumeOptions{NoCopy: true, Labels: maps.Clone(testcontainers.GenericLabels())}}, Target: "/observed"}},
	}, Started: true})
	observerRemoved := false
	if observer != nil {
		t.Cleanup(func() {
			if observerRemoved {
				return
			}
			cleanup, done := context.WithTimeout(context.Background(), time.Minute)
			defer done()
			if err := observer.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	store := "/observed/ceph-" + strconv.Itoa(original.ID)
	clean := retainedStoreFingerprint(t, ctx, observer, store)
	osdMemoryExec(t, ctx, observer, "sh", "-c", `cp -p -- "$1/keyring" /tmp/keyring.orig && printf '# tampered\n' >> "$1/keyring"`, "tamper", store)
	tampered := retainedStoreFingerprint(t, ctx, observer, store)
	if tampered["keyring"].SHA256 == clean["keyring"].SHA256 {
		t.Fatal("the observer did not change the retained keyring")
	}
	containers := retainedContainerIDs(t, ctx, docker.Client)
	if refused, err := detached.Reopen(ctx); err == nil || refused != nil || !strings.Contains(err.Error(), "changed since it was detached") {
		t.Fatal("ReopenOSD of a changed store was not refused", err)
	}
	if !slices.Equal(containers, retainedContainerIDs(t, ctx, docker.Client)) {
		t.Fatal("the store refusal created or removed a container")
	}
	if !slices.Equal(native, noInitialOSDNativeDump(t, ctx, cluster)) {
		t.Fatal("the store refusal changed the detached registration")
	}
	if !maps.Equal(tampered, retainedStoreFingerprint(t, ctx, observer, store)) {
		t.Fatal("the store refusal changed the retained store")
	}
	if len(clusteraccess.OSDs(t, cluster)) != 0 {
		t.Fatal("the store refusal published an OSD")
	}
	// Restore the keyring bytes in place, keeping its inode.
	osdMemoryExec(t, ctx, observer, "sh", "-c", `cat /tmp/keyring.orig > "$1/keyring"`, "restore", store)
	restored := retainedStoreFingerprint(t, ctx, observer, store)
	for name, file := range clean {
		if name == "keyring" {
			file.MtimeNS = restored[name].MtimeNS
		}
		if restored[name] != file {
			t.Fatalf("restored store %s %+v, detached %+v", name, restored[name], file)
		}
	}
	if len(restored) != len(clean) {
		t.Fatal("restored store entries differ")
	}
	if err := observer.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	observerRemoved = true

	reopened, err := detached.Reopen(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reopened == original || reopened.ID != original.ID || reopened.GetContainerID() == oldContainer || original.GetContainerID() != oldContainer {
		t.Fatalf("reopen handle osd.%d %s, previous %s", reopened.ID, reopened.GetContainerID(), original.GetContainerID())
	}
	if again, err := detached.Reopen(ctx); err == nil || again != nil {
		t.Fatal("a detached OSD was reopened twice")
	}
	if current := osdMemoryIdentities(t, ctx, cluster); len(current) != 1 || current[original.ID] != registered[original.ID] {
		t.Fatalf("reopened registration %v, detached %v", current, registered)
	}
	if name, _ := osdVolumeBacking(t, ctx, docker.Client, cluster); name != volume {
		t.Fatalf("reopened OSD mounts %s, not the retained volume %s", name, volume)
	}
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	osdVolumeData(t, ctx, cluster, pool.Name, "reopened")

	newContainer := reopened.GetContainerID()
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	terminated = true
	for _, cid := range []string{oldContainer, newContainer} {
		if _, err := docker.Client.ContainerInspect(ctx, cid, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Error("owned OSD container remains", err)
		}
	}
	if _, err := docker.Client.VolumeInspect(ctx, volume, mobycl.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Error("owned volume remains", err)
	}
	t.Logf("OSD_RETAINED_REFUSALS osd=%d paused_detach=refused_unfenced store_change=refused containers_unchanged=true registration_unchanged=true store_unchanged=true restored=true remove_detached=refused second_reopen=refused", original.ID)
	t.Logf("OSD_RETAINED_REOPEN osd=%d uuid=%s same_fsid=true container_changed=true single_replica=true bytes=1048583 remaining=0", original.ID, detached.UUID())
}

type retainedStoreFile struct {
	Mode    int    `json:"mode"`
	Inode   int64  `json:"ino"`
	Size    int64  `json:"size"`
	MtimeNS int64  `json:"mtime_ns"`
	SHA256  string `json:"sha256"`
}

// retainedStoreFingerprint describes every entry of a store directory: the
// mode, inode, size and modification time, and a digest of every regular
// file except the large block file.
func retainedStoreFingerprint(t *testing.T, ctx context.Context, observer testcontainers.Container, dir string) map[string]retainedStoreFile {
	t.Helper()
	data := osdMemoryExec(t, ctx, observer, "python3", "-c", `import hashlib,json,os,stat,sys
d=sys.argv[1]; out={}
for name in sorted(os.listdir(d)):
    p=os.path.join(d,name); st=os.lstat(p); digest=''
    if stat.S_ISREG(st.st_mode) and name!='block':
        with open(p,'rb') as f: digest=hashlib.sha256(f.read()).hexdigest()
    out[name]={'mode':st.st_mode,'ino':st.st_ino,'size':st.st_size,'mtime_ns':st.st_mtime_ns,'sha256':digest}
print(json.dumps(out))`, dir)
	var files map[string]retainedStoreFile
	if err := json.Unmarshal(data, &files); err != nil || files["keyring"].SHA256 == "" || files["block"].Size == 0 {
		t.Fatalf("retained store fingerprint %s: %v", data, err)
	}
	return files
}

func retainedContainerIDs(t *testing.T, ctx context.Context, docker *mobycl.Client) []string {
	t.Helper()
	list, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(list.Items))
	for _, c := range list.Items {
		ids = append(ids, c.ID)
	}
	slices.Sort(ids)
	return ids
}
