//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/google/uuid"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	mobycl "github.com/moby/moby/client"
)

const osdVolumeLabel = "org.testcontainers.ceph.osd-volume-owner"

func osdVolumeInventory(t *testing.T, ctx context.Context, docker *mobycl.Client) ([]string, []string) {
	t.Helper()
	filter := make(mobycl.Filters).Add("label", osdVolumeLabel)
	volumes, err := docker.VolumeList(ctx, mobycl.VolumeListOptions{Filters: filter})
	if err != nil {
		t.Fatal(err)
	}
	helpers, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: filter})
	if err != nil {
		t.Fatal(err)
	}
	var names, ids []string
	for _, v := range volumes.Items {
		names = append(names, v.Name)
	}
	for _, c := range helpers.Items {
		ids = append(ids, c.ID)
	}
	slices.Sort(names)
	slices.Sort(ids)
	return names, ids
}

func osdVolumeBacking(t *testing.T, ctx context.Context, docker *mobycl.Client, cluster *ceph.Container) (string, container.InspectResponse) {
	t.Helper()
	var name string
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		raw := osdMemoryInspect(t, ctx, docker, osd.GetContainerID())
		found := 0
		for _, mounted := range raw.Mounts {
			if mounted.Destination != "/var/lib/ceph/osd" {
				continue
			}
			if mounted.Type != mount.TypeVolume || mounted.Name == "" || !mounted.RW || name != "" && name != mounted.Name {
				t.Fatal("OSD ordinary volume mount differs")
			}
			name = mounted.Name
			found++
		}
		if found != 1 {
			t.Fatal("missing/duplicate ordinary volume mount")
		}
	}
	owner := strings.TrimPrefix(name, "tc-ceph-osd-volume-")
	if id, err := uuid.Parse(owner); err != nil || id == uuid.Nil || name != "tc-ceph-osd-volume-"+id.String() {
		t.Fatal("volume owner UUID differs")
	}
	v, err := docker.VolumeInspect(ctx, name, mobycl.VolumeInspectOptions{})
	if err != nil || v.Volume.Driver != "local" || len(v.Volume.Options) != 0 || v.Volume.Labels[osdVolumeLabel] != owner || v.Volume.CreatedAt == "" {
		t.Fatal("ordinary local volume metadata differs", err)
	}
	list, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", osdVolumeLabel+"="+owner)})
	if err != nil || len(list.Items) != 1 {
		t.Fatal("owned helper inventory differs", err)
	}
	helper := osdMemoryInspect(t, ctx, docker, list.Items[0].ID)
	control := osdMemoryInspect(t, ctx, docker, cluster.GetContainerID())
	if helper.Image != control.Image || !helper.State.Running || helper.State.StartedAt == "" || string(helper.HostConfig.NetworkMode) != "none" || !slices.Equal(helper.Config.Entrypoint, []string{"sleep"}) || !slices.Equal(helper.Config.Cmd, []string{"infinity"}) {
		t.Fatal("isolated idle essential-image helper differs")
	}
	found := 0
	for _, m := range helper.HostConfig.Mounts {
		if m.Source == name && m.Target == "/tc/osd-volume" && m.Type == mount.TypeVolume && !m.ReadOnly && m.VolumeOptions != nil && m.VolumeOptions.NoCopy {
			found++
		}
	}
	if found != 1 {
		t.Fatal("helper missing owned no-copy mount")
	}
	return name, helper
}

func osdVolumeData(t *testing.T, ctx context.Context, cluster *ceph.Container, pool, phase string) {
	t.Helper()
	control, err := cluster.ControlContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := osdMemoryExec(t, ctx, control, "python3", "-c", `import rados,sys
pool,phase=sys.argv[1:]
payload=bytes((i*13+3)%251 for i in range(1048583))
with rados.Rados(conffile='/etc/ceph/ceph.conf',name='client.admin',conf={'client_mount_timeout':'10','rados_mon_op_timeout':'10','rados_osd_op_timeout':'30'}) as c:
    with c.open_ioctx(pool) as io:
        if phase=='seed': io.write_full('known-payload',payload)
        size,_=io.stat('known-payload')
        assert size==len(payload) and io.read('known-payload',size)==payload
print('verified')`, pool, phase)
	if string(data) != "verified\n" {
		t.Fatal("known native payload differs")
	}
	t.Logf("OSD_VOLUME_BYTES phase=%s bytes=1048583 verified=true", phase)
}

// Match the actual owned registration set; this fixture grows beyond two OSDs.
func osdVolumeWaitState(t *testing.T, parent context.Context, cluster *ceph.Container, expected map[int]string, up bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	for {
		if up {
			for _, osd := range clusteraccess.OSDs(t, cluster) {
				state, err := osd.State(ctx)
				if err != nil || state == nil || !state.Running {
					t.Fatal("restarted volume OSD exited", err)
				}
			}
		}
		states, err := harness.ObserveCluster(cluster).OSDStates(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ready := len(states) == len(expected)
		for _, state := range states {
			ready = ready && expected[state.ID] == state.UUID && state.Up == up && state.In
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("owned volume OSD membership did not converge", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}
