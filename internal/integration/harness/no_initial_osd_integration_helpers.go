//go:build all || integration

package harness

import (
	"context"
	"slices"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

type NoInitialOSDOracle struct {
	Client   *mobycl.Client
	Engine   string
	Before   map[string]bool
	Owned    []string
	Networks []string
}

func NewNoInitialOSDOracle(t *testing.T, ctx context.Context) *NoInitialOSDOracle {
	t.Helper()
	client, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	info, err := client.Client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || info.Info.ID == "" {
		t.Fatal("original native engine identity unavailable", err)
	}
	listed, err := client.Client.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	o := &NoInitialOSDOracle{Client: client.Client, Engine: info.Info.ID, Before: map[string]bool{}}
	for _, item := range listed.Items {
		if len(item.ID) != 64 || o.Before[item.ID] {
			t.Fatal("invalid original full-CID inventory")
		}
		o.Before[item.ID] = true
	}
	return o
}

// The exclusive test may create one standard session Ryuk in addition to the
// exact MON/MGR handles. Every other new running or stopped container rejects
// the zero-storage resource proof, including an OSD or allocation residue.
func (o *NoInitialOSDOracle) AssertOnlyControl(t *testing.T, ctx context.Context, cluster *ceph.Container, phase string) {
	t.Helper()
	info, err := o.Client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || info.Info.ID != o.Engine {
		t.Fatal("original native engine changed", err)
	}
	managers := clusteraccess.Managers(t, cluster)
	if len(managers) != 1 || managers[0].Container == nil {
		t.Fatal("default manager ownership unavailable")
	}
	expected := map[string]string{cluster.GetContainerID(): "mon", managers[0].GetContainerID(): "mgr"}
	if len(expected) != 2 {
		t.Fatal("MON/MGR share a handle")
	}
	mon, err := o.Client.ContainerInspect(ctx, cluster.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil || mon.Container.Config == nil {
		t.Fatal("fresh MON configuration unavailable", err)
	}
	session := mon.Container.Config.Labels["org.testcontainers.sessionId"]
	if session == "" {
		t.Fatal("MON native session identity unavailable")
	}
	listed, err := o.Client.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	seen, auxiliaries := map[string]bool{}, 0
	for _, item := range listed.Items {
		if o.Before[item.ID] {
			continue
		}
		fresh, err := o.Client.ContainerInspect(ctx, item.ID, mobycl.ContainerInspectOptions{})
		if err != nil || len(item.ID) != 64 || fresh.Container.ID != item.ID || fresh.Container.Config == nil || fresh.Container.State == nil {
			t.Fatal("fresh exact-CID construction inspection unavailable", err)
		}
		config, state := fresh.Container.Config, fresh.Container.State
		if !state.Running || state.Paused || state.Restarting || state.Dead {
			t.Fatal("unexpected non-running construction resource")
		}
		role, owned := expected[item.ID]
		if !owned {
			if config.Labels["org.testcontainers.reaper"] != "true" || config.Labels["org.testcontainers.ryuk"] != "true" || config.Labels["org.testcontainers.sessionId"] != session {
				t.Fatal("unexpected new container outside MON/MGR and session Ryuk")
			}
			auxiliaries++
			if auxiliaries > 1 {
				t.Fatal("multiple newly created session reapers")
			}
			continue
		}
		if !slices.Equal(config.Entrypoint, []string{"/bin/sh", "/tc/" + role + ".sh"}) || len(config.Cmd) != 0 {
			t.Fatal("MON/MGR construction entrypoint changed")
		}
		seen[item.ID] = true
	}
	if len(seen) != 2 {
		t.Fatal("construction lost an original MON/MGR container")
	}
	o.Owned = []string{cluster.GetContainerID(), managers[0].GetContainerID()}
	if len(o.Networks) == 0 && !StaticFacts(cluster).UsesHostNetwork {
		names := []string{StaticFacts(cluster).NetworkName}
		if StaticFacts(cluster).HasSeparateClusterNetwork {
			names = append(names, StaticFacts(cluster).ClusterNetworkName)
		}
		for _, name := range names {
			native, err := o.Client.NetworkInspect(ctx, name, mobycl.NetworkInspectOptions{})
			if err != nil || native.Network.ID == "" || native.Network.Name != name {
				t.Fatal("owned bridge identity unavailable", err)
			}
			o.Networks = append(o.Networks, native.Network.ID)
		}
	}
	t.Logf("NO_INITIAL_OSD_RESOURCES phase=%s engine=%s session=%s primary=2 auxiliary=%d mon_cid=%s mgr_cid=%s", phase, o.Engine, session, auxiliaries, o.Owned[0], o.Owned[1])
}

func (o *NoInitialOSDOracle) AssertRemoved(t *testing.T, ctx context.Context) {
	t.Helper()
	info, err := o.Client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || info.Info.ID != o.Engine {
		t.Fatal("cleanup engine changed", err)
	}
	for _, id := range o.Owned {
		if _, err := o.Client.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Fatal("owned original container still present after cleanup", err)
		}
	}
	for _, id := range o.Networks {
		if _, err := o.Client.NetworkInspect(ctx, id, mobycl.NetworkInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Fatal("owned original network still present after cleanup", err)
		}
	}
	t.Logf("NO_INITIAL_OSD_CLEANUP engine=%s owned_containers=%d owned_networks=%d remaining=0", o.Engine, len(o.Owned), len(o.Networks))
}

func NoInitialOSDCleanup(t *testing.T, cluster *ceph.Container) {
	t.Helper()
	if cluster != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			if err := cluster.Terminate(ctx); err != nil {
				t.Error("whole-cluster cleanup failed", err)
			}
		})
	}
}
