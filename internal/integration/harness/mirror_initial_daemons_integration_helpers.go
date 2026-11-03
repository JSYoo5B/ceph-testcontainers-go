//go:build all || (integration && multicluster)

package harness

import (
	"context"
	"path"
	"slices"
	"testing"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

type MirrorInitialDockerOracle struct {
	Client *mobycl.Client
	Engine string
	Before map[string]bool
}

func MirrorInitialNewDockerOracle(t *testing.T, ctx context.Context) MirrorInitialDockerOracle {
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
		t.Fatal("uncached original Docker engine identity unavailable", err)
	}
	listed, err := client.Client.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	before := map[string]bool{}
	for _, item := range listed.Items {
		if item.ID == "" || before[item.ID] {
			t.Fatal("invalid original Docker full container inventory")
		}
		before[item.ID] = true
	}
	return MirrorInitialDockerOracle{Client: client.Client, Engine: info.Info.ID, Before: before}
}

// This test owns a fresh pair and runs exclusively. The uncached engine/list/
// exact-CID oracle counts new containers, including stopped partial processes.
// It is a local resource proof, not a promise that a shared pool has no peers.
func (o MirrorInitialDockerOracle) AssertPolicyOnly(t *testing.T, ctx context.Context, cliCount int, kind string) {
	t.Helper()
	info, err := o.Client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || info.Info.ID != o.Engine {
		t.Fatal("original Docker engine changed", err)
	}
	listed, err := o.Client.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	created := 0
	for _, item := range listed.Items {
		if item.ID == "" {
			t.Fatal("empty native full container ID")
		}
		if o.Before[item.ID] {
			continue
		}
		created++
		inspected, err := o.Client.ContainerInspect(ctx, item.ID, mobycl.ContainerInspectOptions{})
		if err != nil || inspected.Container.ID != item.ID || inspected.Container.Config == nil || inspected.Container.State == nil {
			t.Fatal("fresh exact-CID construction inspection unavailable", err)
		}
		config := inspected.Container.Config
		for _, entry := range config.Entrypoint {
			if path.Base(entry) == "rbd-mirror" || path.Base(entry) == "cephfs-mirror" {
				t.Fatal("zero construction launched a hidden mirror process")
			}
		}
		if !slices.Equal(config.Entrypoint, []string{"sleep"}) || !slices.Equal(config.Cmd, []string{"infinity"}) || !inspected.Container.State.Running {
			t.Fatal("unexpected construction container outside retained setup CLIs")
		}
	}
	after, err := o.Client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || after.Info.ID != o.Engine || created != cliCount {
		t.Fatalf("zero construction resource graph: kind=%s new=%d want=%d err=%v", kind, created, cliCount, err)
	}
	t.Logf("MIRROR_INITIAL_ZERO kind=%s engine=%s new_cli_containers=%d owned_daemons=0", kind, o.Engine, created)
}

func MirrorInitialPair(t *testing.T, host bool) (*ceph.Container, *ceph.Container, testcontainers.Container, testcontainers.Container) {
	t.Helper()
	opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	return NewMultiClusterPair(t, opts...)
}
