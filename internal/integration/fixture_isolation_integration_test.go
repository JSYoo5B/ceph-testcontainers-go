//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cluster_fixtures))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,cephfs,rgw

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/expect"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// fixtureIsolationProbe reads the same-name object of one cluster and checks
// that it holds that cluster's own payload, then writes and reads back a
// fresh object, so a phase proves both retained data and new I/O. It prints
// the FSID that the client reached.
const fixtureIsolationProbe = `import json, sys, rados
phase, payload = sys.argv[1], sys.argv[2].encode()
c = rados.Rados(conffile='/etc/ceph/ceph.conf')
c.connect(timeout=30)
try:
    with c.open_ioctx('tc-isolation') as io:
        if phase == 'seed':
            io.write_full('same-name', payload)
        assert io.read('same-name', 4096) == payload, 'same-name object lost its payload'
        fresh = ('fresh-' + phase).encode()
        io.write_full('fresh-' + phase, fresh)
        assert io.read('fresh-' + phase, 4096) == fresh, 'fresh object did not read back'
    print(json.dumps({'phase': phase, 'fsid': c.get_fsid()}))
finally:
    c.shutdown()
`

// isolationFixture is one cluster of the isolation test with the resources it
// owns, recorded from Docker by the cluster's owner label.
type isolationFixture struct {
	name       string
	cluster    *ceph.Container
	client     testcontainers.Container
	owner      string
	fsid       string
	containers []string
	network    string // the owned bridge's ID; empty in host mode
}

// TestFixtureIsolation checks that two clusters on the same Docker engine do
// not interfere: a fault in A and the termination of A leave B's identity,
// owned resources and data intact, and so does a cluster C whose bootstrap
// fails. Both clusters use the same pool and object names with different
// payloads, so a client that reached the wrong cluster would read the wrong
// bytes.
func TestFixtureIsolation(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
			defer cancel()
			docker, err := testcontainers.NewDockerClientWithOpts(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = docker.Close() }()
			opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1))}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			start := func(name, payload string) *isolationFixture {
				t.Helper()
				cluster, client := harness.NewServiceCluster(t, opts...)
				if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-isolation", Application: "rados"}); err != nil {
					t.Fatal(err)
				}
				if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
					t.Fatal(err)
				}
				f := &isolationFixture{name: name, cluster: cluster, client: client, owner: isolationOwner(t, ctx, docker, cluster)}
				f.fsid = isolationProbe(t, ctx, f, "seed", payload)
				f.containers = isolationOwnedContainers(t, ctx, docker, f.owner)
				if !host {
					f.network = isolationNetworkID(t, ctx, docker, cluster)
				}
				t.Logf("%s: owner %s fsid %s, %d owned containers, network %q", name, f.owner, f.fsid, len(f.containers), f.network)
				return f
			}
			a := start("A", "payload-of-A")
			b := start("B", "payload-of-B")
			if a.owner == b.owner || a.fsid == b.fsid || (!host && a.network == b.network) {
				t.Fatalf("the fixtures share an identity: A %+v, B %+v", a, b)
			}
			if slices.ContainsFunc(a.containers, func(id string) bool { return slices.Contains(b.containers, id) }) {
				t.Fatal("the fixtures share an owned container")
			}
			bOSDs, err := harness.ObserveCluster(b.cluster).OSDStates(ctx)
			if err != nil {
				t.Fatal(err)
			}
			bHealthy := expect.OSDs{Select: bOSDs, Up: true, In: true, Exact: true}
			checkB := func(phase string) {
				t.Helper()
				if fsid := isolationProbe(t, ctx, b, phase, "payload-of-B"); fsid != b.fsid {
					t.Fatalf("%s: B's client reached FSID %s instead of %s", phase, fsid, b.fsid)
				}
				requireOSDs(t, ctx, b.cluster, phase+": B's OSDs", bHealthy)
				if got := isolationOwnedContainers(t, ctx, docker, b.owner); !slices.Equal(got, b.containers) {
					t.Fatalf("%s: B's owned containers changed: %v, want %v", phase, got, b.containers)
				}
				for _, id := range b.containers {
					inspect, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
					if err != nil || !inspect.Container.State.Running {
						t.Fatalf("%s: B's container %.12s is not running: %v", phase, id, err)
					}
				}
				if b.network != "" {
					if _, err := docker.NetworkInspect(ctx, b.network, mobycl.NetworkInspectOptions{}); err != nil {
						t.Fatalf("%s: B's network is gone: %v", phase, err)
					}
				}
				t.Logf("%s: B kept FSID %s, its %d containers, network and data, and accepted new I/O", phase, b.fsid, len(b.containers))
			}

			// A fault in A: its only OSD stops. A's own OSDMap reports it
			// down, so the fault took effect where it was aimed.
			aOSDs, err := harness.ObserveCluster(a.cluster).OSDStates(ctx)
			if err != nil {
				t.Fatal(err)
			}
			osd := clusteraccess.OSDs(t, a.cluster)[0]
			if err := osd.Stop(ctx, nil); err != nil {
				t.Fatal(err)
			}
			waitOSDPolicyState(t, ctx, a.cluster, expect.OSDs{Select: aOSDs, Up: false, In: true})
			checkB("A faulted")

			// A terminates. Its owned containers and network are removed,
			// and nothing of B.
			if err := a.client.Terminate(ctx); err != nil {
				t.Fatal(err)
			}
			if err := a.cluster.Terminate(ctx); err != nil {
				t.Fatal(err)
			}
			if left := isolationOwnedContainers(t, ctx, docker, a.owner); len(left) != 0 {
				t.Fatalf("A's owned containers remain after Terminate: %v", left)
			}
			if a.network != "" {
				if _, err := docker.NetworkInspect(ctx, a.network, mobycl.NetworkInspectOptions{}); err == nil {
					t.Fatal("A's network remains after Terminate")
				}
			}
			checkB("A terminated")

			// C's bootstrap fails after it created containers. It starts
			// after A and B, so no image pull counts against its short
			// startup timeout.
			image, imageOpts := harness.IntegrationImages(t)
			partial, err := ceph.Run(ctx, image, append(append(imageOpts, opts...), ceph.WithStartupTimeout(3*time.Second))...)
			if partial == nil || err == nil {
				if partial != nil {
					_ = partial.Terminate(context.WithoutCancel(ctx))
				}
				t.Fatalf("a three second bootstrap returned %v, %v", partial, err)
			}
			owner := isolationOwner(t, ctx, docker, partial)
			created := isolationOwnedContainers(t, ctx, docker, owner)
			network := ""
			if !host {
				network = isolationNetworkID(t, ctx, docker, partial)
			}
			if err := partial.Terminate(context.WithoutCancel(ctx)); err != nil {
				t.Fatal(err)
			}
			if left := isolationOwnedContainers(t, ctx, docker, owner); len(left) != 0 {
				t.Fatalf("the failed bootstrap's containers remain after Terminate: %v", left)
			}
			if network != "" {
				if _, err := docker.NetworkInspect(ctx, network, mobycl.NetworkInspectOptions{}); err == nil {
					t.Fatal("the failed bootstrap's network remains after Terminate")
				}
			}
			t.Logf("C: bootstrap failed (%v) with %d owned containers and network %q, all removed", err, len(created), network)
			checkB("C failed and terminated")
		})
	}
}

// isolationProbe runs fixtureIsolationProbe on the fixture's client and
// returns the FSID it reached.
func isolationProbe(t *testing.T, ctx context.Context, f *isolationFixture, phase, payload string) string {
	t.Helper()
	out := harness.ExecOutput(t, ctx, f.client, "python3", "-c", fixtureIsolationProbe, phase, payload)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	var result struct {
		Phase, FSID string
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &result); err != nil || result.Phase != phase || result.FSID == "" {
		t.Fatalf("%s %s: unreadable probe result %q: %v", f.name, phase, out, err)
	}
	return result.FSID
}

// isolationOwner returns the owner label of the cluster's first MON, which
// every container that the cluster creates carries.
func isolationOwner(t *testing.T, ctx context.Context, docker *testcontainers.DockerClient, cluster *ceph.Container) string {
	t.Helper()
	inspect, err := docker.ContainerInspect(ctx, cluster.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	owner := inspect.Container.Config.Labels[clusterutil.OwnerLabel]
	if owner == "" {
		t.Fatal("the cluster's first MON has no owner label")
	}
	return owner
}

// isolationOwnedContainers lists the IDs of every container, running or not,
// that carries owner, sorted.
func isolationOwnedContainers(t *testing.T, ctx context.Context, docker *testcontainers.DockerClient, owner string) []string {
	t.Helper()
	list, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", clusterutil.OwnerLabel+"="+owner)})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(list.Items))
	for _, ctr := range list.Items {
		ids = append(ids, ctr.ID)
	}
	slices.Sort(ids)
	return ids
}

// isolationNetworkID returns the ID of the cluster's public bridge.
func isolationNetworkID(t *testing.T, ctx context.Context, docker *testcontainers.DockerClient, cluster *ceph.Container) string {
	t.Helper()
	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		t.Fatal(err)
	}
	network, err := docker.NetworkInspect(ctx, facts.NetworkName, mobycl.NetworkInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return network.Network.ID
}
