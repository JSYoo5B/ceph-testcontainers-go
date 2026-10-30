//go:build all || (integration && topology && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_infra))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs,rbd-aux,rgw

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
	"slices"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/testcontainers/testcontainers-go"
)

func TestMultiClusterTopologySnapshotsHonorBusyOwners(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	const pool = "tc-context-snapshot"
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t,
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(1).WithOSDCount(1).WithPools(ceph.PoolConfig{Name: pool, Application: "rados", Replicas: 1, MinSize: 1})))
	type state struct {
		config, keyring []byte
		managers        []*ceph.ManagerContainer
		gateways        []*rgw.Gateway
		osdIDs          []int
	}
	before := make([]state, 2)
	clusters := []*ceph.Container{source, destination}
	clients := []testcontainers.Container{sourceClient, destinationClient}
	payload := []byte("busy constructor admission retains exact Ceph client data")
	for i, cluster := range clusters {
		cc, err := cluster.ConnectionConfig(ctx)
		if err != nil {
			t.Fatal(err)
		}
		before[i].config, before[i].keyring = cc.Conf, cc.Keyring
		before[i].managers, err = cluster.Managers(ctx)
		if err != nil {
			t.Fatal(err)
		}
		before[i].gateways, err = rgw.Gateways(ctx, cluster)
		if err != nil {
			t.Fatal(err)
		}
		data, err := cluster.Ceph(ctx, "osd", "ls", "--format", "json")
		if err != nil || json.Unmarshal(data, &before[i].osdIDs) != nil {
			t.Fatalf("read original OSD membership: %v", err)
		}
		if err := clients[i].CopyToContainer(ctx, payload, "/tmp/tc-context-input", 0o600); err != nil {
			t.Fatal(err)
		}
		harness.MultiClusterExecOutput(t, ctx, clients[i], "rados", "-p", pool, "put", "retained", "/tmp/tc-context-input")
	}
	for index, busy := range clusters {
		t.Run([]string{"source", "destination"}[index], func(t *testing.T) {
			releaseOwner, finish, wrappers := topologySnapshotHoldOwner(t, ctx, clusters, busy)
			defer finish()
			for _, kind := range []string{"rbd", "cephfs", "rgw", "rgw-topology"} {
				t.Run(kind, func(t *testing.T) {
					topologySnapshotQueue(t, releaseOwner, func(callCtx context.Context) (topologySnapshotPartial, error) {
						switch kind {
						case "rbd":
							result, err := rbdmirror.Run(callCtx, harness.NativeMirrorImage(t, "rbd-aux", harness.StaticFacts(source).Images.RBDAux), rbdmirror.WithConfig(rbdmirror.Config{Source: source, Destination: destination, Pool: pool}))
							if result == nil {
								return nil, err
							}
							return result, err
						case "cephfs":
							result, err := cephfsmirror.Run(callCtx, harness.NativeMirrorImage(t, "cephfs", harness.StaticFacts(source).Images.CephFS), cephfsmirror.WithConfig(cephfsmirror.Config{Source: source, Destination: destination, SourceFilesystem: "fs", DestinationFilesystem: "fs", Directories: []string{"/owned"}}))
							if result == nil {
								return nil, err
							}
							return result, err
						case "rgw":
							result, err := multisite.Run(callCtx, harness.StaticFacts(source).Images.RGW, multisite.WithConfig(multisite.Config{Source: source, Destination: destination}))
							if result == nil {
								return nil, err
							}
							return result, err
						default:
							result, err := multisite.Run(callCtx, harness.StaticFacts(source).Images.RGW, multisite.WithTopology(multisite.TopologyConfig{Zones: []multisite.ZoneConfig{{Name: "a", Cluster: source}, {Name: "b", Cluster: destination}}}))
							if result == nil {
								return nil, err
							}
							return result, err
						}
					})
				})
				if t.Failed() {
					return
				}
			}
			for _, wrapper := range wrappers {
				if wrapper.queries.Load() != 0 || wrapper.copies.Load() != 0 || wrapper.cleanups.Load() != 0 {
					t.Errorf("queued consumers attempted native work/copy/cleanup: %d/%d/%d", wrapper.queries.Load(), wrapper.copies.Load(), wrapper.cleanups.Load())
				}
			}
		})
		if t.Failed() {
			return
		}
	}
	for i, cluster := range clusters {
		cc, err := cluster.ConnectionConfig(ctx)
		config := cc.Conf
		keyring := cc.Keyring
		if err != nil || !bytes.Equal(config, before[i].config) || !bytes.Equal(keyring, before[i].keyring) {
			t.Fatalf("fresh bootstrap snapshot changed after canceled admission: %v", err)
		}
		managers, err := cluster.Managers(ctx)
		if err != nil || !slices.Equal(managers, before[i].managers) {
			t.Fatalf("manager descriptors changed: %v", err)
		}
		gateways, err := rgw.Gateways(ctx, cluster)
		if err != nil || !slices.Equal(gateways, before[i].gateways) {
			t.Fatalf("gateway descriptors changed: %v", err)
		}
		data, err := cluster.Ceph(ctx, "osd", "ls", "--format", "json")
		var current []int
		if err != nil || json.Unmarshal(data, &current) != nil || !slices.Equal(current, before[i].osdIDs) {
			t.Fatalf("OSD membership changed: %v", err)
		}
		harness.MultiClusterExecOutput(t, ctx, clients[i], "rados", "-p", pool, "get", "retained", "/tmp/tc-context-output")
		if !bytes.Equal(harness.MultiClusterReadFile(t, ctx, clients[i], "/tmp/tc-context-output"), payload) {
			t.Fatal("retained RADOS bytes differ after owner admission cancellation")
		}
	}
}
