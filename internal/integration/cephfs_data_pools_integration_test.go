//go:build integration && features

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestCephFSDynamicDataPools(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) { testCephFSDynamicDataPools(t, host) })
	}
}

func testCephFSDynamicDataPools(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	const filesystem, replicated, erasure, unused = "dynamic", "dynamic-replicated", "dynamic-erasure", "dynamic-unused"
	options := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(3), ceph.WithCephFS(ceph.CephFSConfig{Name: filesystem})}
	if host {
		options = append(options, ceph.WithHostNetwork())
	}
	cluster, client := newServiceCluster(t, options...)
	fs := cluster.Filesystems()[0]
	for _, config := range []ceph.PoolConfig{
		{Name: replicated, Application: "cephfs"}, {Name: unused, Application: "cephfs"},
		{Name: erasure, Application: "cephfs", ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true}},
	} {
		if _, err := cluster.CreatePool(ctx, config); err != nil {
			t.Fatal(err)
		}
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	replicatedHandle, err := fs.AddDataPool(ctx, replicated)
	if err != nil {
		t.Fatal(err)
	}
	erasureHandle, err := fs.AddDataPool(ctx, erasure)
	if err != nil {
		t.Fatal(err)
	}
	unusedHandle, err := fs.AddDataPool(ctx, unused)
	if err != nil {
		t.Fatal(err)
	}
	states, err := fs.DataPools(ctx)
	if err != nil || len(states) != 4 || !states[0].Default || states[0].Name != fs.DataPool {
		t.Fatalf("dynamic native registration=%+v error=%v", states, err)
	}
	for _, handle := range []*ceph.CephFSDataPool{replicatedHandle, erasureHandle, unusedHandle} {
		pool, err := cluster.PoolStatus(ctx, handle.Name)
		if err != nil || pool.ID != handle.ID || !slices.Contains(states, ceph.CephFSDataPoolState{Name: handle.Name, ID: handle.ID}) {
			t.Fatalf("pool identity was not registered: %+v state=%+v error=%v", handle, pool, err)
		}
	}
	for _, name := range []string{replicated, fs.MetadataPool, fs.DataPool} {
		if handle, err := fs.AddDataPool(ctx, name); err == nil || handle != nil {
			t.Fatalf("already registered/metadata pool %s accepted", name)
		}
	}
	// A namespace outside the default namespace also prevents removal. No
	// external clients or POSIX layouts use this dedicated fresh setup pool.
	cephFSSubvolumeExec(t, ctx, client, "rados", "-p", unused, "-N", "hidden", "put", "marker", "/etc/ceph/ceph.conf")
	if err := fs.RemoveUnusedDataPool(ctx, unusedHandle); err == nil {
		t.Fatal("non-default namespace objects were ignored")
	}
	cephFSSubvolumeExec(t, ctx, client, "rados", "-p", unused, "-N", "hidden", "rm", "marker")
	copy := *unusedHandle
	if err := fs.RemoveUnusedDataPool(ctx, &copy); err != nil {
		t.Fatal(err)
	}
	if err := fs.RemoveUnusedDataPool(ctx, unusedHandle); err != nil {
		t.Fatal("copied removal state did not converge", err)
	}
	if pool, err := cluster.PoolStatus(ctx, unused); err != nil || pool.ID != unusedHandle.ID {
		t.Fatal("detachment deleted or replaced the pool", err)
	}
	data, err := cluster.Ceph(ctx, "osd", "pool", "application", "get", unused, "--format", "json")
	var apps map[string]map[string]string
	if err != nil || json.Unmarshal(data, &apps) != nil || apps["cephfs"]["data"] != filesystem {
		t.Fatalf("detachment changed native tag: %s error=%v", data, err)
	}
	if _, err := fs.AddDataPool(ctx, unused); err == nil {
		t.Fatal("native retained tag was silently cleared for reattachment")
	}
	if volume, err := fs.CreateSubvolume(ctx, ceph.CephFSSubvolumeConfig{Name: "detached", DataPool: unused}); err == nil || volume != nil {
		t.Fatal("detached pool accepted for volume creation")
	}
	// A pool attached after startup can serve namespace-isolated volume data.
	volume, err := fs.CreateSubvolume(ctx, ceph.CephFSSubvolumeConfig{Name: "live", DataPool: replicated, NamespaceIsolated: true})
	if err != nil {
		t.Fatal(err)
	}
	info, err := fs.SubvolumeInfo(ctx, volume.Name, volume.GroupName)
	if err != nil || info.DataPool != replicated || info.PoolNamespace == "" {
		t.Fatalf("dynamic volume layout=%+v error=%v", info, err)
	}
	cephFSSubvolumeIO(t, ctx, client, filesystem, info, "seed", "dynamic-data", 0)
	objects := cephFSSubvolumeExec(t, ctx, client, "rados", "-p", replicated, "-N", info.PoolNamespace, "ls")
	if strings.TrimSpace(string(objects)) == "" {
		t.Fatal("selected dynamic pool contains no actual file data")
	}
	if err := fs.RemoveUnusedDataPool(ctx, replicatedHandle); err == nil {
		t.Fatal("managed-use pool registration removed")
	}
	// Clone placement also consults the current FSMap instead of the startup
	// AdditionalDataPools list. Actual bytes reach the overwrite-enabled EC pool.
	snapshot, err := fs.CreateSubvolumeSnapshot(ctx, volume, "checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	clone, err := fs.CloneSubvolumeSnapshot(ctx, snapshot, ceph.CephFSCloneConfig{Name: "ec-copy", DataPool: erasure})
	if err != nil {
		t.Fatal(err)
	}
	cloned, err := fs.WaitForSubvolumeClone(ctx, clone)
	if err != nil {
		t.Fatal(err)
	}
	clonedInfo, err := fs.SubvolumeInfo(ctx, cloned.Name, cloned.GroupName)
	if err != nil || clonedInfo.DataPool != erasure || clonedInfo.PoolNamespace != "" {
		t.Fatalf("dynamic clone layout=%+v error=%v", clonedInfo, err)
	}
	cephFSSubvolumeIO(t, ctx, client, filesystem, clonedInfo, "verify", "dynamic-data", 0)
	objects = cephFSSubvolumeExec(t, ctx, client, "rados", "-p", erasure, "--all", "ls")
	if strings.TrimSpace(string(objects)) == "" {
		t.Fatal("dynamic EC pool contains no cloned file data")
	}
	if err := fs.RemoveUnusedDataPool(ctx, erasureHandle); err == nil {
		t.Fatal("managed clone pool registration removed")
	}
	states, err = fs.DataPools(ctx)
	if err != nil || len(states) != 3 {
		t.Fatalf("unexpected final registered pools=%+v error=%v", states, err)
	}
	cephFSSubvolumeIO(t, ctx, client, filesystem, info, "verify", "dynamic-data", 0)
	if err := fs.RemoveSubvolume(ctx, cloned); err != nil {
		t.Fatal(err)
	}
	if err := fs.RemoveSubvolumeSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := fs.RemoveSubvolume(ctx, volume); err != nil {
		t.Fatal(err)
	}
	if err := fs.RemoveUnusedDataPool(ctx, replicatedHandle); err == nil {
		t.Fatal("historically used pool became removable after logical volume removal")
	}
	t.Log("dynamic replicated/EC attachment; live native pool IDs; fresh libcephfs bytes and clone placement; namespace object and managed-use guards; unused registration removal preserves pool/tag; detached layout selection and implicit tag cleanup refused")
}
