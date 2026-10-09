//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_pins))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
	"github.com/testcontainers/testcontainers-go"
)

// Pin readback is complemented by native MDS subtree-authority queries. The
// client writes and rereads actual files in fresh Linux libcephfs sessions.
func TestCephFSPins(t *testing.T) {
	for _, host := range []bool{false, true} {
		network := "bridge"
		if host {
			network = "host"
		}
		t.Run(network, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 16*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), cephfs.WithFilesystems(cephfs.Config{Name: "pins", ActiveMDS: 2})}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceCluster(t, opts...)
			fs := cephfs.Filesystems(cluster)[0]
			if err := fs.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}
			for _, setting := range []ceph.ConfigSetting{
				{Section: "mds", Name: "mds_export_ephemeral_distributed", Value: "true"},
				{Section: "mds", Name: "mds_export_ephemeral_random", Value: "true"},
				{Section: "mds", Name: "mds_export_ephemeral_random_max", Value: "1"},
			} {
				change, err := cluster.TemporaryConfig(ctx, setting)
				if change != nil {
					t.Cleanup(func() {
						cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
						defer stop()
						if err := change.Restore(cleanup); err != nil {
							t.Errorf("restore ephemeral feature configuration: %v", err)
						}
					})
				}
				if err != nil {
					t.Fatal(err)
				}
				cephFSPinWaitConfig(t, ctx, cluster, fs, setting.Name, setting.Value)
			}
			group, err := fs.CreateSubvolumeGroup(ctx, cephfs.SubvolumeGroupConfig{Name: "export"})
			if err != nil {
				t.Fatal(err)
			}
			volume, err := fs.CreateSubvolume(ctx, cephfs.SubvolumeConfig{Name: "data", GroupName: group.Name})
			if err != nil {
				t.Fatal(err)
			}
			groupPrior, err := fs.SubvolumeGroupPinPolicy(ctx, group)
			if err != nil {
				t.Fatal(err)
			}
			volumePrior, err := fs.SubvolumePinPolicy(ctx, volume)
			if err != nil || volumePrior.Path != path.Dir(volume.Path) || volumePrior.Path == volume.Path {
				t.Fatalf("pin query did not address native base directory: %+v error=%v", volumePrior, err)
			}
			for _, kind := range []cephfs.PinType{cephfs.PinDistributed, cephfs.PinRandom} {
				cephFSPinGroupLease(t, ctx, fs, group, cephfs.PinSetting{Type: kind, Value: 0})
				cephFSPinVolumeLease(t, ctx, fs, volume, cephfs.PinSetting{Type: kind, Value: 0})
			}
			paths := []string{volume.Path}
			cephFSPinIO(t, ctx, client, fs.FilesystemName, "write", paths)
			groupExport := cephFSPinGroupLease(t, ctx, fs, group, cephfs.PinSetting{Type: cephfs.PinExport, Value: 1})
			cephFSPinWaitAuthority(t, ctx, cluster, fs, map[string]bool{group.Path: true}, 1, cephfs.PinExport)
			cephFSPinIO(t, ctx, client, fs.FilesystemName, "read", paths)
			// A closer subvolume pin overrides the parent group's rank. Verify
			// actual native authority rather than only the assigned xattr.
			volumeExport := cephFSPinVolumeLease(t, ctx, fs, volume, cephfs.PinSetting{Type: cephfs.PinExport, Value: 0})
			cephFSPinWaitAuthority(t, ctx, cluster, fs, map[string]bool{volumePrior.Path: true}, 0, cephfs.PinExport)
			cephFSPinIO(t, ctx, client, fs.FilesystemName, "read", paths)
			if err := volumeExport.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			if err := groupExport.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			groupAfter, err := fs.SubvolumeGroupPinPolicy(ctx, group)
			if err != nil || groupAfter.Inode != groupPrior.Inode || groupAfter.ExportRank != groupPrior.ExportRank {
				t.Fatalf("group original export policy/inode not restored: %+v error=%v", groupAfter, err)
			}
			volumeAfter, err := fs.SubvolumePinPolicy(ctx, volume)
			if err != nil || volumeAfter.Inode != volumePrior.Inode || volumeAfter.ExportRank != volumePrior.ExportRank {
				t.Fatalf("subvolume original export policy/inode not restored: %+v error=%v", volumeAfter, err)
			}
			cephFSPinIO(t, ctx, client, fs.FilesystemName, "read", paths)
			for _, kind := range []cephfs.PinType{cephfs.PinDistributed, cephfs.PinRandom} {
				name := string(kind)
				t.Run(name, func(t *testing.T) {
					group, err := fs.CreateSubvolumeGroup(ctx, cephfs.SubvolumeGroupConfig{Name: name})
					if err != nil {
						t.Fatal(err)
					}
					prior, err := fs.SubvolumeGroupPinPolicy(ctx, group)
					if err != nil {
						t.Fatal(err)
					}
					cephFSPinGroupLease(t, ctx, fs, group, cephfs.PinSetting{Type: cephfs.PinExport, Value: -1})
					other := cephfs.PinDistributed
					if kind == other {
						other = cephfs.PinRandom
					}
					cephFSPinGroupLease(t, ctx, fs, group, cephfs.PinSetting{Type: other, Value: 0})
					change := cephFSPinGroupLease(t, ctx, fs, group, cephfs.PinSetting{Type: kind, Value: 1})
					var paths []string
					ownedBases := make(map[string]bool)
					for index := range 16 {
						volume, err := fs.CreateSubvolume(ctx, cephfs.SubvolumeConfig{Name: fmt.Sprintf("data-%02d", index), GroupName: group.Name})
						if err != nil {
							t.Fatal(err)
						}
						paths = append(paths, volume.Path)
						ownedBases[path.Dir(volume.Path)] = true
					}
					cephFSPinIO(t, ctx, client, fs.FilesystemName, "write", paths)
					if kind == cephfs.PinDistributed {
						// Distributed pins fragment the group directory itself;
						// its children reside under those dirfrag subtrees. Random
						// pins instead export the descendant directory inodes.
						ownedBases = map[string]bool{group.Path: true}
					}
					cephFSPinWaitAuthority(t, ctx, cluster, fs, ownedBases, -1, kind)
					cephFSPinIO(t, ctx, client, fs.FilesystemName, "read", paths)
					if err := change.Restore(ctx); err != nil {
						t.Fatal(err)
					}
					after, err := fs.SubvolumeGroupPinPolicy(ctx, group)
					if err != nil || after.Inode != prior.Inode || (kind == cephfs.PinDistributed && after.Distributed != prior.Distributed) || (kind == cephfs.PinRandom && after.RandomProbability != prior.RandomProbability) {
						t.Fatalf("ephemeral policy restoration changed original field/identity: %+v error=%v", after, err)
					}
					cephFSPinIO(t, ctx, client, fs.FilesystemName, "read", paths)
					t.Logf("native %s pin: owned policy subtrees observed on both active ranks, 16 files retained, original local field restored; distributed pins use group directory fragments and random pins use descendant inodes", kind)
				})
			}
			t.Log("native export pins: parent group rank1 and closer owned subvolume rank0 authority observed, unchanged client bytes and explicit original-value restoration verified")
		})
	}
}
