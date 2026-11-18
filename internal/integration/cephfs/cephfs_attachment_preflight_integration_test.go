//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_data))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/subvolume"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestCephFSUnusedAttachmentAfterPreflight(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	cluster, _ := harness.NewServiceClusterRun(t, harness.CEPHFSRun, image, append(opts, ceph.WithOSDStorage(ceph.OSDStorage{}.WithBlockSize(1<<30).WithStartupBenchmark(ceph.OSDBenchmarkSkip)))...)
	fs := cephfs.Filesystems(cluster)[0]
	if _, err := subvolume.CreateGroup(ctx, fs, subvolume.GroupConfig{Name: "existing"}); err != nil {
		t.Fatal(err)
	}
	volume, err := subvolume.Create(ctx, fs, subvolume.Config{Name: "existing"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := volume.CreateSnapshot(ctx, "source")
	if err != nil {
		t.Fatal(err)
	}
	for index, mode := range []string{"group duplicate", "subvolume duplicate", "clone duplicate", "removed source"} {
		t.Run(mode, func(t *testing.T) {
			pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: fmt.Sprintf("preflight-%d", index), Application: "cephfs"})
			if err != nil {
				t.Fatal(err)
			}
			attachment, err := fs.AddDataPool(ctx, pool.Name)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "group duplicate":
				_, err = subvolume.CreateGroup(ctx, fs, subvolume.GroupConfig{Name: "existing", DataPool: pool.Name})
			case "subvolume duplicate":
				_, err = subvolume.Create(ctx, fs, subvolume.Config{Name: "existing", DataPool: pool.Name})
			case "clone duplicate":
				_, err = snapshot.Clone(ctx, subvolume.CloneConfig{Name: "existing", DataPool: pool.Name})
			case "removed source":
				if err := snapshot.Remove(ctx); err != nil {
					t.Fatal(err)
				}
				_, err = snapshot.Clone(ctx, subvolume.CloneConfig{Name: "copy", DataPool: pool.Name})
			}
			if err == nil {
				t.Fatal("invalid creation accepted")
			}
			if err := attachment.RemoveUnused(ctx); err != nil {
				t.Fatalf("preflight made attachment sticky: %v", err)
			}
			if err := pool.Remove(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
