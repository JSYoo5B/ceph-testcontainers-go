//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_topology))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
)

func TestCephFSMultiActiveStandbyFailoverAndFilesystems(t *testing.T) {
	parallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, cephfs.WithFilesystems(
		cephfs.Config{Name: "alpha", ActiveMDS: 2, StandbyMDS: 1},
		cephfs.Config{Name: "beta"},
	))
	alpha, beta := cephFSOwnedFilesystem(t, cluster, "alpha"), cephFSOwnedFilesystem(t, cluster, "beta")
	alphaBefore := cephFSTopologyStatus(t, ctx, alpha)
	if len(alphaBefore.Active) != 2 || len(alphaBefore.Standby) != 1 || len(alpha.MDSs()) != 3 {
		t.Fatalf("multi-active topology was not built: %+v", alphaBefore)
	}
	betaBefore := cephFSTopologyStatus(t, ctx, beta)
	if alphaBefore.FilesystemID == betaBefore.FilesystemID || alpha.MetadataPool == beta.MetadataPool || alpha.DataPool == beta.DataPool {
		t.Fatal("named filesystems share an identity or backing pool")
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	// Mount each named filesystem using a new userspace session. The same path
	// receives different bytes, proving filesystem selection and isolation.
	cephFSTopologyIO(t, ctx, client, alpha.FilesystemName, "alpha-data", "seed", 1, "")
	cephFSTopologyIO(t, ctx, client, beta.FilesystemName, "beta-data", "seed", 0, "")
	cephFSWaitForPinnedSubtree(t, ctx, cluster, alpha, "/pinned", 1)
	cephFSTopologyIO(t, ctx, client, alpha.FilesystemName, "alpha-data", "verify", 1, "")
	cephFSTopologyIO(t, ctx, client, beta.FilesystemName, "beta-data", "verify", 0, "")

	old := alphaBefore.Active[1]
	standby := alphaBefore.Standby[0]
	failed := cephFSOwnedMDS(t, alpha, old.Name)
	// SIGKILL models a daemon failure without flushing its MDS journal. Native
	// mds fail removes the old GID and triggers replacement by the standby.
	cephFSKillMDS(t, ctx, failed)
	if _, err := cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(old.GID, 10)); err != nil {
		t.Fatal(err)
	}
	cephFSWaitForReplacement(t, ctx, alpha, old, standby)
	cephFSWaitForPinnedSubtree(t, ctx, cluster, alpha, "/pinned", 1)
	cephFSTopologyIO(t, ctx, client, alpha.FilesystemName, "alpha-data", "after-failure", 1, "")
	cephFSTopologyIO(t, ctx, client, beta.FilesystemName, "beta-data", "verify", 0, "")
	betaAfter := cephFSTopologyStatus(t, ctx, beta)
	if len(betaAfter.Active) != 1 || betaAfter.Active[0].GID != betaBefore.Active[0].GID {
		t.Fatal("alpha failure changed beta's active MDS")
	}
	if err := failed.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := alpha.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSTopologyIO(t, ctx, client, alpha.FilesystemName, "alpha-data", "final", 1, "")
	cephFSTopologyIO(t, ctx, client, beta.FilesystemName, "beta-data", "verify", 0, "")
	t.Logf("two isolated filesystems; alpha active ranks 0/1 plus standby; rank1 owns /pinned; killed mds.%s gid=%d replaced by mds.%s; fresh sessions preserved bytes and new writes; restarted daemon restored standby capacity; beta MDS unchanged", old.Name, old.GID, standby.Name)
}

func TestCephFSStandbyReplayFailover(t *testing.T) {
	parallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t)
	fs, err := cephfs.Start(ctx, cluster, cephfs.Config{Name: "hot", StandbyMDS: 1, StandbyReplay: true})
	if err != nil {
		t.Fatal(err)
	}
	before := cephFSTopologyStatus(t, ctx, fs)
	if len(before.Active) != 1 || len(before.StandbyReplay) != 1 || before.StandbyReplay[0].Rank != 0 {
		t.Fatalf("journal-following standby not present: %+v", before)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSTopologyIO(t, ctx, client, fs.FilesystemName, "hot-data", "seed", 0, "")
	old, replay := before.Active[0], before.StandbyReplay[0]
	failed := cephFSOwnedMDS(t, fs, old.Name)
	cephFSKillMDS(t, ctx, failed)
	if _, err := cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(old.GID, 10)); err != nil {
		t.Fatal(err)
	}
	cephFSWaitForReplacement(t, ctx, fs, old, replay)
	cephFSTopologyIO(t, ctx, client, fs.FilesystemName, "hot-data", "after-failure", 0, "")
	if err := failed.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	after := cephFSTopologyStatus(t, ctx, fs)
	if len(after.StandbyReplay) != 1 || after.StandbyReplay[0].Name != old.Name || after.StandbyReplay[0].GID == old.GID {
		t.Fatalf("restarted daemon did not rejoin as a fresh journal follower: %+v", after)
	}
	cephFSTopologyIO(t, ctx, client, fs.FilesystemName, "hot-data", "final", 0, "")
	t.Logf("standby-replay mds.%s took over rank0 after mds.%s gid=%d died; retained file and new write/read verified in fresh sessions; old daemon returned with fresh GID=%d as replay standby", replay.Name, old.Name, old.GID, after.StandbyReplay[0].GID)
}
