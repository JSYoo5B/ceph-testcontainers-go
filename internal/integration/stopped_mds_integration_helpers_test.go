//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func testStoppedMDSRetirement(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	started := time.Now()
	image, opts := integrationImages(t)
	// v20.2.4 MON checks this startup value before automatic MDS expiry. The
	// operation umbrella is shorter than the grace; effective MON readback and
	// native registration before/after refusal are mandatory, never a skip.
	opts = append(opts, ceph.WithOSDCount(2), testcontainers.WithEnv(map[string]string{"CEPH_ARGS": "--mds-beacon-grace=3600"}))
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	oracle := newNoInitialOSDOracle(t, ctx)
	cluster, err := ceph.Run(ctx, image, opts...)
	noInitialOSDCleanup(t, cluster)
	coldMDSFailureLogs(t, cluster)
	if err != nil {
		t.Fatal("stopped-MDS cluster setup failed", err)
	}
	q, err := cluster.QuorumStatus(ctx)
	if err != nil {
		t.Fatal("original quorum unavailable", err)
	}
	fsid, err := uuid.Parse(q.MonMap.FSID)
	if err != nil || fsid == uuid.Nil || fsid.String() != q.MonMap.FSID {
		t.Fatal("original cluster FSID unavailable")
	}
	client, err := testcontainers.Run(ctx, image, cluster.WithClient(), ceph.WithIdleEntrypoint(), testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import cephfs, rados"})))
	clientTerminated := false
	if client != nil {
		t.Cleanup(func() {
			if clientTerminated {
				return
			}
			cleanup, done := context.WithTimeout(context.Background(), time.Minute)
			defer done()
			if err := client.Terminate(cleanup); err != nil {
				t.Error("native client cleanup failed", err)
			}
		})
	}
	if err != nil {
		t.Fatal("native client setup failed", err)
	}
	pool := func(name string) ceph.PoolConfig {
		return ceph.PoolConfig{Name: name, PGNum: 8, Replicas: 2, MinSize: 1}
	}
	target, err := cephfs.Start(ctx, cluster, cephfs.Config{Name: "replacement-target", StandbyMDS: 1, MetadataPool: pool("replacement-meta"), DataPool: pool("replacement-data"), AdditionalDataPools: []ceph.PoolConfig{pool("replacement-extra")}})
	if err != nil || target == nil {
		t.Fatal("ordinary target setup failed", err)
	}
	sibling, err := cephfs.Start(ctx, cluster, cephfs.Config{Name: "replacement-sibling", MetadataPool: pool("replacement-sibling-meta"), DataPool: pool("replacement-sibling-data")})
	if err != nil || sibling == nil {
		t.Fatal("ordinary sibling setup failed", err)
	}
	report := cephFSTopologyStatus(t, ctx, target)
	if report.MaxMDS != 1 || len(report.Active) != 1 || len(report.Standby) != 1 || len(report.StandbyReplay) != 0 || !report.Active[0].Owned || !report.Standby[0].Owned {
		t.Fatal("original target active/ordinary standby unavailable")
	}
	old, takeover := report.Active[0], report.Standby[0]
	failed := cephFSOwnedMDS(t, target, old.Name)
	survivor := cephFSOwnedMDS(t, target, takeover.Name)
	if failed.ID != "replacement-target-0" || survivor.ID != "replacement-target-1" || target.Container != failed.Container {
		t.Fatal("original indexed MDS/compatibility handle differs")
	}
	oldCID, survivorCID := failed.GetContainerID(), survivor.GetContainerID()
	oldTask := coldMDSProcess(t, ctx, oracle, failed)
	survivorTask := coldMDSProcess(t, ctx, oracle, survivor)
	siblingMembers := sibling.MDSs()
	if len(siblingMembers) != 1 {
		t.Fatal("exact sibling ownership unavailable")
	}
	siblingTask := coldMDSProcess(t, ctx, oracle, siblingMembers[0])
	before := stoppedMDSMap(t, ctx, cluster, target.FilesystemName, old.Name, takeover.Name, sibling.FilesystemName, siblingMembers[0].ID)
	if before.target.active.gid != old.GID || before.standby.gid != takeover.GID || old.GID == takeover.GID || before.target.id == before.sibling.id {
		t.Fatal("public observations differ from independent original FSMap")
	}
	identities := map[string]int64{}
	for _, fs := range []*cephfs.Filesystem{target, sibling} {
		for _, name := range append([]string{fs.MetadataPool, fs.DataPool}, fs.AdditionalDataPools...) {
			p, err := cluster.PoolStatus(ctx, name)
			if err != nil || p.ID < 0 || p.Size != 2 || p.MinSize != 1 {
				t.Fatal("original replicated pool identity unavailable", err)
			}
			identities[name] = p.ID
		}
	}
	if before.target.metadata != identities[target.MetadataPool] || !slices.Equal(before.target.data, []int64{identities[target.DataPool], identities["replacement-extra"]}) || before.sibling.metadata != identities[sibling.MetadataPool] || !slices.Equal(before.sibling.data, []int64{identities[sibling.DataPool]}) {
		t.Fatal("original native backing pools differ")
	}
	recheck := func(current stoppedMDSScopes) {
		t.Helper()
		if current.target.id != before.target.id || current.target.metadata != before.target.metadata || !slices.Equal(current.target.data, before.target.data) || current.sibling.id != before.sibling.id || current.sibling.metadata != before.sibling.metadata || !slices.Equal(current.sibling.data, before.sibling.data) || current.sibling.active != before.sibling.active || coldMDSProcess(t, ctx, oracle, siblingMembers[0]) != siblingTask || coldMDSProcess(t, ctx, oracle, survivor) != survivorTask || survivor.GetContainerID() != survivorCID {
			t.Fatal("original survivor/sibling/FS/pool authority changed")
		}
		fresh, err := cluster.QuorumStatus(ctx)
		if err != nil || fresh.MonMap.FSID != q.MonMap.FSID {
			t.Fatal("original cluster changed", err)
		}
		for name, id := range identities {
			p, err := cluster.PoolStatus(ctx, name)
			if err != nil || p.ID != id || p.Size != 2 || p.MinSize != 1 {
				t.Fatal("original pool changed", err)
			}
		}
	}
	originalAuth := []string{"mds.replacement-sibling-0", "mds.replacement-target-0", "mds.replacement-target-1"}
	coldMDSAuth(t, ctx, cluster, originalAuth)
	coldMDSResources(t, ctx, oracle, cluster, client, "replacement-initial")
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	coldMDSHealth(t, ctx, cluster, "replacement-initial", target.FilesystemName, true)
	siblingNonce, originalNonce, takeoverNonce, replacementNonce := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, sibling, siblingNonce, "seed", sibling.DataPool)
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, originalNonce, "seed", "replacement-extra")
	stoppedMDSGrace(t, ctx, oracle, cluster)
	killed, done := context.WithTimeout(ctx, 20*time.Second)
	_, err = oracle.client.ContainerKill(killed, oldCID, mobycl.ContainerKillOptions{Signal: "KILL"})
	done()
	if err != nil {
		t.Fatal("original CID kill failed", err)
	}
	stoppedMDSExited(t, ctx, oracle, failed, oldTask)
	if time.Since(started) >= 15*time.Minute {
		t.Fatal("preflight escaped the operation umbrella")
	}
	registered := stoppedMDSMap(t, ctx, cluster, target.FilesystemName, old.Name, takeover.Name, sibling.FilesystemName, siblingMembers[0].ID)
	if registered.target.active != before.target.active || registered.standby != before.standby {
		t.Fatal("original native registration expired before negative admission")
	}
	preflight, stop := context.WithTimeout(ctx, 30*time.Second)
	refused := target.RemoveStoppedMDS(preflight, failed)
	contextErr := preflight.Err()
	stop()
	if refused == nil || contextErr != nil || errors.Is(refused, context.Canceled) || errors.Is(refused, context.DeadlineExceeded) || refused.Error() != "original stopped MDS name is still registered" {
		t.Fatal("stopped registered MDS removal was not a completed preflight refusal", refused, contextErr)
	}
	stoppedMDSExited(t, ctx, oracle, failed, oldTask)
	afterRefusal := stoppedMDSMap(t, ctx, cluster, target.FilesystemName, old.Name, takeover.Name, sibling.FilesystemName, siblingMembers[0].ID)
	if afterRefusal.target.active != before.target.active || afterRefusal.standby != before.standby || len(target.MDSs()) != 2 || len(cluster.ServiceContainers()) != 3 || target.Container != failed.Container {
		t.Fatal("refused preflight mutated original registry/native registration")
	}
	recheck(afterRefusal)
	stoppedMDSGrace(t, ctx, oracle, cluster)
	coldMDSAuth(t, ctx, cluster, originalAuth)
	t.Logf("STOPPED_MDS_REFUSED fsid=%s fscid=%d name=%s gid=%d cid=%s started_at=%s registered=true owned_mds=2 grace_seconds=3600", q.MonMap.FSID, before.target.id, old.Name, old.GID, oldCID, oldTask.startedAt)
	// Native GID failure is caller-controlled and separate from CID retirement.
	if _, err := cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(old.GID, 10)); err != nil {
		t.Fatal("explicit observed native GID fail failed", err)
	}
	cephFSWaitForReplacement(t, ctx, target, old, takeover)
	promoted := stoppedMDSMap(t, ctx, cluster, target.FilesystemName, takeover.Name, "", sibling.FilesystemName, siblingMembers[0].ID)
	if promoted.target.active.gid != takeover.GID || slices.Contains(promoted.names, old.Name) || slices.Contains(promoted.gids, old.GID) {
		t.Fatal("original name/GID remains globally registered or wrong owned takeover")
	}
	recheck(promoted)
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, originalNonce, "verify", "replacement-extra")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, takeoverNonce, "seed", "replacement-extra")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, sibling, siblingNonce, "verify", sibling.DataPool)
	t.Logf("STOPPED_MDS_TAKEOVER fsid=%s fscid=%d old_name=%s old_gid=%d absent_globally=true active_name=%s active_gid=%d active_cid=%s", q.MonMap.FSID, before.target.id, old.Name, old.GID, takeover.Name, takeover.GID, survivorCID)
	if err := target.RemoveStoppedMDS(ctx, failed); err != nil {
		t.Fatal("original stopped CID retirement failed", err)
	}
	stoppedMDSRemoved(t, ctx, oracle, oldCID)
	members := target.MDSs()
	if len(members) != 1 || members[0] != survivor || target.Container != survivor.Container || len(cluster.ServiceContainers()) != 2 {
		t.Fatal("retirement did not remove only original canonical member/service")
	}
	recheck(stoppedMDSMap(t, ctx, cluster, target.FilesystemName, takeover.Name, "", sibling.FilesystemName, siblingMembers[0].ID))
	coldMDSAuth(t, ctx, cluster, originalAuth)
	coldMDSResources(t, ctx, oracle, cluster, client, "replacement-retired")
	short, stop := context.WithTimeout(ctx, 3*time.Second)
	err = target.WaitReady(short)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("missing desired standby implied readiness or lost deadline", err)
	}
	copy := *failed
	if err := target.RemoveStoppedMDS(ctx, &copy); err != nil {
		t.Fatal("completed copied receipt retry failed", err)
	}
	stoppedMDSRemoved(t, ctx, oracle, oldCID)
	t.Logf("STOPPED_MDS_RETIRED fsid=%s fscid=%d old_name=%s old_gid=%d old_cid=%s owned_mds=1 max_mds=1 standby_wanted=1 auth_retained=true", q.MonMap.FSID, before.target.id, old.Name, old.GID, oldCID)
	if err := target.ScaleMDS(ctx, 1, 1); err != nil {
		t.Fatal("existing ScaleMDS replacement failed", err)
	}
	if err := target.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	members = target.MDSs()
	if len(members) != 2 || members[0] != survivor || members[1].ID != "replacement-target-2" || members[1].GetContainerID() == oldCID || members[1].GetContainerID() == survivorCID || len(cluster.ServiceContainers()) != 3 || target.Container != survivor.Container {
		t.Fatal("replacement adopted old name/CID or changed original survivor")
	}
	replacement := members[1]
	current := stoppedMDSMap(t, ctx, cluster, target.FilesystemName, takeover.Name, replacement.ID, sibling.FilesystemName, siblingMembers[0].ID)
	if current.target.active.gid != takeover.GID || current.standby.gid == old.GID || current.standby.gid == takeover.GID || slices.Contains(current.names, old.Name) || slices.Contains(current.gids, old.GID) {
		t.Fatal("replacement native identity collides or old name/GID returned")
	}
	newTask := coldMDSProcess(t, ctx, oracle, replacement)
	recheck(current)
	if err := target.RemoveStoppedMDS(ctx, &copy); err != nil {
		t.Fatal("completed original receipt rejected after replacement", err)
	}
	currentMembers := target.MDSs()
	if len(currentMembers) != 2 || currentMembers[0] != survivor || currentMembers[1] != replacement || target.Container != survivor.Container || coldMDSProcess(t, ctx, oracle, replacement) != newTask {
		t.Fatal("completed original receipt changed replacement cohort/embed/task")
	}
	stoppedMDSRemoved(t, ctx, oracle, oldCID)
	coldMDSAuth(t, ctx, cluster, append(slices.Clone(originalAuth), "mds.replacement-target-2"))
	coldMDSResources(t, ctx, oracle, cluster, client, "replacement-final")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, originalNonce, "verify", "replacement-extra")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, takeoverNonce, "verify", "replacement-extra")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, replacementNonce, "seed", "replacement-extra")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, replacementNonce, "verify", "replacement-extra")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, sibling, siblingNonce, "verify", sibling.DataPool)
	final := stoppedMDSMap(t, ctx, cluster, target.FilesystemName, takeover.Name, replacement.ID, sibling.FilesystemName, siblingMembers[0].ID)
	if final.target.active != current.target.active || final.standby != current.standby || coldMDSProcess(t, ctx, oracle, replacement) != newTask {
		t.Fatal("replacement native identity/task changed during final data checks")
	}
	recheck(final)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	coldMDSHealth(t, ctx, cluster, "replacement-final", target.FilesystemName, true)
	t.Logf("STOPPED_MDS_REPLACEMENT fsid=%s fscid=%d name=%s gid=%d cid=%s started_at=%s pid=%d active_name=%s active_gid=%d owned_mds=2", q.MonMap.FSID, before.target.id, replacement.ID, current.standby.gid, replacement.GetContainerID(), newTask.startedAt, newTask.pid, takeover.Name, takeover.GID)
	cleanup, finish := context.WithTimeout(context.Background(), 3*time.Minute)
	defer finish()
	if err := client.Terminate(cleanup); err != nil {
		t.Fatal("native client termination failed", err)
	}
	clientTerminated = true
	if err := cluster.Terminate(cleanup); err != nil {
		t.Fatal("cluster termination failed", err)
	}
	oracle.assertRemoved(t, cleanup)
	t.Logf("STOPPED_MDS_COMPLETE fsid=%s target_fscid=%d sibling_fscid=%d old_cid=%s replacement_cid=%s", q.MonMap.FSID, before.target.id, before.sibling.id, oldCID, replacement.GetContainerID())
}

func stoppedMDSGrace(t *testing.T, parent context.Context, o *noInitialOSDOracle, cluster *ceph.Container) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	before, err := o.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || before.Info.ID != o.engine {
		t.Fatal("original MON engine unavailable")
	}
	cid := cluster.GetContainerID()
	raw, err := o.client.ContainerInspect(ctx, cid, mobycl.ContainerInspectOptions{})
	if err != nil || len(cid) != 64 || raw.Container.ID != cid || raw.Container.Config == nil || raw.Container.State == nil || !raw.Container.State.Running || !slices.Contains(raw.Container.Config.Env, "CEPH_ARGS=--mds-beacon-grace=3600") {
		t.Fatal("original MON startup grace environment unavailable")
	}
	data := topologyExecOutput(t, ctx, cluster.ControlContainer(), "ceph", "--connect-timeout", "5", "config", "show", "mon.a", "mds_beacon_grace")
	value, err := strconv.ParseFloat(strings.TrimSpace(string(data)), 64)
	if err != nil || value != 3600 {
		t.Fatalf("MON effective beacon grace not positively 3600; fixture configuration failed: %q", data)
	}
	after, err := o.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || after.Info.ID != o.engine {
		t.Fatal("original MON engine changed")
	}
	t.Logf("STOPPED_MDS_GRACE mon_cid=%s engine=%s seconds=3600", cid, o.engine)
}

func stoppedMDSExited(t *testing.T, parent context.Context, o *noInitialOSDOracle, mds *cephfs.MDS, original coldMDSTask) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	for {
		before, err := o.client.Info(ctx, mobycl.InfoOptions{})
		if err != nil || before.Info.ID != o.engine {
			t.Fatal("original stopped-CID engine unavailable")
		}
		cid := mds.GetContainerID()
		raw, err := o.client.ContainerInspect(ctx, cid, mobycl.ContainerInspectOptions{})
		if err != nil || len(cid) != 64 || raw.Container.ID != cid || raw.Container.State == nil || raw.Container.Config == nil {
			t.Fatal("original stopped CID not positively inspectable")
		}
		s := raw.Container.State
		if s.Running {
			select {
			case <-ctx.Done():
				t.Fatal("killed original CID remained running", ctx.Err())
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		finish, err := time.Parse(time.RFC3339Nano, s.FinishedAt)
		begin, beginErr := time.Parse(time.RFC3339Nano, s.StartedAt)
		if err != nil || beginErr != nil || finish.IsZero() || !finish.After(begin) || s.StartedAt != original.startedAt || s.Status != "exited" || s.Paused || s.Restarting || s.Dead || s.Pid != 0 || s.Error != "" || s.ExitCode != 137 || !slices.Equal(raw.Container.Config.Entrypoint, []string{"/bin/sh", "/tc/mds.sh"}) || !slices.Contains(raw.Container.Config.Env, "CEPH_MDS_ID="+mds.ID) || !slices.Contains(raw.Container.Config.Env, "CEPH_FILESYSTEM="+mds.FilesystemName) {
			t.Fatal("original killed CID has incoherent stopped task/configuration")
		}
		actual, err := mds.State(ctx)
		if err != nil || actual == nil || actual.StartedAt != s.StartedAt || actual.FinishedAt != s.FinishedAt || actual.Running || actual.Status != s.Status || actual.Pid != s.Pid || actual.ExitCode != s.ExitCode {
			t.Fatal("returned stopped handle differs from exact original raw CID")
		}
		after, err := o.client.Info(ctx, mobycl.InfoOptions{})
		if err != nil || after.Info.ID != o.engine {
			t.Fatal("original stopped-CID engine changed")
		}
		t.Logf("STOPPED_MDS_EXITED engine=%s cid=%s name=%s started_at=%s finished_at=%s exit_code=%d", o.engine, cid, mds.ID, s.StartedAt, s.FinishedAt, s.ExitCode)
		return
	}
}

func stoppedMDSRemoved(t *testing.T, parent context.Context, o *noInitialOSDOracle, cid string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	before, err := o.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || before.Info.ID != o.engine {
		t.Fatal("original retirement engine unavailable")
	}
	if _, err := o.client.ContainerInspect(ctx, cid, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("exact original CID remains present or removal is uncertain", err)
	}
	after, err := o.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || after.Info.ID != o.engine {
		t.Fatal("original retirement engine changed")
	}
	t.Logf("STOPPED_MDS_REMOVED engine=%s cid=%s absent=true", o.engine, cid)
}

type stoppedMDSScopes struct {
	target, sibling coldMDSScope
	standby         coldMDSActive
	names           []string
	gids            []uint64
}

// All fixture FSMap rows are parsed independently of MDSStatus. Exact settled
// rank and ordinary-standby membership make old-name/GID global absence useful;
// omitted/null collections never stand for absence.
func stoppedMDSMap(t *testing.T, parent context.Context, cluster *ceph.Container, target, active, standby, sibling, siblingActive string) stoppedMDSScopes {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	data, err := cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		t.Fatal("strict original FSMap unavailable", ctx.Err())
	}
	var native struct {
		Standbys    *[]coldMDSInfo `json:"standbys"`
		Filesystems *[]struct {
			ID  *int64 `json:"id"`
			Map *struct {
				Name     *string                 `json:"fs_name"`
				Max      *int                    `json:"max_mds"`
				Wanted   *int                    `json:"standby_count_wanted"`
				Metadata *int64                  `json:"metadata_pool"`
				Data     *[]int64                `json:"data_pools"`
				In       *[]int                  `json:"in"`
				Up       *map[string]uint64      `json:"up"`
				Info     *map[string]coldMDSInfo `json:"info"`
				Failed   *[]int                  `json:"failed"`
				Damaged  *[]int                  `json:"damaged"`
				Stopped  *[]int                  `json:"stopped"`
				Flags    *struct {
					Replay   *bool `json:"allow_standby_replay"`
					Refuse   *bool `json:"refuse_standby_for_another_fs"`
					Joinable *bool `json:"joinable"`
				} `json:"flags_state"`
			} `json:"mdsmap"`
		} `json:"filesystems"`
	}
	if json.Unmarshal(data, &native) != nil || native.Standbys == nil || native.Filesystems == nil || len(*native.Filesystems) != 2 {
		t.Fatal("strict complete two-filesystem/global standby map unavailable")
	}
	out := stoppedMDSScopes{}
	names, gids, ids := map[string]bool{}, map[uint64]bool{}, map[int64]bool{}
	row := func(info coldMDSInfo, name, state string, rank int, join int64) coldMDSActive {
		if info.Name == nil || *info.Name != name || name == "" || info.GID == nil || *info.GID == 0 || info.Rank == nil || *info.Rank != rank || info.State == nil || *info.State != state || info.Join == nil || *info.Join != join || names[name] || gids[*info.GID] {
			t.Fatal("strict independent native name/GID/rank/state/affinity unavailable")
		}
		names[name], gids[*info.GID] = true, true
		out.names = append(out.names, name)
		out.gids = append(out.gids, *info.GID)
		return coldMDSActive{name, *info.GID}
	}
	for _, fs := range *native.Filesystems {
		m := fs.Map
		if fs.ID == nil || *fs.ID <= 0 || ids[*fs.ID] || m == nil || m.Name == nil || (*m.Name != target && *m.Name != sibling) || m.Max == nil || *m.Max != 1 || m.Wanted == nil || m.Metadata == nil || *m.Metadata < 0 || m.Data == nil || len(*m.Data) == 0 || m.In == nil || !slices.Equal(*m.In, []int{0}) || m.Up == nil || len(*m.Up) != 1 || m.Info == nil || len(*m.Info) != 1 || m.Failed == nil || m.Damaged == nil || m.Stopped == nil || len(*m.Failed)+len(*m.Damaged)+len(*m.Stopped) != 0 || m.Flags == nil || m.Flags.Replay == nil || *m.Flags.Replay || m.Flags.Refuse == nil || !*m.Flags.Refuse || m.Flags.Joinable == nil || !*m.Flags.Joinable {
			t.Fatal("strict settled original FS identity/policy/collections unavailable")
		}
		ids[*fs.ID] = true
		seenPool := map[int64]bool{}
		for _, id := range *m.Data {
			if id < 0 || seenPool[id] {
				t.Fatal("native original data attachment invalid/duplicated")
			}
			seenPool[id] = true
		}
		expected, wanted := siblingActive, 0
		if *m.Name == target {
			expected, wanted = active, 1
		}
		if *m.Wanted != wanted {
			t.Fatal("original standby-count policy changed")
		}
		scope := coldMDSScope{id: *fs.ID, metadata: *m.Metadata, data: slices.Clone(*m.Data)}
		for key, info := range *m.Info {
			scope.active = row(info, expected, "up:active", 0, *fs.ID)
			if key != "gid_"+strconv.FormatUint(scope.active.gid, 10) || (*m.Up)["mds_0"] != scope.active.gid {
				t.Fatal("native rank/up/info membership contradicts")
			}
		}
		if *m.Name == target {
			if out.target.id != 0 {
				t.Fatal("duplicate target filesystem")
			}
			out.target = scope
		} else {
			if out.sibling.id != 0 {
				t.Fatal("duplicate sibling filesystem")
			}
			out.sibling = scope
		}
	}
	if out.target.id == 0 || out.sibling.id == 0 {
		t.Fatal("original target/sibling missing")
	}
	wantStandbys := 0
	if standby != "" {
		wantStandbys = 1
	}
	if len(*native.Standbys) != wantStandbys {
		t.Fatal("unexpected global ordinary standby inventory")
	}
	for _, info := range *native.Standbys {
		out.standby = row(info, standby, "up:standby", -1, out.target.id)
	}
	t.Logf("STOPPED_MDS_MAP target=%s fscid=%d metadata_pool_id=%d data_pool_ids=%v active_name=%s active_gid=%d standby_name=%s standby_gid=%d max_mds=1 standby_wanted=1 sibling=%s sibling_fscid=%d sibling_active_name=%s sibling_active_gid=%d", target, out.target.id, out.target.metadata, out.target.data, out.target.active.name, out.target.active.gid, out.standby.name, out.standby.gid, sibling, out.sibling.id, out.sibling.active.name, out.sibling.active.gid)
	return out
}

// Reuse the existing bounded libcephfs reader with one independent path per
// nonce. Later writes cannot overwrite the earlier takeover/sibling controls.
func stoppedMDSBytes(t *testing.T, ctx context.Context, client testcontainers.Container, fsid string, fs *cephfs.Filesystem, nonce, phase, pool string) {
	t.Helper()
	id, err := uuid.Parse(nonce)
	if err != nil || id == uuid.Nil || id.String() != nonce {
		t.Fatal("native nonce path is not canonical")
	}
	script := strings.ReplaceAll(coldMDSBytesScript, "/nonce", "/stopped-"+nonce)
	data := topologyExecOutput(t, ctx, client, "python3", "-c", script, fsid, fs.FilesystemName, nonce, phase, pool)
	var result struct {
		FSID       string `json:"fsid"`
		Filesystem string `json:"filesystem"`
		Phase      string `json:"phase"`
		Pool       string `json:"pool"`
		Bytes      int    `json:"bytes"`
		SHA256     string `json:"sha256"`
	}
	expected := []byte(strings.Repeat(nonce, (128<<10)/len(nonce)+1))[:128<<10]
	// libcephfs logs to stderr around the JSON result.
	if json.Unmarshal([]byte(lastJSONLine(string(data))), &result) != nil || result.FSID != fsid || result.Filesystem != fs.FilesystemName || result.Phase != phase || result.Pool != pool || result.Bytes != len(expected) || result.SHA256 != fmt.Sprintf("%x", sha256.Sum256(expected)) {
		t.Fatalf("fresh scoped libcephfs identity/layout/nonce bytes differ: %s", data)
	}
	t.Logf("STOPPED_MDS_BYTES filesystem=%s phase=%s fsid=%s pool=%s bytes=%d sha256=%s client_cid=%s", fs.FilesystemName, phase, fsid, pool, result.Bytes, result.SHA256, client.GetContainerID())
}
