//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func testLastMDSReplacement(t *testing.T, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	started := time.Now()
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithOSDCount(2), testcontainers.WithEnv(map[string]string{"CEPH_ARGS": "--mds-beacon-grace=3600"}))
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	oracle := newNoInitialOSDOracle(t, ctx)
	cluster, err := ceph.Run(ctx, image, opts...)
	noInitialOSDCleanup(t, cluster)
	coldMDSFailureLogs(t, cluster)
	if err != nil {
		t.Fatal("last-MDS cluster setup failed", err)
	}
	q, err := cluster.QuorumStatus(ctx)
	if err != nil {
		t.Fatal("original quorum unavailable", err)
	}
	fsid, err := uuid.Parse(q.MonMap.FSID)
	if err != nil || fsid == uuid.Nil || fsid.String() != q.MonMap.FSID {
		t.Fatal("original cluster FSID unavailable")
	}
	client, err := testcontainers.Run(ctx, image, cluster.WithClient(), testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"), testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import cephfs, rados"})))
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
	var customizers atomic.Int32
	target, err := cluster.StartCephFSWithConfig(ctx, ceph.CephFSConfig{Name: "last-target", MetadataPool: pool("last-meta"), DataPool: pool("last-data"), AdditionalDataPools: []ceph.PoolConfig{pool("last-extra")}}, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
	if err != nil || target == nil {
		t.Fatal("ordinary sole-MDS target setup failed", err)
	}
	sibling, err := cluster.StartCephFSWithConfig(ctx, ceph.CephFSConfig{Name: "last-sibling", MetadataPool: pool("last-sibling-meta"), DataPool: pool("last-sibling-data")})
	if err != nil || sibling == nil {
		t.Fatal("ordinary sibling setup failed", err)
	}
	members, siblingMembers := target.MDSs(), sibling.MDSs()
	if len(members) != 1 || len(siblingMembers) != 1 || members[0].ID != "last-target-0" || siblingMembers[0].ID != "last-sibling-0" || target.Container != members[0].Container || sibling.Container != siblingMembers[0].Container || customizers.Load() != 1 || len(cluster.ServiceContainers()) != 2 {
		t.Fatal("original sole target/sibling ownership unavailable")
	}
	old := members[0]
	oldHandle := old.Container
	oldCID := old.GetContainerID()
	oldTask := coldMDSProcess(t, ctx, oracle, old)
	siblingTask := coldMDSProcess(t, ctx, oracle, siblingMembers[0])
	before := lastMDSMap(t, ctx, cluster, "initial", target.FilesystemName, old.ID, sibling.FilesystemName, siblingMembers[0].ID, false)
	originalGID := before.target.active.gid
	identities := map[string]int64{}
	for _, fs := range []*ceph.CephFSContainer{target, sibling} {
		for _, name := range append([]string{fs.MetadataPool, fs.DataPool}, fs.AdditionalDataPools...) {
			p, err := cluster.PoolStatus(ctx, name)
			if err != nil || p.ID < 0 || p.Size != 2 || p.MinSize != 1 {
				t.Fatal("original replicated pool identity unavailable", err)
			}
			identities[name] = p.ID
		}
	}
	if before.target.metadata != identities[target.MetadataPool] || !slices.Equal(before.target.data, []int64{identities[target.DataPool], identities["last-extra"]}) || before.sibling.metadata != identities[sibling.MetadataPool] || !slices.Equal(before.sibling.data, []int64{identities[sibling.DataPool]}) {
		t.Fatal("original native backing pools differ")
	}
	recheck := func(current lastMDSScope) {
		t.Helper()
		if current.target.id != before.target.id || current.target.metadata != before.target.metadata || !slices.Equal(current.target.data, before.target.data) || current.sibling.id != before.sibling.id || current.sibling.metadata != before.sibling.metadata || !slices.Equal(current.sibling.data, before.sibling.data) || current.sibling.active != before.sibling.active || coldMDSProcess(t, ctx, oracle, siblingMembers[0]) != siblingTask {
			t.Fatal("original sibling/FS/pool authority changed")
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
	originalAuth := []string{"mds.last-sibling-0", "mds.last-target-0"}
	coldMDSAuth(t, ctx, cluster, originalAuth)
	coldMDSResources(t, ctx, oracle, cluster, client, "last-initial")
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	coldMDSHealth(t, ctx, cluster, "last-initial", target.FilesystemName, true)
	siblingNonce, originalNonce, newNonce, retiredNonce := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, sibling, siblingNonce, "seed", sibling.DataPool)
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, originalNonce, "seed", "last-extra")
	stoppedMDSGrace(t, ctx, oracle, cluster)
	killed, done := context.WithTimeout(ctx, 20*time.Second)
	_, err = oracle.client.ContainerKill(killed, oldCID, mobycl.ContainerKillOptions{Signal: "KILL"})
	done()
	if err != nil {
		t.Fatal("original target CID kill failed", err)
	}
	stoppedMDSExited(t, ctx, oracle, old, oldTask)
	if time.Since(started) >= 15*time.Minute {
		t.Fatal("preflight escaped the operation umbrella")
	}
	registered := lastMDSMap(t, ctx, cluster, "registered-before", target.FilesystemName, old.ID, sibling.FilesystemName, siblingMembers[0].ID, false)
	if registered.target.active != before.target.active {
		t.Fatal("original registration expired before negative preflight")
	}
	short, stop := context.WithTimeout(ctx, 30*time.Second)
	unexpected, refused := target.AddMDSReplacement(short, old)
	contextErr := short.Err()
	stop()
	if unexpected != nil || refused == nil || contextErr != nil || errors.Is(refused, context.Canceled) || errors.Is(refused, context.DeadlineExceeded) || refused.Error() != "original stopped MDS name is still registered" {
		t.Fatal("still-registered sole MDS was not a completed preflight refusal", refused, contextErr)
	}
	stoppedMDSExited(t, ctx, oracle, old, oldTask)
	afterRefusal := lastMDSMap(t, ctx, cluster, "registered-after", target.FilesystemName, old.ID, sibling.FilesystemName, siblingMembers[0].ID, false)
	if afterRefusal.target.active != before.target.active || len(target.MDSs()) != 1 || target.MDSs()[0] != old || target.Container != old.Container || len(cluster.ServiceContainers()) != 2 || customizers.Load() != 1 {
		t.Fatal("refused preflight mutated original registration/cohort")
	}
	recheck(afterRefusal)
	stoppedMDSGrace(t, ctx, oracle, cluster)
	coldMDSAuth(t, ctx, cluster, originalAuth)
	lastMDSRetainedResources(t, ctx, oracle, cluster, client, "last-registered", old, oldTask)
	t.Logf("LAST_MDS_REFUSED fsid=%s fscid=%d name=%s gid=%d cid=%s started_at=%s registered=true owned_mds=1 customizers=1", q.MonMap.FSID, before.target.id, old.ID, originalGID, oldCID, oldTask.startedAt)
	// Native failure is explicitly caller-controlled, never hidden inside Add.
	if _, err := cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(originalGID, 10)); err != nil {
		t.Fatal("explicit observed native GID fail failed", err)
	}
	failed := lastMDSMap(t, ctx, cluster, "failed", target.FilesystemName, "", sibling.FilesystemName, siblingMembers[0].ID, true)
	if slices.Contains(failed.names, old.ID) || slices.Contains(failed.gids, originalGID) {
		t.Fatal("old name/GID remains globally registered")
	}
	recheck(failed)
	lastMDSFailedHealth(t, ctx, cluster, target.FilesystemName)
	coldMDSAvailability(t, ctx, client, target.FilesystemName)
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, sibling, siblingNonce, "verify", sibling.DataPool)
	recheck(lastMDSMap(t, ctx, cluster, "after-unavailable-client", target.FilesystemName, "", sibling.FilesystemName, siblingMembers[0].ID, true))
	coldMDSAuth(t, ctx, cluster, originalAuth)
	if customizers.Load() != 1 || len(target.MDSs()) != 1 || target.Container != old.Container {
		t.Fatal("read-only failed-rank observation launched a worker")
	}
	lastMDSRetainedResources(t, ctx, oracle, cluster, client, "last-failed", old, oldTask)
	t.Logf("LAST_MDS_FAILED fsid=%s fscid=%d old_name=%s old_gid=%d old_cid=%s global_absent=true in=[0] failed=[0] owned_mds=1", q.MonMap.FSID, before.target.id, old.ID, originalGID, oldCID)
	replacement, err := target.AddMDSReplacement(ctx, old)
	if err != nil || replacement == nil {
		t.Fatal("sole stopped MDS replacement failed", err)
	}
	members = target.MDSs()
	if len(members) != 2 || members[0] != old || old.Container != oldHandle || old.GetContainerID() != oldCID || members[1] != replacement || replacement.ID != "last-target-1" || replacement.FilesystemName != target.FilesystemName || replacement.GetContainerID() == oldCID || customizers.Load() != 2 || target.Container != old.Container || len(cluster.ServiceContainers()) != 3 {
		t.Fatal("new replacement lost exact publication or retained original")
	}
	newTask := coldMDSProcess(t, ctx, oracle, replacement)
	current := lastMDSMap(t, ctx, cluster, "replacement", target.FilesystemName, replacement.ID, sibling.FilesystemName, siblingMembers[0].ID, false)
	if current.target.active.gid == originalGID || slices.Contains(current.names, old.ID) || slices.Contains(current.gids, originalGID) {
		t.Fatal("replacement adopted original native identity")
	}
	recheck(current)
	checkPublic := func(native lastMDSScope) {
		t.Helper()
		status, err := target.MDSStatus(ctx)
		if err != nil || status.FilesystemID != before.target.id || status.MaxMDS != 1 || len(status.Active) != 1 || len(status.Standby) != 0 || len(status.StandbyReplay) != 0 || !status.Active[0].Owned || status.Active[0].Rank != 0 || status.Active[0].Name != replacement.ID || status.Active[0].GID != native.target.active.gid || status.Active[0].State != "up:active" {
			t.Fatal("public replacement logical rank differs from strict native map", err)
		}
	}
	checkPublic(current)
	copy := *old
	retried, err := target.AddMDSReplacement(ctx, &copy)
	if err != nil || retried != replacement || old.Container != oldHandle || old.GetContainerID() != oldCID || customizers.Load() != 2 || target.Container != old.Container || coldMDSProcess(t, ctx, oracle, replacement) != newTask {
		t.Fatal("completed copied replacement retry changed original result", err)
	}
	lastMDSRetainedResources(t, ctx, oracle, cluster, client, "last-replacement", old, oldTask)
	coldMDSAuth(t, ctx, cluster, []string{"mds.last-sibling-0", "mds.last-target-0", "mds.last-target-1"})
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, originalNonce, "verify", "last-extra")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, newNonce, "seed", "last-extra")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, newNonce, "verify", "last-extra")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, sibling, siblingNonce, "verify", sibling.DataPool)
	t.Logf("LAST_MDS_ACTIVE fsid=%s fscid=%d name=%s gid=%d cid=%s started_at=%s pid=%d old_cid=%s owned_mds=2 customizers=2", q.MonMap.FSID, before.target.id, replacement.ID, current.target.active.gid, replacement.GetContainerID(), newTask.startedAt, newTask.pid, oldCID)
	if err := target.RemoveStoppedMDS(ctx, old); err != nil {
		t.Fatal("unchanged Q original stopped CID retirement failed", err)
	}
	stoppedMDSRemoved(t, ctx, oracle, oldCID)
	members = target.MDSs()
	if len(members) != 1 || members[0] != replacement || target.Container != replacement.Container || len(cluster.ServiceContainers()) != 2 {
		t.Fatal("Q retirement did not remove only original CID/registry")
	}
	retried, err = target.AddMDSReplacement(ctx, &copy)
	if err != nil || retried != replacement || customizers.Load() != 2 || coldMDSProcess(t, ctx, oracle, replacement) != newTask || target.Container != replacement.Container {
		t.Fatal("completed copied replacement retry failed after confirmed Q retirement", err)
	}
	if err := target.RemoveStoppedMDS(ctx, &copy); err != nil {
		t.Fatal("completed Q copied receipt retry failed", err)
	}
	stoppedMDSRemoved(t, ctx, oracle, oldCID)
	if err := target.WaitReady(ctx); err != nil {
		t.Fatal("healthy sole replacement did not meet ordinary readiness", err)
	}
	coldMDSResources(t, ctx, oracle, cluster, client, "last-retired")
	coldMDSAuth(t, ctx, cluster, []string{"mds.last-sibling-0", "mds.last-target-0", "mds.last-target-1"})
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, originalNonce, "verify", "last-extra")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, newNonce, "verify", "last-extra")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, retiredNonce, "seed", "last-extra")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, target, retiredNonce, "verify", "last-extra")
	stoppedMDSBytes(t, ctx, client, q.MonMap.FSID, sibling, siblingNonce, "verify", sibling.DataPool)
	final := lastMDSMap(t, ctx, cluster, "final", target.FilesystemName, replacement.ID, sibling.FilesystemName, siblingMembers[0].ID, false)
	if final.target.active != current.target.active || coldMDSProcess(t, ctx, oracle, replacement) != newTask || len(target.MDSs()) != 1 || target.MDSs()[0] != replacement || target.Container != replacement.Container || customizers.Load() != 2 || len(cluster.ServiceContainers()) != 2 {
		t.Fatal("final replacement task/ownership changed after copied retries")
	}
	checkPublic(final)
	recheck(final)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	coldMDSHealth(t, ctx, cluster, "last-final", target.FilesystemName, true)
	t.Logf("LAST_MDS_RETIRED fsid=%s fscid=%d old_cid=%s replacement_cid=%s name=%s gid=%d owned_mds=1 old_auth_retained=true customizers=2", q.MonMap.FSID, before.target.id, oldCID, replacement.GetContainerID(), replacement.ID, final.target.active.gid)
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
	t.Logf("LAST_MDS_COMPLETE fsid=%s target_fscid=%d sibling_fscid=%d old_cid=%s replacement_cid=%s", q.MonMap.FSID, before.target.id, before.sibling.id, oldCID, replacement.GetContainerID())
}

type lastMDSScope struct {
	target, sibling coldMDSScope
	names           []string
	gids            []uint64
}

// The failed state is an established rank in [0], not a cold filesystem in [].
// Two complete FS rows and empty global standbys make old-name absence exact.
func lastMDSMap(t *testing.T, parent context.Context, cluster *ceph.Container, phase, target, active, sibling, siblingActive string, failed bool) lastMDSScope {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	data, err := cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		t.Fatal("strict last-MDS FSMap unavailable", err, ctx.Err())
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
	if json.Unmarshal(data, &native) != nil || native.Standbys == nil || len(*native.Standbys) != 0 || native.Filesystems == nil || len(*native.Filesystems) != 2 {
		t.Fatal("complete two-filesystem/global standby absence unavailable")
	}
	out := lastMDSScope{}
	names, gids, ids := map[string]bool{}, map[uint64]bool{}, map[int64]bool{}
	for _, fs := range *native.Filesystems {
		m := fs.Map
		if fs.ID == nil || *fs.ID <= 0 || ids[*fs.ID] || m == nil || m.Name == nil || (*m.Name != target && *m.Name != sibling) || m.Max == nil || *m.Max != 1 || m.Wanted == nil || *m.Wanted != 0 || m.Metadata == nil || *m.Metadata < 0 || m.Data == nil || len(*m.Data) == 0 || m.In == nil || !slices.Equal(*m.In, []int{0}) || m.Up == nil || m.Info == nil || m.Failed == nil || m.Damaged == nil || m.Stopped == nil || len(*m.Damaged)+len(*m.Stopped) != 0 || m.Flags == nil || m.Flags.Replay == nil || *m.Flags.Replay || m.Flags.Refuse == nil || !*m.Flags.Refuse || m.Flags.Joinable == nil || !*m.Flags.Joinable {
			t.Fatal("strict original FS identity/policy/rank collections unavailable")
		}
		ids[*fs.ID] = true
		seenPool := map[int64]bool{}
		for _, id := range *m.Data {
			if id < 0 || seenPool[id] {
				t.Fatal("original native data attachment invalid/duplicated")
			}
			seenPool[id] = true
		}
		scope := coldMDSScope{id: *fs.ID, metadata: *m.Metadata, data: slices.Clone(*m.Data)}
		if *m.Name == target && failed {
			if active != "" || len(*m.Info) != 0 || len(*m.Up) != 0 || !slices.Equal(*m.Failed, []int{0}) {
				t.Fatal("established failed rank0 not exact empty up/info and failed[0]")
			}
		} else {
			expected := siblingActive
			if *m.Name == target {
				expected = active
			}
			if expected == "" || len(*m.Info) != 1 || len(*m.Up) != 1 || len(*m.Failed) != 0 {
				t.Fatal("original healthy rank0/native inventory incomplete")
			}
			for key, info := range *m.Info {
				if info.Name == nil || *info.Name != expected || names[expected] || info.GID == nil || *info.GID == 0 || gids[*info.GID] || info.Rank == nil || *info.Rank != 0 || info.State == nil || *info.State != "up:active" || info.Join == nil || *info.Join != *fs.ID || key != "gid_"+strconv.FormatUint(*info.GID, 10) || (*m.Up)["mds_0"] != *info.GID {
					t.Fatal("native logical name/GID/rank/affinity contradiction")
				}
				names[expected], gids[*info.GID] = true, true
				out.names = append(out.names, expected)
				out.gids = append(out.gids, *info.GID)
				scope.active = coldMDSActive{expected, *info.GID}
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
	t.Logf("LAST_MDS_MAP phase=%s target=%s fscid=%d metadata_pool_id=%d data_pool_ids=%v active_name=%s active_gid=%d failed=%t max_mds=1 standby_wanted=0 sibling=%s sibling_fscid=%d sibling_active_name=%s sibling_active_gid=%d", phase, target, out.target.id, out.target.metadata, out.target.data, out.target.active.name, out.target.active.gid, failed, sibling, out.sibling.id, out.sibling.active.name, out.sibling.active.gid)
	return out
}

// Only one exact positively exited original CID is admitted; every other new
// session resource must be normally running. Existing all-live readers stay intact.
func lastMDSRetainedResources(t *testing.T, parent context.Context, o *noInitialOSDOracle, cluster *ceph.Container, client testcontainers.Container, phase string, old *ceph.MDSContainer, task coldMDSTask) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	before, err := o.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || before.Info.ID != o.engine {
		t.Fatal("original raw resource engine unavailable")
	}
	expected := map[string]string{}
	add := func(c testcontainers.Container, role string) {
		if c == nil || len(c.GetContainerID()) != 64 || expected[c.GetContainerID()] != "" {
			t.Fatal("missing/shared exact returned container")
		}
		expected[c.GetContainerID()] = role
	}
	add(cluster.ControlContainer(), "mon")
	for _, mgr := range cluster.Managers() {
		add(mgr.Container, "mgr")
	}
	for _, osd := range cluster.OSDs() {
		add(osd.Container, "osd")
	}
	for _, fs := range cluster.Filesystems() {
		for _, mds := range fs.MDSs() {
			add(mds.Container, "mds")
		}
	}
	add(client, "client")
	mon, err := o.client.ContainerInspect(ctx, cluster.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil || mon.Container.ID != cluster.GetContainerID() || mon.Container.Config == nil {
		t.Fatal("original MON/session raw inspection unavailable")
	}
	session := mon.Container.Config.Labels["org.testcontainers.sessionId"]
	if session == "" {
		t.Fatal("original session absent")
	}
	listed, err := o.client.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal("raw complete resource inventory unavailable")
	}
	seen, auxiliary := map[string]bool{}, 0
	for _, item := range listed.Items {
		if o.before[item.ID] {
			continue
		}
		raw, err := o.client.ContainerInspect(ctx, item.ID, mobycl.ContainerInspectOptions{})
		if err != nil || len(item.ID) != 64 || raw.Container.ID != item.ID || raw.Container.Config == nil || raw.Container.State == nil {
			t.Fatal("fresh exact resource CID unavailable")
		}
		config, state := raw.Container.Config, raw.Container.State
		role, owned := expected[item.ID]
		if config.Labels["org.testcontainers.sessionId"] != session {
			t.Fatal("unexpected raw retained resource session")
		}
		if item.ID == old.GetContainerID() {
			start, startErr := time.Parse(time.RFC3339Nano, state.StartedAt)
			finish, finishErr := time.Parse(time.RFC3339Nano, state.FinishedAt)
			if !owned || role != "mds" || state.Status != "exited" || state.Running || state.Paused || state.Restarting || state.Dead || state.Pid != 0 || state.Error != "" || state.ExitCode != 137 || state.StartedAt != task.startedAt || startErr != nil || finishErr != nil || start.IsZero() || !finish.After(start) || !slices.Contains(config.Env, "CEPH_MDS_ID="+old.ID) || !slices.Contains(config.Env, "CEPH_FILESYSTEM="+old.FilesystemName) {
				t.Fatal("exact original exited task changed in retained inventory")
			}
		} else if state.Status != "running" || !state.Running || state.Paused || state.Restarting || state.Dead || state.Pid <= 0 || state.Error != "" {
			t.Fatal("unexpected non-original resource task state")
		}
		if !owned {
			if config.Labels["org.testcontainers.reaper"] != "true" || config.Labels["org.testcontainers.ryuk"] != "true" {
				t.Fatal("unexpected new process outside exact returned owned set")
			}
			auxiliary++
			if auxiliary > 1 {
				t.Fatal("multiple session reapers")
			}
			continue
		}
		entry, cmd := []string{"/bin/sh", "/tc/" + role + ".sh"}, []string(nil)
		if role == "client" {
			entry, cmd = []string{"sleep"}, []string{"infinity"}
		}
		if !slices.Equal(config.Entrypoint, entry) || !slices.Equal(config.Cmd, cmd) {
			t.Fatal("owned resource role changed")
		}
		seen[item.ID] = true
		if !slices.Contains(o.owned, item.ID) {
			o.owned = append(o.owned, item.ID)
		}
	}
	if len(seen) != len(expected) {
		t.Fatal("raw inventory lost exact original returned resources")
	}
	if len(o.networks) == 0 && !cluster.UsesHostNetwork() {
		n, err := o.client.NetworkInspect(ctx, cluster.NetworkName(), mobycl.NetworkInspectOptions{})
		if err != nil || n.Network.ID == "" {
			t.Fatal("original bridge unavailable")
		}
		o.networks = append(o.networks, n.Network.ID)
	}
	after, err := o.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || after.Info.ID != o.engine {
		t.Fatal("raw resource engine changed")
	}
	t.Logf("LAST_MDS_RESOURCES phase=%s engine=%s owned=%d auxiliary=%d stopped_cid=%s", phase, o.engine, len(expected), auxiliary, old.GetContainerID())
}

// This deliberate established failed rank admits exactly the three target
// checks emitted by pinned FSMap/MDSMap; unrelated warnings never pass.
func lastMDSFailedHealth(t *testing.T, parent context.Context, cluster *ceph.Container, target string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	for {
		data := topologyExecOutput(t, ctx, cluster.ControlContainer(), "ceph", "--connect-timeout", "5", "health", "detail", "--format", "json")
		var native struct {
			Status *string `json:"status"`
			Checks *map[string]struct {
				Severity *string `json:"severity"`
				Detail   *[]struct {
					Message *string `json:"message"`
				} `json:"detail"`
			} `json:"checks"`
			Mutes *[]json.RawMessage `json:"mutes"`
		}
		if json.Unmarshal(data, &native) != nil || native.Status == nil || native.Checks == nil || native.Mutes == nil || len(*native.Mutes) != 0 {
			t.Fatal("strict unmuted failed-target health unavailable")
		}
		maxSeverity := "HEALTH_OK"
		for code, check := range *native.Checks {
			if strings.HasPrefix(code, "MGR_MODULE") {
				t.Fatalf("observed required manager module/dependency failure; stop for fixed image-policy classification: %s", data)
			}
			message, severity := "", "HEALTH_WARN"
			switch code {
			case "FS_WITH_FAILED_MDS":
				message = "fs " + target + " has 1 failed mds"
			case "FS_DEGRADED":
				message = "fs " + target + " is degraded"
			case "MDS_ALL_DOWN":
				message, severity = "fs "+target+" is offline because no MDS is active for it.", "HEALTH_ERR"
			default:
				t.Fatalf("unexpected health beyond exact established target failure: %s", data)
			}
			if check.Severity == nil || *check.Severity != severity || check.Detail == nil || len(*check.Detail) != 1 || (*check.Detail)[0].Message == nil || *(*check.Detail)[0].Message != message {
				t.Fatalf("failed-target health cause/severity is not exact: %s", data)
			}
			if severity == "HEALTH_ERR" || maxSeverity == "HEALTH_OK" {
				maxSeverity = severity
			}
		}
		if *native.Status != maxSeverity {
			t.Fatal("failed-target health status/checks contradict")
		}
		if len(*native.Checks) == 3 && *native.Status == "HEALTH_ERR" {
			t.Logf("LAST_MDS_HEALTH phase=last-failed detail=%s", data)
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("native failed health did not reach exact three target checks", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	lastMDSModules(t, ctx, cluster, "last-failed")
}

func lastMDSModules(t *testing.T, ctx context.Context, cluster *ceph.Container, phase string) {
	t.Helper()
	data := topologyExecOutput(t, ctx, cluster.ControlContainer(), "ceph", "--connect-timeout", "5", "mgr", "dump", "--format", "json")
	var native struct {
		Available *bool               `json:"available"`
		GID       *uint64             `json:"active_gid"`
		Modules   *[]string           `json:"modules"`
		Always    map[string][]string `json:"always_on_modules"`
		Catalog   *[]struct {
			Name   *string `json:"name"`
			CanRun *bool   `json:"can_run"`
			Error  *string `json:"error_string"`
		} `json:"available_modules"`
	}
	if json.Unmarshal(data, &native) != nil || native.Available == nil || !*native.Available || native.GID == nil || *native.GID == 0 || native.Modules == nil || native.Always == nil || len(native.Always) == 0 || native.Catalog == nil {
		t.Fatal("strict active MGR catalog unavailable")
	}
	required := map[string]bool{"volumes": true, "rbd_support": true, "mirroring": true}
	for _, name := range *native.Modules {
		if name == "" {
			t.Fatal("empty enabled MGR module")
		}
		required[name] = true
	}
	for _, names := range native.Always {
		if names == nil {
			t.Fatal("null always-on release set")
		}
		for _, name := range names {
			if name == "" {
				t.Fatal("empty always-on MGR module")
			}
			required[name] = true
		}
	}
	seen, selected := map[string]bool{}, map[string]bool{}
	for _, module := range *native.Catalog {
		if module.Name == nil || *module.Name == "" || seen[*module.Name] || module.CanRun == nil || module.Error == nil {
			t.Fatal("MGR available catalog malformed")
		}
		seen[*module.Name] = true
		if required[*module.Name] {
			if !*module.CanRun || *module.Error != "" {
				t.Fatalf("required manager payload/dependency unavailable; stop for fixed image-policy classification: module=%s can_run=%t error=%s", *module.Name, *module.CanRun, *module.Error)
			}
			selected[*module.Name] = true
		}
	}
	for name := range required {
		if !selected[name] {
			t.Fatalf("required manager module absent; stop for fixed image-policy classification: %s", name)
		}
	}
	encoded, _ := json.Marshal(selected)
	t.Logf("LAST_MDS_MODULES phase=%s active_gid=%d can_run=%s", phase, *native.GID, encoded)
}
