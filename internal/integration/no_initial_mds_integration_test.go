//go:build integration && topology

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// A cold filesystem retains its original storage and first-start ownership;
// the ordinary sibling is an independent live client/identity control.
func TestNoInitialMDSTopology(t *testing.T) {
	for _, host := range []bool{false, true} {
		network := "bridge"
		if host {
			network = "host"
		}
		if !t.Run(network, func(t *testing.T) { testNoInitialMDS(t, host) }) {
			return
		}
	}
}

func testNoInitialMDS(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithOSDCount(2))
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	oracle := newNoInitialOSDOracle(t, ctx)
	cluster, err := ceph.Run(ctx, image, opts...)
	noInitialOSDCleanup(t, cluster) // Install partial ownership cleanup before checking error.
	coldMDSFailureLogs(t, cluster)  // LIFO: preserve native startup grounds before cluster removal.
	if err != nil {
		t.Fatal(err)
	}
	q, err := cluster.QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
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
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal("native client setup failed", err)
	}
	pool := func(name string) ceph.PoolConfig {
		return ceph.PoolConfig{Name: name, PGNum: 8, Replicas: 2, MinSize: 1}
	}
	sibling, err := cluster.StartCephFSWithConfig(ctx, ceph.CephFSConfig{Name: "warm-sibling", MetadataPool: pool("warm-sibling-meta"), DataPool: pool("warm-sibling-data")})
	if err != nil || sibling == nil {
		t.Fatal("ordinary sibling setup failed", err)
	}
	beforeSibling := coldMDSMap(t, ctx, cluster, sibling.FilesystemName, "warm-sibling-0")
	siblingMembers := sibling.MDSs()
	if len(siblingMembers) != 1 || sibling.Container != siblingMembers[0].Container {
		t.Fatal("ordinary sibling compatibility/owned handle differs")
	}
	siblingProcess := coldMDSProcess(t, ctx, oracle, siblingMembers[0])
	siblingNonce, targetNonce := uuid.NewString(), uuid.NewString()
	coldMDSBytes(t, ctx, client, q.MonMap.FSID, sibling, siblingNonce, "seed", sibling.DataPool)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	coldMDSHealth(t, ctx, cluster, "before-cold-target", "cold-target", true)
	coldMDSResources(t, ctx, oracle, cluster, client, "before-cold-target")
	var customizers atomic.Int32
	target, err := cluster.StartCephFSWithConfig(ctx, ceph.CephFSConfig{Name: "cold-target", MetadataPool: pool("cold-target-meta"), DataPool: pool("cold-target-data"), AdditionalDataPools: []ceph.PoolConfig{pool("cold-target-extra")}, NoInitialMDS: true}, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
	if err != nil || target == nil {
		t.Fatal("cold target construction failed", err)
	}
	beforeTarget := coldMDSMap(t, ctx, cluster, target.FilesystemName, "")
	if target.Container != nil || len(target.MDSs()) != 0 || customizers.Load() != 0 || len(cluster.ServiceContainers()) != 1 {
		t.Fatal("cold target launched or customized an MDS")
	}
	identities := map[string]int64{}
	for _, fs := range []*ceph.CephFSContainer{sibling, target} {
		for _, name := range append([]string{fs.MetadataPool, fs.DataPool}, fs.AdditionalDataPools...) {
			p, err := cluster.PoolStatus(ctx, name)
			if err != nil || p.ID < 0 || p.Size != 2 || p.MinSize != 1 {
				t.Fatal("original replicated FS pool identity unavailable", err)
			}
			identities[name] = p.ID
		}
	}
	if beforeTarget.metadata != identities[target.MetadataPool] || !slices.Equal(beforeTarget.data, []int64{identities[target.DataPool], identities["cold-target-extra"]}) || beforeSibling.metadata != identities[sibling.MetadataPool] || !slices.Equal(beforeSibling.data, []int64{identities[sibling.DataPool]}) {
		t.Fatal("native FSMap does not retain exact original pool attachments")
	}
	recheck := func(cold bool) {
		t.Helper()
		expectedID := "cold-target-0"
		if cold {
			expectedID = ""
		}
		current := coldMDSMap(t, ctx, cluster, target.FilesystemName, expectedID)
		if current.id != beforeTarget.id || current.metadata != beforeTarget.metadata || !slices.Equal(current.data, beforeTarget.data) {
			t.Fatal("first start replaced original FS/pools/attachments")
		}
		other := coldMDSMap(t, ctx, cluster, sibling.FilesystemName, "warm-sibling-0")
		if other.id != beforeSibling.id || other.metadata != beforeSibling.metadata || !slices.Equal(other.data, beforeSibling.data) || other.active != beforeSibling.active || coldMDSProcess(t, ctx, oracle, siblingMembers[0]) != siblingProcess {
			t.Fatal("cold target affected original ordinary sibling")
		}
		for name, id := range identities {
			p, err := cluster.PoolStatus(ctx, name)
			if err != nil || p.ID != id || p.Size != 2 || p.MinSize != 1 {
				t.Fatal("original FS pool changed", err)
			}
		}
		currentQuorum, err := cluster.QuorumStatus(ctx)
		if err != nil || currentQuorum.MonMap.FSID != q.MonMap.FSID {
			t.Fatal("original cluster changed", err)
		}
	}
	coldMDSAuth(t, ctx, cluster, []string{"mds.warm-sibling-0"})
	coldMDSResources(t, ctx, oracle, cluster, client, "cold-target")
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	coldMDSHealth(t, ctx, cluster, "cold-target", "cold-target", false)
	short, stop := context.WithTimeout(ctx, 5*time.Second)
	err = target.WaitReady(short)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("cold WaitReady implied client readiness or lost caller deadline", err)
	}
	coldMDSAvailability(t, ctx, client, target.FilesystemName)
	coldMDSBytes(t, ctx, client, q.MonMap.FSID, sibling, siblingNonce, "verify", sibling.DataPool)
	recheck(true)
	coldMDSAuth(t, ctx, cluster, []string{"mds.warm-sibling-0"})
	coldMDSResources(t, ctx, oracle, cluster, client, "after-cold-client")
	if customizers.Load() != 0 || target.Container != nil || len(target.MDSs()) != 0 {
		t.Fatal("read-only cold observation launched first worker")
	}
	t.Logf("NO_INITIAL_MDS_ZERO fsid=%s filesystem=%s fscid=%d metadata_pool_id=%d data_pool_ids=%v max_mds=1 owned_mds=0 customizers=0", q.MonMap.FSID, target.FilesystemName, beforeTarget.id, beforeTarget.metadata, beforeTarget.data)
	if err := target.ScaleMDS(ctx, 1, 0); err != nil {
		t.Fatal("first explicit cold ScaleMDS failed", err)
	}
	if err := target.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	members := target.MDSs()
	if len(members) != 1 || members[0].ID != "cold-target-0" || members[0].FilesystemName != target.FilesystemName || target.Container != members[0].Container || customizers.Load() != 1 {
		t.Fatal("first ScaleMDS lost original handle/customizer/daemon ownership")
	}
	live := coldMDSMap(t, ctx, cluster, target.FilesystemName, members[0].ID)
	process := coldMDSProcess(t, ctx, oracle, members[0])
	reported, err := target.MDSStatus(ctx)
	if err != nil || reported.FilesystemID != beforeTarget.id || reported.MaxMDS != 1 || len(reported.Active) != 1 || len(reported.Standby) != 0 || len(reported.StandbyReplay) != 0 || !reported.Active[0].Owned || reported.Active[0].Name != live.active.name || reported.Active[0].GID != live.active.gid || reported.Active[0].Rank != 0 {
		t.Fatal("public first rank does not match strict original native FSMap", err)
	}
	coldMDSAuth(t, ctx, cluster, []string{"mds.cold-target-0", "mds.warm-sibling-0"})
	coldMDSResources(t, ctx, oracle, cluster, client, "after-first-scale")
	t.Logf("NO_INITIAL_MDS_ACTIVE fsid=%s filesystem=%s fscid=%d rank=0 gid=%d name=%s cid=%s started_at=%s pid=%d customizers=%d", q.MonMap.FSID, target.FilesystemName, live.id, live.active.gid, live.active.name, members[0].GetContainerID(), process.startedAt, process.pid, customizers.Load())
	coldMDSBytes(t, ctx, client, q.MonMap.FSID, target, targetNonce, "seed", "cold-target-extra")
	coldMDSBytes(t, ctx, client, q.MonMap.FSID, target, targetNonce, "verify", "cold-target-extra")
	coldMDSBytes(t, ctx, client, q.MonMap.FSID, sibling, siblingNonce, "verify", sibling.DataPool)
	recheck(false)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	coldMDSHealth(t, ctx, cluster, "after-first-scale", "cold-target", true)
	cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
	defer done()
	if err := client.Terminate(cleanup); err != nil {
		t.Fatal(err)
	}
	clientTerminated = true
	if err := cluster.Terminate(cleanup); err != nil {
		t.Fatal(err)
	}
	oracle.assertRemoved(t, cleanup)
	t.Logf("NO_INITIAL_MDS_COMPLETE fsid=%s target_fscid=%d sibling_fscid=%d first_gid=%d owned_mds=1 customizers=1", q.MonMap.FSID, beforeTarget.id, beforeSibling.id, live.active.gid)
}

// Returned native daemon logs preserve loader/module/startup grounds on failure.
// This reads existing logs only; it never queries auth dumps or keyring files.
func coldMDSFailureLogs(t *testing.T, cluster *ceph.Container) {
	if cluster == nil {
		return
	}
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		containers := []testcontainers.Container{}
		for _, mon := range cluster.Monitors() {
			containers = append(containers, mon.Container)
		}
		for _, mgr := range cluster.Managers() {
			containers = append(containers, mgr.Container)
		}
		for _, osd := range cluster.OSDs() {
			containers = append(containers, osd.Container)
		}
		containers = append(containers, cluster.ServiceContainers()...)
		seen := map[string]bool{}
		for _, ctr := range containers {
			if ctr == nil || seen[ctr.GetContainerID()] {
				continue
			}
			seen[ctr.GetContainerID()] = true
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			logs, err := ctr.Logs(ctx)
			if err != nil {
				t.Logf("NO_INITIAL_MDS_FAILURE_LOG cid=%s unavailable=%v", ctr.GetContainerID(), err)
			} else {
				data, readErr := io.ReadAll(io.LimitReader(logs, 2<<20))
				_ = logs.Close()
				t.Logf("NO_INITIAL_MDS_FAILURE_LOG cid=%s read_error=%v\n%s", ctr.GetContainerID(), readErr, data)
			}
			cancel()
		}
	})
}

type coldMDSInfo struct {
	Name  *string `json:"name"`
	GID   *uint64 `json:"gid"`
	Rank  *int    `json:"rank"`
	State *string `json:"state"`
	Join  *int64  `json:"join_fscid"`
}
type coldMDSActive struct {
	name string
	gid  uint64
}
type coldMDSScope struct {
	id, metadata int64
	data         []int64
	active       coldMDSActive
}

func coldMDSMap(t *testing.T, parent context.Context, cluster *ceph.Container, name, activeID string) coldMDSScope {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	data, err := cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	if err != nil {
		t.Fatal("native FSMap read unavailable", ctx.Err())
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
	if json.Unmarshal(data, &native) != nil || native.Standbys == nil || native.Filesystems == nil || len(*native.Standbys) != 0 {
		t.Fatal("strict fresh FSMap/global standby absence unavailable")
	}
	names, ids, gids, daemonNames := map[string]bool{}, map[int64]bool{}, map[uint64]bool{}, map[string]bool{}
	var selected coldMDSScope
	found := 0
	for _, fs := range *native.Filesystems {
		m := fs.Map
		if fs.ID == nil || *fs.ID <= 0 || m == nil || m.Name == nil || *m.Name == "" || names[*m.Name] || ids[*fs.ID] || m.Max == nil || m.Wanted == nil || m.Metadata == nil || *m.Metadata < 0 || m.Data == nil || len(*m.Data) == 0 || m.In == nil || m.Up == nil || m.Info == nil || m.Failed == nil || m.Damaged == nil || m.Stopped == nil || m.Flags == nil || m.Flags.Replay == nil || m.Flags.Refuse == nil || m.Flags.Joinable == nil || *m.Max != 1 || *m.Wanted != 0 || *m.Flags.Replay || !*m.Flags.Refuse || !*m.Flags.Joinable || len(*m.Failed)+len(*m.Damaged)+len(*m.Stopped) != 0 {
			t.Fatal("strict original FS identity/policy/rank arrays unavailable")
		}
		names[*m.Name], ids[*fs.ID] = true, true
		for key, info := range *m.Info {
			if info.Name == nil || *info.Name == "" || info.GID == nil || *info.GID == 0 || info.Rank == nil || *info.Rank != 0 || info.State == nil || *info.State != "up:active" || info.Join == nil || *info.Join != *fs.ID || key != "gid_"+strconv.FormatUint(*info.GID, 10) || gids[*info.GID] || daemonNames[*info.Name] {
				t.Fatal("strict FSMap native worker identity/state/affinity unavailable")
			}
			gids[*info.GID], daemonNames[*info.Name] = true, true
		}
		if *m.Name != name {
			continue
		}
		found++
		selected = coldMDSScope{id: *fs.ID, metadata: *m.Metadata, data: slices.Clone(*m.Data)}
		if activeID == "" {
			if len(*m.Info) != 0 || len(*m.Up) != 0 || len(*m.In) != 0 {
				t.Fatal("cold original FS has active/replay/assigned native worker")
			}
		} else {
			if len(*m.Info) != 1 || len(*m.Up) != 1 || !slices.Equal(*m.In, []int{0}) {
				t.Fatal("first original FS rank is not exact active rank zero")
			}
			for _, info := range *m.Info {
				if *info.Name != activeID || (*m.Up)["mds_0"] != *info.GID {
					t.Fatal("native active GID/name/rank differs from original owner")
				}
				selected.active = coldMDSActive{*info.Name, *info.GID}
			}
		}
	}
	if found != 1 {
		t.Fatal("original filesystem missing/ambiguous")
	}
	t.Logf("NO_INITIAL_MDS_MAP filesystem=%s fscid=%d metadata_pool_id=%d data_pool_ids=%v active_name=%s active_gid=%d", name, selected.id, selected.metadata, selected.data, selected.active.name, selected.active.gid)
	return selected
}

func coldMDSAuth(t *testing.T, parent context.Context, cluster *ceph.Container, want []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	data, err := cluster.Ceph(ctx, "auth", "ls", "--format", "json")
	if err != nil {
		t.Fatal("native MDS auth listing unavailable", ctx.Err())
	}
	var native struct {
		Entries *[]struct {
			Entity *string `json:"entity"`
		} `json:"auth_dump"`
	}
	if json.Unmarshal(data, &native) != nil || native.Entries == nil {
		t.Fatal("native auth entity schema unavailable")
	}
	names, seen := []string{}, map[string]bool{}
	for _, entry := range *native.Entries {
		if entry.Entity == nil || *entry.Entity == "" || seen[*entry.Entity] {
			t.Fatal("native auth entity missing/duplicated")
		}
		seen[*entry.Entity] = true
		if strings.HasPrefix(*entry.Entity, "mds.") {
			names = append(names, *entry.Entity)
		}
	}
	slices.Sort(names)
	if !slices.Equal(names, want) {
		t.Fatal("native MDS auth entity names differ from exact owned set")
	}
	// Key-bearing JSON is private input and never emitted, including error chains.
	t.Logf("NO_INITIAL_MDS_AUTH entities=%v count=%d", names, len(names))
}

type coldMDSTask struct {
	startedAt string
	pid       int
}

func coldMDSProcess(t *testing.T, parent context.Context, o *noInitialOSDOracle, mds *ceph.MDSContainer) coldMDSTask {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	before, err := o.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || before.Info.ID != o.engine {
		t.Fatal("original raw engine unavailable")
	}
	cid := mds.GetContainerID()
	if len(cid) != 64 || strings.Trim(cid, "0123456789abcdef") != "" {
		t.Fatal("original MDS full CID unavailable")
	}
	raw, err := o.client.ContainerInspect(ctx, cid, mobycl.ContainerInspectOptions{})
	if err != nil || raw.Container.ID != cid || raw.Container.Config == nil || raw.Container.State == nil {
		t.Fatal("original MDS exact raw inspection unavailable")
	}
	s := raw.Container.State
	start, err := time.Parse(time.RFC3339Nano, s.StartedAt)
	if err != nil || start.IsZero() || !s.Running || s.Paused || s.Restarting || s.Dead || s.Status != "running" || s.Pid <= 0 || s.Error != "" || !slices.Equal(raw.Container.Config.Entrypoint, []string{"/bin/sh", "/tc/mds.sh"}) || !slices.Contains(raw.Container.Config.Env, "CEPH_MDS_ID="+mds.ID) || !slices.Contains(raw.Container.Config.Env, "CEPH_FILESYSTEM="+mds.FilesystemName) {
		t.Fatal("original MDS task/configuration incoherent")
	}
	actual, err := mds.State(ctx)
	if err != nil || actual == nil || actual.StartedAt != s.StartedAt || actual.Pid != s.Pid || !actual.Running {
		t.Fatal("original returned MDS handle differs from raw process")
	}
	after, err := o.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || after.Info.ID != o.engine {
		t.Fatal("original engine changed during process proof")
	}
	return coldMDSTask{s.StartedAt, s.Pid}
}

func coldMDSResources(t *testing.T, parent context.Context, o *noInitialOSDOracle, cluster *ceph.Container, client testcontainers.Container, phase string) {
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
		if state.Status != "running" || !state.Running || state.Paused || state.Restarting || state.Dead || state.Pid <= 0 || state.Error != "" || config.Labels["org.testcontainers.sessionId"] != session {
			t.Fatal("unexpected raw resource task/session")
		}
		role, owned := expected[item.ID]
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
	t.Logf("NO_INITIAL_MDS_RESOURCES phase=%s engine=%s owned=%d auxiliary=%d", phase, o.engine, len(expected), auxiliary)
}

func coldMDSBytes(t *testing.T, ctx context.Context, client testcontainers.Container, fsid string, fs *ceph.CephFSContainer, nonce, phase, pool string) {
	t.Helper()
	data := topologyExecOutput(t, ctx, client, "python3", "-c", coldMDSBytesScript, fsid, fs.FilesystemName, nonce, phase, pool)
	var result struct {
		FSID       string `json:"fsid"`
		Filesystem string `json:"filesystem"`
		Phase      string `json:"phase"`
		Pool       string `json:"pool"`
		Bytes      int    `json:"bytes"`
		SHA256     string `json:"sha256"`
	}
	expected := []byte(strings.Repeat(nonce, (128<<10)/len(nonce)+1))[:128<<10]
	if json.Unmarshal(data, &result) != nil || result.FSID != fsid || result.Filesystem != fs.FilesystemName || result.Phase != phase || result.Pool != pool || result.Bytes != len(expected) || result.SHA256 != fmt.Sprintf("%x", sha256.Sum256(expected)) {
		t.Fatal("fresh userspace CephFS identity/layout/nonce bytes differ")
	}
	t.Logf("NO_INITIAL_MDS_BYTES filesystem=%s phase=%s fsid=%s pool=%s bytes=%d sha256=%s client_cid=%s", fs.FilesystemName, phase, fsid, pool, result.Bytes, result.SHA256, client.GetContainerID())
}

func coldMDSAvailability(t *testing.T, ctx context.Context, client testcontainers.Container, name string) {
	t.Helper()
	data := topologyExecOutput(t, ctx, client, "python3", "-c", coldMDSMountScript, name)
	var result struct {
		Filesystem     string `json:"filesystem"`
		Operation      string `json:"operation"`
		Classification string `json:"classification"`
		Errno          *int   `json:"errno"`
		Prepared       bool   `json:"prepared"`
		Mounted        bool   `json:"mounted"`
		Killed         bool   `json:"killed"`
		ReturnCode     *int   `json:"return_code"`
	}
	if json.Unmarshal(data, &result) != nil || result.Filesystem != name || result.Operation != "mount" || !result.Prepared || result.Mounted || !(result.Classification == "native-availability" && result.Errno != nil && (*result.Errno == 110 || *result.Errno == 112) && !result.Killed && result.ReturnCode != nil && *result.ReturnCode == 0 || result.Classification == "outer-deadline" && result.Errno == nil && result.Killed && result.ReturnCode == nil) {
		t.Fatalf("cold mount was not a classified availability failure; inspect payload/auth separately: %s", data)
	}
	t.Logf("NO_INITIAL_MDS_AVAILABILITY %s", data)
}

// Cold health admits only exact target-absence details from pinned v20.2.4
// MDSMap::get_health_checks; damage/laggy/module/other warnings never pass.
func coldMDSHealth(t *testing.T, parent context.Context, cluster *ceph.Container, phase, target string, wantOK bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	for {
		data := topologyExecOutput(t, ctx, cluster.ControlContainer(), "ceph", "--connect-timeout", "5", "health", "detail", "--format", "json")
		var health struct {
			Status *string `json:"status"`
			Checks *map[string]struct {
				Severity *string `json:"severity"`
				Detail   *[]struct {
					Message *string `json:"message"`
				} `json:"detail"`
			} `json:"checks"`
			Mutes *[]json.RawMessage `json:"mutes"`
		}
		if json.Unmarshal(data, &health) != nil || health.Status == nil || health.Checks == nil || health.Mutes == nil || len(*health.Mutes) != 0 || !slices.Contains([]string{"HEALTH_OK", "HEALTH_WARN", "HEALTH_ERR"}, *health.Status) {
			t.Fatal("strict unmuted native health unavailable")
		}
		t.Logf("NO_INITIAL_MDS_HEALTH phase=%s detail=%s", phase, data)
		maxSeverity := "HEALTH_OK"
		for code, check := range *health.Checks {
			if strings.HasPrefix(code, "MGR_MODULE") {
				t.Fatalf("observed required manager module/dependency failure; stop for fixed image-policy classification: %s", data)
			}
			expected, severity := "", "HEALTH_WARN"
			switch code {
			case "MDS_ALL_DOWN":
				expected = "fs " + target + " is offline because no MDS is active for it."
				severity = "HEALTH_ERR"
			case "MDS_UP_LESS_THAN_MAX":
				expected = "fs " + target + " has 0 MDS online, but wants 1"
			case "FS_DEGRADED":
				expected = "fs " + target + " is degraded"
			default:
				t.Fatalf("unexpected native health beyond exact cold target absence: %s", data)
			}
			if check.Severity == nil || *check.Severity != severity || check.Detail == nil || len(*check.Detail) != 1 || (*check.Detail)[0].Message == nil || *(*check.Detail)[0].Message != expected {
				t.Fatalf("cold health cause/severity is not original target absence: %s", data)
			}
			if severity == "HEALTH_ERR" || maxSeverity == "HEALTH_OK" {
				maxSeverity = severity
			}
		}
		if *health.Status != maxSeverity {
			t.Fatal("native health status/checks contradict")
		}
		if !wantOK || *health.Status == "HEALTH_OK" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("native health did not reach strict OK", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
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
	t.Logf("NO_INITIAL_MDS_MODULES phase=%s active_gid=%d can_run=%s", phase, *native.GID, encoded)
}

const coldMDSBytesScript = `import cephfs, hashlib, json, os, rados, sys
fsid, filesystem, nonce, phase, pool = sys.argv[1:]
with rados.Rados(conffile='/etc/ceph/ceph.conf', name='client.admin', conf={'rados_mon_op_timeout':'10'}) as cluster:
    assert cluster.get_fsid() == fsid
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '15')
fs.conf_set('rados_osd_op_timeout', '10')
fs.mount(filesystem_name=filesystem.encode())
payload = (nonce.encode() * ((128 << 10) // len(nonce) + 1))[:128 << 10]
try:
    if phase == 'seed':
        fs.mkdir('/nonce', 0o700)
        fs.setxattr('/nonce', 'ceph.dir.layout.pool', pool.encode(), 0)
        fd = fs.open('/nonce/payload', os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
        try:
            assert fs.write(fd, payload, 0) == len(payload)
            fs.fsync(fd, False)
        finally:
            fs.close(fd)
    else:
        assert phase == 'verify'
    fd = fs.open('/nonce/payload', os.O_RDONLY)
    try:
        actual = fs.read(fd, 0, len(payload) + 1)
        assert actual == payload
    finally:
        fs.close(fd)
    assert fs.getxattr('/nonce/payload', 'ceph.file.layout.pool_name').decode() == pool
    fs.sync_fs()
    print(json.dumps({'fsid': fsid, 'filesystem': filesystem, 'phase': phase, 'pool': pool, 'bytes':len(payload), 'sha256':hashlib.sha256(actual).hexdigest()}))
finally:
    fs.shutdown()
`

// The parent Python subprocess watchdog kills/waits the native mount child;
// payload import/setup and auth-denied results stay distinct from availability.
const coldMDSMountScript = `import json, subprocess, sys
filesystem = sys.argv[1]
child = r'''import json, sys
name = sys.argv[1]
try:
    import cephfs
    fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
    fs.conf_set('client_mount_timeout', '8')
    fs.conf_set('rados_mon_op_timeout', '8')
except Exception as error:
    print(json.dumps({'stage':'setup-failure', 'error_class':type(error).__name__}), flush=True)
    sys.exit(42)
print(json.dumps({'stage':'mount-admitted'}), flush=True)
try:
    fs.mount(filesystem_name=name.encode())
    print(json.dumps({'stage':'mounted'}), flush=True)
except Exception as error:
    number = getattr(error, 'errno', None)
    if number is None and error.args and isinstance(error.args[0], int):
        number = error.args[0]
    if isinstance(number, int):
        number = abs(number)
    print(json.dumps({'stage':'mount-error', 'errno':number, 'error_class':type(error).__name__}), flush=True)
finally:
    fs.shutdown()
'''
killed = False
return_code = None
try:
    result = subprocess.run([sys.executable, '-c', child, filesystem], capture_output=True, text=True, timeout=20)
    text = result.stdout
    return_code = result.returncode
except subprocess.TimeoutExpired as error:
    killed = True
    text = error.stdout or ''
    if isinstance(text, bytes):
        text = text.decode(errors='strict')
records = [json.loads(line) for line in text.splitlines() if line]
prepared = any(x.get('stage') == 'mount-admitted' for x in records)
mounted = any(x.get('stage') == 'mounted' for x in records)
failures = [x for x in records if x.get('stage') == 'mount-error']
number = failures[0].get('errno') if len(failures) == 1 else None
setup = [x for x in records if x.get('stage') == 'setup-failure']
error_class = setup[0].get('error_class') if len(setup) == 1 else failures[0].get('error_class') if len(failures) == 1 else None
classification = 'unexpected-result'
if any(x.get('stage') == 'setup-failure' for x in records):
    classification = 'payload-or-setup-failure'
elif number in (110, 112) and return_code == 0:
    classification = 'native-availability'
elif number in (1, 13):
    classification = 'auth-denied'
elif killed and prepared and not mounted and not failures:
    classification = 'outer-deadline'
print(json.dumps({'filesystem':filesystem, 'operation':'mount', 'classification':classification, 'errno':number, 'prepared':prepared, 'mounted':mounted, 'killed':killed, 'error_class':error_class, 'return_code':return_code}))
`
