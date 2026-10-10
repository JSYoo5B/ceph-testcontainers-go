//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func testNoInitialManager(t *testing.T, host, noStorage bool) {
	t.Helper()
	budget := 15 * time.Minute
	if noStorage {
		budget = 10 * time.Minute
	}
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	defer cancel()
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithNoInitialManagers())
	const pool = "tc-cold-manager"
	if noStorage {
		opts = append(opts, ceph.WithNoInitialOSDs())
	} else {
		opts = append(opts, ceph.WithPools(ceph.PoolConfig{Name: pool, Application: "rados"}))
	}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	var customizers atomic.Int32
	opts = append(opts, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
	oracle := newNoInitialOSDOracle(t, ctx)
	cluster, err := ceph.Run(ctx, image, opts...)
	noInitialOSDCleanup(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	q, err := cluster.QuorumStatus(ctx)
	if err != nil {
		t.Fatal("original MON quorum unavailable", err)
	}
	id, parseErr := uuid.Parse(q.MonMap.FSID)
	if parseErr != nil || id == uuid.Nil || id.String() != q.MonMap.FSID || len(q.MonMap.Mons) != 1 || !slices.Equal(q.QuorumNames, []string{"a"}) {
		t.Fatal("original MON quorum/FSID unavailable", parseErr)
	}
	fsid := q.MonMap.FSID
	originalMONCID := cluster.GetContainerID()
	if len(cluster.Managers()) != 0 || cluster.ManagerContainer() != nil || customizers.Load() != 1 {
		t.Fatal("cold construction launched a manager or changed MON customizer scope")
	}
	noInitialManagerMap(t, ctx, cluster, false)
	noInitialManagerAuth(t, ctx, cluster, nil)
	states, err := cluster.OSDStates(ctx)
	wantOSDs := 2
	if noStorage {
		wantOSDs = 0
	}
	if err != nil || len(states) != wantOSDs || len(cluster.OSDs()) != wantOSDs || len(noInitialOSDNativeDump(t, ctx, cluster)) != wantOSDs {
		t.Fatal("cold constructor changed selected storage inventory", err)
	}
	for _, state := range states {
		if !state.Up || !state.In || state.UUID == "" {
			t.Fatal("initial storage is not exact captured up/in")
		}
	}
	originalOSDCIDs := make([]string, 0, wantOSDs)
	for _, osd := range cluster.OSDs() {
		originalOSDCIDs = append(originalOSDCIDs, osd.GetContainerID())
	}
	noInitialManagerResources(t, ctx, oracle, cluster, nil, "cold")
	noInitialManagerHealth(t, ctx, cluster, "cold", false)
	integrationHealthDetails(t, ctx, cluster, fsid, "cold-manager")
	var restoreHealthCondition, restoreHealthMute func()
	if !noStorage {
		restoreHealthCondition, restoreHealthMute = noInitialManagerHealthMutes(t, ctx, cluster, fsid)
	}
	t.Logf("NO_INITIAL_MGR_ZERO fsid=%s owned_managers=0 native_managers=0 osds=%d mon_cid=%s", fsid, wantOSDs, cluster.GetContainerID())
	short, stop := context.WithTimeout(ctx, 200*time.Millisecond)
	err = cluster.WaitForClean(short)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("cold constructor implied manager-backed clean statistics", err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if mgr, err := cluster.AddManager(canceled, "a"); mgr != nil || !errors.Is(err, context.Canceled) || len(cluster.Managers()) != 0 {
		t.Fatal("canceled first manager changed owned state", err)
	}
	noInitialManagerAuth(t, ctx, cluster, nil)
	client, err := testcontainers.Run(ctx, image, cluster.WithClient(), ceph.WithIdleEntrypoint(), testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import rados"})))
	clientRemoved := false
	if client != nil {
		t.Cleanup(func() {
			if clientRemoved {
				return
			}
			cleanup, done := context.WithTimeout(context.Background(), 2*time.Minute)
			defer done()
			if err := client.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	oracle.owned = append(oracle.owned, client.GetContainerID())
	topologyExecOutput(t, ctx, client, "python3", "-c", noInitialManagerMonProbe, fsid, fmt.Sprint(wantOSDs))
	t.Logf("NO_INITIAL_MGR_AUTH fsid=%s osds=%d client_cid=%s", fsid, wantOSDs, client.GetContainerID())
	nonce := uuid.NewString()
	var originalPoolID int64
	if !noStorage {
		p, err := cluster.PoolStatus(ctx, pool)
		if err != nil || p.ID <= 0 || p.Size != 2 || p.MinSize != 1 {
			t.Fatal("initial pool before MGR lost native identity/defaults", err)
		}
		originalPoolID = p.ID
		for _, phase := range []string{"seed", "readback"} {
			noInitialManagerBytes(t, ctx, client, fsid, pool, nonce, phase, "before-manager", originalPoolID)
		}
		noInitialManagerMap(t, ctx, cluster, false)
		noInitialManagerAuth(t, ctx, cluster, nil)
	}
	manager, err := cluster.AddManager(ctx, "a")
	if err != nil || manager == nil || manager.Container == nil || manager.DaemonName != "a" || cluster.ManagerContainer() != manager.Container {
		t.Fatal("first explicit manager lost initial-a ownership", err)
	}
	oracle.owned = append(oracle.owned, manager.GetContainerID())
	originalMGRCID := manager.GetContainerID()
	ownedManagers := cluster.Managers()
	if len(ownedManagers) != 1 || ownedManagers[0] != manager {
		t.Fatal("first manager did not retain exact owned descriptor")
	}
	gid := noInitialManagerMap(t, ctx, cluster, true)
	noInitialManagerAuth(t, ctx, cluster, []string{"mgr.a"})
	managerLifecycleRBDReady(t, ctx, cluster)
	if noStorage {
		if len(cluster.OSDs()) != 0 || len(noInitialOSDNativeDump(t, ctx, cluster)) != 0 {
			t.Fatal("first manager implicitly provisioned storage")
		}
		noInitialManagerResources(t, ctx, oracle, cluster, client, "manager-before-storage")
		noInitialManagerHealth(t, ctx, cluster, "manager-before-storage", false)
		for range 2 {
			osd, err := cluster.AddOSD(ctx)
			if err != nil || osd == nil || osd.Container == nil {
				t.Fatal("late explicit OSD failed", err)
			}
			oracle.owned = append(oracle.owned, osd.GetContainerID())
		}
		created, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados"})
		if err != nil || created == nil || created.Replicas != 2 || created.MinSize != 1 {
			t.Fatal("late pool lost prospective defaults", err)
		}
		p, err := cluster.PoolStatus(ctx, pool)
		if err != nil || p.ID <= 0 || p.Size != 2 || p.MinSize != 1 {
			t.Fatal("late pool native identity unavailable", err)
		}
		originalPoolID = p.ID
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	healthCtx, healthCancel := context.WithTimeout(ctx, 90*time.Second)
	defer healthCancel()
	if restoreHealthMute != nil {
		restoreHealthCondition()
		noInitialManagerAwaitHealth(t, healthCtx, cluster, fsid, "recovered-sticky", func(snapshot ceph.HealthSnapshot) bool {
			if _, present := snapshot.Checks["OSDMAP_FLAGS"]; present {
				return false
			}
			mute := noInitialManagerMute(snapshot, "OSDMAP_FLAGS")
			if mute == nil || !mute.Sticky || mute.ExpiresAt != "" {
				t.Fatal("resolved owned flag check lost its native sticky mute")
			}
			return true
		})
		restoreHealthMute()
	}
	topologyWait(t, healthCtx, func() bool { return noInitialManagerHealth(t, healthCtx, cluster, "after-provision", true) })
	health := integrationHealthDetails(t, healthCtx, cluster, fsid, "recovered-manager")
	if health.Status != "HEALTH_OK" || len(health.Checks) != 0 || len(health.Mutes) != 0 {
		t.Fatal("recovered health did not retain strict closure and mute restoration")
	}
	if noStorage {
		noInitialManagerBytes(t, ctx, client, fsid, pool, nonce, "seed", "after-provision", originalPoolID)
	}
	noInitialManagerBytes(t, ctx, client, fsid, pool, nonce, "readback", "after-provision", originalPoolID)
	p, err := cluster.PoolStatus(ctx, pool)
	if err != nil || p.ID != originalPoolID || p.Size != 2 || p.MinSize != 1 {
		t.Fatal("manager/storage addition replaced original pool", err)
	}
	q, err = cluster.QuorumStatus(ctx)
	if err != nil || q.MonMap.FSID != fsid || cluster.GetContainerID() != originalMONCID || manager.GetContainerID() != originalMGRCID || cluster.ManagerContainer() != manager.Container || noInitialManagerMap(t, ctx, cluster, true) != gid || customizers.Load() != 1 {
		t.Fatal("provisioning replaced original MON/MGR identity", err)
	}
	finalOSDCIDs := make([]string, 0, 2)
	for _, osd := range cluster.OSDs() {
		finalOSDCIDs = append(finalOSDCIDs, osd.GetContainerID())
	}
	finalStates, err := cluster.OSDStates(ctx)
	if err != nil || len(finalStates) != 2 || !noStorage && (!slices.Equal(states, finalStates) || !slices.Equal(originalOSDCIDs, finalOSDCIDs)) {
		t.Fatal("first management provisioning changed original storage", err)
	}
	for _, state := range finalStates {
		if !state.Up || !state.In {
			t.Fatal("late storage is not up/in")
		}
	}
	if err := cluster.RemoveManager(ctx, "a"); err == nil || !strings.Contains(err.Error(), "cannot remove the last manager") || noInitialManagerMap(t, ctx, cluster, true) != gid {
		t.Fatal("zero-initial option bypassed last-manager protection", err)
	}
	noInitialManagerResources(t, ctx, oracle, cluster, client, "after-provision")
	cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
	defer done()
	if err := client.Terminate(cleanup); err != nil {
		t.Fatal(err)
	}
	clientRemoved = true
	if err := cluster.Terminate(cleanup); err != nil {
		t.Fatal(err)
	}
	for _, cid := range oracle.owned {
		if _, err := oracle.client.ContainerInspect(cleanup, cid, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Fatal("owned original container remains after cleanup", err)
		}
	}
	for _, nid := range oracle.networks {
		if _, err := oracle.client.NetworkInspect(cleanup, nid, mobycl.NetworkInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Fatal("owned original bridge remains after cleanup", err)
		}
	}
	t.Logf("NO_INITIAL_MGR_CLEANUP owned_containers=%d owned_networks=%d remaining=0", len(oracle.owned), len(oracle.networks))
	t.Logf("NO_INITIAL_MGR_COMPLETE combined_zero=%t fsid=%s mgr_gid=%d pool_id=%d", noStorage, fsid, gid, originalPoolID)
}

// An owned noout flag prepares an immediate warning independently of the new
// MON's MGR grace period. Raw mute commands test TTL and sticky observations;
// both the flag and mute are restored before the original strict closure.
func noInitialManagerHealthMutes(t *testing.T, ctx context.Context, cluster *ceph.Container, fsid string) (func(), func()) {
	t.Helper()
	flags, err := cluster.OSDFlags(ctx)
	if err != nil || slices.Contains(flags, "noout") {
		t.Fatal("fresh fixture has no exact unmodified noout baseline", err)
	}
	flag, err := cluster.TemporaryOSDFlag(ctx, "noout", true)
	flagActive := flag != nil
	restoreCondition := func() {
		if !flagActive {
			return
		}
		cleanup, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		if err := flag.Restore(cleanup); err != nil {
			t.Error("restore owned health flag", err)
			return
		}
		flagActive = false
	}
	t.Cleanup(restoreCondition)
	if err != nil || flag == nil {
		t.Fatal("prepare owned health warning", err)
	}
	active := false
	restore := func() {
		if !active {
			return
		}
		cleanup, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		if _, err := cluster.Ceph(cleanup, "health", "unmute", "OSDMAP_FLAGS"); err != nil {
			t.Error("restore native health mute", err)
			return
		}
		active = false
	}
	t.Cleanup(restore)
	probe, done := context.WithTimeout(ctx, 90*time.Second)
	defer done()
	noInitialManagerAwaitHealth(t, probe, cluster, fsid, "before-mute", func(snapshot ceph.HealthSnapshot) bool {
		if len(snapshot.Mutes) != 0 {
			t.Fatal("fresh fixture has an unexpected native health mute")
		}
		_, present := snapshot.Checks["OSDMAP_FLAGS"]
		return present
	})
	// Mark the mutation before issuing it so an ambiguous reply still restores.
	active = true
	cephCommand(t, probe, cluster, "health", "mute", "OSDMAP_FLAGS", "20s")
	noInitialManagerAwaitHealth(t, probe, cluster, fsid, "ttl-muted", func(snapshot ceph.HealthSnapshot) bool {
		mute := noInitialManagerMute(snapshot, "OSDMAP_FLAGS")
		check, present := snapshot.Checks["OSDMAP_FLAGS"]
		if mute == nil || !present || !check.Muted || mute.Sticky || mute.ExpiresAt == "" {
			t.Fatal("native TTL mute was not retained with the matching check")
		}
		return true
	})
	noInitialManagerAwaitHealth(t, probe, cluster, fsid, "ttl-expired", func(snapshot ceph.HealthSnapshot) bool {
		check, present := snapshot.Checks["OSDMAP_FLAGS"]
		return present && !check.Muted && len(snapshot.Mutes) == 0
	})
	cephCommand(t, probe, cluster, "health", "mute", "OSDMAP_FLAGS", "--sticky")
	noInitialManagerAwaitHealth(t, probe, cluster, fsid, "sticky-muted", func(snapshot ceph.HealthSnapshot) bool {
		mute := noInitialManagerMute(snapshot, "OSDMAP_FLAGS")
		check, present := snapshot.Checks["OSDMAP_FLAGS"]
		if mute == nil || !present || !check.Muted || !mute.Sticky || mute.ExpiresAt != "" {
			t.Fatal("native sticky mute was not retained with the matching check")
		}
		return true
	})
	return restoreCondition, restore
}

func noInitialManagerAwaitHealth(t *testing.T, ctx context.Context, cluster *ceph.Container, fsid, phase string, ready func(ceph.HealthSnapshot) bool) {
	t.Helper()
	topologyWait(t, ctx, func() bool {
		snapshot := integrationHealthDetails(t, ctx, cluster, fsid, phase)
		for code := range snapshot.Checks {
			if code != "OSDMAP_FLAGS" && code != "MGR_DOWN" && code != "TOO_FEW_OSDS" && code != "PG_AVAILABILITY" && code != "PG_DEGRADED" && code != "POOL_NO_REDUNDANCY" {
				t.Fatal("unexpected native health check during mute fixture", code)
			}
		}
		return ready(snapshot)
	})
}

func noInitialManagerMute(snapshot ceph.HealthSnapshot, code string) *ceph.HealthMute {
	for _, mute := range snapshot.Mutes {
		if mute.Code == code {
			return &mute
		}
	}
	return nil
}

func noInitialManagerMap(t *testing.T, ctx context.Context, cluster *ceph.Container, active bool) uint64 {
	t.Helper()
	data := topologyExecOutput(t, ctx, cluster.ControlContainer(), "ceph", "--connect-timeout", "5", "mgr", "dump", "--format", "json")
	var status struct {
		Available  *bool              `json:"available"`
		ActiveName *string            `json:"active_name"`
		ActiveGID  *uint64            `json:"active_gid"`
		Standbys   *[]json.RawMessage `json:"standbys"`
	}
	if err := json.Unmarshal(data, &status); err != nil || status.Available == nil || status.ActiveName == nil || status.ActiveGID == nil || status.Standbys == nil || len(*status.Standbys) != 0 {
		t.Fatalf("strict native MGR map unavailable: error=%v raw=%q", err, data)
	}
	if active && (!*status.Available || *status.ActiveName != "a" || *status.ActiveGID == 0) || !active && (*status.Available || *status.ActiveName != "" || *status.ActiveGID != 0) {
		t.Fatalf("unexpected native MGR registration: active=%t raw=%q", active, data)
	}
	t.Logf("NO_INITIAL_MGR_MAP active=%t gid=%d raw=%q", active, *status.ActiveGID, data)
	return *status.ActiveGID
}

func noInitialManagerAuth(t *testing.T, parent context.Context, cluster *ceph.Container, want []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	data, err := cluster.Ceph(ctx, "auth", "ls", "--format", "json")
	if err != nil {
		t.Fatal("native manager auth listing unavailable", ctx.Err())
	}
	var result struct {
		AuthDump *[]struct{ Entity *string } `json:"auth_dump"`
	}
	if err := json.Unmarshal(data, &result); err != nil || result.AuthDump == nil {
		t.Fatal("native manager auth schema unavailable")
	}
	var names []string
	seen := map[string]bool{}
	for _, entry := range *result.AuthDump {
		if entry.Entity == nil || *entry.Entity == "" || strings.TrimSpace(*entry.Entity) != *entry.Entity || seen[*entry.Entity] {
			t.Fatal("native auth entity missing, empty or duplicated")
		}
		seen[*entry.Entity] = true
		if strings.HasPrefix(*entry.Entity, "mgr.") {
			names = append(names, *entry.Entity)
		}
	}
	slices.Sort(names)
	t.Logf("NO_INITIAL_MGR_IDENTITIES mgr_entities=%v count=%d", names, len(names))
	if !slices.Equal(names, want) {
		t.Fatalf("native manager auth differs: got=%v want=%v", names, want)
	}
}

func noInitialManagerHealth(t *testing.T, ctx context.Context, cluster *ceph.Container, phase string, wantOK bool) bool {
	t.Helper()
	data := topologyExecOutput(t, ctx, cluster.ControlContainer(), "ceph", "--connect-timeout", "5", "health", "detail", "--format", "json")
	t.Logf("NO_INITIAL_MGR_HEALTH phase=%s detail=%s", phase, data)
	var health struct {
		Status string                     `json:"status"`
		Checks map[string]json.RawMessage `json:"checks"`
	}
	if err := json.Unmarshal(data, &health); err != nil || health.Checks == nil || health.Status != "HEALTH_OK" && health.Status != "HEALTH_WARN" && health.Status != "HEALTH_ERR" {
		t.Fatalf("native health schema/severity unavailable: %v", err)
	}
	for code, check := range health.Checks {
		if code == "POOL_APP_NOT_ENABLED" && poolApplicationHealthLags(t, ctx, cluster, check) {
			continue
		}
		if code != "MGR_DOWN" && code != "TOO_FEW_OSDS" && code != "PG_AVAILABILITY" && code != "PG_DEGRADED" && code != "POOL_NO_REDUNDANCY" {
			t.Fatalf("unexpected health check %s; inspect actual detail for fixed image requirement failure", code)
		}
	}
	// Deliberate MGR absence may progress from MGR_DOWN warning to error.
	// An aggregate error is admitted only with that exact observed cause;
	// after provision it is pending, never a successful HEALTH_OK closure.
	if health.Status == "HEALTH_ERR" {
		var down struct{ Severity string }
		if err := json.Unmarshal(health.Checks["MGR_DOWN"], &down); err != nil || down.Severity != "HEALTH_ERR" {
			t.Fatal("unexpected native health error without explicitly severe MGR_DOWN")
		}
	}
	if health.Status == "HEALTH_OK" && len(health.Checks) != 0 || health.Status != "HEALTH_OK" && len(health.Checks) == 0 {
		t.Fatal("native health summary/checks contradict")
	}
	return !wantOK || health.Status == "HEALTH_OK"
}

func noInitialManagerBytes(t *testing.T, ctx context.Context, client testcontainers.Container, fsid, pool, nonce, phase, stage string, poolID int64) {
	t.Helper()
	data := topologyExecOutput(t, ctx, client, "python3", "-c", noInitialOSDBytesProbe, fsid, pool, nonce, phase)
	var result struct {
		FSID, SHA256 string
		Bytes        int
	}
	want := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Repeat(nonce, (128<<10)/len(nonce)+1))[:128<<10]))
	if err := json.Unmarshal(data, &result); err != nil || result.FSID != fsid || result.Bytes != 128<<10 || result.SHA256 != want {
		t.Fatal("fresh native client payload mismatch", err)
	}
	t.Logf("NO_INITIAL_MGR_BYTES stage=%s phase=%s fsid=%s pool_id=%d bytes=%d sha256=%s client_cid=%s", stage, phase, fsid, poolID, result.Bytes, result.SHA256, client.GetContainerID())
}

func noInitialManagerResources(t *testing.T, ctx context.Context, o *noInitialOSDOracle, cluster *ceph.Container, client testcontainers.Container, phase string) {
	t.Helper()
	info, err := o.client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || info.Info.ID != o.engine {
		t.Fatal("original native engine changed", err)
	}
	expected := map[string]string{cluster.GetContainerID(): "mon"}
	add := func(ctr testcontainers.Container, role string) {
		if ctr == nil || len(ctr.GetContainerID()) != 64 || expected[ctr.GetContainerID()] != "" {
			t.Fatal("missing/shared original daemon CID", role)
		}
		expected[ctr.GetContainerID()] = role
	}
	for _, osd := range cluster.OSDs() {
		add(osd.Container, "osd")
	}
	for _, mgr := range cluster.Managers() {
		add(mgr.Container, "mgr")
	}
	if client != nil {
		add(client, "client")
	}
	mon, err := o.client.ContainerInspect(ctx, cluster.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil || mon.Container.Config == nil || mon.Container.Config.Labels["org.testcontainers.sessionId"] == "" {
		t.Fatal("original MON/session inspection unavailable", err)
	}
	session := mon.Container.Config.Labels["org.testcontainers.sessionId"]
	listed, err := o.client.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	seen, auxiliary := map[string]bool{}, 0
	for _, item := range listed.Items {
		if o.before[item.ID] {
			continue
		}
		fresh, err := o.client.ContainerInspect(ctx, item.ID, mobycl.ContainerInspectOptions{})
		if err != nil || len(item.ID) != 64 || fresh.Container.ID != item.ID || fresh.Container.Config == nil || fresh.Container.State == nil {
			t.Fatal("fresh fullCID construction inspection unavailable", err)
		}
		config, state := fresh.Container.Config, fresh.Container.State
		if !state.Running || state.Paused || state.Restarting || state.Dead || config.Labels["org.testcontainers.sessionId"] != session {
			t.Fatal("unexpected native construction state/session")
		}
		role, owned := expected[item.ID]
		if !owned {
			if config.Labels["org.testcontainers.reaper"] != "true" || config.Labels["org.testcontainers.ryuk"] != "true" {
				t.Fatal("unexpected new container outside owned construction and session Ryuk")
			}
			auxiliary++
			if auxiliary > 1 {
				t.Fatal("multiple new session reapers")
			}
			continue
		}
		entry, cmd := []string{"/bin/sh", "/tc/" + role + ".sh"}, []string(nil)
		if role == "client" {
			entry, cmd = []string{"sleep"}, []string{"infinity"}
		}
		if !slices.Equal(config.Entrypoint, entry) || !slices.Equal(config.Cmd, cmd) {
			t.Fatal("owned role entrypoint changed", role)
		}
		seen[item.ID] = true
		if !slices.Contains(o.owned, item.ID) {
			o.owned = append(o.owned, item.ID)
		}
	}
	if len(seen) != len(expected) {
		t.Fatal("construction lost an original owned container")
	}
	if len(o.networks) == 0 && !cluster.UsesHostNetwork() {
		network, err := o.client.NetworkInspect(ctx, cluster.NetworkName(), mobycl.NetworkInspectOptions{})
		if err != nil || network.Network.ID == "" || network.Network.Name != cluster.NetworkName() {
			t.Fatal("original owned bridge identity unavailable", err)
		}
		o.networks = append(o.networks, network.Network.ID)
	}
	t.Logf("NO_INITIAL_MGR_RESOURCES phase=%s engine=%s session=%s owned=%d auxiliary=%d mon_cid=%s", phase, o.engine, session, len(expected), auxiliary, cluster.GetContainerID())
}

const noInitialManagerMonProbe = `import json, rados, sys
with rados.Rados(conffile='/etc/ceph/ceph.conf', name='client.admin', conf={'client_mount_timeout':'10', 'rados_mon_op_timeout':'10'}) as cluster:
    assert cluster.get_fsid() == sys.argv[1]
    code, out, _ = cluster.mon_command(json.dumps({'prefix':'status', 'format':'json'}), b'')
    status = json.loads(out)
    assert code == 0 and status['fsid'] == sys.argv[1]
    assert status['mgrmap']['available'] is False and status['osdmap']['num_osds'] == int(sys.argv[2])
print('native authenticated MON client connected before first manager')
`
