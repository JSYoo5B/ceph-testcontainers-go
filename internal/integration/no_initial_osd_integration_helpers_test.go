//go:build all || integration

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

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/expect"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func noInitialOSDNativeDump(t *testing.T, ctx context.Context, cluster *ceph.Container) []struct {
	ID     int
	UUID   string
	Up, In int
} {
	t.Helper()
	data, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var dump struct {
		OSDs *[]struct {
			ID     int    `json:"osd"`
			UUID   string `json:"uuid"`
			Up, In int
		} `json:"osds"`
	}
	if err := json.Unmarshal(data, &dump); err != nil || dump.OSDs == nil {
		t.Fatal("strict native OSD inventory unavailable", err)
	}
	result := make([]struct {
		ID     int
		UUID   string
		Up, In int
	}, len(*dump.OSDs))
	for i, item := range *dump.OSDs {
		id, err := uuid.Parse(item.UUID)
		if err != nil || id == uuid.Nil || id.String() != item.UUID || item.ID < 0 {
			t.Fatal("native OSD registration identity invalid")
		}
		result[i] = struct {
			ID     int
			UUID   string
			Up, In int
		}{item.ID, item.UUID, item.Up, item.In}
	}
	return result
}

func noInitialOSDBootstrap(t *testing.T, ctx context.Context, cluster *ceph.Container) (string, uint64) {
	t.Helper()
	status, err := harness.ObserveCluster(cluster).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := uuid.Parse(status.FSID)
	if err != nil || id == uuid.Nil || id.String() != status.FSID {
		t.Fatal("native original FSID unavailable")
	}
	q, err := harness.ObserveCluster(cluster).QuorumStatus(ctx)
	if err != nil || q.MonMap.FSID != status.FSID || len(q.MonMap.Mons) != 1 || !slices.Equal(q.QuorumNames, []string{"a"}) {
		t.Fatal("MON-only native majority unavailable", err)
	}
	mgr, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
	if err != nil || !mgr.Available || mgr.ActiveName != "a" || mgr.ActiveGID == 0 || !status.MgrMap.Available {
		t.Fatal("actual active MGR unavailable", err)
	}
	if status.OSDMap.NumOSDs != 0 || status.OSDMap.NumUpOSDs != 0 || status.OSDMap.NumInOSDs != 0 || len(clusteraccess.OSDs(t, cluster)) != 0 || len(noInitialOSDNativeDump(t, ctx, cluster)) != 0 {
		t.Fatal("zero construction registered or launched storage")
	}
	// Only storage-related checks are allowed without OSDs. Missing modules or
	// dependencies are image-contract failures, not zero-storage readiness.
	noInitialOSDHealth(t, ctx, cluster, "zero", false)
	t.Logf("NO_INITIAL_OSD_ZERO fsid=%s mgr_gid=%d health=%s pgs=%d owned_osds=0 native_osds=0", status.FSID, mgr.ActiveGID, status.Health.Status, status.PGMap.NumPGs)
	return status.FSID, mgr.ActiveGID
}

func noInitialOSDPoolDefaults(t *testing.T, parent context.Context, cluster *ceph.Container, size, minSize string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	monCID := cluster.GetContainerID()
	if len(monCID) != 64 {
		t.Fatalf("original MON full CID unavailable: %q", monCID)
	}
	const socket = "/var/run/ceph/ceph-mon.a.asok"
	for _, setting := range []struct{ name, value string }{{"osd_pool_default_size", size}, {"osd_pool_default_min_size", minSize}} {
		// config get reads the central database or compiled default; the MON's
		// local ceph.conf is authoritative only in its effective runtime config.
		central, err := cluster.Ceph(ctx, "config", "get", "mon.a", setting.name)
		t.Logf("NO_INITIAL_OSD_POOL_DEFAULT source=central mon_cid=%s key=%s value=%q error=%q", monCID, setting.name, central, fmt.Sprint(err))
		if err != nil {
			t.Fatalf("central pool default diagnostic failed: mon_cid=%s key=%s value=%q error=%q", monCID, setting.name, central, fmt.Sprint(err))
		}
		t.Logf("NO_INITIAL_OSD_POOL_DEFAULT_QUERY source=runtime mon_cid=%s socket=%s key=%s expected=%q", monCID, socket, setting.name, setting.value)
		data := harness.TopologyExecOutput(t, ctx, cluster.Container, "ceph", "--admin-daemon", socket, "config", "get", setting.name)
		t.Logf("NO_INITIAL_OSD_POOL_DEFAULT source=runtime mon_cid=%s key=%s value=%q expected=%q", monCID, setting.name, data, setting.value)
		var values map[string]*string
		if err := json.Unmarshal(data, &values); err != nil || len(values) != 1 || values[setting.name] == nil || *values[setting.name] != setting.value {
			t.Fatalf("effective MON pool default mismatch: mon_cid=%s key=%s value=%q expected=%q error=%q", monCID, setting.name, data, setting.value, fmt.Sprint(err))
		}
	}
}

func runNoInitialOSDStorage(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs()))
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	} else {
		opts = append(opts, ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithSeparateClusterNetwork()))
	}
	var customizers atomic.Int32
	opts = append(opts, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
	oracle := harness.NewNoInitialOSDOracle(t, ctx)
	cluster, err := ceph.Run(ctx, image, opts...)
	harness.NoInitialOSDCleanup(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	fsid, mgrGID := noInitialOSDBootstrap(t, ctx, cluster)
	oracle.AssertOnlyControl(t, ctx, cluster, "normal-first-zero")
	if customizers.Load() != 1 {
		t.Fatal("constructor customizer no longer targets only the MON")
	}
	noInitialOSDPoolDefaults(t, ctx, cluster, "2", "1")
	if pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "not-yet"}); pool != nil || err == nil {
		t.Fatal("initial storage absence bypassed CreatePool domain guard")
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if osd, err := cluster.AddOSD(canceled); osd != nil || !errors.Is(err, context.Canceled) || len(noInitialOSDNativeDump(t, ctx, cluster)) != 0 {
		t.Fatal("canceled first Add registered storage")
	}
	short, stop := context.WithTimeout(ctx, 150*time.Millisecond)
	err = harness.ObserveCluster(cluster).WaitForClean(short)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("zero-storage constructor implied PG clean", err)
	}
	client, err := testcontainers.Run(ctx, harness.NativeClientImage(t, image), cluster.WithClient(), ceph.WithIdleEntrypoint(), testcontainers.WithWaitStrategy(wait.ForExec([]string{"python3", "-c", "import rados"})))
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
	oracle.Owned = append(oracle.Owned, client.GetContainerID())
	harness.TopologyExecOutput(t, ctx, client, "python3", "-c", noInitialOSDMonProbe, fsid)
	first, err := cluster.AddOSD(ctx)
	if err != nil || first == nil || first.Container == nil {
		t.Fatal("first explicit storage launch failed", err)
	}
	oracle.Owned = append(oracle.Owned, first.GetContainerID())
	before := requireOSDs(t, ctx, cluster, "first explicit OSD exact up/in", expect.OSDs{Select: osdSelection([]*ceph.OSDContainer{first}), Up: true, In: true, Exact: true})
	if err := first.Remove(ctx); err == nil || !strings.Contains(err.Error(), "cannot remove the last OSD") {
		t.Fatal("zero-initial option changed last-OSD protection", err)
	}
	after, err := harness.ObserveCluster(cluster).OSDStates(ctx)
	if err != nil || !slices.Equal(before, after) {
		t.Fatal("rejected last removal changed native registration")
	}
	second, err := cluster.AddOSD(ctx)
	if err != nil || second == nil || second.Container == nil || second.ID == first.ID {
		t.Fatal("second explicit storage launch failed", err)
	}
	oracle.Owned = append(oracle.Owned, second.GetContainerID())
	states := requireOSDs(t, ctx, cluster, "explicit OSDs exact up/in", expect.OSDs{Select: osdSelection([]*ceph.OSDContainer{first, second}), Up: true, In: true, Exact: true})
	if states[0].UUID == states[1].UUID || len(noInitialOSDNativeDump(t, ctx, cluster)) != 2 {
		t.Fatal("explicit storage identities are not distinct native registrations")
	}
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-deferred-storage", Application: "rados"})
	if err != nil || pool == nil || pool.Replicas != 2 || pool.MinSize != 1 {
		t.Fatal("later pool lost original defaults", err)
	}
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	// HEALTH_OK additionally closes the always-on module/dependency contract;
	// clean PGs alone must not hide a missing MGR payload. Native health can lag.
	healthCtx, healthCancel := context.WithTimeout(ctx, 90*time.Second)
	defer healthCancel()
	topologyWait(t, healthCtx, func() bool { return noInitialOSDHealth(t, healthCtx, cluster, "after-storage", true) })
	poolState, err := pool.Status(ctx)
	if err != nil || poolState.ID <= 0 || poolState.Size != 2 || poolState.MinSize != 1 {
		t.Fatal("native provisioned pool identity/defaults unavailable", err)
	}
	nonce := uuid.NewString()
	for _, phase := range []string{"seed", "readback"} {
		data := harness.TopologyExecOutput(t, ctx, client, "python3", "-c", noInitialOSDBytesProbe, fsid, pool.Name, nonce, phase)
		var result struct {
			FSID, SHA256 string
			Bytes        int
		}
		if err := json.Unmarshal(data, &result); err != nil || result.FSID != fsid || result.Bytes != 128<<10 || result.SHA256 != fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Repeat(nonce, (128<<10)/len(nonce)+1))[:128<<10])) {
			t.Fatal("independent native payload mismatch", err)
		}
		t.Logf("NO_INITIAL_OSD_BYTES phase=%s fsid=%s pool_id=%d bytes=%d sha256=%s", phase, fsid, poolState.ID, result.Bytes, result.SHA256)
	}
	finalStatus, err := harness.ObserveCluster(cluster).Status(ctx)
	if err != nil || finalStatus.FSID != fsid || finalStatus.OSDMap.NumOSDs != 2 || finalStatus.OSDMap.NumUpOSDs != 2 || finalStatus.OSDMap.NumInOSDs != 2 {
		t.Fatal("explicit storage replaced original cluster", err)
	}
	mgr, err := harness.ObserveCluster(cluster).ManagerStatus(ctx)
	if err != nil || !mgr.Available || mgr.ActiveGID != mgrGID || customizers.Load() != 1 {
		t.Fatal("explicit storage replaced original manager/customizer authority", err)
	}
	cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
	defer done()
	if err := client.Terminate(cleanup); err != nil {
		t.Fatal(err)
	}
	clientRemoved = true
	if err := cluster.Terminate(cleanup); err != nil {
		t.Fatal(err)
	}
	oracle.AssertRemoved(t, cleanup)
	t.Log("NO_INITIAL_OSD_COMPLETE kind=normal-first")
}

func runNoInitialOSDPartial(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs().WithDefaultCRUSHRoot("osd-0").WithPoolDefaults(1, 1)))
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	oracle := harness.NewNoInitialOSDOracle(t, ctx)
	cluster, err := ceph.Run(ctx, image, opts...)
	harness.NoInitialOSDCleanup(t, cluster)
	if err != nil {
		t.Fatal(err)
	}
	fsid, _ := noInitialOSDBootstrap(t, ctx, cluster)
	oracle.AssertOnlyControl(t, ctx, cluster, "partial-first-zero")
	noInitialOSDPoolDefaults(t, ctx, cluster, "1", "1")
	partial, err := cluster.AddOSD(ctx)
	if err == nil || partial == nil || partial.Container != nil || partial.ID != 0 || partial.Placement().Host != "osd-0" || len(clusteraccess.OSDs(t, cluster)) != 1 || clusteraccess.OSDs(t, cluster)[0] != partial {
		t.Fatal("generated-host conflict lost original partial registration", err)
	}
	states, err := harness.ObserveCluster(cluster).OSDStates(ctx)
	if err != nil || len(states) != 1 || states[0].ID != partial.ID || states[0].Up || len(noInitialOSDNativeDump(t, ctx, cluster)) != 1 {
		t.Fatal("partial native registration unavailable", err)
	}
	oracle.AssertOnlyControl(t, ctx, cluster, "partial-registered-no-process")
	if err := partial.Remove(ctx); err == nil || !strings.Contains(err.Error(), "cannot remove the last OSD") {
		t.Fatal("partial first registration bypassed last-OSD guard", err)
	}
	t.Logf("NO_INITIAL_OSD_PARTIAL fsid=%s osd_id=%d uuid=%s owned=1 container=absent", fsid, partial.ID, states[0].UUID)
	cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
	defer done()
	if err := cluster.Terminate(cleanup); err != nil {
		t.Fatal(err)
	}
	oracle.AssertRemoved(t, cleanup)
	t.Log("NO_INITIAL_OSD_COMPLETE kind=partial-first")
}

// The health codes identify storage absence or the explicitly selected replica
// policy. Everything else rejects, with the actual native detail retained in the
// log. Failures stop the parent before another fixture can be constructed.
func noInitialOSDHealth(t *testing.T, ctx context.Context, cluster *ceph.Container, phase string, requireOK bool) bool {
	t.Helper()
	data, err := cluster.Ceph(ctx, "health", "detail", "--format", "json")
	if err != nil {
		t.Fatal("native health detail unavailable", err)
	}
	var detail struct {
		Status string                     `json:"status"`
		Checks map[string]json.RawMessage `json:"checks"`
	}
	if err := json.Unmarshal(data, &detail); err != nil || detail.Checks == nil || !slices.Contains([]string{"HEALTH_OK", "HEALTH_WARN", "HEALTH_ERR"}, detail.Status) {
		t.Fatal("strict native health detail unavailable", err)
	}
	compact, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("NO_INITIAL_OSD_HEALTH phase=%s detail=%s", phase, compact)
	for code, check := range detail.Checks {
		if code == "POOL_APP_NOT_ENABLED" && poolApplicationHealthLags(t, ctx, cluster, check) {
			continue
		}
		if strings.HasPrefix(code, "MGR_MODULE") {
			t.Fatalf("image requirement or MGR module/dependency failure: %s; stop dependent acceptance and report native health detail", code)
		}
		if !slices.Contains([]string{"TOO_FEW_OSDS", "PG_AVAILABILITY", "PG_DEGRADED", "POOL_NO_REDUNDANCY"}, code) {
			t.Fatalf("unexpected non-storage native health check: %s", code)
		}
	}
	if detail.Status == "HEALTH_OK" && len(detail.Checks) != 0 || detail.Status != "HEALTH_OK" && len(detail.Checks) == 0 {
		t.Fatal("native health status/checks disagree")
	}
	return !requireOK || detail.Status == "HEALTH_OK"
}

const noInitialOSDMonProbe = `import json, rados, sys
with rados.Rados(conffile='/etc/ceph/ceph.conf', name='client.admin', conf={'client_mount_timeout':'10', 'rados_mon_op_timeout':'10'}) as cluster:
    assert cluster.get_fsid() == sys.argv[1]
    code, out, _ = cluster.mon_command(json.dumps({'prefix':'status', 'format':'json'}), b'')
    status = json.loads(out)
    assert code == 0 and status['fsid'] == sys.argv[1]
    assert status['mgrmap']['available'] and status['osdmap']['num_osds'] == 0
print('native authenticated MON client connected without storage')
`

const noInitialOSDBytesProbe = `import hashlib, json, rados, sys
fsid, pool, nonce, phase = sys.argv[1:]
payload = (nonce.encode() * (131072 // len(nonce) + 1))[:131072]
with rados.Rados(conffile='/etc/ceph/ceph.conf', name='client.admin', conf={'client_mount_timeout':'10', 'rados_mon_op_timeout':'10', 'rados_osd_op_timeout':'20'}) as cluster:
    assert cluster.get_fsid() == fsid
    with cluster.open_ioctx(pool) as io:
        if phase == 'seed':
            io.write_full('retained', payload)
        data = io.read('retained', len(payload))
        assert data == payload and io.stat('retained')[0] == len(payload)
print(json.dumps({'FSID':fsid, 'SHA256':hashlib.sha256(data).hexdigest(), 'Bytes':len(data)}))
`
