//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_container_pause))))

//ci: timeout=25m job-timeout=35 representative=recovery image-roles=essential,client,cephfs

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/expect"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

// Ceph's own messenger fault options reproduce client-facing faults with the
// official image alone, which has no tc, ip or iptables, and work on host
// networks too. ms_blackhole_client on one OSD drops the client messages it
// receives while it stays up for its peers, so only objects whose primary is
// that OSD time out. ms_inject_delay_* delays the client messages OSDs receive.
// ms_blackhole_client on the active MDS stops new mounts without a failover,
// unlike a paused MDS, whose missing beacons make the MONs replace it.
func TestMessengerFaultInjection(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			var opts []testcontainers.ContainerCustomizer
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := harness.NewServiceCluster(t, opts...)
			const pool = "tc-messenger-faults"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados", Replicas: 2, MinSize: 1}); err != nil {
				t.Fatal(err)
			}
			if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			osds := clusteraccess.OSDs(t, cluster)
			if len(osds) != 2 {
				t.Fatalf("want two owned OSDs, got %d", len(osds))
			}
			target := osds[1]
			// Pick objects until each OSD is the acting primary of two of them.
			primaries := map[string]int{}
			counts := map[int]int{}
			var names []string
			for index := 0; index < 64 && (counts[osds[0].ID] < 2 || counts[target.ID] < 2); index++ {
				object := fmt.Sprintf("object-%02d", index)
				var mapping struct {
					Primary *int `json:"acting_primary"`
				}
				if err := json.Unmarshal(harness.MustCeph(t, ctx, cluster, "osd", "map", pool, object, "--format", "json"), &mapping); err != nil || mapping.Primary == nil {
					t.Fatal("native object mapping unavailable", err)
				}
				primaries[object], names = *mapping.Primary, append(names, object)
				counts[*mapping.Primary]++
			}
			if counts[osds[0].ID] < 2 || counts[target.ID] < 2 {
				t.Fatalf("objects do not cover both primaries: %v", counts)
			}
			probe := func(t *testing.T, generation, timeout string) messengerFaultRun {
				t.Helper()
				encoded, err := json.Marshal(names)
				if err != nil {
					t.Fatal(err)
				}
				out := harness.ExecOutput(t, ctx, client, "python3", "-c", messengerFaultProbe, pool, generation, timeout, string(encoded))
				var run messengerFaultRun
				if err := json.Unmarshal([]byte(harness.LastLine(out)), &run); err != nil || run.ClientID == 0 || len(run.Results) != len(names) {
					t.Fatalf("messenger fault probe output: %s %v", out, err)
				}
				t.Logf("MESSENGER_FAULT_CLIENT generation=%s results=%s", generation, harness.LastLine(out))
				return run
			}
			allWritten := func(t *testing.T, generation string) {
				t.Helper()
				if err := probe(t, generation, "60").checkWrites(); err != nil {
					t.Fatalf("%s: %v", generation, err)
				}
			}
			allWritten(t, "seed")

			t.Run("client-blackhole", func(t *testing.T) {
				hold := messengerFaultConfig(t, ctx, cluster, ceph.ConfigSetting{Section: fmt.Sprintf("osd.%d", target.ID), Name: "ms_blackhole_client", Value: "true"})
				for _, result := range probe(t, "blocked", "5").Results {
					blocked := primaries[result.Name] == target.ID
					if blocked && result.Errno != 110 || !blocked && (result.Errno != 0 || !result.Verified) {
						t.Fatalf("object %s with primary osd.%d: %+v", result.Name, primaries[result.Name], result)
					}
				}
				requireOSDs(t, ctx, cluster, "OSDs stay in the map while dropping client messages", expect.OSDs{Select: osdSelection(osds), Up: true, In: true, Exact: true})
				hold()
				allWritten(t, "after-blackhole")
			})

			t.Run("client-delay", func(t *testing.T) {
				settings := []ceph.ConfigSetting{
					{Section: "osd", Name: "ms_inject_delay_type", Value: "client"},
					{Section: "osd", Name: "ms_inject_delay_probability", Value: "1"},
					{Section: "osd", Name: "ms_inject_delay_max", Value: "2"},
				}
				previous := make(map[int][]ceph.ConfigSetting)
				for _, osd := range osds {
					for _, setting := range settings {
						value, err := messengerFaultEffective(ctx, osd, setting.Name)
						if err != nil {
							t.Fatal(err)
						}
						previous[osd.ID] = append(previous[osd.ID], ceph.ConfigSetting{Name: setting.Name, Value: value})
					}
					debug, err := override.SetDebugLevel(ctx, cluster, override.DebugLevelSetting{
						Daemon: fmt.Sprintf("osd.%d", osd.ID), Name: "debug_ms", Level: "1",
					})
					t.Cleanup(func() {
						cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
						defer cancel()
						if err := debug.Restore(cleanup); err != nil {
							t.Errorf("restore osd.%d messenger debug level: %v", osd.ID, err)
						}
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				check := func(phase string, run messengerFaultRun, wantDelay bool) {
					t.Helper()
					for _, osd := range osds {
						n, err := run.checkDelay(harness.ContainerLogLines(t, ctx, osd), wantDelay)
						if err != nil {
							t.Fatalf("%s osd.%d: %v", phase, osd.ID, err)
						}
						t.Logf("%s osd.%d queued %d delayed OSD operations from client.%d", phase, osd.ID, n, run.ClientID)
					}
				}
				hold := messengerFaultConfig(t, ctx, cluster, settings...)
				for _, osd := range osds {
					waitMessengerFaultEffective(t, ctx, osd, settings)
				}
				delayed := probe(t, "delayed", "60")
				check("delayed", delayed, true)
				hold()
				for _, osd := range osds {
					waitMessengerFaultEffective(t, ctx, osd, previous[osd.ID])
				}
				restored := probe(t, "after-delay", "60")
				if restored.ClientID == delayed.ClientID {
					t.Fatalf("restored phase reused delayed client.%d", restored.ClientID)
				}
				check("restored", restored, false)
			})

			t.Run("mds-client-blackhole", func(t *testing.T) {
				fs, err := cephfs.Start(ctx, cluster, cephfs.Config{})
				if err != nil {
					t.Fatal(err)
				}
				if err := fs.WaitReady(ctx); err != nil {
					t.Fatal(err)
				}
				mount := func(t *testing.T, phase string) messengerFaultMount {
					t.Helper()
					out := harness.ExecOutput(t, ctx, client, "python3", "-c", messengerFaultMountProbe, fs.FilesystemName, phase)
					var result messengerFaultMount
					if err := json.Unmarshal([]byte(harness.LastJSONLine(out)), &result); err != nil {
						t.Fatalf("CephFS mount probe output: %s %v", out, err)
					}
					t.Logf("MESSENGER_FAULT_MOUNT %s", harness.LastJSONLine(out))
					return result
				}
				active := func(t *testing.T) cephfs.MDSDaemonStatus {
					t.Helper()
					status, err := fs.MDSStatus(ctx)
					if err != nil || len(status.Active) != 1 {
						t.Fatalf("one active MDS expected: %+v %v", status, err)
					}
					return status.Active[0]
				}
				if result := mount(t, "seed"); !result.Mounted || !result.Verified {
					t.Fatalf("seed mount: %+v", result)
				}
				before := active(t)
				hold := messengerFaultConfig(t, ctx, cluster, ceph.ConfigSetting{Section: "mds." + before.Name, Name: "ms_blackhole_client", Value: "true"})
				if result := mount(t, "blocked"); result.Mounted || result.Errno != 110 {
					t.Fatalf("mount reached an MDS that drops client messages: %+v", result)
				}
				if after := active(t); after.Name != before.Name || after.GID != before.GID {
					t.Fatalf("active MDS changed while dropping client messages: before=%+v after=%+v", before, after)
				}
				hold()
				if result := mount(t, "verify"); !result.Mounted || !result.Verified {
					t.Fatalf("mount after restore: %+v", result)
				}
			})
		})
	}
}

func messengerFaultEffective(ctx context.Context, osd *ceph.OSDContainer, name string) (string, error) {
	out, err := osd.AdminSocket(ctx, "config", "get", name)
	if err != nil {
		return "", err
	}
	var config map[string]string
	if err := json.Unmarshal(out, &config); err != nil {
		return "", fmt.Errorf("osd.%d config get %s: %w", osd.ID, name, err)
	}
	value, found := config[name]
	if !found {
		return "", fmt.Errorf("osd.%d config get %s omitted the value", osd.ID, name)
	}
	return value, nil
}

func waitMessengerFaultEffective(t *testing.T, ctx context.Context, osd *ceph.OSDContainer, settings []ceph.ConfigSetting) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for _, setting := range settings {
		for {
			got, err := messengerFaultEffective(ctx, osd, setting.Name)
			if err == nil && equalMessengerFaultSetting(got, setting.Value) {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("osd.%d %s = %q, want %q: last read error %v", osd.ID, setting.Name, got, setting.Value, err)
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

func equalMessengerFaultSetting(got, want string) bool {
	if got == want {
		return true
	}
	x, xErr := strconv.ParseFloat(got, 64)
	y, yErr := strconv.ParseFloat(want, 64)
	return xErr == nil && yErr == nil && x == y
}

type messengerFaultMount struct {
	Phase    string `json:"phase"`
	Mounted  bool   `json:"mounted"`
	Verified bool   `json:"verified"`
	Errno    int    `json:"errno"`
	Killed   bool   `json:"killed"`
}

// messengerFaultConfig applies settings and returns a function that restores
// them; cleanup also restores them if the test stops first.
func messengerFaultConfig(t *testing.T, ctx context.Context, cluster *ceph.Container, settings ...ceph.ConfigSetting) func() {
	t.Helper()
	var overrides []*override.ConfigHandle
	restore := func() {
		for _, overridden := range overrides {
			cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
			err := overridden.Restore(cleanup)
			stop()
			if err != nil {
				t.Error("restore messenger fault option", err)
			}
		}
	}
	t.Cleanup(restore)
	for _, setting := range settings {
		overridden, err := override.SetConfig(ctx, cluster, setting)
		overrides = append(overrides, overridden)
		if err != nil {
			t.Fatal("apply", setting.Section, setting.Name, err)
		}
	}
	return restore
}

// A fresh client per phase opens new OSD sessions under the current options.
const messengerFaultProbe = `import rados,sys,json,time,hashlib
pool,generation,timeout,names=sys.argv[1],sys.argv[2],sys.argv[3],json.loads(sys.argv[4])
client=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':timeout,'rados_mon_op_timeout':'30'})
client.connect()
client_id=client.get_instance_id()
try:
    io=client.open_ioctx(pool); results=[]
    for name in names:
        payload=hashlib.sha256((name+':'+generation).encode()).digest()*256
        started=time.monotonic(); result={'name':name,'errno':0,'verified':False}
        try:
            io.write_full(name,payload)
            result['verified']=io.read(name,len(payload)+1,0)==payload
        except rados.Error as error:
            result['errno']=error.errno
        result['seconds']=round(time.monotonic()-started,6)
        results.append(result)
    io.close()
finally:
    client.shutdown()
print(json.dumps({"client_id":client_id,"results":results}))
`

// The mount runs in a child so a mount stuck past client_mount_timeout is
// killed and reported instead of hanging the probe. Only the child's JSON is
// printed; libcephfs logs stay in its captured stderr.
const messengerFaultMountProbe = `import json,subprocess,sys
filesystem,phase=sys.argv[1:]
child=r'''import cephfs,hashlib,json,os,sys
name,phase=sys.argv[1:]
payload=hashlib.sha256(b'messenger-fault').digest()*512
result={'phase':phase,'mounted':False,'verified':False}
fs=cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf',auth_id='admin')
fs.conf_set('client_mount_timeout','5')
try:
    fs.mount(filesystem_name=name.encode()); result['mounted']=True
    if phase=='seed':
        fd=fs.open('/messenger-fault',os.O_CREAT|os.O_WRONLY|os.O_TRUNC,0o600)
        try: fs.write(fd,payload,0); fs.fsync(fd,False)
        finally: fs.close(fd)
    fd=fs.open('/messenger-fault',os.O_RDONLY,0)
    try: result['verified']=fs.read(fd,0,len(payload)+1)==payload
    finally: fs.close(fd)
except Exception as error:
    number=getattr(error,'errno',None)
    if number is None and error.args and isinstance(error.args[0],int): number=error.args[0]
    result['errno']=abs(number) if isinstance(number,int) else -1
finally:
    fs.shutdown()
print(json.dumps(result),flush=True)
'''
try:
    done=subprocess.run([sys.executable,'-c',child,filesystem,phase],capture_output=True,text=True,timeout=40)
    lines=[line for line in done.stdout.splitlines() if line.startswith('{')]
    print(lines[-1] if lines else json.dumps({'phase':phase,'errno':-1,'stderr':done.stderr[-500:]}))
except subprocess.TimeoutExpired:
    print(json.dumps({'phase':phase,'killed':True}))
`
