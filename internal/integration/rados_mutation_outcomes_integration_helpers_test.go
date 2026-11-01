//go:build all || (integration && features)

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

// radosMutationOutcomeScript is the native oracle of write outcomes. Every
// phase that reads uses a client of its own, independent of the writer, and
// prints one JSON receipt. Each client shuts down before the script exits.
//
// Phases, each given the pool and namespace:
//   - acked: asynchronous writes of objects with nonaligned sizes and an
//     append, then a read of each by the independent client after every
//     completion returned 0.
//   - verify RECEIPT: the acked objects again, with fresh clients.
//   - late OBJECT TIMEOUT LINGER: one asynchronous append with
//     rados_osd_op_timeout TIMEOUT (0 waits forever); the client stays
//     connected for LINGER seconds after the completion and reports every
//     callback. It reports its client ID, so the OSD's own record of the
//     operation can be found.
//   - put OBJECT DATA: write_full, then the state.
//   - state OBJECT: the object's bytes and the version of that read.
//   - retry OBJECT VERSION: appends guarded by assert_version with a version
//     newer than the object's and with VERSION, then an unguarded append,
//     with reads after the guarded ones and after the last.
const radosMutationOutcomeScript = `import hashlib,json,sys,time,rados
phase,pool,namespace=sys.argv[1],sys.argv[2],sys.argv[3]
CHUNK=b'late;'
def connected(timeout='10'):
    c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':timeout,'rados_mon_op_timeout':'10'})
    c.connect(); return c
def opened(c):
    i=c.open_ioctx(pool); i.set_namespace(namespace); return i
def state(ioctx,name):
    data=ioctx.read(name,1<<20,0)
    return data,ioctx.get_last_version()
def view(ioctx,name):
    data,version=state(ioctx,name)
    return {'data':data.decode(),'version':version}
def acked_objects():
    objects={'acked-%d'%n:bytes([n+1])*(4096*n+517*(n+1)) for n in range(8)}
    objects['acked-0']+=b'appended'
    return objects
clients=[]
try:
    if phase in ('acked','verify'):
        writer,reader=connected(),connected(); clients+=[writer,reader]
        a,b=opened(writer),opened(reader)
        expected=acked_objects()
        if phase=='acked':
            completions=[a.aio_write_full(name,data[:-8] if name=='acked-0' else data) for name,data in sorted(expected.items())]
            completions.append(a.aio_append('acked-0',b'appended'))
            for c in completions: c.wait_for_complete()
            returns=[c.get_return_value() for c in completions]
            assert returns==[0]*len(completions),returns
        objects={}
        for name,data in sorted(expected.items()):
            got,version=state(b,name)
            assert got==data,(name,len(got),len(data))
            objects[name]=[len(got),hashlib.sha256(got).hexdigest(),version]
        if phase=='verify':
            receipt=json.loads(sys.argv[4])
            assert objects==receipt['objects'],(objects,receipt['objects'])
        print(json.dumps({'objects':objects}))
    elif phase=='late':
        name,timeout,linger=sys.argv[4],sys.argv[5],float(sys.argv[6])
        writer=connected(timeout); clients.append(writer)
        a=opened(writer)
        callbacks=[]
        completion=a.aio_append(name,CHUNK,oncomplete=lambda c: callbacks.append([time.time(),c.get_return_value()]))
        sent=time.time()
        completion.wait_for_complete()
        completed=time.time()
        time.sleep(linger)
        print(json.dumps({'client':'client.%d'%writer.get_instance_id(),'sent':sent,'completed':completed,
                          'return':completion.get_return_value(),'callbacks':callbacks}))
    elif phase=='put':
        writer,reader=connected(),connected(); clients+=[writer,reader]
        opened(writer).write_full(sys.argv[4],sys.argv[5].encode())
        print(json.dumps(view(opened(reader),sys.argv[4])))
    elif phase=='state':
        reader=connected(); clients.append(reader)
        print(json.dumps(view(opened(reader),sys.argv[4])))
    elif phase=='retry':
        name,version=sys.argv[4],int(sys.argv[5])
        writer,reader=connected(),connected(); clients+=[writer,reader]
        a,b=opened(writer),opened(reader)
        def guarded(expected):
            try:
                with rados.WriteOpCtx() as op:
                    op.assert_version(expected)
                    op.append(CHUNK)
                    a.operate_write_op(op,name)
            except rados.Error as e:
                return getattr(e,'errno',None)
            return 0
        future=guarded(state(b,name)[1]+1000)
        refused=guarded(version)
        after=view(b,name)
        a.append(name,CHUNK)
        print(json.dumps({'future_errno':future,'guarded_errno':refused,'after_guarded':after,'after_unguarded':view(b,name)}))
finally:
    for c in clients: c.shutdown()
`

// mutationOutcomePhase runs one phase of radosMutationOutcomeScript in client,
// decodes its receipt into out and returns it.
func mutationOutcomePhase(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, out any, args ...string) []byte {
	t.Helper()
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: append([]string{"python3", "-c", radosMutationOutcomeScript}, args...), Timeout: 2 * time.Minute,
		MaxStdout: 1 << 20, MaxStderr: 1 << 20, Label: "mutation outcome " + args[0],
	})
	if err != nil {
		t.Fatalf("%s phase: %v", args[0], err)
	}
	if !result.Complete() || result.ExitCode != 0 || json.Unmarshal(result.Stdout, out) != nil {
		t.Fatalf("%s phase: exit %d, complete %t\nstdout: %s\nstderr: %s", args[0], result.ExitCode, result.Complete(), result.Stdout, result.Stderr)
	}
	t.Logf("%s receipt: %s", args[0], result.Stdout)
	return result.Stdout
}

// mutationOutcomeObject is the receipt of the state phase.
type mutationOutcomeObject struct {
	Data    string `json:"data"`
	Version uint64 `json:"version"`
}

// osdTrackedOp is one operation from an OSD's dump_ops_in_flight or
// dump_historic_ops.
type osdTrackedOp struct {
	Description string `json:"description"`
	TypeData    struct {
		Events []struct {
			Event string `json:"event"`
			Time  string `json:"time"`
		} `json:"events"`
	} `json:"type_data"`
}

// event returns the time of the operation's first event called name.
func (op osdTrackedOp) event(name string) (time.Time, bool) {
	for _, event := range op.TypeData.Events {
		if event.Event == name {
			at, err := time.Parse("2006-01-02T15:04:05.999999-0700", event.Time)
			return at, err == nil
		}
	}
	return time.Time{}, false
}

// osdClientWrites lists the client writes that the admin socket command
// (dump_ops_in_flight or dump_historic_ops) reports for PG pg, such as "2.0".
// Before the OSD decodes an operation, its description names the client, the
// PG and the flags, but not the object.
func osdClientWrites(t *testing.T, ctx context.Context, cluster *ceph.Container, osd, command, pg string) []osdTrackedOp {
	t.Helper()
	out, err := harness.AdminSocket(t, ctx, cluster, osd, command)
	if err != nil {
		t.Fatalf("%s %s: %v", osd, command, err)
	}
	var dump struct {
		Ops []osdTrackedOp `json:"ops"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		t.Fatalf("%s %s: %v", osd, command, err)
	}
	var writes []osdTrackedOp
	for _, op := range dump.Ops {
		if strings.HasPrefix(op.Description, "osd_op(client.") && strings.Contains(op.Description, " "+pg+" ") && strings.Contains(op.Description, "write") {
			writes = append(writes, op)
		}
	}
	return writes
}

// finishedClientWrite waits until the OSD's history has the finished write of
// client in PG pg. The history keeps the longest operations, which the
// dispatch delay makes these, and records each one shortly after it ended.
func finishedClientWrite(t *testing.T, ctx context.Context, cluster *ceph.Container, osd, pg, client string) osdTrackedOp {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); ; {
		for _, op := range osdClientWrites(t, ctx, cluster, osd, "dump_historic_ops_by_duration", pg) {
			if trackedClient(op.Description) == client {
				return op
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s has no finished write of %s", osd, client)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// trackedClient returns the client of an operation description such as
// "osd_op(client.4199.0:1 2.0 ...)": "client.4199".
func trackedClient(description string) string {
	request, _, _ := strings.Cut(strings.TrimPrefix(description, "osd_op("), " ")
	name, _, _ := strings.Cut(request, ".0:")
	return name
}

// osdUpFrom returns the OSD map epoch from which the OSD's current process is
// up, or 0 while it is down.
func osdUpFrom(t *testing.T, ctx context.Context, cluster *ceph.Container, id int) int {
	t.Helper()
	out, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var dump struct {
		OSDs []struct {
			ID     int `json:"osd"`
			Up     int `json:"up"`
			UpFrom int `json:"up_from"`
		} `json:"osds"`
	}
	if err := json.Unmarshal(out, &dump); err != nil {
		t.Fatal(err)
	}
	for _, osd := range dump.OSDs {
		if osd.ID == id && osd.Up == 1 {
			return osd.UpFrom
		}
	}
	return 0
}

// holdDispatchDelay makes the OSD sleep for delay at each of the points where
// osd_debug_inject_dispatch_delay_probability applies, so every client
// operation waits several times delay before it runs. The OSDMap is settled
// first (settleOSDMapForDispatchDelay), so that an operation's cost under the
// delay is its own sleeps and the queue ahead of it, not a map wait.
func holdDispatchDelay(t *testing.T, ctx context.Context, cluster *ceph.Container, osd string, delay time.Duration) func() {
	t.Helper()
	settleOSDMapForDispatchDelay(t, ctx, cluster, osd)
	var holds []*override.ConfigHandle
	restore := func() {
		for i := len(holds) - 1; i >= 0; i-- {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			if err := holds[i].Restore(cleanup); err != nil {
				t.Error(err)
			}
			cancel()
		}
		holds = nil
	}
	t.Cleanup(restore)
	for _, setting := range []ceph.ConfigSetting{
		{Section: osd, Name: "osd_debug_inject_dispatch_delay_duration", Value: fmt.Sprint(delay.Seconds())},
		{Section: osd, Name: "osd_debug_inject_dispatch_delay_probability", Value: "1"},
	} {
		hold, err := override.SetConfig(ctx, cluster, setting)
		holds = append(holds, hold)
		if err != nil {
			t.Fatal(err)
		}
	}
	return restore
}

// settleOSDMapForDispatchDelay returns once the MON has recorded the OSD's
// beacon and every PG of the OSD has applied the MON's current OSDMap epoch,
// the epoch that the clients of the delayed phase receive.
//
// While the dispatch delay is held, a new epoch costs the OSD two sleeps per
// map in consume_map and each PG two more for the peering item that advances
// it. An operation stamped with the new epoch that reaches a PG still on the
// old one waits for the map and is requeued, paying the queue's two sleeps
// again: one extra cycle that a client timeout sized for the delay cannot
// absorb. An idle cluster still publishes one such epoch on its own. A booted
// OSD sends its first beacon only once the host's monotonic clock exceeds
// osd_beacon_report_interval (300 s by default), because the time of its
// last beacon starts at the clock's zero. That beacon carries the time of the
// purged_snaps scrub the OSD ran before it booted, and the MON records it in
// osd_xinfo as a new epoch that changes nothing else. On a freshly booted CI
// host the cluster starts before that point, so the epoch arrives at a host
// uptime of about 300 s, wherever the test happens to be. send_beacon makes
// the OSD send that beacon now, before the delay.
func settleOSDMapForDispatchDelay(t *testing.T, ctx context.Context, cluster *ceph.Container, osd string) {
	t.Helper()
	id, err := strconv.Atoi(strings.TrimPrefix(osd, "osd."))
	if err != nil || !strings.HasPrefix(osd, "osd.") {
		t.Fatalf("%q is not an osd.<id>", osd)
	}
	if _, err := harness.AdminSocket(t, ctx, cluster, osd, "send_beacon"); err != nil {
		t.Fatalf("%s send_beacon: %v", osd, err)
	}
	var last string
	for deadline := time.Now().Add(time.Minute); ; {
		epoch, settled, reason := osdMapSettledForDispatchDelay(ctx, cluster, osd, id)
		if settled {
			t.Logf("%s: every PG applied OSDMap e%d, which holds the OSD's beacon, before the dispatch delay", osd, epoch)
			return
		}
		last = reason
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("%s: the OSDMap did not settle before the dispatch delay: %s", osd, last)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// osdMapSettledForDispatchDelay reads the MON's epoch, the PGs of the OSD and
// each PG's applied epoch, then the MON's epoch again. It reports the epoch
// when the OSD's beacon is recorded, every PG holds that epoch and no newer
// one was published meanwhile, and otherwise why not.
func osdMapSettledForDispatchDelay(ctx context.Context, cluster *ceph.Container, osd string, id int) (uint32, bool, string) {
	dump, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return 0, false, fmt.Sprintf("osd dump: %v", err)
	}
	epoch, beaconed, err := parseBeaconedOSDMap(dump, id)
	if err != nil {
		return 0, false, err.Error()
	}
	if !beaconed {
		return epoch, false, fmt.Sprintf("OSDMap e%d does not hold a beacon of %s yet", epoch, osd)
	}
	listed, err := cluster.Ceph(ctx, "pg", "ls-by-osd", osd, "--format", "json")
	if err != nil {
		return epoch, false, fmt.Sprintf("pg ls-by-osd %s: %v", osd, err)
	}
	pgs, err := parsePGIDs(listed)
	if err != nil {
		return epoch, false, err.Error()
	}
	for _, pg := range pgs {
		out, err := cluster.Ceph(ctx, "pg", pg, "query", "--format", "json")
		if err != nil {
			return epoch, false, fmt.Sprintf("pg %s query: %v", pg, err)
		}
		applied, err := harness.ParsePGMapEpoch(out)
		if err != nil {
			return epoch, false, fmt.Sprintf("pg %s: %v", pg, err)
		}
		if applied < epoch {
			return epoch, false, fmt.Sprintf("pg %s applied e%d, the MON has e%d", pg, applied, epoch)
		}
	}
	again, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		return epoch, false, fmt.Sprintf("osd dump: %v", err)
	}
	now, _, err := parseBeaconedOSDMap(again, id)
	if err != nil {
		return epoch, false, err.Error()
	}
	if now != epoch {
		return now, false, fmt.Sprintf("the MON published e%d while the PGs were read at e%d", now, epoch)
	}
	return epoch, true, ""
}

// parseBeaconedOSDMap returns the epoch of an "osd dump" and whether its
// osd_xinfo for OSD id holds a last_purged_snaps_scrub, which only a beacon
// sets; until then the field is the zero time "0.000000".
func parseBeaconedOSDMap(dump []byte, id int) (uint32, bool, error) {
	var parsed struct {
		Epoch *uint32 `json:"epoch"`
		XInfo []struct {
			OSD                  *int    `json:"osd"`
			LastPurgedSnapsScrub *string `json:"last_purged_snaps_scrub"`
		} `json:"osd_xinfo"`
	}
	if err := json.Unmarshal(dump, &parsed); err != nil {
		return 0, false, fmt.Errorf("osd dump: %w", err)
	}
	if parsed.Epoch == nil || *parsed.Epoch == 0 {
		return 0, false, errors.New("osd dump has no epoch")
	}
	for _, info := range parsed.XInfo {
		if info.OSD == nil || *info.OSD != id {
			continue
		}
		if info.LastPurgedSnapsScrub == nil {
			return *parsed.Epoch, false, fmt.Errorf("osd dump e%d: osd_xinfo of osd.%d has no last_purged_snaps_scrub", *parsed.Epoch, id)
		}
		stamp := *info.LastPurgedSnapsScrub
		return *parsed.Epoch, stamp != "" && strings.Trim(stamp, "0.") != "", nil
	}
	return *parsed.Epoch, false, fmt.Errorf("osd dump e%d has no osd_xinfo of osd.%d", *parsed.Epoch, id)
}

// parsePGIDs returns the PG IDs of a "pg ls-by-osd" answer.
func parsePGIDs(out []byte) ([]string, error) {
	var listed struct {
		PGStats *[]struct {
			PGID string `json:"pgid"`
		} `json:"pg_stats"`
	}
	if err := json.Unmarshal(out, &listed); err != nil {
		return nil, fmt.Errorf("pg ls-by-osd: %w", err)
	}
	if listed.PGStats == nil || len(*listed.PGStats) == 0 {
		return nil, errors.New("pg ls-by-osd lists no PG")
	}
	pgs := make([]string, 0, len(*listed.PGStats))
	for _, pg := range *listed.PGStats {
		if pg.PGID == "" {
			return nil, errors.New("pg ls-by-osd lists a PG without pgid")
		}
		pgs = append(pgs, pg.PGID)
	}
	return pgs, nil
}
