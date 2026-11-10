//go:build (all || (integration && features)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_rbd_trash_purge_timezone)))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package rbd_trash_purge_timezone_test

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// trashPurgeDelays are the deferment delays of the images of one pool, by
// image name. The cutoff of the purge lies trashPurgeCutoff after the
// images were moved to the trash, between the second and the third
// deadline. A cutoff read nine hours early falls between the first and the
// second, and one read nine hours late between the third and the fourth.
var trashPurgeDelays = map[string]time.Duration{
	"due-10m": 10 * time.Minute,
	"due-6h":  6 * time.Hour,
	"due-15h": 15 * time.Hour,
	"due-30h": 30 * time.Hour,
}

const trashPurgeCutoff = 10 * time.Hour

// trashPurgeScript moves the images to the trash of a pool, purges it with
// a timezone-aware datetime of a fixed instant and reports the deadlines the
// cluster recorded and the images left. It runs in the client process
// timezone its environment gives it.
const trashPurgeScript = `import calendar,datetime,json,sys,time
import rados,rbd
pool,cutoff_after=sys.argv[1],int(sys.argv[2])
delays=json.loads(sys.argv[3])
def epoch(d):
    return int(d.timestamp()) if d.tzinfo else calendar.timegm(d.timetuple())
with rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'30','rados_mon_op_timeout':'30'}) as cluster:
    with cluster.open_ioctx(pool) as ioctx:
        r=rbd.RBD()
        moved=int(time.time())
        for name,delay in delays.items():
            r.create(ioctx,name,1<<20)
            r.trash_move(ioctx,name,delay)
        deadlines={e['name']:epoch(e['deferment_end_time']) for e in r.trash_list(ioctx)}
        cutoff=datetime.datetime.fromtimestamp(moved+cutoff_after,datetime.timezone.utc)
        r.trash_purge(ioctx,cutoff)
        retained=sorted(e['name'] for e in r.trash_list(ioctx))
        print(json.dumps({'tzname':time.tzname[0],'utc_offset':-time.timezone,'moved':moved,'cutoff':int(cutoff.timestamp()),'cutoff_text':cutoff.isoformat(),'mktime':int(time.mktime(cutoff.timetuple())),'deadlines':deadlines,'retained':retained}))
`

// trashPurgeOutcome is what trashPurgeScript reports, with times in seconds
// since the epoch. MkTime is the local-time reading of the cutoff's fields.
type trashPurgeOutcome struct {
	TZName     string           `json:"tzname"`
	UTCOffset  int64            `json:"utc_offset"`
	Moved      int64            `json:"moved"`
	Cutoff     int64            `json:"cutoff"`
	CutoffText string           `json:"cutoff_text"`
	MkTime     int64            `json:"mktime"`
	Deadlines  map[string]int64 `json:"deadlines"`
	Retained   []string         `json:"retained"`
}

// TestRBDTrashPurgeCutoffAcrossClientTimezones purges equivalent trash
// contents with the same timezone-aware cutoff from client processes in
// three timezones and requires each to remove exactly the images whose
// deferment ended by that instant.
func TestRBDTrashPurgeCutoffAcrossClientTimezones(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("client: %s", harness.ExecOutput(t, ctx, client, "ceph", "--version"))
	delays := map[string]int64{}
	for name, delay := range trashPurgeDelays {
		delays[name] = int64(delay / time.Second)
	}
	encoded, err := json.Marshal(delays)
	if err != nil {
		t.Fatal(err)
	}
	// POSIX timezone strings need no zone database: "TST-9" is nine hours
	// ahead of UTC and "TST9" nine hours behind.
	for _, zone := range []struct {
		name, tz string
		offset   int64
	}{{"utc", "UTC0", 0}, {"ahead-9h", "TST-9", 9 * 3600}, {"behind-9h", "TST9", -9 * 3600}} {
		t.Run(zone.name, func(t *testing.T) {
			pool := "tc-trash-purge-" + zone.name
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 1, Application: "rbd"}); err != nil {
				t.Fatal(err)
			}
			before := time.Now().Unix()
			out := harness.ExecOutput(t, ctx, client, "env", "TZ="+zone.tz, "python3", "-c", trashPurgeScript, pool, fmt.Sprint(int64(trashPurgeCutoff/time.Second)), string(encoded))
			after := time.Now().Unix()
			var got trashPurgeOutcome
			if err := json.Unmarshal([]byte(out), &got); err != nil {
				t.Fatalf("%v: %s", err, out)
			}
			// Conditions: the client process ran in the timezone, the
			// cluster recorded each deadline at its delay after the move,
			// whatever the timezone, and the cutoff is the fixed instant.
			if got.UTCOffset != zone.offset {
				t.Fatalf("condition not established: the client's UTC offset is %d s, want %d: %s", got.UTCOffset, zone.offset, out)
			}
			if got.Moved < before-60 || got.Moved > after+60 || got.Cutoff != got.Moved+int64(trashPurgeCutoff/time.Second) || len(got.Deadlines) != len(trashPurgeDelays) {
				t.Fatalf("condition not established: moved %d (test clock %d to %d), cutoff %d, deadlines %v", got.Moved, before, after, got.Cutoff, got.Deadlines)
			}
			var want []string
			for name, delay := range delays {
				deadline, ok := got.Deadlines[name]
				if !ok || deadline < got.Moved+delay-120 || deadline > got.Moved+delay+120 {
					t.Fatalf("condition not established: the deadline of %s is %d, want %d within two minutes: %s", name, deadline, got.Moved+delay, out)
				}
				if deadline > got.Cutoff {
					want = append(want, name)
				}
			}
			slices.Sort(want)
			t.Logf("RBD_TRASH_PURGE_TIMEZONE_RECEIPT client timezone %s (UTC%+dh): cutoff %s; the cutoff's fields read as local time are %+dh from it; deadlines after the move %v; retained %v, by the instant %v",
				zone.tz, got.UTCOffset/3600, got.CutoffText, (got.MkTime-got.Cutoff)/3600, trashPurgeRelative(got), got.Retained, want)
			if !slices.Equal(got.Retained, want) {
				t.Fatalf("observed defect: a client process %+d h from UTC purged with a cutoff %+d h from the instant it passed: the trash retains %v, and by the instant it must retain %v",
					got.UTCOffset/3600, (got.MkTime-got.Cutoff)/3600, got.Retained, want)
			}
		})
	}
}

// trashPurgeRelative formats each deadline as its distance from the move.
func trashPurgeRelative(got trashPurgeOutcome) []string {
	var out []string
	for _, name := range slices.Sorted(maps.Keys(got.Deadlines)) {
		out = append(out, fmt.Sprintf("%s=%s", name, time.Duration(got.Deadlines[name]-got.Moved)*time.Second))
	}
	return out
}
