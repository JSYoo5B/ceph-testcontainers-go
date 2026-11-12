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
// image name. The cutoff of the purge lies trashPurgeCutoff after the last
// image was moved to the trash, between the second and the third deadline.
// A cutoff read trashPurgeShift early falls between the first and the
// second, and one read that much late between the third and the fourth.
var trashPurgeDelays = map[string]time.Duration{
	"due-10m": 10 * time.Minute,
	"due-6h":  6 * time.Hour,
	"due-15h": 15 * time.Hour,
	"due-30h": 30 * time.Hour,
}

const (
	trashPurgeCutoff = 10 * time.Hour
	trashPurgeShift  = 9 * time.Hour
	// trashPurgeMargin is how far every deadline of every pool must lie
	// from the cutoff and from the cutoff shifted either way.
	trashPurgeMargin = 30 * time.Minute
)

// trashPurgeScript has two modes. "move" creates the images of a pool, moves
// them to its trash and reports the trash. "purge" purges the trash of a
// pool with a timezone-aware datetime of the instant it is given, in seconds
// since the epoch, and reports the trash before and after. It runs in the
// client process timezone its environment gives it.
const trashPurgeScript = `import calendar,datetime,json,sys,time
import rados,rbd
mode,pool=sys.argv[1],sys.argv[2]
def epoch(d):
    return int(d.timestamp()) if d.tzinfo else calendar.timegm(d.timetuple())
def trash(r,ioctx):
    return sorted(({'id':e['id'],'name':e['name'],'deadline':epoch(e['deferment_end_time'])} for e in r.trash_list(ioctx)),key=lambda e:e['name'])
with rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'30','rados_mon_op_timeout':'30'}) as cluster:
    with cluster.open_ioctx(pool) as ioctx:
        r=rbd.RBD()
        out={'tzname':time.tzname[0],'utc_offset':-time.timezone}
        if mode=='move':
            out['started']=int(time.time())
            for name,delay in json.loads(sys.argv[3]).items():
                r.create(ioctx,name,1<<20)
                r.trash_move(ioctx,name,delay)
            out['moved']=int(time.time())
            out['before']=trash(r,ioctx)
        else:
            cutoff=datetime.datetime.fromtimestamp(int(sys.argv[3]),datetime.timezone.utc)
            out.update({'cutoff':int(cutoff.timestamp()),'cutoff_text':cutoff.isoformat(),'mktime':int(time.mktime(cutoff.timetuple()))})
            out['before']=trash(r,ioctx)
            r.trash_purge(ioctx,cutoff)
            out['after']=trash(r,ioctx)
        print(json.dumps(out))
`

// trashPurgeOutcome is what trashPurgeScript reports, with times in seconds
// since the epoch. Started and Moved bound the moves to the trash. MkTime is
// the local-time reading of the cutoff's fields.
type trashPurgeOutcome struct {
	TZName     string            `json:"tzname"`
	UTCOffset  int64             `json:"utc_offset"`
	Started    int64             `json:"started"`
	Moved      int64             `json:"moved"`
	Cutoff     int64             `json:"cutoff"`
	CutoffText string            `json:"cutoff_text"`
	MkTime     int64             `json:"mktime"`
	Before     []trashPurgeEntry `json:"before"`
	After      []trashPurgeEntry `json:"after"`
}

// trashPurgeZones are the client timezones. POSIX timezone strings need no
// zone database: "TST-9" is nine hours ahead of UTC and "TST9" nine behind.
var trashPurgeZones = []struct {
	name, tz string
	offset   int64
}{{"utc", "UTC0", 0}, {"ahead-9h", "TST-9", 9 * 3600}, {"behind-9h", "TST9", -9 * 3600}}

// TestRBDTrashPurgeCutoffAcrossClientTimezones purges equivalent trash
// contents with one timezone-aware cutoff from client processes in three
// timezones and requires each to remove exactly the images whose deferment
// ended by that instant. It reports one rbd_trash_purge_timezone verdict
// with the observations ahead-9h and behind-9h; the purge from UTC is their
// control. judgeTrashPurge tells the failed UTC control and a result no
// reading of the cutoff explains from the timezone shift.
func TestRBDTrashPurgeCutoffAcrossClientTimezones(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	repro := harness.StartReproduction(t, "rbd_trash_purge_timezone", "ahead-9h", "behind-9h")
	cluster, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("client: %s", harness.ExecOutput(t, ctx, client, "ceph", "--version"))
	script := func(t *testing.T, tz string, args ...string) (trashPurgeOutcome, string) {
		t.Helper()
		out := harness.ExecOutput(t, ctx, client, append([]string{"env", "TZ=" + tz, "python3", "-c", trashPurgeScript}, args...)...)
		var got trashPurgeOutcome
		if err := json.Unmarshal([]byte(out), &got); err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		return got, out
	}
	delays := map[string]int64{}
	for name, delay := range trashPurgeDelays {
		delays[name] = int64(delay / time.Second)
	}
	encoded, err := json.Marshal(delays)
	if err != nil {
		t.Fatal(err)
	}

	// Every pool gets its images in the trash before any purge, by a
	// process in UTC, so that one cutoff can be chosen for all of them.
	moved := map[string][]trashPurgeEntry{}
	var last int64
	for _, zone := range trashPurgeZones {
		pool := trashPurgePool(zone.name)
		if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 1, Application: "rbd"}); err != nil {
			t.Fatal(err)
		}
		got, out := script(t, "UTC0", "move", pool, string(encoded))
		if len(got.Before) != len(delays) {
			repro.Inconclusive(t, "condition not established: the trash of %s holds %d images, want %d: %s", pool, len(got.Before), len(delays), out)
		}
		for _, entry := range got.Before {
			delay, ok := delays[entry.Name]
			if !ok || entry.ID == "" || entry.Deadline < got.Started+delay-120 || entry.Deadline > got.Moved+delay+120 {
				repro.Inconclusive(t, "condition not established: %s in the trash of %s has the deadline %d, want its delay after the moves (%d to %d) within two minutes: %s", trashPurgeName(entry), pool, entry.Deadline, got.Started, got.Moved, out)
			}
		}
		moved[pool], last = got.Before, max(last, got.Moved)
	}
	// The one cutoff of all three purges. Every recorded deadline of every
	// pool must lie clear of it and of where a shifted reading puts it, with
	// the same images on either side in each pool.
	cutoff := last + int64(trashPurgeCutoff/time.Second)
	if err := trashPurgeBracket(moved, cutoff); err != nil {
		repro.Inconclusive(t, "condition not established: %v", err)
	}
	t.Logf("cutoff of every purge: %d (%s)", cutoff, time.Unix(cutoff, 0).UTC().Format(time.RFC3339))

	// The purge from UTC is the control and runs first. The other purges
	// are judged only after it removed what the instant requires.
	control := true
	for _, zone := range trashPurgeZones {
		passed := t.Run(zone.name, func(t *testing.T) {
			pool := trashPurgePool(zone.name)
			if !control {
				repro.Inconclusive(t, "control failed: the purge from a client process in UTC failed, and the trash of %s was not purged", pool)
			}
			got, out := script(t, zone.tz, "purge", pool, fmt.Sprint(cutoff))
			// Conditions: the client process ran in the timezone, it built
			// its datetime from the one cutoff, and it found the trash as
			// the moves left it, with the deadlines read the same in its
			// timezone.
			if got.UTCOffset != zone.offset {
				repro.Inconclusive(t, "condition not established: the client's UTC offset is %d s, want %d: %s", got.UTCOffset, zone.offset, out)
			}
			if got.Cutoff != cutoff {
				repro.Inconclusive(t, "condition not established: the client purged with the cutoff %d, want %d: %s", got.Cutoff, cutoff, out)
			}
			if !slices.Equal(got.Before, moved[pool]) {
				repro.Inconclusive(t, "condition not established: the trash before the purge is %v, and the moves left %v", got.Before, moved[pool])
			}
			// The fields of the cutoff read as the client's local time name
			// an instant the client's UTC offset earlier.
			if got.MkTime-got.Cutoff != -zone.offset {
				repro.Inconclusive(t, "condition not established: the cutoff's fields read as local time are %d s from the cutoff, want %d: %s", got.MkTime-got.Cutoff, -zone.offset, out)
			}
			t.Logf("RBD_TRASH_PURGE_TIMEZONE_RECEIPT client timezone %s (UTC%+dh): cutoff %d (%s); the cutoff's fields read as local time are %+dh from it; deadlines from the cutoff %v; retained %v, by the instant %v, by the local reading %v",
				zone.tz, got.UTCOffset/3600, got.Cutoff, got.CutoffText, (got.MkTime-got.Cutoff)/3600, trashPurgeRelative(moved[pool], cutoff), trashPurgeNames(got.After), trashPurgeKept(moved[pool], cutoff), trashPurgeKept(moved[pool], got.MkTime))
			symptom, detail, err := judgeTrashPurge(got.UTCOffset, got.Cutoff, got.MkTime, moved[pool], got.After)
			switch {
			case err != nil:
				repro.Inconclusive(t, "%v", err)
			case zone.offset == 0:
				t.Logf("control held: %s", detail)
			case symptom:
				repro.Symptom(t, zone.name, "%s", detail)
			default:
				repro.Absent(t, zone.name, "%s", detail)
			}
		})
		if zone.offset == 0 {
			control = passed
		}
	}
	repro.Conclude()
}

func trashPurgePool(zone string) string { return "tc-trash-purge-" + zone }

// trashPurgeBracket checks that, in every pool, no deadline lies within
// trashPurgeMargin of the cutoff or of the cutoff shifted by trashPurgeShift
// either way, and that the cutoff has the two shortest delays before it, the
// early reading one and the late reading three.
func trashPurgeBracket(pools map[string][]trashPurgeEntry, cutoff int64) error {
	shift, margin := int64(trashPurgeShift/time.Second), int64(trashPurgeMargin/time.Second)
	for _, pool := range slices.Sorted(maps.Keys(pools)) {
		for at, due := range map[int64]int{cutoff - shift: 1, cutoff: 2, cutoff + shift: 3} {
			ended := 0
			for _, entry := range pools[pool] {
				if entry.Deadline > at-margin && entry.Deadline < at+margin {
					return fmt.Errorf("%s in the trash of %s has the deadline %d, within %s of %d (the cutoff %d shifted by %s)", trashPurgeName(entry), pool, entry.Deadline, trashPurgeMargin, at, cutoff, time.Duration(at-cutoff)*time.Second)
				}
				if entry.Deadline <= at {
					ended++
				}
			}
			if ended != due {
				return fmt.Errorf("%d deadlines in the trash of %s end by %d (the cutoff %d shifted by %s), want %d: %v", ended, pool, at, cutoff, time.Duration(at-cutoff)*time.Second, due, pools[pool])
			}
		}
	}
	return nil
}

// trashPurgeRelative formats each deadline as its distance from the cutoff.
func trashPurgeRelative(entries []trashPurgeEntry, cutoff int64) []string {
	var out []string
	for _, entry := range entries {
		out = append(out, fmt.Sprintf("%s=%s", entry.Name, time.Duration(entry.Deadline-cutoff)*time.Second))
	}
	return out
}
