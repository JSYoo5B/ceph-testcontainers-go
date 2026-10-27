//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

// ecEligibleScript is the native client of the scenario. Every phase uses a
// writer and a reader connection of its own and prints one JSON receipt that
// carries both RADOS instance IDs: the primary's log names a client write by
// its instance ID, which is how a log entry is tied to an acknowledged write.
//
//   - write: an object of unaligned size and a self-managed snapshot of it.
//   - overwrite RECEIPT: a write under the snapshot's context, acknowledged
//     before the call returns, then reads of the head and of the snapshot by
//     the independent reader.
//   - read RECEIPT: a new reader checks the head and the snapshot against the
//     overwrite receipt, including the versions the reads report.
//   - verify RECEIPT: read, then remove the snapshot.
const ecEligibleScript = `import hashlib,json,sys,rados
phase,pool=sys.argv[1],sys.argv[2]
NAME='eligible'
HEAD=rados.LIBRADOS_SNAP_HEAD
def connected():
    c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'30','rados_mon_op_timeout':'10'})
    c.connect(); return c
def digest(data): return hashlib.sha256(data).hexdigest()
def read(ioctx,snap=HEAD):
    ioctx.set_read(snap)
    try:
        data=ioctx.read(NAME,1<<20,0)
        return data,ioctx.get_last_version()
    finally: ioctx.set_read(HEAD)
original=b''.join(hashlib.sha256(b'%d'%n).digest() for n in range(625))
writer,reader=connected(),connected()
a,b=writer.open_ioctx(pool),reader.open_ioctx(pool)
out={'phase':phase,'writer':writer.get_instance_id(),'reader':reader.get_instance_id()}
assert out['writer']!=out['reader'],out
try:
    if phase=='write':
        a.write_full(NAME,original)
        out['snap']=a.create_self_managed_snap()
        seen,version=read(b)
        assert seen==original,len(seen)
        out.update({'head_sha256':digest(seen),'head_version':version,'size':len(seen)})
    elif phase=='overwrite':
        receipt=json.loads(sys.argv[3]); snap=receipt['snap']
        assert digest(original)==receipt['head_sha256']
        a.set_self_managed_snap_write([snap])
        try: a.write(NAME,b'DEGRADED',4097)
        finally: a.set_self_managed_snap_write([])
        want=original[:4097]+b'DEGRADED'+original[4105:]
        head,head_version=read(b)
        clone,clone_version=read(b,snap)
        assert head==want and clone==original,(len(head),len(clone))
        assert head_version>receipt['head_version'],(head_version,receipt['head_version'])
        out.update({'snap':snap,'head_sha256':digest(head),'head_version':head_version,'snap_sha256':digest(clone),'snap_version':clone_version})
    else:
        receipt=json.loads(sys.argv[3]); snap=receipt['snap']
        head,head_version=read(b)
        clone,clone_version=read(b,snap)
        assert digest(head)==receipt['head_sha256'] and head_version==receipt['head_version'],(digest(head),head_version,receipt)
        assert digest(clone)==receipt['snap_sha256'] and clone_version==receipt['snap_version'],(digest(clone),clone_version,receipt)
        out.update({'snap':snap,'head_sha256':digest(head),'head_version':head_version,'snap_sha256':digest(clone),'snap_version':clone_version})
        if phase=='verify': a.remove_self_managed_snap(snap)
    print(json.dumps(out))
finally:
    a.close(); b.close(); writer.shutdown(); reader.shutdown()
`

// ecEligibleReceipt is what one phase of ecEligibleScript prints.
type ecEligibleReceipt struct {
	Phase       string `json:"phase"`
	Writer      uint64 `json:"writer"`
	Reader      uint64 `json:"reader"`
	Snap        uint64 `json:"snap"`
	HeadSHA256  string `json:"head_sha256"`
	HeadVersion uint64 `json:"head_version"`
	SnapSHA256  string `json:"snap_sha256"`
	SnapVersion uint64 `json:"snap_version"`
	Size        int    `json:"size"`
}

func ecEligiblePhase(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, pool, phase string, previous *ecEligibleReceipt) ecEligibleReceipt {
	t.Helper()
	argv := []string{"python3", "-c", ecEligibleScript, phase, pool}
	if previous != nil {
		raw, err := json.Marshal(previous)
		if err != nil {
			t.Fatal(err)
		}
		argv = append(argv, string(raw))
	}
	result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
		Argv: argv, Timeout: 2 * time.Minute, MaxStdout: 1 << 16, MaxStderr: 1 << 16, Label: "EC eligible " + phase,
	})
	var receipt ecEligibleReceipt
	if err != nil || !result.Complete() || result.ExitCode != 0 || json.Unmarshal([]byte(lastJSONLine(string(result.Stdout))), &receipt) != nil ||
		receipt.Phase != phase || receipt.Writer == 0 || receipt.Reader == 0 || receipt.Writer == receipt.Reader {
		t.Fatalf("%s phase: exit %d, complete %t: %v\nstdout: %s\nstderr: %s", phase, result.ExitCode, result.Complete(), err, result.Stdout, result.Stderr)
	}
	t.Logf("EC_ELIGIBLE_CLIENT %s", lastJSONLine(string(result.Stdout)))
	return receipt
}

// ecPGCommand runs one primary-side PG command through the control CLI. The
// "pg <pgid> <command>" form is the one that "query" uses; the "tell" form is
// the fallback and the raw error of the first form is retained when both fail.
func ecPGCommand(t *testing.T, ctx context.Context, cluster *ceph.Container, pgid, command string) []byte {
	t.Helper()
	out, err := cluster.Ceph(ctx, "pg", pgid, command, "--format", "json")
	if err == nil {
		return out
	}
	t.Logf("EC_ELIGIBLE_COMMAND ceph pg %s %s failed: %v", pgid, command, err)
	out, tellErr := cluster.Ceph(ctx, "tell", pgid, command, "--format", "json")
	if tellErr != nil {
		t.Fatalf("pg %s %s: %v; tell form: %v", pgid, command, err, tellErr)
	}
	return out
}

// ecObserve reads the PG report before and after the log and the unfound
// list, and retries while the epoch moves between the two reports.
func ecObserve(t *testing.T, ctx context.Context, cluster *ceph.Container, pool string, label string) ecObservation {
	t.Helper()
	var last ecObservation
	for attempt := 1; attempt <= 5; attempt++ {
		snapshot, err := poolHandle(t, ctx, cluster, pool).PGs(ctx)
		if err != nil || len(snapshot.PGs) != 1 {
			t.Fatalf("PGs of %s: %+v, %v", pool, snapshot.PGs, err)
		}
		pg := snapshot.PGs[0]
		before := ecPGCommand(t, ctx, cluster, pg.PGID.String(), "query")
		logRaw := ecPGCommand(t, ctx, cluster, pg.PGID.String(), "log")
		unfoundRaw := ecPGCommand(t, ctx, cluster, pg.PGID.String(), "list_unfound")
		after := ecPGCommand(t, ctx, cluster, pg.PGID.String(), "query")
		obs := ecObservation{Primary: pg.ActingPrimary}
		var errs [4]error
		obs.Before, errs[0] = parseECPGReport(before)
		obs.Log, errs[1] = parseECPGLog(logRaw)
		obs.Unfound, errs[2] = parseECUnfound(unfoundRaw)
		obs.After, errs[3] = parseECPGReport(after)
		for _, err := range errs {
			if err != nil {
				t.Fatalf("%s observation: %v\nquery: %.4096s\nlog: %.4096s\nlist_unfound: %.4096s", label, err, before, logRaw, unfoundRaw)
			}
		}
		t.Logf("EC_ELIGIBLE_RAW %s attempt %d pg %s: query %.16384s", label, attempt, pg.PGID, before)
		t.Logf("EC_ELIGIBLE_RAW %s attempt %d pg %s: log %.16384s", label, attempt, pg.PGID, logRaw)
		t.Logf("EC_ELIGIBLE_RAW %s attempt %d pg %s: list_unfound %.16384s", label, attempt, pg.PGID, unfoundRaw)
		last = obs
		if obs.Before.Epoch == obs.After.Epoch && obs.Before.State == obs.After.State && slices.Equal(obs.Before.Acting, obs.After.Acting) {
			return obs
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("%s: the PG kept moving: epoch %d to %d, %s to %s", label, last.Before.Epoch, last.After.Epoch, last.Before.State, last.After.State)
	return last
}

// ecClassify classifies the head and the clone of the scenario object against
// the writer that produced them.
func ecClassify(t *testing.T, obs ecObservation, writer, snap uint64, withClone bool) (head, clone ecClassification) {
	t.Helper()
	headTarget := ecTarget{Name: "eligible"}
	need, err := ecRequiredVersion(obs.Log, headTarget, writer)
	head = classifyECObject(obs, 2, headTarget, need, err)
	if withClone {
		cloneTarget := ecTarget{Name: "eligible", Snap: snap}
		need, err = ecRequiredVersion(obs.Log, cloneTarget, writer)
		clone = classifyECObject(obs, 2, cloneTarget, need, err)
	}
	return head, clone
}

func ecExpect(t *testing.T, label string, c ecClassification, verdict ecVerdict, status string, indices []int) {
	t.Helper()
	got := c.indices(status)
	t.Logf("EC_ELIGIBLE_CLASS %s %v: need %v, verdict %s, fragments %+v, %s", label, c.Target, c.Need, c.Verdict, c.Fragments, c.Detail)
	if c.Verdict != verdict || !slices.Equal(got, indices) {
		t.Fatalf("%s %v: verdict %s with %s %v (%s), want %s with %v", label, c.Target, c.Verdict, status, got, c.Detail, verdict, indices)
	}
}

func ecWaitPG(t *testing.T, parent context.Context, cluster *ceph.Container, pool, what string, ok func(ceph.PGState) bool) ceph.PGState {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	var last string
	for {
		snapshot, err := poolHandle(t, ctx, cluster, pool).PGs(ctx)
		if err == nil && len(snapshot.PGs) == 1 {
			if ok(snapshot.PGs[0]) {
				return snapshot.PGs[0]
			}
			last = fmt.Sprintf("%s up %v acting %v", snapshot.PGs[0].State, snapshot.PGs[0].Up, snapshot.PGs[0].Acting)
		} else {
			last = fmt.Sprintf("%v, %d PGs", err, len(snapshot.PGs))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("PG of %s never %s: %s", pool, what, last)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// ecPoolFacts records the pool's native k, m, min_size, plugin and overwrite
// flag, and requires the ones that this scenario depends on.
func ecPoolFacts(t *testing.T, ctx context.Context, cluster *ceph.Container, pool *ceph.Pool) {
	t.Helper()
	detail, err := cluster.Ceph(ctx, "osd", "pool", "ls", "detail", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var pools []struct {
		Name      string `json:"pool_name"`
		Size      int    `json:"size"`
		MinSize   int    `json:"min_size"`
		Profile   string `json:"erasure_code_profile"`
		FlagNames string `json:"flags_names"`
	}
	if err := json.Unmarshal(detail, &pools); err != nil {
		t.Fatalf("osd pool ls detail: %v: %.2048s", err, detail)
	}
	index := slices.IndexFunc(pools, func(p struct {
		Name      string `json:"pool_name"`
		Size      int    `json:"size"`
		MinSize   int    `json:"min_size"`
		Profile   string `json:"erasure_code_profile"`
		FlagNames string `json:"flags_names"`
	}) bool {
		return p.Name == pool.Name
	})
	if index < 0 {
		t.Fatalf("pool %s is not in osd pool ls detail: %.2048s", pool.Name, detail)
	}
	facts := pools[index]
	profile, err := cluster.Ceph(ctx, "osd", "erasure-code-profile", "get", facts.Profile, "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var native map[string]string
	if err := json.Unmarshal(profile, &native); err != nil {
		t.Fatalf("erasure-code-profile get: %v: %.2048s", err, profile)
	}
	t.Logf("EC_ELIGIBLE_POOL %s: size %d min_size %d flags %q profile %s %v", pool.Name, facts.Size, facts.MinSize, facts.FlagNames, facts.Profile, native)
	if facts.Size != 3 || facts.MinSize != 2 || native["k"] != "2" || native["m"] != "1" || !strings.Contains(facts.FlagNames, "ec_overwrites") {
		t.Fatalf("pool %s is not the permissive 2+1 overwrite pool: %+v profile %v", pool.Name, facts, native)
	}
}

// testECEligibleFragments is the receipt of eligible fragment indices for an
// acknowledged degraded overwrite in a K=2, M=1 pool with an explicit
// min_size of 2 (K), the permissive case that the default K+1 would refuse.
// With one non-primary fragment holder stopped, a write under a snapshot
// context is acknowledged by the two remaining fragments. The holder then
// returns while recovery is held by norecover, and recovery is released.
// Three primary-side observations (clean, degraded, held) and a final clean
// one are joined with the writer's identity, and an independent reader checks
// bytes and versions of the head and the snapshot in every phase.
func testECEligibleFragments(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithOSDCount(3))
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{
		Name: "tc-ec-eligible", Application: "rados", PGNum: 1, MinSize: 2,
		ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	ecPoolFacts(t, ctx, cluster, pool)
	name := pool.Name
	clean := ecWaitPG(t, ctx, cluster, name, "clean on three OSDs", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && len(pg.Acting) == 3 && !slices.Contains(pg.Acting, ecFragmentNone)
	})
	var victim *ceph.OSDContainer
	victimIndex := -1
	for index, id := range clean.Acting {
		if id == clean.ActingPrimary {
			continue
		}
		for _, osd := range clusteraccess.OSDs(t, cluster) {
			if osd.ID == id && victim == nil {
				victim, victimIndex = osd, index
			}
		}
	}
	if victim == nil {
		t.Fatalf("no owned non-primary fragment holder: %+v", clean)
	}
	primaryIndex := slices.Index(clean.Acting, clean.ActingPrimary)
	t.Logf("EC_ELIGIBLE_PLACEMENT pg %s acting %v primary osd.%d (fragment %d) victim osd.%d (fragment %d)",
		clean.PGID, clean.Acting, clean.ActingPrimary, primaryIndex, victim.ID, victimIndex)

	written := ecEligiblePhase(t, ctx, cluster, client, name, "write", nil)
	baseline := ecObserve(t, ctx, cluster, name, "clean")
	head, _ := ecClassify(t, baseline, written.Writer, written.Snap, false)
	ecExpect(t, "clean", head, ecSufficient, ecEligible, []int{0, 1, 2})

	noout, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := noout.Restore(cleanup); err != nil {
			t.Errorf("restore noout: %v", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	stopTimeout := 20 * time.Second
	if err := victim.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	wantActing := slices.Clone(clean.Acting)
	wantActing[victimIndex] = ecFragmentNone
	ecWaitPG(t, ctx, cluster, name, "active without the fragment holder", func(pg ceph.PGState) bool {
		return strings.HasPrefix(pg.State, "active+") && strings.Contains(pg.State, "degraded") && slices.Equal(pg.Acting, wantActing)
	})
	overwritten := ecEligiblePhase(t, ctx, cluster, client, name, "overwrite", &written)
	if overwritten.Writer == written.Writer {
		t.Fatalf("the degraded overwrite reused client.%d", written.Writer)
	}
	others := []int{}
	for index := range clean.Acting {
		if index != victimIndex {
			others = append(others, index)
		}
	}

	degraded := ecObserve(t, ctx, cluster, name, "degraded")
	head, clone := ecClassify(t, degraded, overwritten.Writer, overwritten.Snap, true)
	ecExpect(t, "degraded", head, ecSufficient, ecEligible, others)
	ecExpect(t, "degraded", clone, ecSufficient, ecEligible, others)
	ecExpect(t, "degraded", head, ecSufficient, ecUnobserved, []int{victimIndex})
	if degraded.Unfound.NumUnfound != 0 || len(degraded.Unfound.Objects) != 0 {
		t.Fatalf("degraded list_unfound: %+v", degraded.Unfound)
	}

	hold, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoRecover, true)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := hold.Restore(cleanup); err != nil {
			t.Errorf("restore norecover: %v", err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := victim.Start(ctx); err != nil {
		t.Fatal(err)
	}
	ecWaitPG(t, ctx, cluster, name, "held in recovery with the fragment holder back", func(pg ceph.PGState) bool {
		return slices.Equal(pg.Acting, clean.Acting) && strings.Contains(pg.State, "recover")
	})
	held := ecObserve(t, ctx, cluster, name, "held")
	head, clone = ecClassify(t, held, overwritten.Writer, overwritten.Snap, true)
	for _, c := range []ecClassification{head, clone} {
		ecExpect(t, "held", c, ecSufficient, ecEligible, others)
		ecExpect(t, "held", c, ecSufficient, ecBehind, []int{victimIndex})
	}
	// The recovery map lists the objects that the returned fragment misses.
	// The list of unfound objects is empty and complete: it does not name the
	// recoverable objects, and its absence says nothing about their fragments.
	if held.Unfound.NumMissing < 1 || held.Unfound.NumUnfound != 0 || len(held.Unfound.Objects) != 0 || held.Unfound.More {
		t.Fatalf("held list_unfound: %+v", held.Unfound)
	}
	ecEligiblePhase(t, ctx, cluster, client, name, "read", &overwritten)
	heldRecovered := held.After.Info.Stats.StatSum.Recovered

	if err := hold.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	restored := ecWaitPG(t, ctx, cluster, name, "clean on three OSDs again", func(pg ceph.PGState) bool {
		return pg.State == "active+clean" && slices.Equal(pg.Acting, clean.Acting)
	})
	final := ecObserve(t, ctx, cluster, name, "recovered")
	head, clone = ecClassify(t, final, overwritten.Writer, overwritten.Snap, true)
	ecExpect(t, "recovered", head, ecSufficient, ecEligible, []int{0, 1, 2})
	ecExpect(t, "recovered", clone, ecSufficient, ecEligible, []int{0, 1, 2})
	if delta := final.After.Info.Stats.StatSum.Recovered - heldRecovered; delta < 1 {
		t.Fatalf("num_objects_recovered went from %d to %d across the release", heldRecovered, final.After.Info.Stats.StatSum.Recovered)
	}
	ecEligiblePhase(t, ctx, cluster, client, name, "verify", &overwritten)

	receipt, err := json.Marshal(map[string]any{
		"pg": restored.PGID, "acting": clean.Acting, "primary": clean.ActingPrimary, "victim": victim.ID, "victim_fragment": victimIndex,
		"writer": overwritten.Writer, "snap": overwritten.Snap,
		"required":         map[string]string{"head": fmt.Sprint(head.Need), "clone": fmt.Sprint(clone.Need)},
		"held_num_missing": held.Unfound.NumMissing, "recovered_objects": final.After.Info.Stats.StatSum.Recovered - heldRecovered,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EC_ELIGIBLE_RECEIPT %s", receipt)
}
