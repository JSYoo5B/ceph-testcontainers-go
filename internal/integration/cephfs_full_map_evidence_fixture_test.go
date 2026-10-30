//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// These controls feed the map-evidence reader with synthetic admin socket
// output in the shape of the pinned Ceph source. They test parsing, the
// unavailable rules and the receipt layout; they do not show what a real
// cluster prints and they do not reproduce the native timeout.

const (
	perfDump       = `{"objecter":{"map_epoch":25,"map_full":2,"map_inc":7,"osdop_read":3}}`
	statusDump     = `{"id":4261,"osd_epoch":25,"osd_epoch_barrier":20,"blocklisted":false}`
	requestsEmpty  = `{"ops":[],"linger_ops":[],"pool_ops":[],"pool_stat_ops":[],"statfs_ops":[],"command_ops":[]}`
	osdStatus      = `{"cluster_fsid":"f","osd_fsid":"o","whoami":0,"state":"active","maps":"[1~31]","oldest_map":"1","newest_map":"31","cluster_osdmap_trim_lower_bound":1,"num_pgs":9}`
	osdDump        = `{"epoch":31,"fsid":"f"}`
	historicClient = `{"size":20,"duration":600,"ops":[` +
		`{"description":"osd_op(client.4261.0:41 3.1 3:8c1:::10000000000.00000000:head [read 0~4096] snapc 0=[] ondisk+read+known_if_redirected e24)","initiated_at":"2026-01-01T00:00:00.1+0000","type_data":{"flag_point":"started","client_info":{"client":"client.4261","client_addr":"v1:10.0.0.9:0/1","tid":41}}},` +
		`{"description":"osd_op(client.4999.0:7 3.2 3:8c2:::other:head [write 0~1] snapc 0=[] e30)","initiated_at":"x","type_data":{"client_info":{"client":"client.4999","tid":7}}},` +
		`{"description":"osd_op()","initiated_at":"y","type_data":{"client_info":{"client":"client.4261","tid":42}}},` +
		`{"description":"pg_scan","initiated_at":"z","type_data":{"flag_point":"started"}}]}`
	inFlightNone = `{"ops":[],"num_ops":0}`
)

type fakeCluster struct {
	client map[string]string // command words -> output
	osd    map[string]string
	mon    map[string]string
	fail   map[string]error
}

func (f fakeCluster) sources(osds ...int) harness.MapEvidenceSources {
	pick := func(table map[string]string, key string) ([]byte, error) {
		if err, ok := f.fail[key]; ok {
			return nil, err
		}
		out, ok := table[key]
		if !ok {
			return nil, errors.New("no such command")
		}
		return []byte(out), nil
	}
	return harness.MapEvidenceSources{
		ClientAdmin: func(_ context.Context, args ...string) ([]byte, error) {
			return pick(f.client, "client "+strings.Join(args, " "))
		},
		OSDAdmin: func(_ context.Context, osd int, args ...string) ([]byte, error) {
			return pick(f.osd, fmt.Sprintf("osd.%d %s", osd, strings.Join(args, " ")))
		},
		MonCommand: func(_ context.Context, args ...string) ([]byte, error) {
			return pick(f.mon, strings.Join(args, " "))
		},
		OSDs: osds,
	}
}

func healthyCluster() fakeCluster {
	return fakeCluster{
		client: map[string]string{
			"client perf dump objecter": perfDump, "client status": statusDump, "client objecter_requests": requestsEmpty,
		},
		osd: map[string]string{
			"osd.0 status": osdStatus, "osd.0 dump_historic_ops": historicClient, "osd.0 dump_ops_in_flight": inFlightNone,
		},
		mon: map[string]string{"osd dump --format json": osdDump},
	}
}

// failedWait runs a map wait that times out and returns its receipt.
func failedWait(t *testing.T, phase string, target uint32) *harness.RetainedClientReceipt {
	t.Helper()
	r := harness.NewRetainedClientReceipt(retainedClientID, 1099511627776, 3)
	err := r.WaitMap(context.Background(), phase, target, true, 20*time.Millisecond, time.Second, time.Millisecond, statusSequence(clientAt(retainedClientID, target-1, 20, false)))
	if err == nil {
		t.Fatal("stale epoch accepted")
	}
	return r
}

func TestMapEvidenceRecordsClientAndOSDViewsOfAFailedWait(t *testing.T) {
	src := healthyCluster().sources(0)
	r := harness.NewRetainedClientReceipt(retainedClientID, 1099511627776, 3)
	r.BeginMapEvidence(context.Background(), "full map", src)
	_ = r.WaitMap(context.Background(), "full map", 26, true, 20*time.Millisecond, time.Second, time.Millisecond, statusSequence(clientAt(retainedClientID, 25, 20, false)))
	// More maps reach the client between the baseline and the capture.
	later := healthyCluster()
	later.client["client perf dump objecter"] = `{"objecter":{"map_epoch":25,"map_full":2,"map_inc":9}}`
	r.CaptureMapEvidence(context.Background(), "full map", later.sources(0))
	if len(r.MapEvidence) != 1 {
		t.Fatalf("evidence = %+v", r.MapEvidence)
	}
	ev := r.MapEvidence[0]
	if ev.Phase != "full map" || ev.TargetEpoch != 26 || ev.WaitOutcome != "timeout" || ev.CaptureToMs < ev.CaptureFromMs {
		t.Fatalf("evidence header = %+v", ev)
	}
	if ev.MapFullDelta == nil || *ev.MapFullDelta != 0 || ev.MapIncDelta == nil || *ev.MapIncDelta != 2 {
		t.Fatalf("deltas = %v %v note %q", ev.MapFullDelta, ev.MapIncDelta, ev.DeltaNote)
	}
	if ev.ClientStatus.Unavailable != "" || *ev.ClientStatus.OSDEpoch != 25 || *ev.ClientStatus.Barrier != 20 {
		t.Fatalf("status = %+v", ev.ClientStatus)
	}
	if ev.Requests.Unavailable != "" || *ev.Requests.Ops != 0 || *ev.Requests.CommandOps != 0 {
		t.Fatalf("requests = %+v", ev.Requests)
	}
	if ev.ClusterEpoch.Epoch == nil || *ev.ClusterEpoch.Epoch != 31 {
		t.Fatalf("cluster epoch = %+v", ev.ClusterEpoch)
	}
	if len(ev.OSDs) != 1 || *ev.OSDs[0].Maps.NewestMap != 31 || *ev.OSDs[0].Maps.OldestMap != 1 {
		t.Fatalf("osds = %+v", ev.OSDs)
	}
	hist := ev.OSDs[0].Historic
	if hist.Unavailable != "" || *hist.Listed != 4 || *hist.Matching != 2 || len(hist.Ops) != 2 {
		t.Fatalf("historic = %+v", hist)
	}
	if hist.Ops[0].Epoch == nil || *hist.Ops[0].Epoch != 24 || *hist.Ops[0].TID != 41 {
		t.Fatalf("op = %+v", hist.Ops[0])
	}
	// The undecoded operation names the client but prints no epoch.
	if hist.Ops[1].Epoch != nil || *hist.Ops[1].TID != 42 {
		t.Fatalf("undecoded op = %+v", hist.Ops[1])
	}
	if got := ev.OSDs[0].InFlight; got.Unavailable != "" || *got.Matching != 0 || *got.Listed != 0 {
		t.Fatalf("in flight = %+v", got)
	}
}

func TestMapEvidenceDoesNotChangeTheWaitRecord(t *testing.T) {
	r := failedWait(t, "full map", 26)
	before := fmt.Sprintf("%+v", r.Waits)
	r.CaptureMapEvidence(context.Background(), "full map", healthyCluster().sources(0))
	if after := fmt.Sprintf("%+v", r.Waits); after != before {
		t.Fatalf("wait changed:\n%s\n%s", before, after)
	}
	if len(r.MapEvidence) != 1 || r.MapEvidence[0].WaitOutcome != "timeout" {
		t.Fatalf("evidence = %+v", r.MapEvidence)
	}
}

func TestMapEvidenceKeepsTheTwoIncidentsApart(t *testing.T) {
	r := failedWait(t, "full map", 26)
	r.CaptureMapEvidence(context.Background(), "full map", healthyCluster().sources(0))
	err := r.WaitMap(context.Background(), "drained map", 31, true, 20*time.Millisecond, time.Second, time.Millisecond, statusSequence(clientAt(retainedClientID, 30, 20, false)))
	if err == nil {
		t.Fatal("stale epoch accepted")
	}
	drained := healthyCluster()
	drained.client["client perf dump objecter"] = `{"objecter":{"map_epoch":30,"map_full":4,"map_inc":11}}`
	r.CaptureMapEvidence(context.Background(), "drained map", drained.sources(0))
	if len(r.MapEvidence) != 2 || r.MapEvidence[0].TargetEpoch != 26 || r.MapEvidence[1].TargetEpoch != 31 || r.MapEvidence[0].Phase != "full map" || r.MapEvidence[1].Phase != "drained map" {
		t.Fatalf("evidence = %+v", r.MapEvidence)
	}
	if *r.MapEvidence[0].Final.MapInc != 7 || *r.MapEvidence[1].Final.MapInc != 11 {
		t.Fatalf("counters mixed: %+v", r.MapEvidence)
	}
}

func TestMapEvidenceIsNotCapturedForAPhaseWithoutAWait(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1, 3)
	r.BeginMapEvidence(context.Background(), "full map", healthyCluster().sources(0))
	r.CaptureMapEvidence(context.Background(), "full map", healthyCluster().sources(0))
	if len(r.MapEvidence) != 0 {
		t.Fatalf("evidence without a wait = %+v", r.MapEvidence)
	}
}

func TestMapEvidenceAbsentOnAReachedWait(t *testing.T) {
	r := harness.NewRetainedClientReceipt(retainedClientID, 1, 3)
	r.BeginMapEvidence(context.Background(), "full map", healthyCluster().sources(0))
	if err := waitFor(r, 24, time.Second, statusSequence(clientAt(retainedClientID, 24, 20, false))); err != nil {
		t.Fatal(err)
	}
	if line := r.Line(); strings.Contains(line, "map_evidence") || strings.Contains(line, "counters_at") {
		t.Fatalf("a passing wait carries evidence: %s", line)
	}
}

func TestMapEvidenceUnsupportedCommandsAreUnavailableNotEvidence(t *testing.T) {
	cluster := healthyCluster()
	unsupported := errors.New("admin socket command not found: exit status 22")
	cluster.fail = map[string]error{
		"client perf dump objecter": unsupported, "client status": unsupported, "client objecter_requests": unsupported,
		"osd dump --format json": unsupported, "osd.0 status": unsupported, "osd.0 dump_historic_ops": unsupported, "osd.0 dump_ops_in_flight": unsupported,
	}
	r := harness.NewRetainedClientReceipt(retainedClientID, 1099511627776, 3)
	r.BeginMapEvidence(context.Background(), "full map", cluster.sources(0))
	_ = r.WaitMap(context.Background(), "full map", 26, true, 20*time.Millisecond, time.Second, time.Millisecond, statusSequence(clientAt(retainedClientID, 25, 20, false)))
	r.CaptureMapEvidence(context.Background(), "full map", cluster.sources(0))
	ev := r.MapEvidence[0]
	for name, unavailable := range map[string]string{
		"baseline": ev.Baseline.Unavailable, "final": ev.Final.Unavailable, "status": ev.ClientStatus.Unavailable, "requests": ev.Requests.Unavailable,
		"cluster": ev.ClusterEpoch.Unavailable, "osd status": ev.OSDs[0].Maps.Unavailable, "historic": ev.OSDs[0].Historic.Unavailable, "in flight": ev.OSDs[0].InFlight.Unavailable,
	} {
		if !strings.Contains(unavailable, "not found") {
			t.Errorf("%s unavailable = %q", name, unavailable)
		}
	}
	if ev.MapFullDelta != nil || ev.MapIncDelta != nil || ev.DeltaNote == "" || ev.Requests.Ops != nil || ev.ClusterEpoch.Epoch != nil ||
		ev.OSDs[0].Historic.Listed != nil || ev.OSDs[0].Historic.Matching != nil || ev.OSDs[0].Maps.NewestMap != nil {
		t.Fatalf("unavailable readings carry values: %+v", ev)
	}
}

func TestMapEvidenceCanceledContextIsUnavailable(t *testing.T) {
	r := failedWait(t, "full map", 26)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	src := healthyCluster().sources(0)
	// The reads ignore the context, as a late result would; a context that
	// ended makes the reading unavailable all the same.
	r.CaptureMapEvidence(ctx, "full map", src)
	ev := r.MapEvidence[0]
	if ev.Final.Unavailable == "" || ev.Requests.Unavailable == "" || ev.OSDs[0].Historic.Unavailable == "" {
		t.Fatalf("a read after the context ended counts as evidence: %+v", ev)
	}
}

func TestMapEvidenceCountersNeedEveryFieldWithItsType(t *testing.T) {
	for name, out := range map[string]string{
		"no objecter":     `{}`,
		"null objecter":   `{"objecter":null}`,
		"missing full":    `{"objecter":{"map_epoch":1,"map_inc":1}}`,
		"null inc":        `{"objecter":{"map_epoch":1,"map_full":1,"map_inc":null}}`,
		"string epoch":    `{"objecter":{"map_epoch":"1","map_full":1,"map_inc":1}}`,
		"negative":        `{"objecter":{"map_epoch":1,"map_full":-1,"map_inc":1}}`,
		"not json":        `perf: no such command`,
		"empty":           ``,
		"wrong top level": `[]`,
	} {
		if got := harness.ParseMapCounters([]byte(out)); got.Unavailable == "" || got.MapEpoch != nil || got.MapFull != nil || got.MapInc != nil {
			t.Errorf("%s: %+v", name, got)
		}
	}
	if got := harness.ParseMapCounters([]byte(perfDump)); got.Unavailable != "" || *got.MapEpoch != 25 || *got.MapFull != 2 || *got.MapInc != 7 {
		t.Fatalf("valid: %+v", got)
	}
	zero := harness.ParseMapCounters([]byte(`{"objecter":{"map_epoch":0,"map_full":0,"map_inc":0}}`))
	if zero.Unavailable != "" || zero.MapFull == nil || *zero.MapFull != 0 {
		t.Fatalf("explicit zeros are values: %+v", zero)
	}
}

func TestMapEvidenceDeltaNeedsBothReadingsAndNoReset(t *testing.T) {
	ok := func(full, inc uint64) harness.MapCounters {
		e := uint64(1)
		return harness.MapCounters{MapEpoch: &e, MapFull: &full, MapInc: &inc}
	}
	if f, i, note := harness.CounterDeltas(ok(1, 5), ok(3, 5)); f == nil || *f != 2 || *i != 0 || note != "" {
		t.Fatalf("delta = %v %v %q", f, i, note)
	}
	for name, pair := range map[string][2]harness.MapCounters{
		"baseline unavailable": {{Unavailable: "x"}, ok(3, 5)},
		"final unavailable":    {ok(1, 5), {Unavailable: "x"}},
		"counter decreased":    {ok(4, 5), ok(3, 5)},
		"inc decreased":        {ok(1, 6), ok(3, 5)},
	} {
		if f, i, note := harness.CounterDeltas(pair[0], pair[1]); f != nil || i != nil || note == "" {
			t.Errorf("%s: %v %v %q", name, f, i, note)
		}
	}
}

func TestMapEvidenceRequestsKeepTheArrayAbsenceApart(t *testing.T) {
	for name, out := range map[string]string{
		"missing ops": `{"linger_ops":[]}`,
		"null ops":    `{"ops":null}`,
		"object ops":  `{"ops":{}}`,
		"not json":    `x`,
	} {
		if got := harness.ParseObjecterRequestsReading([]byte(out)); got.Unavailable == "" || got.Ops != nil {
			t.Errorf("%s: %+v", name, got)
		}
	}
	got := harness.ParseObjecterRequestsReading([]byte(`{"ops":[{"tid":9,"pg":"3.1","osd":0,"paused":1,"attempts":1,"age":2.5}]}`))
	if got.Unavailable != "" || *got.Ops != 1 || got.CommandOps != nil || got.LingerOps != nil || *got.Pending[0].Paused != 1 || *got.Pending[0].OSD != 0 {
		t.Fatalf("one pending op: %+v", got)
	}
	var ops []string
	for i := range harness.MapEvidenceOpLimit + 3 {
		ops = append(ops, fmt.Sprintf(`{"tid":%d}`, i))
	}
	long := harness.ParseObjecterRequestsReading([]byte(`{"ops":[` + strings.Join(ops, ",") + `]}`))
	if *long.Ops != harness.MapEvidenceOpLimit+3 || len(long.Pending) != harness.MapEvidenceOpLimit || !long.Truncated {
		t.Fatalf("truncation: %d listed, %d kept, truncated=%t", *long.Ops, len(long.Pending), long.Truncated)
	}
}

func TestMapEvidenceClientStatusNeedsEveryField(t *testing.T) {
	for name, out := range map[string]string{
		"no id":         `{"osd_epoch":1,"osd_epoch_barrier":1,"blocklisted":false}`,
		"no epoch":      `{"id":1,"osd_epoch_barrier":1,"blocklisted":false}`,
		"null barrier":  `{"id":1,"osd_epoch":1,"osd_epoch_barrier":null,"blocklisted":false}`,
		"no blocklist":  `{"id":1,"osd_epoch":1,"osd_epoch_barrier":1}`,
		"string epoch":  `{"id":1,"osd_epoch":"1","osd_epoch_barrier":1,"blocklisted":false}`,
		"int blocklist": `{"id":1,"osd_epoch":1,"osd_epoch_barrier":1,"blocklisted":0}`,
	} {
		if got := harness.ParseClientStatusReading([]byte(out)); got.Unavailable == "" || got.OSDEpoch != nil {
			t.Errorf("%s: %+v", name, got)
		}
	}
	if got := harness.ParseClientStatusReading([]byte(statusDump)); got.Unavailable != "" || *got.Blocklisted {
		t.Fatalf("valid: %+v", got)
	}
}

func TestMapEvidenceOSDMapRangeNeedsEpochs(t *testing.T) {
	for name, out := range map[string]string{
		"no newest":    `{"state":"active","oldest_map":"1"}`,
		"null newest":  `{"state":"active","oldest_map":"1","newest_map":null}`,
		"text newest":  `{"state":"active","oldest_map":"1","newest_map":"latest"}`,
		"empty newest": `{"state":"active","oldest_map":"1","newest_map":""}`,
		"no state":     `{"oldest_map":"1","newest_map":"2"}`,
		"bool newest":  `{"state":"active","oldest_map":"1","newest_map":true}`,
		"not json":     `x`,
	} {
		if got := harness.ParseOSDMapRange([]byte(out)); got.Unavailable == "" || got.NewestMap != nil {
			t.Errorf("%s: %+v", name, got)
		}
	}
	// The source prints epochs as strings; the same values as numbers are the same epochs.
	for _, out := range []string{osdStatus, `{"state":"active","oldest_map":1,"newest_map":31}`} {
		if got := harness.ParseOSDMapRange([]byte(out)); got.Unavailable != "" || *got.NewestMap != 31 || *got.OldestMap != 1 {
			t.Errorf("%s: %+v", out, got)
		}
	}
}

func TestMapEvidenceOSDOpsMatchOnlyTheRetainedClientAndNeverInventAnEpoch(t *testing.T) {
	for name, out := range map[string]string{
		"no ops":   `{"size":20}`,
		"null ops": `{"ops":null}`,
		"not json": `x`,
	} {
		if got := harness.ParseOSDOpsReading([]byte(out), 4261); got.Unavailable == "" || got.Listed != nil || got.Matching != nil {
			t.Errorf("%s: %+v", name, got)
		}
	}
	// A client whose ID only starts with the retained one's digits does not match.
	other := `{"ops":[{"description":"osd_op(client.42610.0:1 3.1 x e9)","type_data":{"client_info":{"client":"client.42610","tid":1}}}]}`
	if got := harness.ParseOSDOpsReading([]byte(other), 4261); got.Unavailable != "" || *got.Listed != 1 || *got.Matching != 0 || len(got.Ops) != 0 {
		t.Fatalf("other client: %+v", got)
	}
	// A description that names no epoch leaves it absent.
	noEpoch := `{"ops":[{"description":"osd_op(client.4261.0:1 3.1 (undecoded) e)","type_data":{"client_info":{"client":"client.4261"}}},{"type_data":{"client_info":{"client":"client.4261","tid":2}}}]}`
	got := harness.ParseOSDOpsReading([]byte(noEpoch), 4261)
	if *got.Matching != 2 || got.Ops[0].Epoch != nil || got.Ops[1].Epoch != nil || got.Ops[0].TID != nil || got.Ops[1].Description != "" {
		t.Fatalf("no epoch: %+v", got)
	}
	// An empty ops array is an answer, an empty list of this client's ops.
	if got := harness.ParseOSDOpsReading([]byte(`{"ops":[],"num_ops":0}`), 4261); got.Unavailable != "" || *got.Listed != 0 || *got.Matching != 0 {
		t.Fatalf("empty: %+v", got)
	}
	var ops []string
	for i := range harness.MapEvidenceOpLimit + 2 {
		ops = append(ops, fmt.Sprintf(`{"description":"osd_op(client.4261.0:%d 3.1 e%d)","type_data":{"client_info":{"client":"client.4261","tid":%d}}}`, i, i+1, i))
	}
	long := harness.ParseOSDOpsReading([]byte(`{"ops":[`+strings.Join(ops, ",")+`]}`), 4261)
	if *long.Matching != harness.MapEvidenceOpLimit+2 || len(long.Ops) != harness.MapEvidenceOpLimit || !long.Truncated {
		t.Fatalf("truncation: %d matching, %d kept, truncated=%t", *long.Matching, len(long.Ops), long.Truncated)
	}
	huge := strings.Repeat("a", 10*harness.MapEvidenceDescLimit)
	kept := harness.ParseOSDOpsReading([]byte(`{"ops":[{"description":"`+huge+` e5)","type_data":{"client_info":{"client":"client.4261"}}}]}`), 4261)
	if len(kept.Ops[0].Description) != harness.MapEvidenceDescLimit || kept.Ops[0].Epoch == nil || *kept.Ops[0].Epoch != 5 {
		t.Fatalf("long description: %d bytes, epoch %v", len(kept.Ops[0].Description), kept.Ops[0].Epoch)
	}
}

func TestMapEvidenceClusterEpochNeedsAnEpoch(t *testing.T) {
	for name, out := range map[string]string{"missing": `{"fsid":"f"}`, "null": `{"epoch":null}`, "zero": `{"epoch":0}`, "text": `{"epoch":"31"}`, "not json": `x`} {
		if got := harness.ParseClusterEpoch([]byte(out)); got.Unavailable == "" || got.Epoch != nil {
			t.Errorf("%s: %+v", name, got)
		}
	}
}

func TestMapEvidenceMissingBaselineIsUnavailable(t *testing.T) {
	r := failedWait(t, "full map", 26)
	r.CaptureMapEvidence(context.Background(), "full map", healthyCluster().sources(0))
	ev := r.MapEvidence[0]
	if !strings.Contains(ev.Baseline.Unavailable, "no baseline") || ev.MapFullDelta != nil || ev.Final.Unavailable != "" {
		t.Fatalf("evidence = %+v", ev)
	}
}
