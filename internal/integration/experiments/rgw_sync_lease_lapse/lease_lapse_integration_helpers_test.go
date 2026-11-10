//go:build (all || (integration && multicluster)) && native_regression

package rgw_sync_lease_lapse_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const (
	// leaseLapsePeriod is the sync lease period of a fixture gateway
	// (rgw_sync_lease_period). A freeze of leaseLapseFreeze outlasts it;
	// one of leaseLapseShortFreeze does not.
	leaseLapsePeriod      = 30 * time.Second
	leaseLapseFreeze      = leaseLapsePeriod + 6*time.Second
	leaseLapseShortFreeze = 8 * time.Second
	// leaseLapseLogWindow is the time in which the source records one data
	// log entry per bucket shard (rgw_data_log_window); a round waits it
	// out so that its writes make entries of their own.
	leaseLapseLogWindow = 30 * time.Second
	// leaseLapseWindow is the number of sync coroutines of one coroutine
	// manager that may wait for I/O at a time (RGW_ASYNC_OPS_MGR_WINDOW):
	// while that many wait, the manager runs no other coroutine. A zone has
	// 128 data log shards, so the reads of one source that does not answer
	// reach it.
	leaseLapseWindow  = 100
	leaseLapseBuckets = 6
)

// leaseLapse is the two-zone realm of the case: zone a is the source, and
// zone b's gateway is the one whose data sync from a is observed.
type leaseLapse struct {
	t         *testing.T
	ctx       context.Context
	fixture   *multisite.Fixture
	zones     []multisite.Zone
	docker    *testcontainers.DockerClient
	a, b      harness.S3HTTPClient
	buckets   []string
	lastWrite time.Time
	paused    map[string]bool
}

// leaseLapseRound is one round of writes to zone a around a freeze of zone
// b's gateway.
type leaseLapseRound struct {
	Name, Key string
	Body      []byte
	// Started is when the round began; Frozen and Resumed bound the time
	// the round is about: the freeze of zone b's gateway, or the time zone
	// b's sync coroutines waited for zone a's frozen gateway.
	Started, Frozen, Resumed time.Time
	// Shards are the data log shards of zone a that the round's writes
	// added entries to, with a's newest marker of each.
	Shards map[int]string
}

// leaseLapseSeen is one look at zone b after a round: the round's objects b
// does not serve, and the round's shards whose marker stored in b is not
// a's.
type leaseLapseSeen struct {
	Missing []string
	Behind  []harness.RGWShardLag
	Err     string
}

func (s leaseLapseSeen) done() bool {
	return len(s.Missing) == 0 && len(s.Behind) == 0 && s.Err == ""
}

func (s leaseLapseSeen) String() string {
	return fmt.Sprintf("objects not served %v; shards behind %+v; error %q", s.Missing, s.Behind, s.Err)
}

func newLeaseLapse(t *testing.T, ctx context.Context) *leaseLapse {
	t.Helper()
	source, destination, _, _ := harness.NewMultiClusterPairWithContext(t, ctx, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	image, _ := harness.IntegrationImages(t)
	rgwImage := os.Getenv("CEPH_TEST_RGW_IMAGE")
	if rgwImage == "" {
		rgwImage = image
	}
	fixture, err := multisite.Run(ctx, rgwImage, multisite.WithTopology(multisite.TopologyConfig{
		Zonegroup: "us", MetadataMaster: "a",
		Zones: []multisite.ZoneConfig{{Name: "a", Cluster: source}, {Name: "b", Cluster: destination}},
	}))
	l := &leaseLapse{t: t, ctx: ctx, fixture: fixture, paused: map[string]bool{}}
	if fixture != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 3*time.Minute)
			defer stop()
			// A paused gateway cannot be stopped in an orderly way.
			for name := range l.paused {
				if err := l.resume(cleanup, name); err != nil {
					t.Error(err)
				}
			}
			if err := fixture.Terminate(cleanup); err != nil {
				t.Errorf("terminate the realm: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	l.zones = fixture.Zones()
	if len(l.zones) != 2 || l.zones[0].Name != "a" || l.zones[1].Name != "b" {
		t.Fatalf("zones %+v", l.zones)
	}
	if l.docker, err = testcontainers.NewDockerClientWithOpts(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.docker.Close() })
	l.a, l.b = l.client(0), l.client(1)
	seed := []byte("synced before any freeze")
	for i := range leaseLapseBuckets {
		bucket := fmt.Sprintf("/tc-lease-lapse-%d", i)
		l.buckets = append(l.buckets, bucket)
		// Objects written before every zone has the bucket's metadata can
		// be lost by the data sync of a new bucket; wait for the metadata.
		l.a.Request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
		for _, zone := range l.zones {
			if _, err := fixture.WaitSyncReady(ctx, zone.Name); err != nil {
				t.Fatalf("metadata of %s in zone %s: %v", bucket, zone.Name, err)
			}
		}
		l.a.Request(t, ctx, http.MethodPut, bucket+"/seed", seed, http.StatusOK)
	}
	l.lastWrite = time.Now()
	served, err := harness.Observe(ctx, 6*time.Minute, time.Minute, 2*time.Second, func(probe context.Context) ([]string, bool, error) {
		missing := l.missing(probe, "seed", seed)
		return missing, len(missing) == 0, nil
	})
	if err != nil {
		t.Fatalf("zone b does not serve the seeds of %v: %v", served.Value, err)
	}
	if _, err := fixture.WaitSyncReady(ctx, "b", "a"); err != nil {
		t.Fatal(err)
	}
	// Zone b's gateway logs each data sync shard's passes.
	if out, err := l.gatewayExec(1, `for s in /var/run/ceph/*.asok; do ceph --admin-daemon "$s" config set debug_rgw_sync 20/20 >/dev/null && ceph --admin-daemon "$s" config get debug_rgw_sync; done`); err != nil || !strings.Contains(out, "20/20") {
		t.Fatalf("debug_rgw_sync of zone b's gateway: %s, %v", out, err)
	}
	// Zone a's gateway stops telling b which shards changed, so that b
	// learns of a write only by reading a's data log. A notification makes
	// b sync the named bucket shard apart from its data log position, which
	// would hide an entry that the position passed.
	if out, err := l.gatewayExec(0, `for s in /var/run/ceph/*.asok; do ceph --admin-daemon "$s" config set rgw_data_notify_interval_msec 0 >/dev/null && ceph --admin-daemon "$s" config get rgw_data_notify_interval_msec; done`); err != nil || !strings.Contains(out, `"0"`) {
		t.Fatalf("rgw_data_notify_interval_msec of zone a's gateway: %s, %v", out, err)
	}
	return l
}

func (l *leaseLapse) client(zone int) harness.S3HTTPClient {
	l.t.Helper()
	gateway := l.zones[zone].Gateway
	endpoint, err := gateway.S3Endpoint(l.ctx)
	if err != nil {
		l.t.Fatal(err)
	}
	return harness.S3HTTPClient{Endpoint: endpoint, AccessKey: gateway.AccessKey, SecretKey: gateway.SecretKey, Region: gateway.Region, HTTP: &http.Client{Timeout: 20 * time.Second}}
}

// gatewayExec runs a shell script in a zone's gateway container.
func (l *leaseLapse) gatewayExec(zone int, script string) (string, error) {
	code, reader, err := l.zones[zone].Gateway.Exec(l.ctx, []string{"sh", "-c", script}, tcexec.Multiplexed())
	if err != nil {
		return "", err
	}
	out, err := io.ReadAll(reader)
	if err == nil && code != 0 {
		err = fmt.Errorf("exit %d", code)
	}
	return strings.TrimSpace(string(out)), err
}

// pause freezes every process of a zone's gateway container. The gateway is
// recorded as owed a resume before the engine is asked: a request whose
// answer is lost can still have frozen the container. The time returned is
// taken after the engine reported the container paused.
func (l *leaseLapse) pause(name string) time.Time {
	l.t.Helper()
	l.paused[name] = true
	_, err := l.docker.ContainerPause(l.ctx, l.gateway(name).GetContainerID(), mobycl.ContainerPauseOptions{})
	paused, seen := l.frozen(l.ctx, name)
	if err != nil || seen != nil || !paused {
		l.t.Fatalf("pause the gateway of zone %s: request %v; the engine reports paused=%t (%v)", name, err, paused, seen)
	}
	return time.Now()
}

// unpause resumes a zone's gateway container and returns the time right
// before it asked for that: the gateway runs again no earlier. A round goes
// on only with a gateway the engine reports as running.
func (l *leaseLapse) unpause(name string) time.Time {
	l.t.Helper()
	asked := time.Now()
	if err := l.resume(l.ctx, name); err != nil {
		l.t.Fatal(err)
	}
	return asked
}

// resume asks the engine to resume a zone's gateway container and drops the
// gateway from those owed a resume only when the engine then reports it as
// not paused, whatever the request returned. Otherwise the gateway stays
// owed, and the cleanup tries again.
func (l *leaseLapse) resume(ctx context.Context, name string) error {
	_, err := l.docker.ContainerUnpause(ctx, l.gateway(name).GetContainerID(), mobycl.ContainerUnpauseOptions{})
	paused, seen := l.frozen(ctx, name)
	if seen != nil || paused {
		return fmt.Errorf("unpause the gateway of zone %s: request %v; the engine reports paused=%t (%v)", name, err, paused, seen)
	}
	delete(l.paused, name)
	return nil
}

// frozen reads from the engine whether a zone's gateway container is paused.
func (l *leaseLapse) frozen(ctx context.Context, name string) (bool, error) {
	state, err := l.gateway(name).State(ctx)
	if err != nil {
		return false, err
	}
	return state.Paused, nil
}

func (l *leaseLapse) gateway(name string) testcontainers.Container {
	return l.zones[slices.IndexFunc(l.zones, func(zone multisite.Zone) bool { return zone.Name == name })].Gateway
}

func (l *leaseLapse) wait(d time.Duration) {
	l.t.Helper()
	select {
	case <-l.ctx.Done():
		l.t.Fatal(l.ctx.Err())
	case <-time.After(d):
	}
}

// sourceMarkers reads the newest marker of each data log shard of zone a.
// The command reads a's cluster, not its gateway.
func (l *leaseLapse) sourceMarkers() []string {
	l.t.Helper()
	out, err := l.fixture.ZoneAdmin(l.ctx, "a", "datalog", "status")
	if err != nil {
		l.t.Fatalf("datalog status of zone a: %v", err)
	}
	var shards []struct {
		Marker string `json:"marker"`
	}
	if err := json.Unmarshal(out, &shards); err != nil || len(shards) == 0 {
		l.t.Fatalf("datalog status of zone a: %v: %s", err, out)
	}
	markers := make([]string, len(shards))
	for i, shard := range shards {
		markers[i] = shard.Marker
	}
	return markers
}

// missing returns the buckets whose object key zone b does not serve with
// body.
func (l *leaseLapse) missing(ctx context.Context, key string, body []byte) []string {
	var missing []string
	for _, bucket := range l.buckets {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, l.b.Endpoint+bucket+"/"+key, nil)
		if err != nil {
			missing = append(missing, bucket+": "+err.Error())
			continue
		}
		l.b.Sign(request, nil, time.Now().UTC())
		response, err := l.b.HTTP.Do(request)
		if err != nil {
			missing = append(missing, bucket+": "+err.Error())
			continue
		}
		got, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK || !bytes.Equal(got, body) {
			missing = append(missing, fmt.Sprintf("%s: HTTP %d, %d bytes, %v", bucket, response.StatusCode, len(got), err))
		}
	}
	return missing
}

// traces reads what zone b's data sync shards logged since the round began.
func (l *leaseLapse) traces(round *leaseLapseRound) map[int]*shardTrace {
	l.t.Helper()
	reader, err := l.gateway("b").Logs(l.ctx)
	if err != nil {
		l.t.Fatalf("log of zone b's gateway: %v", err)
	}
	data, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		l.t.Fatalf("log of zone b's gateway: %v", err)
	}
	return readShardTraces(string(data), round.Started)
}

// round writes one object per bucket to zone a while zone b's gateway is
// frozen, so that b learns of them from a's data log afterwards.
//
// Without holdSource b's gateway stays frozen for freeze after the writes
// and a's gateway answers: b's shards are idle when b is frozen and when
// it resumes.
//
// With holdSource a's gateway is frozen right after the writes and b's is
// resumed. Each of b's shards reads a's data log within its poll interval
// and gets no answer, and once leaseLapseWindow of b's sync coroutines wait
// for I/O, b's gateway runs no other sync coroutine: no shard starts a read
// and no lease is renewed. From then on a's gateway stays frozen for freeze
// and is resumed; its answers end the waits.
func (l *leaseLapse) round(name string, freeze time.Duration, holdSource bool) *leaseLapseRound {
	l.t.Helper()
	if since := time.Since(l.lastWrite); since < leaseLapseLogWindow+5*time.Second {
		l.wait(leaseLapseLogWindow + 5*time.Second - since)
	}
	l.polling(name)
	round := &leaseLapseRound{Name: name, Key: "late-" + name, Body: []byte("written to zone a in the round " + name), Shards: map[int]string{}}
	before := l.sourceMarkers()
	// The gateway's log stamps have milliseconds.
	round.Started = time.Now().Truncate(time.Millisecond)
	round.Frozen = l.pause("b")
	for _, bucket := range l.buckets {
		l.a.Request(l.t, l.ctx, http.MethodPut, bucket+"/"+round.Key, round.Body, http.StatusOK)
	}
	l.lastWrite = time.Now()
	after := l.sourceMarkers()
	for shard, marker := range after {
		if shard >= len(before) || marker != before[shard] {
			round.Shards[shard] = marker
		}
	}
	if len(round.Shards) == 0 {
		l.t.Fatalf("condition not established in round %s: the writes added no entry to zone a's data log", name)
	}
	if !holdSource {
		l.wait(freeze)
		round.Resumed = l.unpause("b")
		return round
	}
	l.pause("a")
	running := l.unpause("b").Truncate(time.Millisecond)
	// Each shard reads within rgw_data_sync_poll_interval, 20 seconds.
	waiting, err := harness.Observe(l.ctx, 50*time.Second, 20*time.Second, 2*time.Second, func(context.Context) (int, bool, error) {
		waiting := 0
		for _, trace := range readShardTraces(l.gatewayLog(), running) {
			if n := len(trace.Reads); n > 0 && trace.Reads[n-1].End.IsZero() {
				waiting++
			}
		}
		return waiting, waiting >= leaseLapseWindow, nil
	})
	if err != nil {
		l.t.Fatalf("condition not established in round %s: %d of zone b's shards wait in a read of zone a's data log, fewer than %d: %v", name, waiting.Value, leaseLapseWindow, err)
	}
	round.Frozen = time.Now()
	l.wait(freeze)
	round.Resumed = l.unpause("a")
	l.t.Logf("round %s: %d of zone b's shards waited in a read of zone a's data log for %s", name, waiting.Value, round.Resumed.Sub(round.Frozen).Round(time.Millisecond))
	return round
}

// polling waits until every data sync shard of zone b has started a read of
// zone a's data log since the call. A shard that ended on a lost lease reads
// again only after it has taken its lease anew.
func (l *leaseLapse) polling(round string) {
	l.t.Helper()
	since := time.Now().Truncate(time.Millisecond)
	shards := len(l.sourceMarkers())
	silent, err := harness.Observe(l.ctx, 4*time.Minute, 30*time.Second, 2*time.Second, func(context.Context) ([]int, bool, error) {
		traces := readShardTraces(l.gatewayLog(), since)
		var silent []int
		for shard := range shards {
			if trace := traces[shard]; trace == nil || len(trace.Reads) == 0 {
				silent = append(silent, shard)
			}
		}
		return silent, len(silent) == 0, nil
	})
	if err != nil {
		log := l.gatewayLog()
		var tails []string
		for _, shard := range silent.Value[:min(len(silent.Value), 3)] {
			tails = append(tails, fmt.Sprintf("shard %d:\n%s", shard, strings.Join(shardLines(log, shard, 8), "\n")))
		}
		l.t.Fatalf("condition not established in round %s: shards %v of zone b read nothing from zone a's data log in 4m0s: %v\n%s", round, silent.Value, err, strings.Join(tails, "\n"))
	}
	l.t.Logf("round %s: every one of zone b's %d shards read zone a's data log within %s", round, shards, time.Since(since).Round(time.Millisecond))
}

func (l *leaseLapse) gatewayLog() string {
	reader, err := l.gateway("b").Logs(l.ctx)
	if err != nil {
		return ""
	}
	data, _ := io.ReadAll(reader)
	_ = reader.Close()
	return string(data)
}

// see reads one leaseLapseSeen of a round.
func (l *leaseLapse) see(ctx context.Context, round *leaseLapseRound) leaseLapseSeen {
	seen := leaseLapseSeen{Missing: l.missing(ctx, round.Key, round.Body)}
	datalog, err := l.fixture.ZoneAdmin(ctx, "a", "datalog", "status")
	if err == nil {
		var markers []byte
		if markers, err = l.fixture.ZoneAdmin(ctx, "b", "data", "sync", "status", "--source-zone=a"); err == nil {
			var lag []harness.RGWShardLag
			if lag, err = harness.RGWDataSyncLag(datalog, markers); err == nil {
				for _, shard := range lag {
					if _, ok := round.Shards[shard.Shard]; ok {
						seen.Behind = append(seen.Behind, shard)
					}
				}
			}
		}
	}
	if err != nil {
		seen.Err = err.Error()
	}
	return seen
}

// catchUp looks at zone b every five seconds until it serves the round's
// objects and has stored a's markers of the round's shards, for at most
// bound. It returns the last look and how long it took.
func (l *leaseLapse) catchUp(round *leaseLapseRound, since time.Time, bound time.Duration) (leaseLapseSeen, time.Duration, bool) {
	l.t.Helper()
	seen, err := harness.Observe(l.ctx, bound, time.Minute, 5*time.Second, func(probe context.Context) (leaseLapseSeen, bool, error) {
		seen := l.see(probe, round)
		return seen, seen.done(), nil
	})
	return seen.Value, time.Since(since).Round(time.Millisecond), err == nil
}

// pids returns the process of each zone's gateway.
func (l *leaseLapse) pids() map[string]int {
	l.t.Helper()
	pids := map[string]int{}
	for _, zone := range l.zones {
		state, err := zone.Gateway.State(l.ctx)
		if err != nil || !state.Running || state.Paused {
			l.t.Fatalf("gateway of zone %s: %+v, %v", zone.Name, state, err)
		}
		pids[zone.Name] = state.Pid
	}
	return pids
}

// leaseLapseShards formats the traces of a round's shards.
func leaseLapseShards(round *leaseLapseRound, traces map[int]*shardTrace) string {
	var out []string
	for _, shard := range slices.Sorted(maps.Keys(round.Shards)) {
		trace := traces[shard]
		if trace == nil {
			trace = &shardTrace{}
		}
		out = append(out, fmt.Sprintf("shard %d (a's marker %s): lease lost in a batch %d, before a read %d, otherwise %d; entries started %v; reads from %q; read held over that time %v",
			shard, round.Shards[shard], trace.LostInBatch, trace.LostAtStart, trace.LostOther, trace.Entries, trace.Marker, trace.held(round.Frozen, round.Resumed)))
	}
	return strings.Join(out, "\n")
}

// testSyncLeaseLapse runs three rounds and requires zone b to catch up with
// zone a's writes after each, with the gateway processes it started with.
// The first two are controls: zone b's gateway frozen past the sync lease
// period while its shards are idle, and zone a's gateway not answering b's
// reads for less than the lease period. The third is the trigger: zone a's
// gateway not answering for longer than the lease period.
func testSyncLeaseLapse(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Minute)
	defer cancel()
	l := newLeaseLapse(t, ctx)
	pids := l.pids()
	sameProcesses := func(round string) {
		t.Helper()
		if now := l.pids(); !maps.Equal(now, pids) {
			t.Fatalf("round %s: the gateway processes changed from %v to %v", round, pids, now)
		}
	}

	// Control: the leases lapse while every shard is idle. Each shard finds
	// its lease lost before a read, ends and starts again from its stored
	// marker.
	idle := l.round("idle", leaseLapseFreeze, false)
	seen, took, ok := l.catchUp(idle, idle.Resumed, 2*time.Minute)
	traces := l.traces(idle)
	t.Logf("round idle, %s after a freeze of %s: %s\n%s", took, idle.Resumed.Sub(idle.Frozen).Round(time.Millisecond), seen, leaseLapseShards(idle, traces))
	for shard := range idle.Shards {
		if trace := traces[shard]; trace == nil || trace.LostAtStart == 0 || trace.LostInBatch != 0 {
			t.Fatalf("condition not established in round idle: shard %d did not find its lease lost before a read only: %+v", shard, trace)
		}
	}
	if !ok {
		t.Fatalf("control failed: zone b did not catch up within 2m0s of an idle freeze past the lease period: %s", seen)
	}
	sameProcesses("idle")

	// Control: the shards wait in a read of a's data log for less than the
	// lease period. No lease is lost.
	short := l.round("short", leaseLapseShortFreeze, true)
	seen, took, ok = l.catchUp(short, short.Resumed, 2*time.Minute)
	traces = l.traces(short)
	t.Logf("round short, %s after a wait of %s: %s\n%s", took, short.Resumed.Sub(short.Frozen).Round(time.Millisecond), seen, leaseLapseShards(short, traces))
	held := 0
	for shard := range short.Shards {
		trace := traces[shard]
		if trace != nil && trace.LostInBatch+trace.LostAtStart+trace.LostOther != 0 {
			t.Fatalf("condition not established in round short: shard %d lost its lease: %+v", shard, trace)
		}
		if trace.held(short.Frozen, short.Resumed) {
			held++
		}
	}
	if held == 0 {
		t.Fatalf("condition not established in round short: no shard of the round waited in a read while zone a did not answer:\n%s", leaseLapseShards(short, traces))
	}
	if !ok {
		t.Fatalf("control failed: zone b did not catch up within 2m0s of zone a answering again: %s", seen)
	}
	sameProcesses("short")

	// Trigger: the shards wait in the read for longer than the lease period,
	// and no lease is renewed meanwhile. A shard that gets its batch before
	// its lease is renewed finds the lease lost inside the batch.
	lapse := l.round("lapse", leaseLapseFreeze, true)
	l.wait(5 * time.Second)
	traces = l.traces(lapse)
	var inBatch []int
	for shard := range lapse.Shards {
		if trace := traces[shard]; trace != nil && trace.LostInBatch > 0 {
			inBatch = append(inBatch, shard)
		}
	}
	slices.Sort(inBatch)
	if len(inBatch) == 0 {
		t.Fatalf("condition not established in round lapse: no shard of the round found its lease lost inside a batch:\n%s", leaseLapseShards(lapse, traces))
	}
	seen, took, ok = l.catchUp(lapse, lapse.Resumed, 3*time.Minute)
	traces = l.traces(lapse)
	t.Logf("round lapse, %s after a wait of %s: %s\n%s", took, lapse.Resumed.Sub(lapse.Frozen).Round(time.Millisecond), seen, leaseLapseShards(lapse, traces))
	if ok {
		sameProcesses("lapse")
		t.Logf("RGW_SYNC_LEASE_LAPSE_RECEIPT zone b's shards %v of %d found their sync lease lost inside a batch read from zone a, which had not answered for %s, and zone b served every object and stored a's markers %s after a answered again, with its gateway process kept; controls: zone b frozen for %s over %d idle shards, zone a not answering for %s with %d of %d shards waiting in a read",
			inBatch, len(lapse.Shards), lapse.Resumed.Sub(lapse.Frozen).Round(time.Millisecond), took,
			idle.Resumed.Sub(idle.Frozen).Round(time.Millisecond), len(idle.Shards), short.Resumed.Sub(short.Frozen).Round(time.Millisecond), held, len(short.Shards))
		return
	}
	sameProcesses("lapse")
	// The shards still behind must all show the skipped batch; anything
	// else is another failure, and the gateway is left as it is.
	var skipped []int
	for _, shard := range seen.Behind {
		if !skippedBatch(traces[shard.Shard], shard.Source, shard.Destination) {
			t.Fatalf("zone b did not catch up within 3m0s of zone a answering again, and shard %d does not show a batch passed on a lost lease (a's marker %s, b's stored marker %q, trace %+v): %s",
				shard.Shard, shard.Source, shard.Destination, traces[shard.Shard], seen)
		}
		skipped = append(skipped, shard.Shard)
	}
	if len(skipped) == 0 {
		t.Fatalf("zone b did not catch up within 3m0s of zone a answering again with no shard of the round behind: %s", seen)
	}
	errors, err := l.fixture.ZoneAdmin(ctx, "b", "sync", "error", "list")
	t.Logf("sync error list of zone b: %d entries with an error, %v", bytes.Count(errors, []byte(`"error_code"`)), err)
	// A restarted gateway reads its stored markers again.
	gateway := l.zones[1].Gateway
	if err := gateway.Stop(ctx, nil); err != nil {
		t.Fatalf("stop zone b's gateway: %v", err)
	}
	if err := gateway.Start(ctx); err != nil {
		t.Fatalf("start zone b's gateway: %v", err)
	}
	restarted := time.Now()
	l.b = l.client(1)
	after, recovered, ok := l.catchUp(lapse, restarted, 5*time.Minute)
	t.Fatalf("observed defect: zone a did not answer zone b's reads of its data log for %s; zone b's shards %v then found their sync lease lost inside the batch they read, read on from a's newest markers without starting the entries, and left their stored markers behind; 3m0s after a answered again: %s. After a restart of zone b's gateway: caught up %v after %s: %s",
		lapse.Resumed.Sub(lapse.Frozen).Round(time.Millisecond), skipped, seen, ok, recovered, after)
}
