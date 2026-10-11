package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const poolPGFixtureFSID = "9b9b782d-a094-4b2a-b883-87ea6ea6c5f8"
const poolPGFixturePool = `{"pool":7,"pool_name":"fixture","type":1,"size":2,"min_size":1,"pg_num":8,"crush_rule":0,"pg_autoscale_mode":"off","erasure_code_profile":"","flags_names":"","quota_max_bytes":0,"quota_max_objects":0}`
const poolPGFixtureReport = `{"pgid":"7.a","state":"active+future","up":[9,4,2147483647,2147483647],"acting":[4,9,2147483647,2147483647],"up_primary":4,"acting_primary":9,"reported_epoch":19,"reported_seq":9007199254740993,"mapping_epoch":7,"last_epoch_clean":3,"stats_invalid":true,"stat_sum":{"num_objects":-2,"num_bytes":9007199254740993,"num_object_copies":-5,"num_objects_degraded":3,"num_objects_misplaced":0,"num_objects_unfound":-1}}`

func poolPGFixtureMap(pool string, epoch uint32) string {
	return fmt.Sprintf(`{"fsid":%q,"epoch":%d,"pools":[%s,{"pool":8,"pool_name":"other","future_policy":true}],"osds":[]}`, poolPGFixtureFSID, epoch, pool)
}

func poolPGFixtureReports(report string, ready bool) string {
	return fmt.Sprintf(`{"pg_ready":%t,"pg_stats":[%s]}`, ready, report)
}

type poolPGFixtureContainer struct {
	testcontainers.Container
	calls   [][]string
	outputs []string
	exits   map[int]int
	readers map[int]io.Reader
	hook    func(context.Context, int) error
}

func poolPGFixtureStream(channel byte, data string) io.Reader {
	var header [8]byte
	header[0] = channel
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	return io.MultiReader(bytes.NewReader(header[:]), strings.NewReader(data))
}

func (ctr *poolPGFixtureContainer) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	ctr.calls = append(ctr.calls, slices.Clone(args))
	call := len(ctr.calls)
	if ctr.hook != nil {
		if err := ctr.hook(ctx, call); err != nil {
			return 0, nil, err
		}
	}
	if len(args) < 3 || !slices.Equal(args[:3], []string{"ceph", "--connect-timeout", "5"}) || call > len(ctr.outputs) {
		return 0, nil, errors.New("unexpected pool PG fixture query")
	}
	if reader := ctr.readers[call]; reader != nil {
		return ctr.exits[call], reader, nil
	}
	return ctr.exits[call], poolPGFixtureStream(byte(stdcopy.Stdout), ctr.outputs[call-1]), nil
}

func newPoolPGFixture() (*Container, *poolPGFixtureContainer) {
	ctr := &poolPGFixtureContainer{outputs: []string{
		poolPGFixtureFSID + "\n", poolPGFixtureMap(poolPGFixturePool, 20), poolPGFixtureReports(poolPGFixtureReport, true),
		poolPGFixtureMap(poolPGFixturePool, 21), poolPGFixtureFSID + "\n",
	}, exits: map[int]int{}, readers: map[int]io.Reader{}}
	return &Container{Container: ctr, config: []byte("[global]\nfsid = " + poolPGFixtureFSID + "\nmon_host = mon\n"), keyring: []byte("confirmed")}, ctr
}

func assertPoolPGFailure(t *testing.T, cluster *Container, ctx context.Context) error {
	t.Helper()
	result, err := cluster.PoolPGs(ctx, "fixture")
	if err == nil || !reflect.DeepEqual(result, PoolPGSnapshot{}) {
		t.Fatalf("unusable PG observation published: %+v error=%v", result, err)
	}
	return err
}

func TestPoolPGsRetainsReportedOrderIdentityAndNativeIntegers(t *testing.T) {
	cluster, ctr := newPoolPGFixture()
	result, err := cluster.PoolPGs(t.Context(), "fixture")
	wantPool := PoolState{ID: 7, Name: "fixture", Type: "replicated", Size: 2, MinSize: 1, PGNum: 8, CRUSHRule: 0, AutoscaleMode: "off"}
	wantPG := PGState{PGID: "7.a", State: "active+future", Up: []int{9, 4, 2147483647, 2147483647}, Acting: []int{4, 9, 2147483647, 2147483647},
		UpPrimary: 4, ActingPrimary: 9, ReportedEpoch: 19, ReportedSequence: 9007199254740993, MappingEpoch: 7, LastEpochClean: 3,
		StatsInvalid: true, Stats: PGStats{Objects: -2, Bytes: 9007199254740993, ObjectCopies: -5, ObjectsDegraded: 3, ObjectsMisplaced: 0, ObjectsUnfound: -1}}
	want := PoolPGSnapshot{FSID: poolPGFixtureFSID, PoolBefore: wantPool, PoolAfter: wantPool,
		OSDMapEpochBefore: 20, OSDMapEpochAfter: 21, PGReady: true, PGs: []PGState{wantPG}}
	if err != nil || !reflect.DeepEqual(result, want) {
		t.Fatalf("reported placement, signed counters or exact sequence changed: %+v error=%v", result, err)
	}
	wantCalls := [][]string{
		{"ceph", "--connect-timeout", "5", "fsid"},
		{"ceph", "--connect-timeout", "5", "osd", "dump", "--format", "json"},
		{"ceph", "--connect-timeout", "5", "pg", "ls-by-pool", "fixture", "--format", "json"},
		{"ceph", "--connect-timeout", "5", "osd", "dump", "--format", "json"},
		{"ceph", "--connect-timeout", "5", "fsid"},
	}
	if !reflect.DeepEqual(ctr.calls, wantCalls) {
		t.Fatalf("unexpected mutation, owned-OSD query or mapping fanout: %v", ctr.calls)
	}
	// A report can mention removed/unowned OSDs, have a seed beyond the
	// observed PGNum, and differ from the current epoch without being malformed.
	if cluster.osds != nil || len(result.PGs) != 1 {
		t.Fatal("fixture accidentally relied on an owned topology or complete PG count")
	}
}

func TestPoolPGsRetainsColdEmptyUnknownAndUnreadyReports(t *testing.T) {
	unknown := `{"pgid":"7.0","state":"unknown","up":[],"acting":[],"up_primary":-1,"acting_primary":-1,"reported_epoch":0,"reported_seq":0,"mapping_epoch":0,"last_epoch_clean":0,"stats_invalid":false,"stat_sum":{"num_objects":0,"num_bytes":0,"num_object_copies":0,"num_objects_degraded":0,"num_objects_misplaced":0,"num_objects_unfound":0}}`
	for _, ready := range []bool{false, true} {
		for _, tc := range []struct {
			name, report string
			omit         bool
		}{
			{name: "omitted", omit: true},
			{name: "empty"},
			{name: "unknown", report: unknown},
		} {
			t.Run(fmt.Sprintf("ready-%t/%s", ready, tc.name), func(t *testing.T) {
				cluster, ctr := newPoolPGFixture()
				ctr.outputs[2] = poolPGFixtureReports(tc.report, ready)
				if tc.omit {
					ctr.outputs[2] = fmt.Sprintf(`{"pg_ready":%t}`, ready)
				}
				result, err := cluster.PoolPGs(t.Context(), "fixture")
				if err != nil || result.PGReady != ready || result.PGs == nil || len(ctr.calls) != 5 {
					t.Fatalf("unready/empty report rejected: %+v %v", result, err)
				}
				if tc.report == "" {
					if len(result.PGs) != 0 {
						t.Fatal("empty report changed")
					}
				} else {
					pg := result.PGs[0]
					if pg.State != "unknown" || pg.Up == nil || pg.Acting == nil || len(pg.Up) != 0 || len(pg.Acting) != 0 || pg.UpPrimary != -1 || pg.ActingPrimary != -1 || pg.ReportedEpoch != 0 || pg.MappingEpoch != 0 || pg.LastEpochClean != 0 || pg.StatsInvalid || pg.Stats != (PGStats{}) {
						t.Fatalf("cold native values changed: %+v", pg)
					}
				}
			})
		}
	}
}

func TestPoolPGsBracketsChangingPoliciesWithoutRequiringFreshness(t *testing.T) {
	cluster, ctr := newPoolPGFixture()
	changed := strings.Replace(poolPGFixturePool, `"pg_num":8`, `"pg_num":16`, 1)
	changed = strings.Replace(changed, `"quota_max_bytes":0`, `"quota_max_bytes":18446744073709551615`, 1)
	changed = strings.Replace(changed, `"flags_names":""`, `"flags_names":"creating"`, 1)
	changed = strings.Replace(changed, `"crush_rule":0`, `"crush_rule":255`, 1)
	ctr.outputs[3] = poolPGFixtureMap(changed, 19) // No monotonic/equality predicate is invented.
	result, err := cluster.PoolPGs(t.Context(), "fixture")
	if err != nil || result.PoolBefore.PGNum != 8 || result.PoolAfter.PGNum != 16 || result.PoolAfter.Quota.MaxBytes != math.MaxUint64 || result.PoolAfter.Flags != "creating" || result.PoolAfter.CRUSHRule != 255 || result.OSDMapEpochAfter != 19 {
		t.Fatalf("bracket policy change incorrectly treated as replacement: %+v %v", result, err)
	}
	cluster, ctr = newPoolPGFixture()
	ec := strings.Replace(poolPGFixturePool, `"type":1`, `"type":3`, 1)
	ec = strings.Replace(ec, `"size":2`, `"size":3`, 1)
	ec = strings.Replace(ec, `"erasure_code_profile":""`, `"erasure_code_profile":"ec-profile"`, 1)
	ctr.outputs[1], ctr.outputs[3] = poolPGFixtureMap(ec, 20), poolPGFixtureMap(ec, 21)
	result, err = cluster.PoolPGs(t.Context(), "fixture")
	if err != nil || result.PoolBefore.Type != "erasure" || result.PoolAfter.ErasureCodeProfile != "ec-profile" {
		t.Fatalf("EC policy or repeated missing shard slots rejected: %+v %v", result, err)
	}
}

func TestPoolPGsUsesOneCapturedControlHandle(t *testing.T) {
	cluster, embedded := newPoolPGFixture()
	_, control := newPoolPGFixture()
	_, replacement := newPoolPGFixture()
	cluster.controlPlane = control
	control.hook = func(_ context.Context, call int) error {
		if call == 1 {
			cluster.controlMu.Lock()
			cluster.controlPlane = replacement
			cluster.controlMu.Unlock()
		}
		return nil
	}
	result, err := cluster.PoolPGs(t.Context(), "fixture")
	if err != nil || result.FSID != poolPGFixtureFSID || len(control.calls) != 5 || len(embedded.calls) != 0 || len(replacement.calls) != 0 {
		t.Fatalf("observation changed control handle: %+v %v calls=%v/%v/%v", result, err, control.calls, embedded.calls, replacement.calls)
	}
}

func TestPoolPGsRejectsUnavailableBootstrapAndInvalidInputBeforeQuery(t *testing.T) {
	for _, mode := range []string{"closed", "control", "config", "keyring", "duplicate FSID", "zero FSID", "malformed FSID"} {
		t.Run(mode, func(t *testing.T) {
			cluster, ctr := newPoolPGFixture()
			switch mode {
			case "closed":
				cluster.closed = true
			case "control":
				cluster.Container = nil
			case "config":
				cluster.config = nil
			case "keyring":
				cluster.keyring = nil
			case "duplicate FSID":
				cluster.config = append(cluster.config, []byte("fsid = "+poolPGFixtureFSID+"\n")...)
			case "zero FSID":
				cluster.config = []byte("[global]\nfsid = 00000000-0000-0000-0000-000000000000\n")
			case "malformed FSID":
				cluster.config = []byte("[global]\nfsid = SECRET_CONFIG_VALUE\n")
			}
			err := assertPoolPGFailure(t, cluster, t.Context())
			if len(ctr.calls) != 0 || strings.Contains(err.Error(), "SECRET_CONFIG_VALUE") {
				t.Fatalf("unavailable bootstrap queried or exposed: %v %v", err, ctr.calls)
			}
		})
	}
	assertPoolPGFailure(t, nil, t.Context())
	for _, name := range []string{"", "bad name", "--format", strings.Repeat("a", 129)} {
		cluster, ctr := newPoolPGFixture()
		result, err := cluster.PoolPGs(t.Context(), name)
		if err == nil || !reflect.DeepEqual(result, PoolPGSnapshot{}) || len(ctr.calls) != 0 {
			t.Fatalf("invalid name reached CLI: %q %+v %v", name, result, err)
		}
	}
}

func TestPoolPGsRejectsOriginalFSIDAndPoolIdentityDrift(t *testing.T) {
	for _, call := range []int{1, 5} {
		for _, fsid := range []string{"61098f7f-474e-4403-ad31-a976f7c4e731", "", "00000000-0000-0000-0000-000000000000", strings.ToUpper(poolPGFixtureFSID), "SECRET_FOREIGN_FSID"} {
			cluster, ctr := newPoolPGFixture()
			ctr.outputs[call-1] = fsid
			err := assertPoolPGFailure(t, cluster, t.Context())
			if len(ctr.calls) != call || strings.Contains(err.Error(), "SECRET_FOREIGN_FSID") {
				t.Fatalf("FSID mismatch leaked or continued: %v calls=%v", err, ctr.calls)
			}
		}
	}
	for _, call := range []int{2, 4} {
		for mode, data := range map[string]string{
			"foreign FSID": strings.Replace(poolPGFixtureMap(poolPGFixturePool, 20), poolPGFixtureFSID, "61098f7f-474e-4403-ad31-a976f7c4e731", 1),
			"removed pool": poolPGFixtureMap(`{"pool":9,"pool_name":"elsewhere"}`, 20),
			"renamed pool": poolPGFixtureMap(strings.Replace(poolPGFixturePool, `"pool_name":"fixture"`, `"pool_name":"SECRET_RENAMED"`, 1), 20),
		} {
			t.Run(fmt.Sprintf("query-%d/%s", call, mode), func(t *testing.T) {
				cluster, ctr := newPoolPGFixture()
				ctr.outputs[call-1] = data
				err := assertPoolPGFailure(t, cluster, t.Context())
				if len(ctr.calls) != call || strings.Contains(err.Error(), "SECRET_RENAMED") {
					t.Fatalf("identity mismatch continued or leaked: %v %v", err, ctr.calls)
				}
			})
		}
	}
	cluster, ctr := newPoolPGFixture()
	ctr.outputs[3] = poolPGFixtureMap(strings.Replace(poolPGFixturePool, `"pool":7`, `"pool":9`, 1), 21)
	assertPoolPGFailure(t, cluster, t.Context())
	if len(ctr.calls) != 4 {
		t.Fatal("replacement reached final identity query")
	}
}

type poolPGErrorReader struct{ err error }

func (reader poolPGErrorReader) Read([]byte) (int, error) { return 0, reader.err }

func TestPoolPGsRedactsEveryNativeFailureAndRetainsCanonicalContext(t *testing.T) {
	const secret = "SECRET_NATIVE_PG_OUTPUT"
	for call := 1; call <= 5; call++ {
		for _, mode := range []string{"exec", "read", "exit", "stdout and stderr", "wrapped cancel", "wrapped deadline"} {
			t.Run(fmt.Sprintf("query-%d/%s", call, mode), func(t *testing.T) {
				cluster, ctr := newPoolPGFixture()
				cause := errors.New(secret)
				var canonical error
				switch mode {
				case "exec":
					ctr.hook = func(_ context.Context, current int) error {
						if current == call {
							return cause
						}
						return nil
					}
				case "read":
					ctr.readers[call] = poolPGErrorReader{cause}
				case "exit":
					ctr.exits[call], ctr.outputs[call-1] = 1, secret
				case "stdout and stderr":
					ctr.exits[call] = 1
					ctr.readers[call] = io.MultiReader(poolPGFixtureStream(byte(stdcopy.Stdout), secret), poolPGFixtureStream(byte(stdcopy.Stderr), secret))
				case "wrapped cancel", "wrapped deadline":
					canonical = context.Canceled
					if mode == "wrapped deadline" {
						canonical = context.DeadlineExceeded
					}
					cause = fmt.Errorf(secret+": %w", canonical)
					ctr.hook = func(_ context.Context, current int) error {
						if current == call {
							return cause
						}
						return nil
					}
				}
				err := assertPoolPGFailure(t, cluster, t.Context())
				if strings.Contains(fmt.Sprintf("%+v", err), secret) || errors.Is(err, cause) || len(ctr.calls) != call || canonical != nil && !errors.Is(err, canonical) {
					t.Fatalf("native cause exposed, context lost or queries continued: %v calls=%v", err, ctr.calls)
				}
			})
		}
	}
}

func TestPoolPGsCancellationStopsEveryQueryWithoutPublishingPartialResults(t *testing.T) {
	for call := 0; call <= 5; call++ {
		t.Run(fmt.Sprintf("query-%d", call), func(t *testing.T) {
			cluster, ctr := newPoolPGFixture()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if call == 0 {
				cancel()
			}
			ctr.hook = func(_ context.Context, current int) error {
				if current == call {
					cancel()
				}
				return nil
			}
			err := assertPoolPGFailure(t, cluster, ctx)
			if !errors.Is(err, context.Canceled) || len(ctr.calls) != call {
				t.Fatalf("cancellation continued or lost canonical cause: %v %v", err, ctr.calls)
			}
		})
	}
	cluster, ctr := newPoolPGFixture()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctr.hook = func(_ context.Context, call int) error {
		if call == 3 {
			cancel()
			return fmt.Errorf("SECRET_WRAPPED: %w", context.DeadlineExceeded)
		}
		return nil
	}
	err := assertPoolPGFailure(t, cluster, ctx)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "SECRET_WRAPPED") {
		t.Fatalf("combined canonical causes lost or native output leaked: %v", err)
	}
}

type poolPGFinalContext struct {
	context.Context
	cancel   context.CancelFunc
	armed    bool
	checks   int
	cancelAt int
}

func (ctx *poolPGFinalContext) Err() error {
	if ctx.armed {
		ctx.checks++
		if ctx.checks == ctx.cancelAt {
			ctx.cancel()
		}
	}
	return ctx.Context.Err()
}

func TestPoolPGsPreservesPostQueryCancellationDuringDecodeOrIdentityFailure(t *testing.T) {
	for call := 1; call <= 5; call++ {
		t.Run(fmt.Sprintf("query-%d", call), func(t *testing.T) {
			cluster, ctr := newPoolPGFixture()
			ctr.outputs[call-1] = "SECRET_INVALID_NATIVE_JSON_OR_FSID"
			base, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx := &poolPGFinalContext{Context: base, cancel: cancel, cancelAt: 2}
			ctr.hook = func(_ context.Context, current int) error {
				if current == call {
					ctx.armed = true
				}
				return nil
			}
			err := assertPoolPGFailure(t, cluster, ctx)
			if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "SECRET_INVALID") || len(ctr.calls) != call || ctx.checks != 2 {
				t.Fatalf("rejection lost post-query cancellation: %v calls=%v checks=%d", err, ctr.calls, ctx.checks)
			}
		})
	}
}

func TestPoolPGsChecksContextAfterFinalIdentity(t *testing.T) {
	cluster, ctr := newPoolPGFixture()
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &poolPGFinalContext{Context: base, cancel: cancel, cancelAt: 3}
	ctr.hook = func(_ context.Context, call int) error {
		if call == 5 {
			ctx.armed = true
		}
		return nil
	}
	err := assertPoolPGFailure(t, cluster, ctx)
	if !errors.Is(err, context.Canceled) || len(ctr.calls) != 5 || ctx.checks != 3 {
		t.Fatalf("final admission guard missing: %v %v checks=%d", err, ctr.calls, ctx.checks)
	}
}

func TestPoolPGsBoundsOwnerControlAndConfigAdmission(t *testing.T) {
	for _, gate := range []string{"owner", "control", "config"} {
		t.Run(gate, func(t *testing.T) {
			cluster, ctr := newPoolPGFixture()
			var unlock func()
			switch gate {
			case "owner":
				cluster.mu.Lock()
				unlock = cluster.mu.Unlock
			case "control":
				cluster.controlMu.Lock()
				unlock = cluster.controlMu.Unlock
			case "config":
				cluster.configMu.Lock()
				unlock = cluster.configMu.Unlock
			}
			var once sync.Once
			release := func() { once.Do(unlock) }
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
			done, joined := make(chan error, 1), make(chan struct{})
			t.Cleanup(func() { cancel(); release(); <-joined })
			go func() {
				defer close(joined)
				result, err := cluster.PoolPGs(ctx, "fixture")
				if !reflect.DeepEqual(result, PoolPGSnapshot{}) {
					err = errors.Join(err, errors.New("busy gate published a snapshot"))
				}
				done <- err
			}()
			select {
			case err := <-done:
				<-joined
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("busy gate lost deadline: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("PoolPGs remained queued after deadline")
			}
			if len(ctr.calls) != 0 {
				t.Fatal("unadmitted snapshot queried native state")
			}
		})
	}
}

func poolPGTestShape(t *testing.T) (map[string]any, map[string]map[string]any) {
	t.Helper()
	var root map[string]any
	decoder := json.NewDecoder(strings.NewReader(poolPGFixtureReports(poolPGFixtureReport, true)))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		t.Fatal(err)
	}
	pg := root["pg_stats"].([]any)[0].(map[string]any)
	return root, map[string]map[string]any{"root": root, "pg": pg, "stats": pg["stat_sum"].(map[string]any)}
}

func poolPGTestJSON(t *testing.T, value any) string {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func assertPoolPGReportDecodeFailure(t *testing.T, data string) {
	t.Helper()
	ready, pgs, err := decodePoolPGReports([]byte(data), 7)
	if err == nil || ready || pgs != nil {
		t.Fatalf("malformed PG report published: ready=%t pgs=%+v error=%v", ready, pgs, err)
	}
}

func TestPoolPGsRequiresKnownFieldsWithoutAdoptingNullsOrAliases(t *testing.T) {
	fields := map[string][]string{
		"root":  {"pg_ready", "pg_stats"},
		"pg":    {"pgid", "state", "up", "acting", "up_primary", "acting_primary", "reported_epoch", "reported_seq", "mapping_epoch", "last_epoch_clean", "stats_invalid", "stat_sum"},
		"stats": {"num_objects", "num_bytes", "num_object_copies", "num_objects_degraded", "num_objects_misplaced", "num_objects_unfound"},
	}
	for group, names := range fields {
		for _, name := range names {
			for _, mode := range []string{"missing", "null", "case only", "canonical and alias", "wrong type"} {
				if group == "root" && name == "pg_stats" && mode == "missing" {
					continue // Native empty filtered results omit pg_stats.
				}
				t.Run(group+"/"+name+"/"+mode, func(t *testing.T) {
					root, groups := poolPGTestShape(t)
					field := groups[group]
					value := field[name]
					switch mode {
					case "missing":
						delete(field, name)
					case "null":
						field[name] = nil
					case "case only":
						delete(field, name)
						field[strings.ToUpper(name)] = value
					case "canonical and alias":
						field[strings.ToUpper(name)] = value
					case "wrong type":
						field[name] = map[string]any{}
					}
					assertPoolPGReportDecodeFailure(t, poolPGTestJSON(t, root))
				})
			}
		}
	}
}

func TestPoolPGsStrictNumericBoundsAndPGIdentity(t *testing.T) {
	for name, value := range map[string]any{
		"reported_epoch": json.Number("4294967296"), "mapping_epoch": json.Number("-1"), "last_epoch_clean": json.Number("1.5"),
		"reported_seq": json.Number("18446744073709551616"), "up_primary": json.Number("2147483648"), "acting_primary": json.Number("-2147483649"),
		"up": []any{json.Number("2147483648")}, "acting": []any{nil}, "pgid": "7.100000000", "state": "",
	} {
		root, groups := poolPGTestShape(t)
		groups["pg"][name] = value
		assertPoolPGReportDecodeFailure(t, poolPGTestJSON(t, root))
	}
	for _, field := range []string{"num_objects", "num_bytes", "num_object_copies", "num_objects_degraded", "num_objects_misplaced", "num_objects_unfound"} {
		for _, value := range []any{json.Number("9223372036854775808"), json.Number("-9223372036854775809"), json.Number("1.5"), "0", true} {
			root, groups := poolPGTestShape(t)
			groups["stats"][field] = value
			assertPoolPGReportDecodeFailure(t, poolPGTestJSON(t, root))
		}
	}
	for _, id := range []string{"8.0", "07.a", "7.0a", "7.A", "7.-1", "7.", "7.a.extra", "7.1s0", "SECRET_FOREIGN_PG"} {
		assertPoolPGReportDecodeFailure(t, strings.Replace(poolPGFixtureReports(poolPGFixtureReport, true), `"7.a"`, fmt.Sprintf("%q", id), 1))
	}
	root, groups := poolPGTestShape(t)
	groups["pg"]["reported_epoch"], groups["pg"]["mapping_epoch"], groups["pg"]["last_epoch_clean"] = uint32(math.MaxUint32), uint32(math.MaxUint32), uint32(math.MaxUint32)
	groups["pg"]["reported_seq"] = uint64(math.MaxUint64)
	groups["stats"]["num_objects"], groups["stats"]["num_bytes"] = int64(math.MinInt64), int64(math.MaxInt64)
	ready, pgs, err := decodePoolPGReports([]byte(poolPGTestJSON(t, root)), 7)
	if err != nil || !ready || pgs[0].ReportedEpoch != math.MaxUint32 || pgs[0].ReportedSequence != math.MaxUint64 || pgs[0].Stats.Objects != math.MinInt64 || pgs[0].Stats.Bytes != math.MaxInt64 {
		t.Fatalf("native numeric endpoints rounded/rejected: %t %+v %v", ready, pgs, err)
	}
}

func TestPoolPGsRejectsAmbiguousJSONWhileAllowingUnknownFutureFields(t *testing.T) {
	base := poolPGFixtureReports(poolPGFixtureReport, true)
	for _, data := range []string{
		base + `{}`, base + `true`, `null`, `[]`, `{"pg_ready":true,"pg_stats":[null]}`,
		`{"pg_ready":true,"pg_stats":[],"pg_stats":[]}`,
		strings.Replace(base, `"pg_ready":true`, `"pg_ready":false,"pg_ready":true`, 1),
		strings.Replace(base, `"reported_epoch":19`, `"reported_epoch":18,"reported_epoch":19`, 1),
		strings.Replace(base, `"num_objects":-2`, `"num_objects":0,"num_objects":-2`, 1),
		poolPGFixtureReports(poolPGFixtureReport+","+poolPGFixtureReport, true),
		strings.Replace(base, `"state":`, `"ſtate":`, 1),
		strings.Replace(base, `"stats_invalid":true`, `"ſtats_invalid":true`, 1),
		strings.Replace(base, `"num_objects":-2`, `"num_objectſ":-2`, 1),
		strings.Replace(base, `"pg_ready":true`, `"future":{"nested":1,"nested":2},"pg_ready":true`, 1),
	} {
		assertPoolPGReportDecodeFailure(t, data)
	}
	root, groups := poolPGTestShape(t)
	root["future"] = map[string]any{"unicode": "值", "large_number": json.Number("1e999"), "nullable": nil}
	groups["pg"]["future"] = []any{map[string]any{"State": "different unknown context"}, nil}
	groups["stats"]["future"] = "opaque native value"
	ready, pgs, err := decodePoolPGReports([]byte(poolPGTestJSON(t, root)), 7)
	if err != nil || !ready || pgs[0].PGID != "7.a" {
		t.Fatalf("unrelated future fields adopted or rejected: %t %+v %v", ready, pgs, err)
	}
}

func TestPoolPGsRejectsMalformedPoolPolicySnapshotsWithoutLeakingValues(t *testing.T) {
	base := poolPGFixtureMap(poolPGFixturePool, 20)
	cases := map[string]string{
		"null root": "null", "missing pools": `{"fsid":"` + poolPGFixtureFSID + `","epoch":20}`,
		"null pools":           strings.Replace(base, `"pools":[`, `"pools":null,"unused":[`, 1),
		"null pool":            poolPGFixtureMap("null", 20),
		"duplicate ID":         poolPGFixtureMap(poolPGFixturePool+`,{"pool":7,"pool_name":"other-id"}`, 20),
		"duplicate name":       poolPGFixtureMap(poolPGFixturePool+`,{"pool":9,"pool_name":"fixture"}`, 20),
		"epoch overflow":       strings.Replace(base, `"epoch":20`, `"epoch":4294967296`, 1),
		"ID negative":          strings.Replace(base, `"pool":7`, `"pool":-1`, 1),
		"type unsupported":     strings.Replace(base, `"type":1`, `"type":2`, 1),
		"size overflow":        strings.Replace(base, `"size":2`, `"size":256`, 1),
		"PG overflow":          strings.Replace(base, `"pg_num":8`, `"pg_num":4294967296`, 1),
		"CRUSH rule negative":  strings.Replace(base, `"crush_rule":0`, `"crush_rule":-1`, 1),
		"CRUSH rule overflow":  strings.Replace(base, `"crush_rule":0`, `"crush_rule":256`, 1),
		"quota negative":       strings.Replace(base, `"quota_max_bytes":0`, `"quota_max_bytes":-1`, 1),
		"quota overflow":       strings.Replace(base, `"quota_max_objects":0`, `"quota_max_objects":18446744073709551616`, 1),
		"duplicate nested key": strings.Replace(base, `"pool":7`, `"pool":9,"pool":7`, 1),
		"trailing document":    base + `{}`,
		"Unicode FSID alias":   strings.Replace(base, `"fsid":`, `"fſid":`, 1),
		"Unicode rule alias":   strings.Replace(base, `"crush_rule":`, `"cruſh_rule":`, 1),
		"Unicode flags alias":  strings.Replace(base, `"flags_names":`, `"flagſ_names":`, 1),
	}
	for group, names := range map[string][]string{
		"root": {"fsid", "epoch", "pools"},
		"pool": {"pool", "pool_name", "type", "size", "min_size", "pg_num", "crush_rule", "pg_autoscale_mode", "erasure_code_profile", "flags_names", "quota_max_bytes", "quota_max_objects"},
	} {
		for _, name := range names {
			for _, mode := range []string{"missing", "null", "case only", "alias and canonical", "wrong type"} {
				var value map[string]any
				if err := json.Unmarshal([]byte(base), &value); err != nil {
					t.Fatal(err)
				}
				fields := value
				if group == "pool" {
					fields = value["pools"].([]any)[0].(map[string]any)
				}
				old := fields[name]
				switch mode {
				case "missing":
					delete(fields, name)
				case "null":
					fields[name] = nil
				case "case only":
					delete(fields, name)
					fields[strings.ToUpper(name)] = old
				case "alias and canonical":
					fields[strings.ToUpper(name)] = old
				case "wrong type":
					fields[name] = []any{}
				}
				cases[group+"/"+name+"/"+mode] = poolPGTestJSON(t, value)
			}
		}
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			cluster, ctr := newPoolPGFixture()
			ctr.outputs[1] = data
			err := assertPoolPGFailure(t, cluster, t.Context())
			if len(ctr.calls) != 2 || strings.Contains(err.Error(), "other-id") {
				t.Fatalf("bad native policy continued or exposed values: %v %v", err, ctr.calls)
			}
		})
	}
}

func TestPoolPGsBoundsNativeJSONSizeAndNesting(t *testing.T) {
	tooLarge := `{"future":"` + strings.Repeat("x", 4<<20) + `","pg_ready":true,"pg_stats":[]}`
	tooDeep := `{"future":` + strings.Repeat("[", 257) + `0` + strings.Repeat("]", 257) + `,"pg_ready":true,"pg_stats":[]}`
	assertPoolPGReportDecodeFailure(t, tooLarge)
	assertPoolPGReportDecodeFailure(t, tooDeep)
	for _, data := range []string{tooLarge, tooDeep} {
		pool, epoch, err := decodePoolPGOSDMap([]byte(data), poolPGFixtureFSID, "fixture")
		if err == nil || pool != (PoolState{}) || epoch != 0 {
			t.Fatalf("unbounded policy output published: %+v %d %v", pool, epoch, err)
		}
	}
}
