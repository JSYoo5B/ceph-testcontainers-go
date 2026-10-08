package ceph

import (
	"context"
	"errors"
	"io"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const poolUsageFixtureFSID = "9b9b782d-a094-4b2a-b883-87ea6ea6c5f8"
const poolUsageFixturePolicies = `[{"pool_id":7,"pool_name":"fixture"},{"pool_id":8,"pool_name":"other"}]`
const poolUsageFixtureStats = `{"stored":4099,"stored_data":4096,"stored_omap":3,"bytes_used":16384,"data_bytes_used":12288,"omap_bytes_used":4096,"objects":2,"max_avail":1048576,"percent_used":0.125}`

func poolUsageFixtureDF(stats string) string {
	return `{"stats":{"total_bytes":2097152},"pools":[{"name":"fixture","id":7,"stats":` + stats + `}]}`
}

type poolUsageFixtureContainer struct {
	*poolFixtureContainer
	hook func(context.Context, string, int) error
}

func (ctr *poolUsageFixtureContainer) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if ctr.hook != nil {
		if err := ctr.hook(ctx, strings.Join(args[3:], " "), len(ctr.calls)+1); err != nil {
			return 0, nil, err
		}
	}
	return ctr.poolFixtureContainer.Exec(ctx, args, opts...)
}

func newPoolUsageFixture() (*Container, *poolUsageFixtureContainer) {
	ctr := &poolUsageFixtureContainer{poolFixtureContainer: &poolFixtureContainer{output: map[string]string{
		"fsid":                             poolUsageFixtureFSID + "\n",
		"osd pool ls detail --format json": poolUsageFixturePolicies,
		"df detail --format json":          poolUsageFixtureDF(poolUsageFixtureStats),
	}}}
	cluster := &Container{Container: ctr, config: []byte("[global]\nfsid = " + poolUsageFixtureFSID + "\nmon_host = mon\n"), keyring: []byte("confirmed")}
	return cluster, ctr
}

func assertPoolUsageFailure(t *testing.T, cluster *Container, ctx context.Context, want string) error {
	t.Helper()
	usage, err := cluster.PoolUsage(ctx, "fixture")
	if err == nil || usage != (PoolUsageSnapshot{}) || want != "" && !strings.Contains(err.Error(), want) {
		t.Fatalf("unusable observation published: usage=%+v err=%v, want %q", usage, err, want)
	}
	return err
}

func TestPoolUsageReportsNativeUnitsAndReadOnlyScope(t *testing.T) {
	cluster, ctr := newPoolUsageFixture()
	usage, err := cluster.PoolUsage(t.Context(), "fixture")
	want := PoolUsageSnapshot{FSID: poolUsageFixtureFSID, Name: "fixture", ID: 7, StoredBytes: 4099, StoredDataBytes: 4096, StoredOMAPBytes: 3, AllocatedBytes: 16384, AllocatedDataBytes: 12288, AllocatedOMAPBytes: 4096, Objects: 2, MaxAvailableBytes: 1048576, UsedRatio: 0.125}
	if err != nil || usage != want {
		t.Fatalf("logical/allocated bytes or fraction changed: %+v %v", usage, err)
	}
	var calls []string
	for _, args := range ctr.calls {
		calls = append(calls, strings.Join(args, " "))
	}
	if !slices.Equal(calls, []string{"fsid", "osd pool ls detail --format json", "df detail --format json", "osd pool ls detail --format json", "fsid"}) {
		t.Fatalf("missing scope check or unexpected mutation: %v", calls)
	}
	zero := `{"stored":0,"stored_data":0,"stored_omap":0,"bytes_used":0,"data_bytes_used":0,"omap_bytes_used":0,"objects":0,"max_avail":0,"percent_used":0}`
	ctr.output["df detail --format json"] = poolUsageFixtureDF(zero)
	usage, err = cluster.PoolUsage(t.Context(), "fixture")
	if err != nil || usage.ID != 7 || usage.StoredBytes != 0 || usage.AllocatedBytes != 0 || usage.MaxAvailableBytes != 0 {
		t.Fatalf("reported zero confused with missing fields: %+v %v", usage, err)
	}
	large := strings.Replace(poolUsageFixtureStats, `"stored":4099`, `"stored":9007199254740993`, 1)
	large = strings.Replace(large, `"stored_data":4096`, `"stored_data":9007199254740990`, 1)
	ctr.output["df detail --format json"] = poolUsageFixtureDF(large)
	usage, err = cluster.PoolUsage(t.Context(), "fixture")
	if err != nil || usage.StoredBytes != 9007199254740993 {
		t.Fatalf("integer rounded through float64: %+v %v", usage, err)
	}
}

func TestPoolUsageRejectsUnreportedOrAmbiguousStatistics(t *testing.T) {
	cases := map[string]string{
		"absent pool":     `{"pools":[]}`,
		"missing pools":   `{}`,
		"null pools":      `{"pools":null}`,
		"missing stats":   `{"pools":[{"name":"fixture","id":7}]}`,
		"null stats":      `{"pools":[{"name":"fixture","id":7,"stats":null}]}`,
		"changed id":      strings.Replace(poolUsageFixtureDF(poolUsageFixtureStats), `"id":7`, `"id":9`, 1),
		"renamed id":      strings.Replace(poolUsageFixtureDF(poolUsageFixtureStats), `"name":"fixture"`, `"name":"renamed"`, 1),
		"missing id":      strings.Replace(poolUsageFixtureDF(poolUsageFixtureStats), `"id":7,`, "", 1),
		"null id":         strings.Replace(poolUsageFixtureDF(poolUsageFixtureStats), `"id":7`, `"id":null`, 1),
		"negative id":     strings.Replace(poolUsageFixtureDF(poolUsageFixtureStats), `"id":7`, `"id":-1`, 1),
		"duplicate field": strings.Replace(poolUsageFixtureDF(poolUsageFixtureStats), `"id":7`, `"id":9,"id":7`, 1),
		"trailing object": poolUsageFixtureDF(poolUsageFixtureStats) + `{}`,
		"duplicate id":    `{"pools":[{"id":7,"name":"fixture","stats":` + poolUsageFixtureStats + `},{"id":7,"name":"other"}]}`,
		"duplicate name":  `{"pools":[{"id":7,"name":"fixture","stats":` + poolUsageFixtureStats + `},{"id":8,"name":"fixture"}]}`,
	}
	for field, value := range map[string]string{"stored": "4099", "stored_data": "4096", "stored_omap": "3", "bytes_used": "16384", "data_bytes_used": "12288", "omap_bytes_used": "4096", "objects": "2", "max_avail": "1048576", "percent_used": "0.125"} {
		for _, mode := range []string{"missing", "null", "negative", "string"} {
			needle := `"` + field + `":` + value
			replacement := `"` + field + `":null`
			switch mode {
			case "missing":
				needle += ","
				replacement = ""
				if field == "percent_used" {
					needle = "," + `"` + field + `":` + value
				}
			case "negative":
				replacement = `"` + field + `":-1`
			case "string":
				replacement = `"` + field + `":"0"`
			}
			cases[field+" "+mode] = poolUsageFixtureDF(strings.Replace(poolUsageFixtureStats, needle, replacement, 1))
		}
	}
	for _, value := range []string{"1.01", "1e999", "NaN", "true", "-0.01"} {
		cases["invalid ratio "+value] = poolUsageFixtureDF(strings.Replace(poolUsageFixtureStats, `"percent_used":0.125`, `"percent_used":`+value, 1))
	}
	cases["stored total mismatch"] = poolUsageFixtureDF(strings.Replace(poolUsageFixtureStats, `"stored":4099`, `"stored":4100`, 1))
	cases["allocated total mismatch"] = poolUsageFixtureDF(strings.Replace(poolUsageFixtureStats, `"bytes_used":16384`, `"bytes_used":16385`, 1))
	cases["stored sum overflow"] = poolUsageFixtureDF(strings.Replace(poolUsageFixtureStats, `"stored_data":4096`, `"stored_data":18446744073709551615`, 1))
	cases["allocated sum overflow"] = poolUsageFixtureDF(strings.Replace(poolUsageFixtureStats, `"data_bytes_used":12288`, `"data_bytes_used":18446744073709551615`, 1))
	cases["overflow count"] = poolUsageFixtureDF(strings.Replace(poolUsageFixtureStats, `"objects":2`, `"objects":18446744073709551616`, 1))
	cases["fractional count"] = poolUsageFixtureDF(strings.Replace(poolUsageFixtureStats, `"objects":2`, `"objects":1.5`, 1))
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			cluster, ctr := newPoolUsageFixture()
			ctr.output["df detail --format json"] = data
			assertPoolUsageFailure(t, cluster, t.Context(), "")
			if len(ctr.calls) != 3 {
				t.Fatalf("invalid statistics continued observation: %v", ctr.calls)
			}
		})
	}
}

func TestPoolUsageRejectsIdentityDriftAndMalformedPoolLists(t *testing.T) {
	for name, data := range map[string]string{
		"absent":         `[]`,
		"null":           `null`,
		"missing id":     `[{"pool_name":"fixture"}]`,
		"null id":        `[{"pool_id":null,"pool_name":"fixture"}]`,
		"negative id":    `[{"pool_id":-1,"pool_name":"fixture"}]`,
		"duplicate id":   `[{"pool_id":7,"pool_name":"fixture"},{"pool_id":7,"pool_name":"other"}]`,
		"duplicate name": `[{"pool_id":7,"pool_name":"fixture"},{"pool_id":8,"pool_name":"fixture"}]`,
		"duplicate key":  `[{"pool_id":9,"pool_id":7,"pool_name":"fixture"}]`,
	} {
		t.Run(name, func(t *testing.T) {
			cluster, ctr := newPoolUsageFixture()
			ctr.output["osd pool ls detail --format json"] = data
			assertPoolUsageFailure(t, cluster, t.Context(), "")
			if len(ctr.calls) != 2 {
				t.Fatalf("ambiguous pool reached statistics command: %v", ctr.calls)
			}
		})
	}
	for _, point := range []string{"pool replaced", "pool removed", "fsid before", "fsid after"} {
		t.Run(point, func(t *testing.T) {
			cluster, ctr := newPoolUsageFixture()
			ctr.hook = func(_ context.Context, _ string, call int) error {
				if call == 4 && point == "pool replaced" {
					ctr.output["osd pool ls detail --format json"] = strings.Replace(poolUsageFixturePolicies, `"pool_id":7`, `"pool_id":9`, 1)
				}
				if call == 4 && point == "pool removed" {
					ctr.output["osd pool ls detail --format json"] = `[]`
				}
				if call == 1 && point == "fsid before" || call == 5 && point == "fsid after" {
					ctr.output["fsid"] = "61098f7f-474e-4403-ad31-a976f7c4e731"
				}
				return nil
			}
			assertPoolUsageFailure(t, cluster, t.Context(), "")
		})
	}
}

func TestPoolUsageRejectsUnavailableFixtureBeforeQuery(t *testing.T) {
	for _, point := range []string{"terminated", "control", "bootstrap", "keyring", "ambiguous uuid"} {
		t.Run(point, func(t *testing.T) {
			cluster, ctr := newPoolUsageFixture()
			switch point {
			case "terminated":
				cluster.closed = true
			case "control":
				cluster.Container = nil
			case "bootstrap":
				cluster.config = nil
			case "keyring":
				cluster.keyring = nil
			case "ambiguous uuid":
				cluster.config = append(cluster.config, []byte("fsid = "+poolUsageFixtureFSID+"\n")...)
			}
			assertPoolUsageFailure(t, cluster, t.Context(), "")
			if len(ctr.calls) != 0 {
				t.Fatal("unavailable fixture queried native state")
			}
		})
	}
	if usage, err := (*Container)(nil).PoolUsage(t.Context(), "fixture"); err == nil || usage != (PoolUsageSnapshot{}) {
		t.Fatal("nil fixture accepted")
	}
	cluster, ctr := newPoolUsageFixture()
	if _, err := cluster.PoolUsage(t.Context(), "--option"); err == nil || len(ctr.calls) != 0 {
		t.Fatal("invalid name queried native state")
	}
}

func TestPoolUsageCancellationAndQueryFailureDoNotPublishPartialSuccess(t *testing.T) {
	for call := 0; call <= 5; call++ {
		t.Run("cancel at "+string(rune('0'+call)), func(t *testing.T) {
			cluster, ctr := newPoolUsageFixture()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if call == 0 {
				cancel()
			}
			ctr.hook = func(_ context.Context, _ string, current int) error {
				if current == call {
					cancel() // Even valid output returned with nil error cannot succeed.
				}
				return nil
			}
			err := assertPoolUsageFailure(t, cluster, ctx, "")
			if !errors.Is(err, context.Canceled) || len(ctr.calls) != call {
				t.Fatalf("caller cancellation hidden or observation continued: %v calls=%v", err, ctr.calls)
			}
		})
	}
	for call := 1; call <= 5; call++ {
		t.Run("native failure at "+string(rune('0'+call)), func(t *testing.T) {
			cluster, ctr := newPoolUsageFixture()
			cause := errors.New("query transport failed")
			ctr.hook = func(_ context.Context, _ string, current int) error {
				if current == call {
					return cause
				}
				return nil
			}
			err := assertPoolUsageFailure(t, cluster, t.Context(), "")
			if !errors.Is(err, cause) || len(ctr.calls) != call-1 {
				t.Fatalf("native error hidden or observation continued: %v calls=%v", err, ctr.calls)
			}
		})
	}
	cluster, ctr := newPoolUsageFixture()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cause := errors.New("native cause at cancellation")
	ctr.hook = func(_ context.Context, _ string, call int) error {
		if call == 3 {
			cancel()
			return cause
		}
		return nil
	}
	err := assertPoolUsageFailure(t, cluster, ctx, "")
	if !errors.Is(err, cause) || !errors.Is(err, context.Canceled) {
		t.Fatalf("query/context causes not both retained: %v", err)
	}
}

func TestPoolUsageBoundsOwnerControlAndConfigAdmission(t *testing.T) {
	for _, gate := range []string{"owner", "control", "config"} {
		t.Run(gate, func(t *testing.T) {
			cluster, ctr := newPoolUsageFixture()
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
			// Install cancel/release/join before launch, including watchdog failure.
			t.Cleanup(func() { cancel(); release(); <-joined })
			go func() {
				defer close(joined)
				usage, err := cluster.PoolUsage(ctx, "fixture")
				if !reflect.DeepEqual(usage, PoolUsageSnapshot{}) {
					err = errors.Join(err, errors.New("busy admission published partial snapshot"))
				}
				done <- err
			}()
			select {
			case err := <-done:
				<-joined
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("held gate lost deadline: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("PoolUsage remained queued after deadline")
			}
			if len(ctr.calls) != 0 {
				t.Fatal("unadmitted check queried native state")
			}
			release()
			if !cluster.mu.TryLock() {
				t.Fatal("failed admission retained topology gate")
			}
			cluster.mu.Unlock()
			if _, err := cluster.PoolUsage(t.Context(), "fixture"); err != nil {
				t.Fatalf("deadline made fixture unusable: %v", err)
			}
		})
	}
}

var _ testcontainers.Container = (*poolUsageFixtureContainer)(nil)
