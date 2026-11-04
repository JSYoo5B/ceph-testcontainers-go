package ceph

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// pgPrimaryCLI answers the ceph commands of a PG primary override from a
// small explicit state: the stored require_min_compat_client and
// pg-upmap-primary entries of pool 7 ("fixture", pg_num 16), the CRUSH acting
// order of every PG, and what the serving PG reports. It models only the MON
// compatibility gate and the swap of an upmap primary to the front of the
// acting set; it is not Ceph.
type pgPrimaryCLI struct {
	testcontainers.Container
	mu       sync.Mutex
	require  string
	poolType int
	entries  map[string]int
	acting   []int
	down     map[int]bool
	// lag is how many readiness reads the serving PG still reports the
	// CRUSH primary.
	lag   int
	calls []string
	// intercept, when it handles a command, replaces its answer.
	intercept func(command string) (exit int, stderr string, err error, handled bool)
}

var pgPrimaryReleases = []string{"luminous", "mimic", "nautilus", "octopus", "pacific", "quincy", "reef", "squid", "tentacle"}

func newPGPrimaryCLI(require string) (*Container, *pgPrimaryCLI, *OSDContainer, *OSDContainer) {
	cli := &pgPrimaryCLI{require: require, poolType: 1, entries: map[string]int{}, acting: []int{0, 1}, down: map[int]bool{}}
	c := &Container{Container: cli, settings: options{startupTimeout: time.Minute},
		bootstrap: bootstrapRecord{config: []byte("[global]\nfsid = " + poolPGFixtureFSID + "\nmon_host = mon\n"), keyring: []byte("confirmed")}}
	osd0 := &OSDContainer{ID: 0, owner: c, nativeUUID: "uuid-0"}
	osd1 := &OSDContainer{ID: 1, owner: c, nativeUUID: "uuid-1"}
	c.fleet.osdHandles.register(0, osd0)
	c.fleet.osdHandles.register(1, osd1)
	return c, cli, osd0, osd1
}

// mapped is the acting set with an applied upmap primary moved to the front.
func (f *pgPrimaryCLI) mapped(pg string) []int {
	acting := slices.DeleteFunc(slices.Clone(f.acting), func(osd int) bool { return f.down[osd] })
	if entry, ok := f.entries[pg]; ok {
		if i := slices.Index(acting, entry); i > 0 {
			acting[0], acting[i] = acting[i], acting[0]
		}
	}
	return acting
}

func (f *pgPrimaryCLI) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(args) < 4 || !slices.Equal(args[:3], []string{"ceph", "--connect-timeout", "5"}) {
		return 0, nil, errors.New("unexpected command")
	}
	command := strings.Join(args[3:], " ")
	f.calls = append(f.calls, command)
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	if f.intercept != nil {
		if exit, stderr, err, handled := f.intercept(command); handled {
			if err != nil {
				return 0, nil, err
			}
			return exit, poolPGFixtureStream(byte(stdcopy.Stderr), stderr), nil
		}
	}
	out := func(s string) (int, io.Reader, error) { return 0, poolPGFixtureStream(byte(stdcopy.Stdout), s), nil }
	fail := func(errno, text string) (int, io.Reader, error) {
		return 1, poolPGFixtureStream(byte(stdcopy.Stderr), "Error "+errno+": "+text+"\n"), nil
	}
	gate := func() bool {
		return slices.Index(pgPrimaryReleases, f.require) < slices.Index(pgPrimaryReleases, "reef")
	}
	refusal := "min_compat_client " + f.require + " < reef, which is required for pg-upmap-primary. Try 'ceph osd set-require-min-compat-client reef' before using the new interface"
	words := args[3:]
	switch {
	case command == "fsid":
		return out(poolPGFixtureFSID + "\n")
	case command == "osd dump --format json":
		var entries, osds []string
		for _, pg := range slices.Sorted(func(yield func(string) bool) {
			for pg := range f.entries {
				if !yield(pg) {
					return
				}
			}
		}) {
			entries = append(entries, fmt.Sprintf(`{"pgid":%q,"primary_osd":%d}`, pg, f.entries[pg]))
		}
		for _, osd := range []int{0, 1, 2} {
			up := 1
			if f.down[osd] {
				up = 0
			}
			osds = append(osds, fmt.Sprintf(`{"osd":%d,"uuid":"uuid-%d","up":%d,"in":1,"weight":1}`, osd, osd, up))
		}
		pool := strings.NewReplacer(`"type":1`, `"type":`+strconv.Itoa(f.poolType), `"pg_num":8`, `"pg_num":16`).Replace(poolPGFixturePool)
		return out(fmt.Sprintf(`{"fsid":%q,"epoch":20,"require_min_compat_client":%q,"pools":[%s],"osds":[%s],"pg_upmap_primaries":[%s],"primary_temp":[]}`,
			poolPGFixtureFSID, f.require, pool, strings.Join(osds, ","), strings.Join(entries, ",")))
	case len(words) == 5 && words[0] == "pg" && words[1] == "map":
		acting, _ := jsonInts(f.mapped(words[2]))
		return out(fmt.Sprintf(`{"epoch":20,"raw_pgid":%q,"pgid":%q,"up":%s,"acting":%s}`, words[2], words[2], acting, acting))
	case len(words) == 5 && words[0] == "pg" && words[2] == "query":
		mapped := f.mapped(words[1])
		serving := mapped[0]
		if f.lag > 0 {
			f.lag--
			serving = f.acting[0]
		}
		return out(fmt.Sprintf(`{"state":"active+clean","epoch":20,"info":{"pgid":%q,"stats":{"acting_primary":%d}}}`, words[1], serving))
	case strings.HasPrefix(command, "pg ls-by-pool fixture"):
		var reports []string
		for _, pg := range []string{"7.a", "7.b"} {
			mapped, _ := jsonInts(f.mapped(pg))
			reports = append(reports, fmt.Sprintf(`{"pgid":%q,"state":"active+clean","up":%s,"acting":%s,"up_primary":%d,"acting_primary":%d,"reported_epoch":20,"reported_seq":1,"mapping_epoch":20,"last_epoch_clean":20,"stats_invalid":false,"stat_sum":{"num_objects":0,"num_bytes":0,"num_object_copies":0,"num_objects_degraded":0,"num_objects_misplaced":0,"num_objects_unfound":0}}`,
				pg, mapped, mapped, f.mapped(pg)[0], f.mapped(pg)[0]))
		}
		return out(poolPGFixtureReports(strings.Join(reports, ","), true))
	case len(words) == 4 && words[0] == "osd" && words[1] == "pg-upmap-primary":
		if gate() {
			return fail("EPERM", refusal)
		}
		osd, _ := strconv.Atoi(words[3])
		if !slices.Contains(f.acting, osd) {
			return fail("EINVAL", "not in acting set")
		}
		f.entries[words[2]] = osd
		return out("")
	case len(words) == 3 && words[0] == "osd" && words[1] == "rm-pg-upmap-primary":
		if gate() {
			return fail("EPERM", refusal)
		}
		delete(f.entries, words[2])
		return out("")
	case len(words) == 3 && words[0] == "osd" && words[1] == "set-require-min-compat-client":
		if len(f.entries) > 0 && slices.Index(pgPrimaryReleases, words[2]) < slices.Index(pgPrimaryReleases, "reef") {
			return fail("EPERM", "osdmap current utilizes features that require reef")
		}
		f.require = words[2]
		return out("")
	}
	return 0, nil, errors.New("unexpected command " + command)
}

func jsonInts(values []int) (string, error) {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = strconv.Itoa(v)
	}
	return "[" + strings.Join(parts, ",") + "]", nil
}

// mutations returns the commands that change the OSDMap.
func (f *pgPrimaryCLI) mutations() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var result []string
	for _, call := range f.calls {
		if strings.HasPrefix(call, "osd pg-upmap-primary") || strings.HasPrefix(call, "osd rm-pg-upmap-primary") || strings.HasPrefix(call, "osd set-require-min-compat-client") {
			result = append(result, call)
		}
	}
	return result
}

func (f *pgPrimaryCLI) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

func TestOverridePGPrimaryAppliesWaitsForServingPGAndRestoresAbsence(t *testing.T) {
	c, cli, _, osd1 := newPGPrimaryCLI("reef")
	cli.lag = 1
	pool := poolHandle(t, t.Context(), c, "fixture")
	handle, err := pool.OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd1})
	if err != nil || handle == nil {
		t.Fatalf("OverridePGPrimary = %v, %v", handle, err)
	}
	if cli.lag != 0 {
		t.Fatal("the override returned before the serving PG reported the new primary")
	}
	if got := cli.mutations(); !slices.Equal(got, []string{"osd pg-upmap-primary 7.a 1"}) {
		t.Fatalf("mutations = %v", got)
	}
	cli.reset()
	if err := handle.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := cli.mutations(); !slices.Equal(got, []string{"osd rm-pg-upmap-primary 7.a"}) {
		t.Fatalf("restore mutations = %v, want the PG's own removal and never the -all form", got)
	}
	if _, held := cli.entries["7.a"]; held || cli.require != "reef" {
		t.Fatalf("restored state = %v %s", cli.entries, cli.require)
	}
	cli.reset()
	copied := *handle
	if err := copied.Restore(t.Context()); err != nil || len(cli.calls) != 0 {
		t.Fatalf("a restored copy = %v with calls %v", err, cli.calls)
	}
	// The PG is free for a new override.
	again, err := pool.OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd1})
	if err != nil {
		t.Fatal(err)
	}
	_ = again.Restore(t.Context())
}

func TestOverridePGPrimaryRestoresTheExactPriorEntry(t *testing.T) {
	c, cli, _, osd1 := newPGPrimaryCLI("reef")
	cli.acting = []int{0, 1, 2}
	cli.entries["7.a"] = 2
	handle, err := poolHandle(t, t.Context(), c, "fixture").OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd1})
	if err != nil {
		t.Fatal(err)
	}
	cli.reset()
	if err := handle.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := cli.mutations(); !slices.Equal(got, []string{"osd pg-upmap-primary 7.a 2"}) || cli.entries["7.a"] != 2 {
		t.Fatalf("restore mutations = %v, entries %v", got, cli.entries)
	}
}

func TestOverridePGPrimaryOfTheCurrentPrimaryOwnsTheUnchangedEntry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries map[string]int
		osd     int
	}{
		{"CRUSH primary without an entry", map[string]int{}, 0},
		{"entry already present", map[string]int{"7.a": 1}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Below reef: a no-op needs no compatibility either.
			c, cli, osd0, osd1 := newPGPrimaryCLI("luminous")
			cli.entries = tc.entries
			osd := osd0
			if tc.osd == 1 {
				osd = osd1
			}
			handle, err := poolHandle(t, t.Context(), c, "fixture").OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd})
			if err != nil || handle == nil {
				t.Fatalf("OverridePGPrimary = %v, %v", handle, err)
			}
			if err := handle.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got := cli.mutations(); len(got) != 0 || cli.require != "luminous" {
				t.Fatalf("a no-op override wrote %v (require %s)", got, cli.require)
			}
		})
	}
}

func TestOverridePGPrimaryRefusesALowCompatibilityLevelWithoutOptIn(t *testing.T) {
	c, cli, _, osd1 := newPGPrimaryCLI("luminous")
	pool := poolHandle(t, t.Context(), c, "fixture")
	handle, err := pool.OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd1})
	if handle != nil || !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "luminous is below reef") {
		t.Fatalf("OverridePGPrimary = %v, %v", handle, err)
	}
	if got := cli.mutations(); !slices.Equal(got, []string{"osd pg-upmap-primary 7.a 1"}) || cli.require != "luminous" || len(cli.entries) != 0 {
		t.Fatalf("the refusal changed the cluster: %v require %s entries %v", got, cli.require, cli.entries)
	}
	if c.faults.holds(faultPGPrimary, "7.a") || c.faults.holds(faultMinCompatClient, minCompatLeaseKey) {
		t.Fatal("a refused override kept its reservation")
	}
}

func TestOverridePGPrimaryRaisesAndRestoresTheCompatibilityLevelWhenAsked(t *testing.T) {
	c, cli, _, osd1 := newPGPrimaryCLI("luminous")
	handle, err := poolHandle(t, t.Context(), c, "fixture").OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd1, RaiseMinCompatClient: true})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"osd pg-upmap-primary 7.a 1", "osd set-require-min-compat-client reef", "osd pg-upmap-primary 7.a 1"}
	if got := cli.mutations(); !slices.Equal(got, want) {
		t.Fatalf("mutations = %v, want %v", got, want)
	}
	cli.reset()
	if err := handle.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	want = []string{"osd rm-pg-upmap-primary 7.a", "osd set-require-min-compat-client luminous"}
	if got := cli.mutations(); !slices.Equal(got, want) || cli.require != "luminous" {
		t.Fatalf("restore mutations = %v (require %s), want %v", got, cli.require, want)
	}
	if c.faults.holds(faultMinCompatClient, minCompatLeaseKey) {
		t.Fatal("the restored raise is still held")
	}
}

func TestOverridePGPrimaryKeepsASharedRaiseUntilItsLastHolderRestores(t *testing.T) {
	c, cli, _, osd1 := newPGPrimaryCLI("luminous")
	pool := poolHandle(t, t.Context(), c, "fixture")
	first, err := pool.OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd1, RaiseMinCompatClient: true})
	if err != nil {
		t.Fatal(err)
	}
	// The second override relies on the raised level without raising it.
	second, err := pool.OverridePGPrimary(t.Context(), PGPrimary{PG: "7.b", OSD: osd1})
	if err != nil {
		t.Fatal(err)
	}
	if again, err := pool.OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd1}); again != nil || !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a second override of one PG = %v, %v", again, err)
	}
	cli.reset()
	if err := first.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := cli.mutations(); !slices.Equal(got, []string{"osd rm-pg-upmap-primary 7.a"}) || cli.require != "reef" {
		t.Fatalf("the first restore = %v (require %s); the second override still needs reef", got, cli.require)
	}
	cli.reset()
	if err := second.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := cli.mutations(); !slices.Equal(got, []string{"osd rm-pg-upmap-primary 7.b", "osd set-require-min-compat-client luminous"}) {
		t.Fatalf("the last restore = %v", got)
	}
}

func TestOverridePGPrimaryRestoreRefusesOutsideEdits(t *testing.T) {
	c, cli, _, osd1 := newPGPrimaryCLI("luminous")
	cli.acting = []int{0, 1, 2}
	handle, err := poolHandle(t, t.Context(), c, "fixture").OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd1, RaiseMinCompatClient: true})
	if err != nil {
		t.Fatal(err)
	}
	cli.entries["7.a"] = 2
	cli.reset()
	if err := handle.Restore(t.Context()); !errors.Is(err, ErrInvalidRequest) || len(cli.mutations()) != 0 || cli.entries["7.a"] != 2 {
		t.Fatalf("an outside entry edit = %v, mutations %v", err, cli.mutations())
	}
	cli.entries["7.a"] = 1
	cli.intercept = func(command string) (int, string, error, bool) {
		if command == "osd rm-pg-upmap-primary 7.a" {
			// The edit lands with the restoration: lower the level outside.
			delete(cli.entries, "7.a")
			cli.require = "squid"
			return 0, "", nil, true
		}
		return 0, "", nil, false
	}
	if err := handle.Restore(t.Context()); !errors.Is(err, ErrInvalidRequest) || cli.require != "squid" {
		t.Fatalf("an outside level edit = %v (require %s)", err, cli.require)
	}
	cli.intercept = nil
	cli.require = "reef"
	cli.reset()
	if err := handle.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := cli.mutations(); !slices.Equal(got, []string{"osd set-require-min-compat-client luminous"}) {
		t.Fatalf("the retry repeated the restored entry: %v", got)
	}
}

func TestOverridePGPrimaryKeepsAUsableHandleAfterUncertainOrPartialChanges(t *testing.T) {
	t.Run("lost reply of the entry", func(t *testing.T) {
		c, cli, _, osd1 := newPGPrimaryCLI("reef")
		cli.intercept = func(command string) (int, string, error, bool) {
			if command == "osd pg-upmap-primary 7.a 1" {
				cli.entries["7.a"] = 1
				return 0, "", errors.New("connection reset after the command was sent"), true
			}
			return 0, "", nil, false
		}
		handle, err := poolHandle(t, t.Context(), c, "fixture").OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd1})
		if handle == nil || err == nil {
			t.Fatalf("an uncertain write = %v, %v", handle, err)
		}
		cli.intercept = nil
		if err := handle.Restore(t.Context()); err != nil || len(cli.entries) != 0 {
			t.Fatalf("Restore after a lost reply = %v, entries %v", err, cli.entries)
		}
	})
	t.Run("raised level, refused entry", func(t *testing.T) {
		c, cli, _, osd1 := newPGPrimaryCLI("luminous")
		raised := false
		cli.intercept = func(command string) (int, string, error, bool) {
			if command == "osd set-require-min-compat-client reef" {
				raised = true
			}
			if raised && command == "osd pg-upmap-primary 7.a 1" {
				return 1, "Error EINVAL: can't change primary for pg 7.a to osd.1 - illegal pg after the change\n", nil, true
			}
			return 0, "", nil, false
		}
		handle, err := poolHandle(t, t.Context(), c, "fixture").OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd1, RaiseMinCompatClient: true})
		var failure *NativeFailure
		if handle == nil || !errors.As(err, &failure) || cli.require != "reef" {
			t.Fatalf("a refused entry after the raise = %v, %v (require %s)", handle, err, cli.require)
		}
		cli.intercept = nil
		cli.reset()
		if err := handle.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		if got := cli.mutations(); !slices.Equal(got, []string{"osd set-require-min-compat-client luminous"}) {
			t.Fatalf("Restore = %v, want the raised level alone", got)
		}
	})
	t.Run("serving PG never applies", func(t *testing.T) {
		c, cli, _, osd1 := newPGPrimaryCLI("reef")
		cli.lag = 1 << 30
		ctx, cancel := context.WithTimeout(t.Context(), 1200*time.Millisecond)
		defer cancel()
		handle, err := poolHandle(t, ctx, c, "fixture").OverridePGPrimary(ctx, PGPrimary{PG: "7.a", OSD: osd1})
		if handle == nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("an unapplied serving PG = %v, %v", handle, err)
		}
		if err := handle.Restore(t.Context()); err != nil || len(cli.entries) != 0 {
			t.Fatalf("Restore = %v, entries %v", err, cli.entries)
		}
	})
	t.Run("outside edit during the wait", func(t *testing.T) {
		c, cli, _, osd1 := newPGPrimaryCLI("reef")
		cli.acting = []int{0, 1, 2}
		cli.lag = 1 << 30
		cli.intercept = func(command string) (int, string, error, bool) {
			if strings.HasSuffix(command, "query --format json") {
				cli.entries["7.a"] = 2
			}
			return 0, "", nil, false
		}
		handle, err := poolHandle(t, t.Context(), c, "fixture").OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd1})
		if handle == nil || !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("an outside edit during the wait = %v, %v", handle, err)
		}
	})
}

// pgPrimaryRefusals is the reason that each invalid request names.
var pgPrimaryRefusals = map[string]string{
	"nil OSD":                    "owned OSD handle",
	"foreign OSD":                "owned OSD handle",
	"malformed PG":               "invalid native PG ID",
	"PG of another pool":         "not a PG of pool",
	"PG beyond pg_num":           "not a PG of pool",
	"OSD outside the acting set": "not in the acting set",
	"OSD down":                   "is not up",
	"replaced OSD":               "stale",
	"erasure-coded pool":         "not replicated",
	"terminated cluster":         "terminated",
}

func TestOverridePGPrimaryRefusesInvalidRequestsBeforeAnyChange(t *testing.T) {
	_, _, _, foreignOSD := newPGPrimaryCLI("reef")
	for _, tc := range []struct {
		name  string
		setup func(c *Container, cli *pgPrimaryCLI, osd1 *OSDContainer) PGPrimary
	}{
		{"nil OSD", func(*Container, *pgPrimaryCLI, *OSDContainer) PGPrimary { return PGPrimary{PG: "7.a"} }},
		{"foreign OSD", func(*Container, *pgPrimaryCLI, *OSDContainer) PGPrimary { return PGPrimary{PG: "7.a", OSD: foreignOSD} }},
		{"malformed PG", func(_ *Container, _ *pgPrimaryCLI, osd1 *OSDContainer) PGPrimary {
			return PGPrimary{PG: "7.A", OSD: osd1}
		}},
		{"PG of another pool", func(_ *Container, _ *pgPrimaryCLI, osd1 *OSDContainer) PGPrimary {
			return PGPrimary{PG: "8.0", OSD: osd1}
		}},
		{"PG beyond pg_num", func(_ *Container, _ *pgPrimaryCLI, osd1 *OSDContainer) PGPrimary {
			return PGPrimary{PG: "7.10", OSD: osd1}
		}},
		{"OSD outside the acting set", func(_ *Container, cli *pgPrimaryCLI, osd1 *OSDContainer) PGPrimary {
			cli.acting = []int{0, 2}
			return PGPrimary{PG: "7.a", OSD: osd1}
		}},
		{"OSD down", func(_ *Container, cli *pgPrimaryCLI, osd1 *OSDContainer) PGPrimary {
			cli.down[1] = true
			return PGPrimary{PG: "7.a", OSD: osd1}
		}},
		{"replaced OSD", func(c *Container, _ *pgPrimaryCLI, osd1 *OSDContainer) PGPrimary {
			c.fleet.osdHandles.register(1, &OSDContainer{ID: 1, owner: c, nativeUUID: "uuid-1"})
			return PGPrimary{PG: "7.a", OSD: osd1}
		}},
		{"erasure-coded pool", func(_ *Container, cli *pgPrimaryCLI, osd1 *OSDContainer) PGPrimary {
			cli.poolType = 3
			return PGPrimary{PG: "7.a", OSD: osd1}
		}},
		{"terminated cluster", func(c *Container, _ *pgPrimaryCLI, osd1 *OSDContainer) PGPrimary {
			c.closed = true
			return PGPrimary{PG: "7.a", OSD: osd1}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, cli, _, osd1 := newPGPrimaryCLI("reef")
			request := tc.setup(c, cli, osd1)
			handle, err := poolHandle(t, t.Context(), c, "fixture").OverridePGPrimary(t.Context(), request)
			if handle != nil || !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("OverridePGPrimary = %v, %v", handle, err)
			}
			if want := pgPrimaryRefusals[tc.name]; !strings.Contains(err.Error(), want) {
				t.Fatalf("refusal %q does not name %q", err, want)
			}
			if got := cli.mutations(); len(got) != 0 || c.faults.holds(faultPGPrimary, "7.a") {
				t.Fatalf("a refused request changed %v", got)
			}
		})
	}
	var unowned *Pool
	if _, err := unowned.OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a pool without a cluster = %v", err)
	}
}

func TestOverridePGPrimaryBoundsLockAdmissionByContext(t *testing.T) {
	c, cli, _, osd1 := newPGPrimaryCLI("reef")
	handle, err := poolHandle(t, t.Context(), c, "fixture").OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd1})
	if err != nil {
		t.Fatal(err)
	}
	cli.reset()
	c.mu.Lock()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	if _, err := poolHandle(t, ctx, c, "fixture").OverridePGPrimary(ctx, PGPrimary{PG: "7.b", OSD: osd1}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a queued override = %v", err)
	}
	if err := handle.Restore(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("a queued restore = %v", err)
	}
	c.mu.Unlock()
	if len(cli.calls) != 0 {
		t.Fatalf("unadmitted calls ran %v", cli.calls)
	}
	if err := handle.Restore(t.Context()); err != nil || len(cli.entries) != 0 {
		t.Fatalf("Restore with a fresh context = %v", err)
	}
}

func TestOverridePGPrimaryRestoreOnATerminatedClusterKeepsTheOverride(t *testing.T) {
	c, cli, _, osd1 := newPGPrimaryCLI("reef")
	handle, err := poolHandle(t, t.Context(), c, "fixture").OverridePGPrimary(t.Context(), PGPrimary{PG: "7.a", OSD: osd1})
	if err != nil {
		t.Fatal(err)
	}
	c.closed = true
	if err := handle.Restore(t.Context()); !errors.Is(err, ErrInvalidRequest) || !c.faults.holds(faultPGPrimary, "7.a") || cli.entries["7.a"] != 1 {
		t.Fatalf("Restore on a terminated cluster = %v", err)
	}
	var missing *PGPrimaryOverrideHandle
	if err := missing.Restore(t.Context()); err == nil {
		t.Fatal("a nil handle restored")
	}
}
