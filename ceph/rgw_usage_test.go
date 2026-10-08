package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const rgwUsageFixture = `{"stats":{"size":8193,"size_actual":12288,"size_kb":9,"size_kb_actual":12,"num_objects":3},"last_stats_sync":"0.000000","last_stats_update":"2026-10-08T01:02:03.000000Z"}`

func rgwUsageStream(data string) io.Reader {
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(data)
	return &stream
}

type rgwUsageTestContainer struct {
	*rgwAdminTestContainer
	hook func(context.Context, []string, int)
}

func (f *rgwUsageTestContainer) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if f.hook != nil {
		f.hook(ctx, args, len(f.calls))
	}
	return f.rgwAdminTestContainer.Exec(ctx, args, opts...)
}

func rgwUsageUserFixture(id string) string {
	return rgwAdminFixture(id, "PRIVATE-SECRET", false, RGWQuota{MaxSizeBytes: -1, MaxObjects: -1})
}

func newRGWUsageFixture(id string, stats string) (*RGWContainer, *rgwUsageTestContainer, *RGWUser) {
	g, base := newRGWAdminTestGateway(RGWConfig{}, rgwAdminResponse{data: rgwUsageUserFixture(id)}, rgwAdminResponse{data: stats}, rgwAdminResponse{data: rgwUsageUserFixture(id)})
	f := &rgwUsageTestContainer{rgwAdminTestContainer: base}
	g.Container = f
	g.owner.services[rgwServiceName(g.config)] = f
	user := rgwAdminOwnedTestUser(g)
	user.id = id
	return g, f, user
}

func assertRGWUsageReadOnlyCalls(t *testing.T, calls [][]string, id string) {
	t.Helper()
	stats := 0
	for _, args := range calls {
		for _, mutation := range []string{"--sync-stats", "--reset-stats", "flush", "create", "rm", "modify", "reset"} {
			if slices.Contains(args, mutation) {
				t.Fatalf("storage check invoked mutation %q", mutation)
			}
		}
		if slices.Contains(args, "stats") {
			stats++
			if !slices.Contains(args, "user") || !slices.Contains(args, "--uid") || !slices.Contains(args, id) || slices.Contains(args, "account") {
				t.Fatalf("wrong stats owner selector: %v", args)
			}
		}
	}
	if stats != 1 {
		t.Fatalf("expected one storage stats read, got %d", stats)
	}
}

func TestRGWUserUsageReportsNativeStorageAndTenantOwner(t *testing.T) {
	for _, id := range []string{"test-user", "team_a$test-user"} {
		t.Run(id, func(t *testing.T) {
			g, f, user := newRGWUsageFixture(id, rgwUsageFixture)
			usage, err := g.UserUsage(t.Context(), user)
			if err != nil || usage.UserID != id || usage.OwnerID != id || usage.Scope != "user" || usage.SizeBytes != 8193 || usage.SizeActualBytes != 12288 || usage.NumObjects != 3 || usage.LastStatsSync != "0.000000" {
				t.Fatalf("storage counters or aggregation identity changed: %#v %v", usage, err)
			}
			wantTenant := ""
			if strings.Contains(id, "$") {
				wantTenant = "team_a"
			}
			if usage.Tenant != wantTenant || len(f.calls) != 3 {
				t.Fatalf("wrong tenant or extra native reads: %#v %d", usage, len(f.calls))
			}
			assertRGWUsageReadOnlyCalls(t, f.calls, id)
		})
	}
}

type rgwUsageAccountContainer struct {
	*rgwAccountTestContainer
	statsRaw string
	onStats  func()
}

func (f *rgwUsageAccountContainer) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if args[0] == "radosgw-admin" && slices.Contains(args, "user") && slices.Contains(args, "stats") {
		f.calls = append(f.calls, slices.Clone(args))
		if f.onStats != nil {
			f.onStats()
		}
		return 0, rgwUsageStream(f.statsRaw), nil
	}
	return f.rgwAccountTestContainer.Exec(ctx, args, opts...)
}

func newRGWUsageAccountFixture(t *testing.T) (*RGWContainer, *rgwUsageAccountContainer, *RGWAccount, *RGWUser) {
	t.Helper()
	g, base := newRGWAccountTestGateway()
	account, err := g.CreateAccount(t.Context(), RGWAccountConfig{ID: "RGW12345678901234567", Name: "fixture", Tenant: "team_a"})
	if err != nil {
		t.Fatal(err)
	}
	user, err := g.CreateAccountRootUser(t.Context(), account, RGWUserConfig{ID: "root"})
	if err != nil {
		t.Fatal(err)
	}
	f := &rgwUsageAccountContainer{rgwAccountTestContainer: base, statsRaw: rgwUsageFixture}
	g.Container = f
	g.owner.services[rgwServiceName(g.config)] = f
	f.calls = nil
	return g, f, account, user
}

func TestRGWUserUsageAccountRootReportsAccountAggregate(t *testing.T) {
	g, f, account, user := newRGWUsageAccountFixture(t)
	usage, err := g.UserUsage(t.Context(), user)
	if err != nil || usage.Scope != "account" || usage.OwnerID != account.ID() || usage.UserID != "team_a$root" || usage.OwnerID == usage.UserID || usage.Tenant != "team_a" || usage.SizeBytes != 8193 || usage.NumObjects != 3 {
		t.Fatalf("account aggregate mislabeled as per-user storage: %#v %v", usage, err)
	}
	assertRGWUsageReadOnlyCalls(t, f.calls, user.ID())
}

func TestRGWUserUsageRejectsChangedAccountLifetime(t *testing.T) {
	for _, phase := range []string{"before", "after stats"} {
		t.Run(phase, func(t *testing.T) {
			g, f, _, user := newRGWUsageAccountFixture(t)
			if phase == "before" {
				f.versionTag = "recreated-account"
			} else {
				f.onStats = func() { f.versionTag = "recreated-account" }
			}
			usage, err := g.UserUsage(t.Context(), user)
			if err == nil || usage != (RGWUserUsage{}) || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("account lifetime change published usage: %#v %v", usage, err)
			}
			if phase == "before" && slices.ContainsFunc(f.calls, func(args []string) bool { return slices.Contains(args, "stats") }) {
				t.Fatal("invalid account reached storage stats")
			}
		})
	}
}

func TestRGWUserUsageStrictSchemaAndIntegerBounds(t *testing.T) {
	valid := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(rgwUsageFixture, "8193", "0"), "12288", "0"), `"num_objects":3`, `"num_objects":0`)
	if usage, err := decodeRGWUserUsage([]byte(valid)); err != nil || usage.SizeBytes != 0 || usage.SizeActualBytes != 0 || usage.NumObjects != 0 {
		t.Fatal("explicit native zero rejected", usage, err)
	}
	maximum := strings.Replace(rgwUsageFixture, "8193", "18446744073709551615", 1)
	if usage, err := decodeRGWUserUsage([]byte(maximum)); err != nil || usage.SizeBytes != math.MaxUint64 {
		t.Fatal("native uint64 range truncated", usage, err)
	}
	for _, timestamp := range []string{"0.000000", "315359999.999999", time.Unix(315360000, 0).UTC().Format("2006-01-02T15:04:05.000000Z"), "2106-02-07T06:28:15.999999Z"} {
		raw := strings.Replace(rgwUsageFixture, "2026-10-08T01:02:03.000000Z", timestamp, 1)
		usage, err := decodeRGWUserUsage([]byte(raw))
		if err != nil || usage.LastStatsUpdate != timestamp {
			t.Fatalf("native utime representation rejected or changed: %#v %v", usage, err)
		}
	}
	cases := map[string]string{
		"missing stats": `{}`, "null stats": `{"stats":null}`, "array root": `[]`, "null root": `null`,
		"missing size":             strings.Replace(rgwUsageFixture, `"size":8193,`, "", 1),
		"null size":                strings.Replace(rgwUsageFixture, "8193", "null", 1),
		"string size":              strings.Replace(rgwUsageFixture, "8193", `"8193"`, 1),
		"negative size":            strings.Replace(rgwUsageFixture, "8193", "-1", 1),
		"fractional size":          strings.Replace(rgwUsageFixture, "8193", "1.5", 1),
		"overflow size":            strings.Replace(rgwUsageFixture, "8193", "18446744073709551616", 1),
		"missing actual":           strings.Replace(rgwUsageFixture, `"size_actual":12288,`, "", 1),
		"missing count":            strings.Replace(rgwUsageFixture, `,"num_objects":3`, "", 1),
		"missing time":             strings.Replace(rgwUsageFixture, `,"last_stats_update":"2026-10-08T01:02:03.000000Z"`, "", 1),
		"null time":                strings.Replace(rgwUsageFixture, `"2026-10-08T01:02:03.000000Z"`, "null", 1),
		"secret timestamp":         strings.Replace(rgwUsageFixture, "2026-10-08T01:02:03.000000Z", "PRIVATE-SECRET", 1),
		"invalid date":             strings.Replace(rgwUsageFixture, "2026-10-08", "2026-02-30", 1),
		"timezone offset":          strings.Replace(rgwUsageFixture, ".000000Z", ".000000+00:00", 1),
		"short fraction":           strings.Replace(rgwUsageFixture, ".000000Z", ".00000Z", 1),
		"raw leading zero":         strings.Replace(rgwUsageFixture, "0.000000", "00.000000", 1),
		"raw signed fraction":      strings.Replace(rgwUsageFixture, "0.000000", "0.+00000", 1),
		"raw out of range":         strings.Replace(rgwUsageFixture, "0.000000", "315360000.000000", 1),
		"noncanonical epoch":       strings.Replace(rgwUsageFixture, "0.000000", "1970-01-01T00:00:00.000000Z", 1),
		"timestamp overflow":       strings.Replace(rgwUsageFixture, "2026-10-08T01:02:03.000000Z", "2106-02-07T06:28:16.000000Z", 1),
		"empty time":               strings.Replace(rgwUsageFixture, `"2026-10-08T01:02:03.000000Z"`, `""`, 1),
		"duplicate stats":          strings.Replace(rgwUsageFixture, `"stats":{`, `"stats":{},"stats":{`, 1),
		"duplicate size":           strings.Replace(rgwUsageFixture, `"size":8193`, `"size":1,"size":8193`, 1),
		"duplicate unknown nested": strings.Replace(rgwUsageFixture, `"stats":{`, `"future":{"key":null,"key":1},"stats":{`, 1),
		"trailing JSON":            rgwUsageFixture + `{}`, "trailing secret": rgwUsageFixture + "PRIVATE-SECRET", "broken secret": `{"PRIVATE-SECRET":`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			usage, err := decodeRGWUserUsage([]byte(raw))
			if err == nil || usage != (RGWUserUsage{}) || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("malformed accounting data accepted or exposed: %#v %v", usage, err)
			}
		})
	}
}

func TestRGWUserUsageRefusesUnownedOrReplacedGatewayBeforeNative(t *testing.T) {
	cases := map[string]func(*RGWContainer, *RGWUser){
		"foreign user":            func(g *RGWContainer, user *RGWUser) { user.owner = &Container{} },
		"unconfirmed user":        func(g *RGWContainer, user *RGWUser) { user.state.created = false },
		"removed user":            func(g *RGWContainer, user *RGWUser) { user.state.removed = true },
		"closed cluster":          func(g *RGWContainer, user *RGWUser) { g.owner.closed = true },
		"removed gateway":         func(g *RGWContainer, user *RGWUser) { delete(g.owner.services, rgwServiceName(g.config)) },
		"same CID foreign handle": func(g *RGWContainer, user *RGWUser) { g.Container = &rgwAdminTestContainer{id: "gateway"} },
		"typed nil gateway":       func(g *RGWContainer, user *RGWUser) { g.Container = (*rgwAdminTestContainer)(nil) },
		"typed nil service": func(g *RGWContainer, user *RGWUser) {
			g.owner.services[rgwServiceName(g.config)] = (*rgwAdminTestContainer)(nil)
		},
		"changed declared scope": func(g *RGWContainer, user *RGWUser) { user.scope.Zone = "foreign" },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			g, f, user := newRGWUsageFixture("test-user", rgwUsageFixture)
			edit(g, user)
			usage, err := g.UserUsage(t.Context(), user)
			if err == nil || usage != (RGWUserUsage{}) || len(f.calls) != 0 {
				t.Fatalf("invalid fixture authority reached native: %#v %v calls%d", usage, err, len(f.calls))
			}
		})
	}
}

func TestRGWUserUsageFinalIdentityGuardDiscardsCounters(t *testing.T) {
	for _, edit := range []struct{ name, raw string }{
		{"credentials", strings.ReplaceAll(rgwUsageUserFixture("test-user"), "PRIVATE-SECRET", "foreign-secret")},
		{"user ID", rgwUsageUserFixture("other-user")},
		{"account association", strings.Replace(rgwUsageUserFixture("test-user"), `"type":"rgw"`, `"type":"rgw","account_id":"RGW12345678901234567"`, 1)},
		{"native type", strings.Replace(rgwUsageUserFixture("test-user"), `"type":"rgw"`, `"type":"root"`, 1)},
	} {
		t.Run(edit.name, func(t *testing.T) {
			g, f, user := newRGWUsageFixture("test-user", rgwUsageFixture)
			f.responses[2].data = edit.raw
			usage, err := g.UserUsage(t.Context(), user)
			if err == nil || usage != (RGWUserUsage{}) || len(f.calls) != 3 || strings.Contains(err.Error(), "PRIVATE") || strings.Contains(err.Error(), "foreign-secret") {
				t.Fatalf("newer identity published prior counters: %#v %v", usage, err)
			}
		})
	}
}

func TestRGWUserUsageCancellationBeforeNativeAndAfterValidReply(t *testing.T) {
	for _, phase := range []string{"before", "after stats", "after final user"} {
		t.Run(phase, func(t *testing.T) {
			g, f, user := newRGWUsageFixture("test-user", rgwUsageFixture)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if phase == "before" {
				cancel()
			} else {
				at := 1
				if phase == "after final user" {
					at = 2
				}
				f.hook = func(_ context.Context, _ []string, index int) {
					if index == at {
						cancel()
					}
				}
			}
			usage, err := g.UserUsage(ctx, user)
			if !errors.Is(err, context.Canceled) || usage != (RGWUserUsage{}) {
				t.Fatalf("late success or context lost: %#v %v", usage, err)
			}
			want := 0
			if phase == "after stats" {
				want = 2
			}
			if phase == "after final user" {
				want = 3
			}
			if len(f.calls) != want {
				t.Fatalf("canceled read issued extra native queries: %d want%d", len(f.calls), want)
			}
		})
	}
}

func TestRGWUserUsageHeldOwnerDeadlineJoinsWithoutNative(t *testing.T) {
	g, f, user := newRGWUsageFixture("test-user", rgwUsageFixture)
	g.owner.mu.Lock()
	var unlock sync.Once
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	result := make(chan error, 1)
	started := false
	joined := false
	// Cleanup owns exactly one cancel/unlock/join path before launch.
	t.Cleanup(func() {
		cancel()
		unlock.Do(g.owner.mu.Unlock)
		if started && !joined {
			select {
			case <-result:
			case <-time.After(2 * time.Second):
				t.Error("usage worker did not join")
			}
		}
	})
	started = true
	go func() { _, err := g.UserUsage(ctx, user); result <- err }()
	select {
	case err := <-result:
		joined = true
		if !errors.Is(err, context.DeadlineExceeded) || len(f.calls) != 0 {
			t.Fatalf("held owner ignored deadline: %v calls%d", err, len(f.calls))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("held owner worker exceeded watchdog")
	}
	unlock.Do(g.owner.mu.Unlock)
	if !g.owner.mu.TryLock() {
		t.Fatal("usage worker retained owner gate")
	}
	g.owner.mu.Unlock()
}

func TestRGWUserUsageRedactsNativeErrorsAndPreservesContextCause(t *testing.T) {
	for _, phase := range []string{"native", "transport", "deadline"} {
		t.Run(phase, func(t *testing.T) {
			g, f, user := newRGWUsageFixture("test-user", rgwUsageFixture)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			if phase == "native" {
				f.responses[1] = rgwAdminResponse{code: 13, data: "PRIVATE-SECRET native accounting failure"}
			}
			if phase == "transport" {
				f.responses[1] = rgwAdminResponse{err: errors.New("PRIVATE-SECRET transport failure")}
			}
			if phase == "deadline" {
				f.hook = func(ctx context.Context, _ []string, index int) {
					if index == 1 {
						<-ctx.Done()
					}
				}
				f.responses[1] = rgwAdminResponse{err: errors.New("PRIVATE-SECRET timeout transport")}
			}
			usage, err := g.UserUsage(ctx, user)
			if err == nil || usage != (RGWUserUsage{}) || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("native error published or exposed: %#v %v", usage, err)
			}
			if phase == "deadline" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("context cause lost: %v", err)
			}
		})
	}
}

func TestRGWUserUsageGatewayFinalGuardDiscardsCounters(t *testing.T) {
	g, f, user := newRGWUsageFixture("test-user", rgwUsageFixture)
	f.hook = func(_ context.Context, _ []string, index int) {
		if index == 2 {
			g.owner.services[rgwServiceName(g.config)] = &rgwAdminTestContainer{id: "gateway"}
		}
	}
	usage, err := g.UserUsage(t.Context(), user)
	if err == nil || usage != (RGWUserUsage{}) || len(f.calls) != 3 {
		t.Fatalf("gateway replacement published storage: %#v %v", usage, err)
	}
}

// Keep the compile-time interface assumption explicit for test wrappers.
var _ testcontainers.Container = (*rgwUsageTestContainer)(nil)

func TestRGWUserUsageRejectsChangedNativeScopeAndRedactsControlFailure(t *testing.T) {
	for _, phase := range []string{"before scope", "after scope", "control secret"} {
		t.Run(phase, func(t *testing.T) {
			g, f, _, user := newRGWUsageAccountFixture(t)
			switch phase {
			case "before scope":
				f.group["realm_id"] = "foreign-realm"
			case "after scope":
				f.onStats = func() { f.group["realm_id"] = "foreign-realm" }
			case "control secret":
				g.owner.Container = &rgwAdminTestContainer{id: "control", responses: []rgwAdminResponse{{code: 13, data: "PRIVATE-SECRET native service dump failure"}}}
			}
			usage, err := g.UserUsage(t.Context(), user)
			if err == nil || usage != (RGWUserUsage{}) || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("invalid native scope or secret escaped: %#v %v", usage, err)
			}
			if phase != "after scope" && slices.ContainsFunc(f.calls, func(args []string) bool { return slices.Contains(args, "stats") }) {
				t.Fatal("invalid runtime scope reached stats")
			}
		})
	}
}

func TestRGWUserUsageHeldControlDeadlineReleasesOwnerAndDoesNotReadStats(t *testing.T) {
	g, f, _, user := newRGWUsageAccountFixture(t)
	assertSnapshotReadLockQueued(t, &g.owner.controlMu, func(ctx context.Context) error {
		usage, err := g.UserUsage(ctx, user)
		if usage != (RGWUserUsage{}) {
			t.Error("queued native scope published usage")
		}
		return err
	})
	if slices.ContainsFunc(f.calls, func(args []string) bool { return slices.Contains(args, "stats") }) {
		t.Fatal("held control gate reached stats")
	}
	if !g.owner.mu.TryLock() {
		t.Fatal("queued native scope retained owner gate")
	}
	g.owner.mu.Unlock()
	if _, err := g.UserUsage(t.Context(), user); err != nil {
		t.Fatalf("fresh context could not reuse retained fixture: %v", err)
	}
}
