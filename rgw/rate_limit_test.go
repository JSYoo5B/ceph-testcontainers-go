package rgw

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// rateLimitBucket is the native identity of one bucket in rateLimitGateway.
type rateLimitBucket struct{ id, marker, owner string }

// rateLimitGateway is a small model of the radosgw-admin state that
// OverrideRateLimit reads and changes: one user, its buckets and the stored
// rate-limit policy of each scope. ratelimit set changes only the budgets it
// is given and ignores negative ones, enable and disable change only the
// flag, as radosgw-admin does. With unsetENOENT the model is a Ceph 21
// gateway: a user without a stored policy fails ratelimit get with exit 254
// and ENOENT, and a stored policy also carries the list and delete budgets;
// otherwise get reports the native default, as Ceph 20 does. metadata get of
// the user lists the policy attribute while a policy is stored. A command
// materializes the native default before it applies. before, when set, runs
// first for every command and may answer it instead (handled true); after
// runs once the model applied a command and may replace its reply.
type rateLimitGateway struct {
	testcontainers.Container
	userDocument string
	buckets      map[string]rateLimitBucket
	policies     map[string]map[string]any
	calls        [][]string
	before       func(args []string) (code int, err error, handled bool)
	after        func(args []string) error
	// errnoLine, when set, is the error output of a command that before
	// answers.
	errnoLine   string
	unsetENOENT bool
}

func (f *rateLimitGateway) GetContainerID() string { return "gateway" }

func (f *rateLimitGateway) State(context.Context) (*container.State, error) {
	return &container.State{Running: true}, nil
}

func (f *rateLimitGateway) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if gwtest.IsCancel(args) {
		return 0, nil, nil
	}
	original := args
	args = gwtest.Argv(args)
	f.calls = append(f.calls, slices.Clone(args))
	if f.before != nil {
		if code, err, handled := f.before(args); handled {
			if err != nil {
				return 0, nil, err
			}
			output := "PRIVATE-SECRET native failure\n"
			if f.errnoLine != "" {
				output = f.errnoLine + "\n"
			}
			return gwtest.Reply(original, code, gwtest.Stream(true, []byte(output)))
		}
	}
	data, code := f.answer(args)
	if f.after != nil {
		if err := f.after(args); err != nil {
			return 0, nil, err
		}
	}
	if code != 0 {
		data += "\n"
	}
	return gwtest.Reply(original, code, gwtest.Stream(code != 0, []byte(data)))
}

func (f *rateLimitGateway) answer(args []string) (string, int) {
	command := rateLimitCommand(args)
	switch {
	case slices.Equal(command, []string{"user", "info"}):
		return f.userDocument, 0
	case slices.Equal(command, []string{"bucket", "stats"}):
		bucket, ok := f.buckets[rateLimitFlag(args, "--bucket")]
		if !ok {
			return "PRIVATE-SECRET no such bucket", 2
		}
		data, _ := json.Marshal(map[string]any{"bucket": rateLimitFlag(args, "--bucket"), "tenant": "", "id": bucket.id,
			"marker": bucket.marker, "owner": bucket.owner, "usage": map[string]any{}})
		return string(data), 0
	case slices.Equal(command, []string{"metadata", "get"}):
		key := args[slices.Index(args, "get")+1]
		id := strings.TrimPrefix(key, "user:")
		attrs := []map[string]string{{"key": "user.rgw.acl", "val": "AAAA"}}
		if f.policies[key] != nil {
			attrs = append(attrs, map[string]string{"key": "user.rgw.ratelimit", "val": "AAAA"})
		}
		data, _ := json.Marshal(map[string]any{"key": key, "ver": map[string]any{"tag": "tag", "ver": 1}, "mtime": "now",
			"data": map[string]any{"user_id": id, "keys": []map[string]string{{"user": id, "secret_key": "PRIVATE-SECRET"}}, "attrs": attrs}})
		return string(data), 0
	case len(command) == 2 && command[0] == "ratelimit":
		key, section := "user:"+rateLimitFlag(args, "--uid"), "user_ratelimit"
		if rateLimitFlag(args, "--ratelimit-scope") == "bucket" {
			key, section = "bucket:"+rateLimitFlag(args, "--bucket"), "bucket_ratelimit"
		}
		if command[1] == "get" && section == "user_ratelimit" && f.unsetENOENT && f.policies[key] == nil {
			return "ERROR: failed to get a ratelimit for user id: '" + rateLimitFlag(args, "--uid") + "', errno: (2) No such file or directory", 254
		}
		policy := f.policy(key)
		switch command[1] {
		case "get":
			data, _ := json.Marshal(map[string]any{section: policy})
			return string(data), 0
		case "set":
			for _, name := range []string{"max-read-ops", "max-write-ops", "max-read-bytes", "max-write-bytes"} {
				if value := rateLimitFlag(args, "--"+name); value != "" && !strings.HasPrefix(value, "-") {
					policy[strings.ReplaceAll(name, "-", "_")] = json.Number(value)
				}
			}
		case "enable", "disable":
			policy["enabled"] = command[1] == "enable"
		}
		return "", 0
	}
	return "PRIVATE-SECRET unexpected command", 1
}

// policy returns the stored policy of key, creating the native default.
func (f *rateLimitGateway) policy(key string) map[string]any {
	if f.policies[key] == nil {
		f.policies[key] = rateLimitNative(false, 0, 0)
		if f.unsetENOENT {
			f.policies[key] = rateLimitNative21(false, 0, 0)
		}
	}
	return f.policies[key]
}

// mutations returns the ratelimit commands that change state, as
// "set"/"enable"/"disable" with their scope selector.
func (f *rateLimitGateway) mutations() []string {
	var out []string
	for _, args := range f.calls {
		command := rateLimitCommand(args)
		if len(command) == 2 && command[0] == "ratelimit" && command[1] != "get" {
			out = append(out, command[1]+" "+rateLimitFlag(args, "--ratelimit-scope")+" "+rateLimitFlag(args, "--uid")+rateLimitFlag(args, "--bucket"))
		}
	}
	return out
}

func rateLimitNative(enabled bool, readOps, writeOps int64) map[string]any {
	return map[string]any{"max_read_ops": json.Number(itoa(readOps)), "max_write_ops": json.Number(itoa(writeOps)),
		"max_read_bytes": json.Number("0"), "max_write_bytes": json.Number("0"), "enabled": enabled}
}

// rateLimitNative21 is a Ceph 21 policy object, which also carries the list
// and delete budgets.
func rateLimitNative21(enabled bool, readOps, writeOps int64) map[string]any {
	policy := rateLimitNative(enabled, readOps, writeOps)
	policy["max_list_ops"], policy["max_delete_ops"] = json.Number("0"), json.Number("0")
	return policy
}

func itoa(v int64) string { data, _ := json.Marshal(v); return string(data) }

// rateLimitCommand returns the two words of the radosgw-admin operation.
func rateLimitCommand(args []string) []string {
	for i, arg := range args {
		if arg == "json" && i+2 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			return args[i+1 : i+3]
		}
	}
	return nil
}

func rateLimitFlag(args []string, flag string) string {
	for i, arg := range args {
		if arg == flag && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

// newRateLimitFixture returns an owned user test-user of a gateway modelled by
// rateLimitGateway, owning the buckets limited and outside; other-owned
// belongs to another user.
func newRateLimitFixture(t *testing.T) (*User, *rateLimitGateway) {
	t.Helper()
	gateway, _ := newRGWAdminTestGateway(Config{})
	f := &rateLimitGateway{
		userDocument: rgwAdminFixture("test-user", "PRIVATE-SECRET", false, Quota{MaxSizeBytes: -1, MaxObjects: -1}),
		buckets: map[string]rateLimitBucket{
			"limited":     {id: "zone.1", marker: "zone.1", owner: "test-user"},
			"outside":     {id: "zone.2", marker: "zone.2", owner: "test-user"},
			"other-owned": {id: "zone.3", marker: "zone.3", owner: "other-user"},
		},
		policies: map[string]map[string]any{},
	}
	gateway.Container = f
	testhelper.TestRecordService(gateway.base().Cluster(), rgwServiceName(gateway.config), f)
	return rgwAdminOwnedTestUser(gateway), f
}

func requireNoSecret(t *testing.T, err error) {
	t.Helper()
	if err != nil && strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("error exposes native output: %v", err)
	}
}

// A bucket scope binds the bucket the user owns, applies the whole policy,
// reads it back, leaves the sibling bucket and unknown native fields alone and
// restores the exact prior policy once.
func TestRGWRateLimitOverrideBucketScopeRestoresExactPriorPolicy(t *testing.T) {
	user, f := newRateLimitFixture(t)
	prior := rateLimitNative(false, 7, 0)
	prior["future_budget"] = json.Number("3")
	f.policies["bucket:limited"] = maps.Clone(prior)
	sibling := rateLimitNative(true, 0, 9)
	f.policies["bucket:outside"] = maps.Clone(sibling)

	change, err := user.OverrideRateLimit(t.Context(), RateLimitScope{Bucket: "limited"}, RateLimit{Enabled: true, MaxReadOps: 1, MaxReadBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"max_read_ops": json.Number("1"), "max_write_ops": json.Number("0"), "max_read_bytes": json.Number("4096"),
		"max_write_bytes": json.Number("0"), "enabled": true, "future_budget": json.Number("3")}
	if !maps.Equal(f.policies["bucket:limited"], want) || !maps.Equal(f.policies["bucket:outside"], sibling) {
		t.Fatalf("applied policy %v, sibling %v", f.policies["bucket:limited"], f.policies["bucket:outside"])
	}
	// Limits are stored while the policy is still disabled, then enabled.
	if got := f.mutations(); !slices.Equal(got, []string{"set bucket limited", "enable bucket limited"}) {
		t.Fatalf("apply commands = %v", got)
	}
	copied := *change
	if err := copied.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(f.policies["bucket:limited"], prior) || !maps.Equal(f.policies["bucket:outside"], sibling) {
		t.Fatalf("restored policy %v, sibling %v", f.policies["bucket:limited"], f.policies["bucket:outside"])
	}
	// The policy is disabled before the prior limits are stored again.
	if got := f.mutations(); !slices.Equal(got[2:], []string{"disable bucket limited", "set bucket limited"}) {
		t.Fatalf("restore commands = %v", got)
	}
	calls := len(f.calls)
	if err := change.Restore(t.Context()); err != nil || len(f.calls) != calls {
		t.Fatalf("a restored copy ran native commands again: %v", err)
	}
	for _, args := range f.calls {
		for _, word := range []string{"rm", "unlink", "link", "chown", "--purge-objects", "global"} {
			if slices.Contains(args, word) {
				t.Fatalf("rate-limit override ran %v", args)
			}
		}
	}
}

// Applying and restoring an override wait for the cluster mutex only until
// the caller's context ends. A queued call sends no radosgw-admin command and
// leaves the policy and the handle as they were; the same call succeeds with
// a fresh context.
func TestRGWRateLimitOverrideWaitsForTheClusterOnlyUntilTheContextEnds(t *testing.T) {
	user, f := newRateLimitFixture(t)
	prior := rateLimitNative(false, 7, 0)
	f.policies["bucket:limited"] = maps.Clone(prior)
	mutex := testhelper.TestState(user.base().Origin.Cluster()).Mutex
	scope, limit := RateLimitScope{Bucket: "limited"}, RateLimit{Enabled: true, MaxReadOps: 1}
	clustertest.AssertContextLockQueued(t, mutex, func(ctx context.Context) error {
		change, err := user.OverrideRateLimit(ctx, scope, limit)
		// The not-applied handle has nothing to restore.
		if restoreErr := change.Restore(t.Context()); restoreErr != nil {
			t.Errorf("Restore of a queued override: %v", restoreErr)
		}
		return err
	})
	if len(f.calls) != 0 || !maps.Equal(f.policies["bucket:limited"], prior) {
		t.Fatalf("a queued OverrideRateLimit reached radosgw-admin: %v", f.calls)
	}
	change, err := user.OverrideRateLimit(t.Context(), scope, limit)
	if err != nil {
		t.Fatalf("OverrideRateLimit with a fresh context: %v", err)
	}
	applied, calls := maps.Clone(f.policies["bucket:limited"]), len(f.calls)
	clustertest.AssertContextLockQueued(t, mutex, change.Restore)
	if len(f.calls) != calls || !maps.Equal(f.policies["bucket:limited"], applied) {
		t.Fatalf("a queued Restore reached radosgw-admin: %v", f.calls[calls:])
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatalf("Restore with a fresh context: %v", err)
	}
	if !maps.Equal(f.policies["bucket:limited"], prior) {
		t.Fatalf("restored policy %v", f.policies["bucket:limited"])
	}
}

// A user scope selects the user by its canonical ID; from an enabled prior
// policy to a disabled one, the policy is disabled before limits change, and
// the prior one is stored before it is enabled again.
func TestRGWRateLimitOverrideUserScopeOrdersEnforcement(t *testing.T) {
	user, f := newRateLimitFixture(t)
	prior := rateLimitNative(true, 10, 20)
	f.policies["user:test-user"] = maps.Clone(prior)
	change, err := user.OverrideRateLimit(t.Context(), RateLimitScope{}, RateLimit{})
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(f.policies["user:test-user"], rateLimitNative(false, 0, 0)) {
		t.Fatalf("applied policy %v", f.policies["user:test-user"])
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(f.policies["user:test-user"], prior) {
		t.Fatalf("restored policy %v", f.policies["user:test-user"])
	}
	if got := f.mutations(); !slices.Equal(got, []string{"disable user test-user", "set user test-user", "set user test-user", "enable user test-user"}) {
		t.Fatalf("commands = %v", got)
	}
	if len(f.policies) != 1 {
		t.Fatalf("other scopes were touched: %v", f.policies)
	}
}

// On Ceph 19 the user scope is refused as unsupported before any native
// command, naming the release; the bucket scope still applies there.
func TestRGWRateLimitOverrideUserScopeNeedsCeph20(t *testing.T) {
	user, f := newRateLimitFixture(t)
	testhelper.TestSetCephVersion(user.base().Origin.Cluster(), "19.2.5")
	change, err := user.OverrideRateLimit(t.Context(), RateLimitScope{}, RateLimit{Enabled: true, MaxWriteOps: 1})
	var refusal *ceph.OperationError
	if !errors.Is(err, ceph.ErrUnsupported) || !errors.As(err, &refusal) || refusal.Version != "19.2.5" ||
		refusal.Capability != "RGW user-scope rate limits" || !strings.Contains(err.Error(), "need Ceph 20 or later") {
		t.Fatalf("user scope on Ceph 19: %v", err)
	}
	if len(f.mutations()) != 0 || len(f.policies) != 0 || slices.ContainsFunc(f.calls, func(args []string) bool { return slices.Contains(args, "ratelimit") }) {
		t.Fatalf("refused user scope ran rate-limit commands: %v", f.calls)
	}
	calls := len(f.calls)
	if err := change.Restore(t.Context()); err != nil || len(f.calls) != calls {
		t.Fatalf("refused handle's Restore acted: %v", err)
	}
	bucket, err := user.OverrideRateLimit(t.Context(), RateLimitScope{Bucket: "limited"}, RateLimit{Enabled: true, MaxReadOps: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// Refused calls change nothing and return a handle whose Restore does
// nothing.
func TestRGWRateLimitOverrideRefusalsChangeNothing(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name    string
		ctx     context.Context
		scope   RateLimitScope
		limit   RateLimit
		invalid bool
		cause   error
	}{
		{name: "negative budget", scope: RateLimitScope{}, limit: RateLimit{Enabled: true, MaxWriteOps: -1}, invalid: true},
		{name: "flag-like bucket", scope: RateLimitScope{Bucket: "-limited"}, invalid: true},
		{name: "other tenant bucket", scope: RateLimitScope{Bucket: "tenant/limited"}, invalid: true},
		{name: "foreign bucket", scope: RateLimitScope{Bucket: "other-owned"}, invalid: true},
		{name: "missing bucket", scope: RateLimitScope{Bucket: "absent"}},
		{name: "canceled", ctx: canceled, cause: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user, f := newRateLimitFixture(t)
			ctx := tc.ctx
			if ctx == nil {
				ctx = t.Context()
			}
			change, err := user.OverrideRateLimit(ctx, tc.scope, tc.limit)
			requireNoSecret(t, err)
			if err == nil || change == nil || errors.Is(err, ceph.ErrInvalidRequest) != tc.invalid || (tc.cause != nil && !errors.Is(err, tc.cause)) {
				t.Fatalf("got handle %v, error %v", change, err)
			}
			if len(f.mutations()) != 0 || len(f.policies) != 0 {
				t.Fatalf("refused call changed native state: %v", f.mutations())
			}
			calls := len(f.calls)
			if err := change.Restore(t.Context()); err != nil || len(f.calls) != calls {
				t.Fatalf("refused handle's Restore acted: %v", err)
			}
		})
	}
	var nilHandle *RateLimitOverrideHandle
	if nilHandle.Restore(t.Context()) == nil || (&RateLimitOverrideHandle{}).Restore(t.Context()) == nil {
		t.Fatal("an unavailable handle restored")
	}
}

// One override per scope: a second one is refused until the first is
// restored; other scopes stay independent.
func TestRGWRateLimitOverrideRefusesOverlappingScope(t *testing.T) {
	user, f := newRateLimitFixture(t)
	first, err := user.OverrideRateLimit(t.Context(), RateLimitScope{Bucket: "limited"}, RateLimit{Enabled: true, MaxReadOps: 1})
	if err != nil {
		t.Fatal(err)
	}
	mutations := len(f.mutations())
	second, err := user.OverrideRateLimit(t.Context(), RateLimitScope{Bucket: "limited"}, RateLimit{Enabled: true, MaxReadOps: 1})
	if err == nil || len(f.mutations()) != mutations || second.Restore(t.Context()) != nil {
		t.Fatalf("overlapping override was not refused: %v", err)
	}
	if !maps.Equal(f.policies["bucket:limited"], rateLimitNative(true, 1, 0)) {
		t.Fatal("refused overlap changed the first override's policy")
	}
	userScope, err := user.OverrideRateLimit(t.Context(), RateLimitScope{}, RateLimit{Enabled: true, MaxWriteOps: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []*RateLimitOverrideHandle{first, userScope} {
		if err := change.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	again, err := user.OverrideRateLimit(t.Context(), RateLimitScope{Bucket: "limited"}, RateLimit{Enabled: true, MaxReadOps: 1})
	if err != nil {
		t.Fatalf("a restored override still blocked its scope: %v", err)
	}
	if err := again.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// Restore refuses a replaced bucket, a replaced user and an outside policy
// edit instead of overwriting them, and keeps the override tracked.
func TestRGWRateLimitRestoreRefusesReplacedTargetsAndOutsideEdits(t *testing.T) {
	for _, tc := range []struct {
		name   string
		scope  RateLimitScope
		change func(f *rateLimitGateway)
		undo   func(f *rateLimitGateway)
	}{
		{"replaced bucket", RateLimitScope{Bucket: "limited"},
			func(f *rateLimitGateway) {
				f.buckets["limited"] = rateLimitBucket{id: "zone.9", marker: "zone.9", owner: "test-user"}
			},
			func(f *rateLimitGateway) {
				f.buckets["limited"] = rateLimitBucket{id: "zone.1", marker: "zone.1", owner: "test-user"}
			}},
		{"bucket given away", RateLimitScope{Bucket: "limited"},
			func(f *rateLimitGateway) {
				f.buckets["limited"] = rateLimitBucket{id: "zone.1", marker: "zone.1", owner: "other-user"}
			},
			func(f *rateLimitGateway) {
				f.buckets["limited"] = rateLimitBucket{id: "zone.1", marker: "zone.1", owner: "test-user"}
			}},
		{"replaced user keys", RateLimitScope{},
			func(f *rateLimitGateway) {
				f.userDocument = rgwAdminFixture("test-user", "PRIVATE-OTHER", false, Quota{MaxSizeBytes: -1, MaxObjects: -1})
			},
			func(f *rateLimitGateway) {
				f.userDocument = rgwAdminFixture("test-user", "PRIVATE-SECRET", false, Quota{MaxSizeBytes: -1, MaxObjects: -1})
			}},
		{"outside budget edit", RateLimitScope{Bucket: "limited"},
			func(f *rateLimitGateway) { f.policies["bucket:limited"]["max_read_ops"] = json.Number("5") },
			func(f *rateLimitGateway) { f.policies["bucket:limited"]["max_read_ops"] = json.Number("1") }},
		{"outside disable", RateLimitScope{},
			func(f *rateLimitGateway) { f.policies["user:test-user"]["enabled"] = false },
			func(f *rateLimitGateway) { f.policies["user:test-user"]["enabled"] = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user, f := newRateLimitFixture(t)
			change, err := user.OverrideRateLimit(t.Context(), tc.scope, RateLimit{Enabled: true, MaxReadOps: 1})
			if err != nil {
				t.Fatal(err)
			}
			tc.change(f)
			mutations := len(f.mutations())
			err = change.Restore(t.Context())
			requireNoSecret(t, err)
			if err == nil || len(f.mutations()) != mutations {
				t.Fatalf("Restore overwrote a changed target: %v, %v", err, f.mutations())
			}
			if tc.name != "replaced user keys" && !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("refusal is not an invalid request: %v", err)
			}
			tc.undo(f)
			if err := change.Restore(t.Context()); err != nil {
				t.Fatalf("a refused Restore did not keep the override tracked: %v", err)
			}
			if f.policies["bucket:limited"] != nil && !maps.Equal(f.policies["bucket:limited"], rateLimitNative(false, 0, 0)) ||
				f.policies["user:test-user"] != nil && !maps.Equal(f.policies["user:test-user"], rateLimitNative(false, 0, 0)) {
				t.Fatalf("prior policy not restored: %v", f.policies)
			}
		})
	}
}

// A command whose reply is lost, whose context ends, or that fails natively
// returns the real handle with the original error; Restore with a fresh
// context reconciles either side of that command.
func TestRGWRateLimitOverrideUncertainOutcomesStayRestorable(t *testing.T) {
	lost := errors.New("connection to the engine broke")
	for _, tc := range []struct {
		name    string
		applied bool // the failed command reached native state
		cancel  bool
		code    int
	}{
		{name: "lost reply after enable", applied: true},
		{name: "lost reply before enable"},
		{name: "canceled during enable", applied: true, cancel: true},
		{name: "native failure of enable", code: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user, f := newRateLimitFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			isEnable := func(args []string) bool { return slices.Equal(rateLimitCommand(args), []string{"ratelimit", "enable"}) }
			if tc.applied {
				f.after = func(args []string) error {
					if !isEnable(args) {
						return nil
					}
					if tc.cancel {
						cancel()
					}
					return lost
				}
			} else {
				f.before = func(args []string) (int, error, bool) {
					if !isEnable(args) {
						return 0, nil, false
					}
					if tc.code != 0 {
						return tc.code, nil, true
					}
					return 0, lost, true
				}
			}
			change, err := user.OverrideRateLimit(ctx, RateLimitScope{Bucket: "limited"}, RateLimit{Enabled: true, MaxReadOps: 1})
			requireNoSecret(t, err)
			if err == nil || change == nil || change.state.refused != nil {
				t.Fatalf("uncertain apply lost its handle: %v", err)
			}
			if tc.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation was not preserved: %v", err)
			}
			if tc.code != 0 {
				var failure *ceph.NativeFailure
				if !errors.As(err, &failure) || failure.ExitCode != tc.code {
					t.Fatalf("native failure classification lost: %v", err)
				}
			}
			f.before, f.after = nil, nil
			if err := change.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !maps.Equal(f.policies["bucket:limited"], rateLimitNative(false, 0, 0)) {
				t.Fatalf("restored policy %v", f.policies["bucket:limited"])
			}
		})
	}
}

// A failed restoration keeps the original error and the override; a retry
// A native failure during Restore names the step that failed, the exit
// code and the errno, without native output, and keeps its classification;
// the override stays restorable.
func TestRGWRateLimitRestoreFailureNamesTheFailedStep(t *testing.T) {
	for _, tc := range []struct {
		command []string
		want    string
	}{
		{[]string{"user", "info"}, "confirm the owned RGW user (user info): radosgw-admin exited 5 (EIO); native output is redacted"},
		{[]string{"bucket", "stats"}, "read RGW bucket identity (bucket stats): radosgw-admin exited 5 (EIO); native output is redacted"},
		{[]string{"ratelimit", "get"}, "read RGW rate-limit policy (ratelimit get): radosgw-admin exited 5 (EIO); native output is redacted"},
		{[]string{"ratelimit", "disable"}, "change RGW rate-limit policy (ratelimit disable): radosgw-admin exited 5 (EIO); native output is redacted"},
	} {
		t.Run(strings.Join(tc.command, " "), func(t *testing.T) {
			user, f := newRateLimitFixture(t)
			change, err := user.OverrideRateLimit(t.Context(), RateLimitScope{Bucket: "limited"}, RateLimit{Enabled: true, MaxReadOps: 1})
			if err != nil {
				t.Fatal(err)
			}
			// The failed command prints its errno last, as radosgw-admin
			// does, after text that must not reach the error.
			f.errnoLine = "ERROR: PRIVATE-SECRET request failed: (5) Input/output error"
			f.before = func(args []string) (int, error, bool) {
				return 5, nil, slices.Equal(rateLimitCommand(args), tc.command)
			}
			err = change.Restore(t.Context())
			requireNoSecret(t, err)
			var failure *ceph.NativeFailure
			if err == nil || err.Error() != tc.want || !errors.As(err, &failure) || failure.ExitCode != 5 || failure.Errno != "EIO" || failure.Reason != "" {
				t.Fatalf("Restore error = %v, want %q", err, tc.want)
			}
			f.before, f.errnoLine = nil, ""
			if err := change.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// with a fresh context finishes it.
func TestRGWRateLimitRestoreFailureIsPreservedAndRetryable(t *testing.T) {
	user, f := newRateLimitFixture(t)
	change, err := user.OverrideRateLimit(t.Context(), RateLimitScope{}, RateLimit{Enabled: true, MaxReadOps: 1})
	if err != nil {
		t.Fatal(err)
	}
	f.before = func(args []string) (int, error, bool) {
		return 22, nil, slices.Equal(rateLimitCommand(args), []string{"ratelimit", "set"})
	}
	err = change.Restore(t.Context())
	requireNoSecret(t, err)
	var failure *ceph.NativeFailure
	if !errors.As(err, &failure) || failure.ExitCode != 22 {
		t.Fatalf("restoration failure lost its native classification: %v", err)
	}
	// The policy was disabled before the failed set: that state is the
	// override's own, so a retry continues from it.
	if f.policies["user:test-user"]["enabled"] != false {
		t.Fatalf("policy = %v", f.policies["user:test-user"])
	}
	f.before = nil
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(f.policies["user:test-user"], rateLimitNative(false, 0, 0)) {
		t.Fatalf("restored policy %v", f.policies["user:test-user"])
	}
}

// A readback that differs from the requested policy is reported with the real
// handle; Restore accepts a scope that still holds the prior policy.
func TestRGWRateLimitOverrideReadbackMismatchIsReported(t *testing.T) {
	user, f := newRateLimitFixture(t)
	f.before = func(args []string) (int, error, bool) {
		command := rateLimitCommand(args)
		return 0, nil, len(command) == 2 && command[0] == "ratelimit" && command[1] != "get"
	}
	change, err := user.OverrideRateLimit(t.Context(), RateLimitScope{}, RateLimit{Enabled: true, MaxReadOps: 1})
	if err == nil || change == nil || change.state.refused != nil || !strings.Contains(err.Error(), "readback") {
		t.Fatalf("ignored commands were not reported: %v", err)
	}
	f.before = nil
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRGWRateLimitPolicyDecodingIsStrict(t *testing.T) {
	valid := `{"user_ratelimit":{"max_read_ops":1,"max_write_ops":0,"max_read_bytes":0,"max_write_bytes":0,"enabled":true}}`
	if policy, err := decodeRateLimitPolicy([]byte(valid), "user_ratelimit"); err != nil || policy.limit != (RateLimit{Enabled: true, MaxReadOps: 1}) {
		t.Fatalf("valid policy: %+v, %v", policy, err)
	}
	for _, data := range []string{
		strings.Replace(valid, "user_ratelimit", "bucket_ratelimit", 1),
		strings.Replace(valid, `"enabled":true`, `"enabled":1`, 1),
		strings.Replace(valid, `"max_read_ops":1`, `"max_read_ops":-1`, 1),
		strings.Replace(valid, `"max_read_ops":1`, `"max_read_ops":1.5`, 1),
		strings.Replace(valid, `"max_write_bytes":0,`, ``, 1),
		strings.Replace(valid, `"max_read_ops":1`, `"max_read_ops":1,"max_read_ops":2`, 1),
		strings.Replace(valid, `}}`, `},"other":{}}`, 1),
		valid + `{}`,
	} {
		if _, err := decodeRateLimitPolicy([]byte(data), "user_ratelimit"); err == nil {
			t.Fatalf("accepted %s", data)
		}
	}
}

func TestRGWRateLimitTargetSelectsTheNativeScope(t *testing.T) {
	user := rateLimitTarget{uid: "tenant$test-user"}
	if got := user.selector(); !slices.Equal(got, []string{"--ratelimit-scope", "user", "--uid", "tenant$test-user"}) {
		t.Fatalf("user selector = %v", got)
	}
	bucket := rateLimitTarget{uid: "tenant$test-user", bucket: "limited", tenant: "tenant", bucketID: "zone.1"}
	if got := bucket.selector(); !slices.Equal(got, []string{"--ratelimit-scope", "bucket", "--bucket", "limited", "--tenant", "tenant"}) {
		t.Fatalf("bucket selector = %v", got)
	}
	replaced := bucket
	replaced.bucketID = "zone.2"
	if bucket.key() == replaced.key() || bucket.key() == user.key() {
		t.Fatal("distinct targets share a registry key")
	}
}

// newUnsetRateLimitFixture is newRateLimitFixture on a modelled Ceph 21
// gateway, where the user stores no rate-limit policy.
func newUnsetRateLimitFixture(t *testing.T) (*User, *rateLimitGateway) {
	t.Helper()
	user, f := newRateLimitFixture(t)
	testhelper.TestSetCephVersion(user.base().Origin.Cluster(), "21.2.0")
	f.unsetENOENT = true
	return user, f
}

// countCommand counts the calls of one radosgw-admin operation.
func (f *rateLimitGateway) countCommand(command ...string) int {
	n := 0
	for _, args := range f.calls {
		if slices.Equal(rateLimitCommand(args), command) {
			n++
		}
	}
	return n
}

// A Ceph 21 user without a stored policy fails ratelimit get with ENOENT. The
// override takes it as unset once the user's metadata shows the user without
// the policy attribute and the user is still its creation identity, applies
// the requested policy on the native default and restores the native
// default, which radosgw-admin then reports.
func TestRGWRateLimitOverrideUnsetUserPolicyRestoresTheNativeDefault(t *testing.T) {
	user, f := newUnsetRateLimitFixture(t)
	change, err := user.OverrideRateLimit(t.Context(), RateLimitScope{}, RateLimit{Enabled: true, MaxWriteOps: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !change.state.prior.unset {
		t.Fatalf("prior policy = %+v, want unset", change.state.prior)
	}
	if got := f.policies["user:test-user"]; !maps.Equal(got, rateLimitNative21(true, 0, 1)) {
		t.Fatalf("applied policy %v", got)
	}
	if got := f.mutations(); !slices.Equal(got, []string{"set user test-user", "enable user test-user"}) {
		t.Fatalf("apply commands = %v", got)
	}
	// The metadata proves the absence and user info the identity, once
	// before and once after the ENOENT.
	if f.countCommand("metadata", "get") != 1 || f.countCommand("user", "info") != 2 {
		t.Fatalf("unset policy was not confirmed natively: %v", f.calls)
	}
	copied := *change
	if err := copied.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.policies["user:test-user"]; !maps.Equal(got, rateLimitNative21(false, 0, 0)) {
		t.Fatalf("restored policy %v, want the native default", got)
	}
	if got := f.mutations(); !slices.Equal(got[2:], []string{"disable user test-user", "set user test-user"}) {
		t.Fatalf("restore commands = %v", got)
	}
	calls := len(f.calls)
	if err := change.Restore(t.Context()); err != nil || len(f.calls) != calls {
		t.Fatalf("a restored copy ran native commands again: %v", err)
	}
	// The scope is free again; its prior policy is now the stored default.
	again, err := user.OverrideRateLimit(t.Context(), RateLimitScope{}, RateLimit{Enabled: true, MaxReadOps: 2})
	if err != nil || again.state.prior.unset || !maps.Equal(again.state.prior.fields, rateLimitNative21(false, 0, 0)) {
		t.Fatalf("override after restoring an unset policy: %+v, %v", again.state.prior, err)
	}
	if err := again.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(f.policies) != 1 {
		t.Fatalf("other scopes were touched: %v", f.policies)
	}
}

// A disabled override of an unset user policy materializes the native
// default; Restore finds the scope at that default and sends no command.
func TestRGWRateLimitOverrideUnsetUserPolicyDisabledOverride(t *testing.T) {
	user, f := newUnsetRateLimitFixture(t)
	change, err := user.OverrideRateLimit(t.Context(), RateLimitScope{}, RateLimit{})
	if err != nil {
		t.Fatal(err)
	}
	if got := f.mutations(); !slices.Equal(got, []string{"disable user test-user"}) || !maps.Equal(f.policies["user:test-user"], rateLimitNative21(false, 0, 0)) {
		t.Fatalf("apply commands %v, policy %v", got, f.policies["user:test-user"])
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.mutations(); len(got) != 1 {
		t.Fatalf("Restore at the native default ran %v", got[1:])
	}
}

// An explicitly stored user policy on Ceph 21, with the release's extra
// budgets, is restored exactly, without the unset path.
func TestRGWRateLimitOverrideStoredUserPolicyOnCeph21RestoresExactly(t *testing.T) {
	user, f := newUnsetRateLimitFixture(t)
	prior := rateLimitNative21(false, 4, 0)
	prior["max_list_ops"] = json.Number("6")
	f.policies["user:test-user"] = maps.Clone(prior)
	change, err := user.OverrideRateLimit(t.Context(), RateLimitScope{}, RateLimit{Enabled: true, MaxWriteOps: 1})
	if err != nil {
		t.Fatal(err)
	}
	if change.state.prior.unset || f.countCommand("metadata", "get") != 0 {
		t.Fatal("a stored policy took the unset path")
	}
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(f.policies["user:test-user"], prior) {
		t.Fatalf("restored policy %v, want %v", f.policies["user:test-user"], prior)
	}
}

// Restore of an unset prior policy refuses an outside edit, accepts a scope
// that an outside edit put back to the native default, and accepts a scope
// that is still unset after an apply whose first command was lost before it
// reached native state.
func TestRGWRateLimitRestoreOfAnUnsetUserPolicy(t *testing.T) {
	t.Run("outside edit", func(t *testing.T) {
		user, f := newUnsetRateLimitFixture(t)
		change, err := user.OverrideRateLimit(t.Context(), RateLimitScope{}, RateLimit{Enabled: true, MaxWriteOps: 1})
		if err != nil {
			t.Fatal(err)
		}
		f.policies["user:test-user"]["max_write_ops"] = json.Number("5")
		mutations := len(f.mutations())
		if err := change.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || len(f.mutations()) != mutations {
			t.Fatalf("Restore after an outside edit: %v, %v", err, f.mutations())
		}
		// An outside edit back to the native default is the restored state.
		f.policies["user:test-user"] = rateLimitNative21(false, 0, 0)
		if err := change.Restore(t.Context()); err != nil || len(f.mutations()) != mutations {
			t.Fatalf("Restore at the native default: %v, %v", err, f.mutations())
		}
	})
	t.Run("outside edit of a budget this package does not know", func(t *testing.T) {
		user, f := newUnsetRateLimitFixture(t)
		change, err := user.OverrideRateLimit(t.Context(), RateLimitScope{}, RateLimit{Enabled: true, MaxWriteOps: 1})
		if err != nil {
			t.Fatal(err)
		}
		f.policies["user:test-user"]["max_list_ops"] = json.Number("3")
		mutations := len(f.mutations())
		if err := change.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || len(f.mutations()) != mutations {
			t.Fatalf("Restore after an outside edit: %v, %v", err, f.mutations())
		}
	})
	t.Run("lost first command", func(t *testing.T) {
		user, f := newUnsetRateLimitFixture(t)
		f.before = func(args []string) (int, error, bool) {
			return 0, errors.New("connection to the engine broke"), slices.Equal(rateLimitCommand(args), []string{"ratelimit", "set"})
		}
		change, err := user.OverrideRateLimit(t.Context(), RateLimitScope{}, RateLimit{Enabled: true, MaxWriteOps: 1})
		if err == nil || change == nil || change.state.refused != nil {
			t.Fatalf("uncertain apply lost its handle: %v", err)
		}
		f.before = nil
		if err := change.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		if f.policies["user:test-user"] != nil || len(f.mutations()) != 1 {
			t.Fatalf("Restore of a still unset scope changed it: %v, %v", f.policies, f.mutations())
		}
	})
}

// ENOENT from a user-scope ratelimit get is an unset policy only with native
// proof. A user that vanished or was replaced after the ENOENT, a policy
// attribute that get did not report, other failures of get and an ENOENT of
// the bucket scope stay errors and change nothing.
func TestRGWRateLimitUnsetUserPolicyNeedsNativeProof(t *testing.T) {
	isGet := func(args []string) bool { return slices.Equal(rateLimitCommand(args), []string{"ratelimit", "get"}) }
	enoent := "ERROR: PRIVATE-SECRET failed to get a ratelimit: errno: (2) No such file or directory"
	for _, tc := range []struct {
		name     string
		scope    RateLimitScope
		prepare  func(f *rateLimitGateway)
		metadata int // metadata get calls expected
		want     string
	}{
		{name: "user removed", prepare: func(f *rateLimitGateway) {
			f.errnoLine = "ERROR: can't get key: (2) No such file or directory"
			f.before = func(args []string) (int, error, bool) {
				return 2, nil, slices.Equal(rateLimitCommand(args), []string{"metadata", "get"})
			}
		}, metadata: 1, want: "confirm the unset RGW user rate-limit policy (metadata get): radosgw-admin exited 2 (ENOENT); native output is redacted"},
		{name: "user replaced", prepare: func(f *rateLimitGateway) {
			f.after = func(args []string) error {
				if isGet(args) {
					f.userDocument = rgwAdminFixture("test-user", "PRIVATE-OTHER", false, Quota{MaxSizeBytes: -1, MaxObjects: -1})
				}
				return nil
			}
		}, metadata: 1, want: "confirm the owned RGW user (user info): "},
		{name: "stored policy not reported", prepare: func(f *rateLimitGateway) {
			f.policies["user:test-user"] = rateLimitNative21(true, 1, 0)
			f.errnoLine = enoent
			f.before = func(args []string) (int, error, bool) { return 254, nil, isGet(args) }
		}, metadata: 1, want: "RGW user stores a rate-limit policy that ratelimit get does not report"},
		{name: "permission denied", prepare: func(f *rateLimitGateway) {
			f.errnoLine = "ERROR: PRIVATE-SECRET failed to get a ratelimit: errno: (13) Permission denied"
			f.before = func(args []string) (int, error, bool) { return 243, nil, isGet(args) }
		}, want: "read RGW rate-limit policy (ratelimit get): radosgw-admin exited 243 (EACCES); native output is redacted"},
		{name: "ENOENT with another exit code", prepare: func(f *rateLimitGateway) {
			f.errnoLine = enoent
			f.before = func(args []string) (int, error, bool) { return 2, nil, isGet(args) }
		}, want: "read RGW rate-limit policy (ratelimit get): radosgw-admin exited 2 (ENOENT); native output is redacted"},
		{name: "bucket scope ENOENT", scope: RateLimitScope{Bucket: "limited"}, prepare: func(f *rateLimitGateway) {
			f.errnoLine = enoent
			f.before = func(args []string) (int, error, bool) { return 254, nil, isGet(args) }
		}, want: "read RGW rate-limit policy (ratelimit get): radosgw-admin exited 254 (ENOENT); native output is redacted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user, f := newUnsetRateLimitFixture(t)
			tc.prepare(f)
			change, err := user.OverrideRateLimit(t.Context(), tc.scope, RateLimit{Enabled: true, MaxWriteOps: 1})
			requireNoSecret(t, err)
			if err == nil || !strings.HasPrefix(err.Error(), tc.want) || change.state.refused == nil {
				t.Fatalf("OverrideRateLimit = %v, want %q", err, tc.want)
			}
			if len(f.mutations()) != 0 || f.countCommand("metadata", "get") != tc.metadata {
				t.Fatalf("refused call ran %v", f.calls)
			}
			if err := change.Restore(t.Context()); err != nil {
				t.Fatalf("refused handle's Restore: %v", err)
			}
		})
	}
}

// Restore of an unset prior policy keeps the override tracked when the user
// vanished after the ENOENT, and finishes once the proof holds again.
func TestRGWRateLimitRestoreOfAnUnsetUserPolicyNeedsNativeProof(t *testing.T) {
	user, f := newUnsetRateLimitFixture(t)
	f.before = func(args []string) (int, error, bool) {
		return 0, errors.New("connection to the engine broke"), slices.Equal(rateLimitCommand(args), []string{"ratelimit", "set"})
	}
	change, err := user.OverrideRateLimit(t.Context(), RateLimitScope{}, RateLimit{Enabled: true, MaxWriteOps: 1})
	if err == nil {
		t.Fatal("lost command was not reported")
	}
	f.errnoLine = "ERROR: can't get key: (2) No such file or directory"
	f.before = func(args []string) (int, error, bool) {
		return 2, nil, slices.Equal(rateLimitCommand(args), []string{"metadata", "get"})
	}
	if err := change.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "metadata get") {
		t.Fatalf("Restore without proof: %v", err)
	}
	f.before, f.errnoLine = nil, ""
	if err := change.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// A policy materialized from the native default matches a native object only
// with the same named fields and a zero in every other budget.
func TestRGWRateLimitDefaultPolicyMatching(t *testing.T) {
	pattern := defaultRateLimitPolicy().with(RateLimit{Enabled: true, MaxWriteOps: 1})
	decode := func(fields map[string]any) rateLimitPolicy {
		data, _ := json.Marshal(map[string]any{"user_ratelimit": fields})
		policy, err := decodeRateLimitPolicy(data, "user_ratelimit")
		if err != nil {
			t.Fatal(err)
		}
		return policy
	}
	if !decode(rateLimitNative21(true, 0, 1)).equal(pattern) || !pattern.equal(decode(rateLimitNative(true, 0, 1))) {
		t.Fatal("native default with the change does not match")
	}
	for _, fields := range []map[string]any{
		rateLimitNative21(true, 0, 2),
		rateLimitNative21(false, 0, 1),
		func() map[string]any {
			p := rateLimitNative21(true, 0, 1)
			p["max_list_ops"] = json.Number("1")
			return p
		}(),
		func() map[string]any { p := rateLimitNative21(true, 0, 1); p["mode"] = "strict"; return p }(),
	} {
		if decode(fields).equal(pattern) {
			t.Fatalf("matched %v", fields)
		}
	}
	if (rateLimitPolicy{unset: true}).equal(defaultRateLimitPolicy()) || !(rateLimitPolicy{unset: true}).equal(rateLimitPolicy{unset: true}) {
		t.Fatal("unset policy matching")
	}
}
