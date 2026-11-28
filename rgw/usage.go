package rgw

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
)

// The scopes of a UserUsage snapshot: what its counters aggregate.
const (
	// UsageScopeUser: the counters aggregate the selected ordinary user, who
	// is its own owner.
	UsageScopeUser = "user"
	// UsageScopeAccount: the counters aggregate the account the selected user
	// belongs to, which owns the buckets of all its users and roles.
	UsageScopeAccount = "account"
)

// UserUsage is a native storage-accounting snapshot, not dated request or
// bandwidth usage. Scope is UsageScopeUser for an ordinary user and
// UsageScopeAccount for an account identity. OwnerID identifies that effective aggregation owner; account
// stats cover all of the account's users, roles and buckets, not only UserID.
// Native accounting updates asynchronously; this snapshot does not force sync,
// flush quota caches, prove quota enforcement or verify object content.
type UserUsage struct {
	UserID, Tenant, Scope, OwnerID string
	// SizeBytes is native size; SizeActualBytes is native size_actual (rounded
	// accounting bytes), not physical allocation or replica/billing bytes.
	SizeBytes, SizeActualBytes, NumObjects uint64
	// Preserve native timestamp strings, including native zero timestamps.
	LastStatsSync, LastStatsUpdate string
}

// Usage reads current storage counters for an owned user's effective native
// owner. For an account root it returns the account aggregate with Scope account
// and OwnerID equal to the originally owned account ID. It rechecks the user key,
// type, account lifetime and scope before returning; no user, account, bucket or
// statistics are created, changed, synchronized, reset or adopted.
//
// Poll until the expected values appear. For an explicit synchronisation step
// run gateway.Admin(ctx, "user", "stats", "--uid", user.ID(), "--sync-stats")
// as separate setup.
//
//purpose:observation
func (u *User) Usage(ctx context.Context) (UserUsage, error) {
	user := u.base()
	if !user.Owned() {
		return UserUsage{}, servicehook.Invalid(errors.New("RGW user is not owned by a Ceph cluster"))
	}
	admission, err := gwbase.Admit(ctx, user.Origin)
	if err != nil {
		return UserUsage{}, err
	}
	defer admission.Done()
	ctx, g := admission.Ctx, admission.Gateway
	before, err := g.OwnedUser(ctx, user)
	if err != nil {
		return UserUsage{}, rgwUserUsageReadError(ctx, err)
	}
	if err := ctx.Err(); err != nil {
		return UserUsage{}, err
	}
	// OwnedUser already checked the current runtime scope. Use its captured
	// native IDs for this read without repeating the same service-map query.
	var data []byte
	if user.State.NativeScope != nil {
		data, err = g.Command(ctx, *user.State.NativeScope, "user", "stats", "--uid", user.ID)
	} else {
		data, err = g.AdminCommand(ctx, "user", "stats", "--uid", user.ID)
	}
	if err != nil {
		return UserUsage{}, err
	}
	if err := ctx.Err(); err != nil {
		return UserUsage{}, err
	}
	usage, err := decodeRGWUserUsage(data)
	if err != nil {
		return UserUsage{}, err
	}
	after, err := g.OwnedUser(ctx, user)
	if err != nil {
		return UserUsage{}, rgwUserUsageReadError(ctx, err)
	}
	if before.Info.ID != after.Info.ID || before.Info.Tenant != after.Info.Tenant || before.Info.Type != after.Info.Type || before.Info.AccountID != after.Info.AccountID {
		return UserUsage{}, errors.New("RGW user storage aggregation identity changed")
	}
	if err := g.ValidateAdmin(); err != nil {
		return UserUsage{}, err
	}
	if err := ctx.Err(); err != nil {
		return UserUsage{}, err
	}
	usage.UserID, usage.Tenant = after.Info.ID, after.Info.Tenant
	usage.Scope, usage.OwnerID = UsageScopeUser, after.Info.ID
	if after.Info.AccountID != "" {
		usage.Scope, usage.OwnerID = UsageScopeAccount, after.Info.AccountID
	}
	return usage, nil
}

// Service-map/control reads in the existing identity guard may contain native
// output in an error. Only canonical context causes may leave this check.
func rgwUserUsageReadError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("RGW user storage identity read failed; native output is redacted")
}

func decodeRGWUserUsage(data []byte) (UserUsage, error) {
	bad := errors.New("decode RGW user storage usage")
	// Duplicate members are refused in every object, including unknown future
	// fields. Native data and decoder errors are never copied into the error.
	var root map[string]any
	if strictjson.Decode(data, &root, strictjson.Options{UseNumber: true}) != nil || root == nil {
		return UserUsage{}, bad
	}
	stats, ok := root["stats"].(map[string]any)
	if !ok {
		return UserUsage{}, bad
	}
	var usage UserUsage
	for name, dest := range map[string]*uint64{"size": &usage.SizeBytes, "size_actual": &usage.SizeActualBytes, "num_objects": &usage.NumObjects} {
		number, ok := stats[name].(json.Number)
		if !ok {
			return UserUsage{}, bad
		}
		value, err := strconv.ParseUint(string(number), 10, 64)
		if err != nil {
			return UserUsage{}, bad
		}
		*dest = value
	}
	for name, dest := range map[string]*string{"last_stats_sync": &usage.LastStatsSync, "last_stats_update": &usage.LastStatsUpdate} {
		text, ok := root[name].(string)
		if !ok || !rgwUsageNativeTime(text) {
			return UserUsage{}, bad
		}
		*dest = text
	}
	return usage, nil
}

// v20.2.4 encode_json(utime_t) uses gmtime: raw seconds below ten years,
// otherwise UTC with six fractional digits. Preserve zero and reject arbitrary
// native strings instead of publishing malformed data as a timestamp.
func rgwUsageNativeTime(text string) bool {
	const absoluteFrom = 60 * 60 * 24 * 365 * 10
	if seconds, fraction, ok := strings.Cut(text, "."); ok && len(fraction) == 6 {
		for _, digit := range fraction {
			if digit < '0' || digit > '9' {
				return false
			}
		}
		value, err := strconv.ParseUint(seconds, 10, 32)
		return err == nil && value < absoluteFrom && strconv.FormatUint(value, 10) == seconds
	}
	const layout = "2006-01-02T15:04:05.000000Z"
	value, err := time.Parse(layout, text)
	return err == nil && value.Format(layout) == text && value.Unix() >= absoluteFrom && value.Unix() <= 1<<32-1
}
