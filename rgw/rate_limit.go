package rgw

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/strictjson"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
)

// RateLimit is one native RGW rate-limit policy, applied as a whole: the
// enabled flag and four budgets. Each budget counts requests (MaxReadOps,
// MaxWriteOps) or payload bytes (MaxReadBytes, MaxWriteBytes) per minute, and
// zero leaves that dimension unlimited; negative values are refused. GET and
// HEAD count as reads, every other method as a write.
//
// Every gateway process enforces the budgets on its own, with its own
// counters: two gateways that serve the same user or bucket admit up to twice
// the budget between them. A policy is not a cluster-wide request budget.
type RateLimit struct {
	Enabled                                              bool
	MaxReadOps, MaxWriteOps, MaxReadBytes, MaxWriteBytes int64
}

// RateLimitScope selects the native rate-limit scope that
// User.OverrideRateLimit changes. The zero value selects the user itself: the
// policy throttles every request that the user's credentials sign. Bucket
// names one existing bucket that the user natively owns, in the user's
// tenant, and the policy throttles every request to that bucket, whoever
// signs it; the user's other buckets are not affected.
//
// The user scope needs Ceph 20 or later. A Ceph 19 gateway stores and reports
// a user's policy but never applies it to a request that the user's S3 keys
// sign, so OverrideRateLimit refuses it there; the bucket scope works on both.
type RateLimitScope struct {
	Bucket string
}

// RateLimitOverrideHandle manages the restoration of one rate-limit scope of
// an owned user, retaining the prior native policy, the identity of the scope
// and possibly uncertain application. Copies share restoration state.
//
// User.OverrideRateLimit returns a non-nil handle for every outcome. A handle
// of a call that was refused before any native change owns nothing: its
// Restore returns nil and changes nothing. A handle returned with an error
// after the first native change records an uncertain outcome; keep it and call
// Restore with a fresh context, or terminate the disposable cluster.
//
// The handle never owns a bucket: a bucket that the SUT created stays the
// SUT's, and the handle neither deletes it nor keeps it from being deleted.
// There is no expiry and no restoration at scope exit or Terminate.
type RateLimitOverrideHandle struct {
	user  *User
	state *rateLimitState
}

// rateLimitState is the shared restoration state of one override. It is read
// and written with the owning cluster's mutex held.
type rateLimitState struct {
	target rateLimitTarget
	key    string
	prior  rateLimitPolicy
	// owned lists the native policies that this override can have left: the
	// policy after its last confirmed command, or the two sides of a command
	// whose outcome is unknown. Restore refuses any other policy.
	owned    []rateLimitPolicy
	restored bool
	// refused is why the call changed nothing. A handle with it is not
	// tracked.
	refused error
}

// rateLimitTarget is the native identity of one rate-limit scope. A bucket is
// bound by its instance ID and marker, so a bucket deleted and created again
// under the same name is a different target.
type rateLimitTarget struct {
	uid                                    string
	bucket, tenant, bucketID, bucketMarker string
}

// rateLimitBucketName refuses names that radosgw-admin could read as a flag or
// that address another tenant.
var rateLimitBucketName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$`)

// OverrideRateLimit applies limit, as a whole, to one native rate-limit scope
// of this owned user and returns the handle that restores the prior policy.
// It binds the scope's native identity first: the user's created key, type and
// account association, and for a bucket scope the bucket's instance ID, marker,
// tenant and native owner, which must be this user. It reads the prior policy
// of that scope, changes it with radosgw-admin ratelimit set and enable or
// disable, and reads it back. Policy fields that RateLimit does not name are
// kept as they are. A second override of the same scope is refused while the
// first is not restored, even for the same policy.
//
// A user that stores no policy of its own enforces the native default: every
// budget zero and disabled. Ceph 20 reports such a user's policy as that
// default; a Ceph 21 radosgw-admin ratelimit get fails with ENOENT instead,
// exactly as for a missing user. OverrideRateLimit takes the ENOENT as an
// unset prior policy only when the user's native metadata, read in the same
// call, names the user without a stored policy and the user is still the
// identity it created; otherwise it fails. radosgw-admin cannot delete a stored
// policy, so Restore of an unset prior policy stores the native default: the
// scope enforces what it did before, and ratelimit get then reports that
// default instead of failing.
//
// The commands never pass through a policy that enforces a mix of the prior
// and requested budgets: limits are stored before the policy is enabled, and
// the policy is disabled before limits are stored.
//
// Gateways learn the change through their metadata caches, and each gateway
// counts its own budget, so poll with new requests when testing rejection
// (503 SlowDown) or recovery after Restore. With a bucket scope, use a bucket
// that the SUT does not delete and create again while the handle is active.
//
// Outcomes, as the handle it returns and the error:
//   - Applied and read back: real handle; nil.
//   - Invalid limit or bucket name: not applied handle; ceph.ErrInvalidRequest.
//   - User scope on a Ceph 19 cluster: not applied handle; ceph.ErrUnsupported,
//     a *ceph.OperationError that names the release.
//   - Bucket not owned by this user: not applied handle; ceph.ErrInvalidRequest.
//   - The user is no longer the identity it created or is missing (also when
//     a Ceph 21 ratelimit get reports ENOENT), the bucket is missing, the
//     scope is already overridden, the context ended, or a read failed: not
//     applied handle; the original error.
//   - A failure from the first native change on (command, readback, canceled
//     context): real handle (uncertain); the original error.
//
//purpose:condition
func (u *User) OverrideRateLimit(ctx context.Context, scope RateLimitScope, limit RateLimit) (*RateLimitOverrideHandle, error) {
	refuse := func(cause error) (*RateLimitOverrideHandle, error) {
		return &RateLimitOverrideHandle{user: u, state: &rateLimitState{refused: cause}}, cause
	}
	if limit.MaxReadOps < 0 || limit.MaxWriteOps < 0 || limit.MaxReadBytes < 0 || limit.MaxWriteBytes < 0 {
		return refuse(servicehook.Invalid(errors.New("RGW rate-limit budgets must be zero (unlimited) or positive")))
	}
	if scope.Bucket != "" && !rateLimitBucketName.MatchString(scope.Bucket) {
		return refuse(servicehook.Invalid(errors.New("RGW rate-limit bucket must be a plain bucket name of letters, digits, dots, underscores or hyphens")))
	}
	if err := ctx.Err(); err != nil {
		return refuse(err)
	}
	var handle *RateLimitOverrideHandle
	var applyErr error
	err := u.withGateway(ctx, func(ctx context.Context, g *gwbase.Core, user *gwbase.User, native *gwbase.NativeUser) error {
		if scope.Bucket == "" && servicehook.CephBefore(g.Cluster(), 20) {
			return userRateLimitUnsupported(g.Cluster())
		}
		target, err := bindRateLimitTarget(ctx, g, user, native, scope.Bucket)
		if err != nil {
			return err
		}
		overrides := rateLimitOverridesOf(g.Cluster())
		key := target.key()
		if overrides.byTarget[key] != nil {
			return errors.New("restore the existing rate-limit override of this scope first")
		}
		prior, err := readRateLimitPolicy(ctx, g, user, target)
		if err != nil {
			return err
		}
		state := &rateLimitState{target: target, key: key, prior: prior, owned: []rateLimitPolicy{prior}}
		// Tracked before the first native change: an uncertain step still
		// needs Restore.
		overrides.byTarget[key] = state
		handle = &RateLimitOverrideHandle{user: u, state: state}
		applyErr = state.transition(ctx, g, user, prior, prior.with(limit))
		return nil
	})
	if handle == nil {
		if err == nil {
			err = errors.New("RGW rate-limit override was not applied")
		}
		return refuse(err)
	}
	return handle, applyErr
}

// Restore returns the scope to the policy it had before the override, after
// confirming the same user and, for a bucket scope, the same bucket instance
// and owner. For a user that stored no policy it stores the native default, as
// OverrideRateLimit describes. A scope that already holds that policy needs no
// command. It refuses a scope whose identity changed and a policy that is
// neither that one nor one this override left, rather than overwriting an
// outside edit. It is idempotent across copies; after an uncertain failure,
// retry with a fresh context. A handle of a refused call returns nil.
//
// Gateways see the restored policy after their metadata caches update; poll
// with new requests before relying on unthrottled access.
//
//purpose:condition,lifetime
func (change *RateLimitOverrideHandle) Restore(ctx context.Context) error {
	if change == nil || change.state == nil {
		return errors.New("RGW rate-limit override is unavailable")
	}
	if change.state.refused != nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	user := change.user.base()
	if !user.Owned() {
		return servicehook.Invalid(errors.New("RGW rate-limit override is unavailable"))
	}
	cluster := user.Origin.Cluster()
	if err := servicehook.LockTopology(ctx, cluster); err != nil {
		return err
	}
	restored := change.state.restored
	servicehook.Unlock(cluster)
	if restored {
		return nil
	}
	return change.user.withGateway(ctx, func(ctx context.Context, g *gwbase.Core, user *gwbase.User, native *gwbase.NativeUser) error {
		state := change.state
		if state.restored {
			return nil
		}
		overrides := rateLimitOverridesOf(g.Cluster())
		if overrides.byTarget[state.key] != state {
			return errors.New("RGW rate-limit override is not tracked by this cluster")
		}
		if err := checkRateLimitTarget(ctx, g, user, native, state.target); err != nil {
			return err
		}
		current, err := readRateLimitPolicy(ctx, g, user, state.target)
		if err != nil {
			return err
		}
		// A scope that holds the prior policy, or for an unset prior the
		// native default, needs no command.
		target := state.prior.restoration()
		if !current.equal(state.prior) && !current.equal(target) {
			if !state.owns(current) {
				return servicehook.Invalid(errors.New("RGW rate-limit policy changed outside this override; refusing restoration"))
			}
			if err := state.transition(ctx, g, user, current, target); err != nil {
				return err
			}
		}
		state.restored = true
		delete(overrides.byTarget, state.key)
		return nil
	})
}

// userRateLimitUnsupported refuses a user-scope override on cluster, which
// runs Ceph 19. That is a documented limit of the release that no tool
// reveals: its gateway authenticates a request signed with an S3 key into a
// request user that carries the user's information but not the user's stored
// attributes, where the policy lives, so the limiter sees no user policy
// while the bucket's policy, read with the bucket, is applied.
func userRateLimitUnsupported(cluster *ceph.Container) error {
	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		return err
	}
	return &ceph.OperationError{Kind: ceph.ErrUnsupported, Target: "cluster", Version: facts.CephVersion,
		Capability: "RGW user-scope rate limits",
		Err:        errors.New("RGW user-scope rate limits need Ceph 20 or later; a Ceph 19 gateway does not apply a user's policy to requests its S3 keys sign")}
}

// owns reports whether policy is one that this override can have left.
func (s *rateLimitState) owns(policy rateLimitPolicy) bool {
	for _, owned := range s.owned {
		if policy.equal(owned) {
			return true
		}
	}
	return false
}

// transition moves the scope from the native policy from to to, recording
// after every command which policies the override may have left, then reads
// the scope back. The caller holds the cluster mutex.
func (s *rateLimitState) transition(ctx context.Context, g *gwbase.Core, user *gwbase.User, from, to rateLimitPolicy) error {
	limits := rateLimitStep{args: []string{"ratelimit", "set"}, apply: func(p rateLimitPolicy) rateLimitPolicy { return p.withBudgets(to.limit) }}
	for _, budget := range []struct {
		flag  string
		value int64
	}{{"--max-read-ops", to.limit.MaxReadOps}, {"--max-write-ops", to.limit.MaxWriteOps}, {"--max-read-bytes", to.limit.MaxReadBytes}, {"--max-write-bytes", to.limit.MaxWriteBytes}} {
		limits.args = append(limits.args, budget.flag, strconv.FormatInt(budget.value, 10))
	}
	action := "disable"
	if to.limit.Enabled {
		action = "enable"
	}
	toggle := rateLimitStep{args: []string{"ratelimit", action}, apply: func(p rateLimitPolicy) rateLimitPolicy { return p.withEnabled(to.limit.Enabled) }}
	steps := []rateLimitStep{toggle, limits}
	if to.limit.Enabled {
		steps = []rateLimitStep{limits, toggle}
	}
	current := from
	for _, step := range steps {
		next := step.apply(current)
		if next.equal(current) {
			continue
		}
		if _, err := g.UserCommand(ctx, user, slices.Concat(step.args, s.target.selector())...); err != nil {
			s.owned = []rateLimitPolicy{current, next}
			return fmt.Errorf("change RGW rate-limit policy (%s): %w", strings.Join(step.args[:2], " "), err)
		}
		current = next
		s.owned = []rateLimitPolicy{current}
	}
	if !current.equal(to) {
		return errors.New("RGW rate-limit transition did not reach the requested policy")
	}
	actual, err := readRateLimitPolicy(ctx, g, user, s.target)
	if err != nil {
		return err
	}
	if !actual.equal(to) {
		return errors.New("RGW rate-limit readback differs from the requested policy; inspect native state before restoration")
	}
	s.owned = []rateLimitPolicy{actual}
	if s.target.bucket != "" {
		if _, err := bindRateLimitBucket(ctx, g, user, s.target.tenant, s.target.bucket, s.target); err != nil {
			return err
		}
	}
	return nil
}

type rateLimitStep struct {
	args  []string
	apply func(rateLimitPolicy) rateLimitPolicy
}

// key identifies the target in the cluster's override registry.
func (t rateLimitTarget) key() string {
	if t.bucket == "" {
		return "user\x00" + t.uid
	}
	return "bucket\x00" + t.tenant + "\x00" + t.bucket + "\x00" + t.bucketID
}

// selector is the radosgw-admin scope selection of the target.
func (t rateLimitTarget) selector() []string {
	if t.bucket == "" {
		return []string{"--ratelimit-scope", "user", "--uid", t.uid}
	}
	args := []string{"--ratelimit-scope", "bucket", "--bucket", t.bucket}
	if t.tenant != "" {
		args = append(args, "--tenant", t.tenant)
	}
	return args
}

// bindRateLimitTarget captures the native identity of the selected scope of
// an owned user whose identity the caller has just confirmed.
func bindRateLimitTarget(ctx context.Context, g *gwbase.Core, user *gwbase.User, native *gwbase.NativeUser, bucket string) (rateLimitTarget, error) {
	if bucket == "" {
		return rateLimitTarget{uid: user.ID}, nil
	}
	return bindRateLimitBucket(ctx, g, user, native.Info.Tenant, bucket)
}

// checkRateLimitTarget refuses a scope whose native identity changed since the
// override bound it.
func checkRateLimitTarget(ctx context.Context, g *gwbase.Core, user *gwbase.User, native *gwbase.NativeUser, target rateLimitTarget) error {
	if target.uid != user.ID {
		return servicehook.Invalid(errors.New("RGW rate-limit override belongs to another user"))
	}
	if target.bucket == "" {
		return nil
	}
	if native.Info.Tenant != target.tenant {
		return servicehook.Invalid(errors.New("RGW user's tenant changed; refusing rate-limit restoration"))
	}
	_, err := bindRateLimitBucket(ctx, g, user, target.tenant, target.bucket, target)
	return err
}

// bindRateLimitBucket reads the native identity of bucket and requires that
// user owns it in tenant. With an expected identity, a different bucket
// instance is refused as a replaced target.
func bindRateLimitBucket(ctx context.Context, g *gwbase.Core, user *gwbase.User, tenant, bucket string, expected ...rateLimitTarget) (rateLimitTarget, error) {
	args := []string{"bucket", "stats", "--bucket", bucket}
	if tenant != "" {
		args = append(args, "--tenant", tenant)
	}
	data, err := g.UserCommand(ctx, user, args...)
	if err != nil {
		return rateLimitTarget{}, fmt.Errorf("read RGW bucket identity (bucket stats): %w", err)
	}
	var stats struct {
		Bucket *string `json:"bucket"`
		Tenant *string `json:"tenant"`
		ID     *string `json:"id"`
		Marker *string `json:"marker"`
		Owner  *string `json:"owner"`
	}
	if strictjson.Decode(data, &stats, strictjson.Options{RejectNull: true}) != nil || stats.Bucket == nil || stats.ID == nil || stats.Marker == nil || stats.Owner == nil ||
		*stats.ID == "" || *stats.Marker == "" {
		return rateLimitTarget{}, errors.New("decode RGW bucket identity")
	}
	actualTenant := ""
	if stats.Tenant != nil {
		actualTenant = *stats.Tenant
	}
	if *stats.Bucket != bucket || actualTenant != tenant {
		return rateLimitTarget{}, errors.New("RGW bucket statistics name another bucket")
	}
	target := rateLimitTarget{uid: user.ID, bucket: bucket, tenant: tenant, bucketID: *stats.ID, bucketMarker: *stats.Marker}
	if len(expected) > 0 && (target.bucketID != expected[0].bucketID || target.bucketMarker != expected[0].bucketMarker) {
		return rateLimitTarget{}, servicehook.Invalid(errors.New("RGW bucket was replaced since the rate-limit override bound it; refusing mutation"))
	}
	if *stats.Owner != user.ID {
		return rateLimitTarget{}, servicehook.Invalid(errors.New("RGW bucket is not owned by this user; refusing rate-limit override"))
	}
	return target, nil
}

// rateLimitPolicy is one native policy object: the typed budgets and the
// whole native object, so that a field this package does not set is compared
// and kept as well.
//
// Two other forms stand for a user scope that stores no policy. unset is the
// scope itself, as a Ceph 21 radosgw-admin reports it; it has no fields.
// fromDefault is a policy that radosgw-admin materializes from the native
// default, every budget zero and disabled: fields holds the budgets and the
// flag that this package knows, and the native object it stands for has a
// zero budget in every other field, such as a budget of a later release.
type rateLimitPolicy struct {
	limit       RateLimit
	fields      map[string]any
	unset       bool
	fromDefault bool
}

func (p rateLimitPolicy) equal(other rateLimitPolicy) bool {
	switch {
	case p.unset || other.unset:
		return p.unset == other.unset
	case p.limit != other.limit:
		return false
	case p.fromDefault == other.fromDefault:
		return reflect.DeepEqual(p.fields, other.fields)
	case p.fromDefault:
		return other.materializes(p)
	default:
		return p.materializes(other)
	}
}

// materializes reports whether the native object p is the fromDefault policy
// d: the same value in every field d names and a zero budget in every other.
func (p rateLimitPolicy) materializes(d rateLimitPolicy) bool {
	for name, value := range d.fields {
		if actual, ok := p.fields[name]; !ok || !reflect.DeepEqual(actual, value) {
			return false
		}
	}
	for name, value := range p.fields {
		if _, named := d.fields[name]; !named && value != json.Number("0") {
			return false
		}
	}
	return true
}

// defaultRateLimitPolicy is the native default policy that a user scope
// without a stored policy enforces, and that radosgw-admin ratelimit set,
// enable and disable store there before they apply their change.
func defaultRateLimitPolicy() rateLimitPolicy {
	return rateLimitPolicy{fromDefault: true, fields: map[string]any{"enabled": false, "max_read_ops": json.Number("0"),
		"max_write_ops": json.Number("0"), "max_read_bytes": json.Number("0"), "max_write_bytes": json.Number("0")}}
}

// restoration is the policy that restores p: p itself, or the native default
// for an unset scope, which radosgw-admin cannot return to unset.
func (p rateLimitPolicy) restoration() rateLimitPolicy {
	if p.unset {
		return defaultRateLimitPolicy()
	}
	return p
}

// with returns p with every field of limit applied.
func (p rateLimitPolicy) with(limit RateLimit) rateLimitPolicy {
	return p.withBudgets(limit).withEnabled(limit.Enabled)
}

func (p rateLimitPolicy) withBudgets(limit RateLimit) rateLimitPolicy {
	next := p.clone()
	next.limit.MaxReadOps, next.limit.MaxWriteOps = limit.MaxReadOps, limit.MaxWriteOps
	next.limit.MaxReadBytes, next.limit.MaxWriteBytes = limit.MaxReadBytes, limit.MaxWriteBytes
	for name, value := range map[string]int64{"max_read_ops": limit.MaxReadOps, "max_write_ops": limit.MaxWriteOps,
		"max_read_bytes": limit.MaxReadBytes, "max_write_bytes": limit.MaxWriteBytes} {
		next.fields[name] = json.Number(strconv.FormatInt(value, 10))
	}
	return next
}

func (p rateLimitPolicy) withEnabled(enabled bool) rateLimitPolicy {
	next := p.clone()
	next.limit.Enabled = enabled
	next.fields["enabled"] = enabled
	return next
}

// clone copies p; a change to an unset scope applies to the native default
// that radosgw-admin materializes.
func (p rateLimitPolicy) clone() rateLimitPolicy {
	if p.unset {
		return defaultRateLimitPolicy()
	}
	fields := make(map[string]any, len(p.fields))
	for name, value := range p.fields {
		fields[name] = value
	}
	return rateLimitPolicy{limit: p.limit, fields: fields, fromDefault: p.fromDefault}
}

// readRateLimitPolicy reads the native policy of target with radosgw-admin
// ratelimit get. The caller has confirmed user in this admission.
func readRateLimitPolicy(ctx context.Context, g *gwbase.Core, user *gwbase.User, target rateLimitTarget) (rateLimitPolicy, error) {
	data, err := g.UserCommand(ctx, user, slices.Concat([]string{"ratelimit", "get"}, target.selector())...)
	if err != nil {
		if target.bucket == "" && unsetUserRateLimit(err) {
			if err := confirmUnsetUserRateLimit(ctx, g, user); err != nil {
				return rateLimitPolicy{}, err
			}
			return rateLimitPolicy{unset: true}, nil
		}
		return rateLimitPolicy{}, fmt.Errorf("read RGW rate-limit policy (ratelimit get): %w", err)
	}
	section := "user_ratelimit"
	if target.bucket != "" {
		section = "bucket_ratelimit"
	}
	return decodeRateLimitPolicy(data, section)
}

// unsetUserRateLimit reports whether err is the failure of a user-scope
// radosgw-admin ratelimit get of Ceph 21 for a user that stores no policy: exit
// 254 with ENOENT. Ceph 21 fails the same way for a missing user, and Ceph 20
// and 19 report the native default instead, so it is never evidence alone.
func unsetUserRateLimit(err error) bool {
	var failure *ceph.NativeFailure
	return errors.As(err, &failure) && failure.ExitCode == 254 && failure.Errno == "ENOENT"
}

// userRateLimitAttr is the user attribute that stores a user's rate-limit
// policy.
const userRateLimitAttr = "user.rgw.ratelimit"

// confirmUnsetUserRateLimit proves that user, which a ratelimit get reported
// as ENOENT, exists and stores no policy: its native metadata names it and
// lists its attributes without the policy attribute, and it is still the
// identity it created. A missing or replaced user, and a policy attribute that
// ratelimit get did not report, are errors.
func confirmUnsetUserRateLimit(ctx context.Context, g *gwbase.Core, user *gwbase.User) error {
	data, err := g.UserCommand(ctx, user, "metadata", "get", "user:"+user.ID)
	if err != nil {
		return fmt.Errorf("confirm the unset RGW user rate-limit policy (metadata get): %w", err)
	}
	var metadata struct {
		Key  *string `json:"key"`
		Data *struct {
			UserID *string `json:"user_id"`
			Attrs  *[]struct {
				Key *string `json:"key"`
			} `json:"attrs"`
		} `json:"data"`
	}
	if strictjson.Decode(data, &metadata, strictjson.Options{RejectNull: true}) != nil || metadata.Key == nil || metadata.Data == nil ||
		metadata.Data.UserID == nil || metadata.Data.Attrs == nil {
		return errors.New("decode RGW user metadata")
	}
	if *metadata.Key != "user:"+user.ID || *metadata.Data.UserID != user.ID {
		return errors.New("RGW user metadata names another user")
	}
	for _, attr := range *metadata.Data.Attrs {
		if attr.Key == nil {
			return errors.New("decode RGW user metadata")
		}
		if *attr.Key == userRateLimitAttr {
			return errors.New("RGW user stores a rate-limit policy that ratelimit get does not report")
		}
	}
	if _, err := g.OwnedUser(ctx, user); err != nil {
		return fmt.Errorf("confirm the owned RGW user (user info): %w", err)
	}
	return nil
}

// decodeRateLimitPolicy decodes the one policy object named section. Native
// output never appears in its error.
func decodeRateLimitPolicy(data []byte, section string) (rateLimitPolicy, error) {
	bad := errors.New("decode RGW rate-limit policy")
	var root map[string]any
	if strictjson.Decode(data, &root, strictjson.Options{UseNumber: true}) != nil || len(root) != 1 {
		return rateLimitPolicy{}, bad
	}
	fields, ok := root[section].(map[string]any)
	if !ok {
		return rateLimitPolicy{}, bad
	}
	policy := rateLimitPolicy{fields: fields}
	enabled, ok := fields["enabled"].(bool)
	if !ok {
		return rateLimitPolicy{}, bad
	}
	policy.limit.Enabled = enabled
	for name, dest := range map[string]*int64{"max_read_ops": &policy.limit.MaxReadOps, "max_write_ops": &policy.limit.MaxWriteOps,
		"max_read_bytes": &policy.limit.MaxReadBytes, "max_write_bytes": &policy.limit.MaxWriteBytes} {
		number, ok := fields[name].(json.Number)
		if !ok {
			return rateLimitPolicy{}, bad
		}
		value, err := strconv.ParseInt(string(number), 10, 64)
		// radosgw-admin ratelimit set ignores a negative budget, so a negative
		// stored value could not be restored exactly.
		if err != nil || value < 0 || strconv.FormatInt(value, 10) != string(number) {
			return rateLimitPolicy{}, bad
		}
		*dest = value
	}
	return policy, nil
}

// rateLimitOverrideKind is the cluster extension that holds the rate-limit
// overrides of the cluster that are not restored yet.
const rateLimitOverrideKind = "rgw.ratelimits"

// rateLimitOverrides is the registry of one cluster's active rate-limit
// overrides, keyed by target, so a second override never captures the first
// one's policy as its prior state. The cluster mutex guards it.
type rateLimitOverrides struct {
	byTarget map[string]*rateLimitState
}

func rateLimitOverridesOf(c *ceph.Container) *rateLimitOverrides {
	return servicehook.Extension(c, rateLimitOverrideKind, func() any {
		return &rateLimitOverrides{byTarget: make(map[string]*rateLimitState)}
	}).(*rateLimitOverrides)
}
