package rgw

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/google/uuid"
)

// UserConfig describes a new S3 identity. Empty ID generates a unique ID;
// DisplayName defaults to Testcontainers. AdminCaps is an explicit native
// expression such as "users=read;usage=read" for the RGW Admin Ops REST API.
// Neither the global admin nor the multisite system flag is granted. MaxBuckets
// nil preserves Ceph's default; zero allows unlimited buckets and -1 forbids
// creating buckets. Tenant selects a native namespace; empty preserves the
// legacy namespace. ID is the local user ID, without a tenant prefix.
//
// Tenant is letters, digits and underscores and must not look like an account
// ID; a tenant user's canonical ID is tenant$id, and buckets with the same name
// can exist in different tenants.
//
// Account, when set, makes the new user the root user of that owned account,
// which package rgw/account created through a gateway of the same cluster and
// scope. Empty Tenant inherits the account tenant; a different tenant is
// rejected, and DisplayName must be a valid IAM user name. Account root grants
// permissions within the account; global admin and multisite system flags
// remain false. IAM operations remain on the consuming client.
type UserConfig struct {
	ID, DisplayName, Email string
	Tenant                 string
	AdminCaps              string
	MaxBuckets             *int
	Account                RootAccount
}

// User is an identity created through a gateway. Credentials are returned
// explicitly by Credentials; formatting this handle never includes secrets.
// Users remain in Ceph when a gateway is removed, until explicitly removed or
// the disposable cluster is terminated. Its operations run through the first
// running registered gateway, by name, in the scope of the gateway that created it, so
// they keep working after that gateway is removed if another one serves it.
// Copies of a user handle share its lifecycle state.
type User struct {
	userHandle
}

// ID returns the canonical native ID, tenant$localID for a tenanted user.
// Empty-tenant identities retain their existing unprefixed IDs.
//
//purpose:observation
func (u *User) ID() string {
	if user := u.base(); user != nil {
		return user.ID
	}
	return ""
}

// Credentials returns the generated S3 access and secret keys as explicit
// copies. Previously copied credentials are not erased by user removal.
//
//purpose:handoff
func (u *User) Credentials() (accessKey, secretKey string, err error) {
	user := u.base()
	if user == nil || user.AccessKey == "" || user.SecretKey == "" {
		return "", "", errors.New("RGW S3 credentials are unavailable")
	}
	return user.AccessKey, user.SecretKey, nil
}

// String names the user without its keys.
func (u User) String() string { return "RGW user " + u.ID() }

// GoString formats the user like String, so %#v does not print its keys.
func (u User) GoString() string { return u.String() }

// Format prints String for every verb and flag, so no verb, such as %d or %t,
// walks the fields.
func (u User) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, u.String()) }

// Quota uses bytes, matching modern radosgw-admin output. For each limit,
// -1 disables that limit; zero is an actual zero limit. Enabled controls whether
// the configured limits are enforced. Native RGW quota caches and asynchronous
// accounting can delay enforcement; reading this policy does not flush caches.
type Quota struct {
	Enabled      bool
	MaxSizeBytes int64
	MaxObjects   int64
}

// AdminCapability is one admin capability of a user. Type is the
// capability type, such as users, usage, ratelimit or buckets; Permission is
// read, write, or * when both were granted (* does not mean global admin).
type AdminCapability struct {
	Type       string `json:"type"`
	Permission string `json:"perm"`
}

// UserInfo is a current native policy snapshot, deliberately excluding keys.
// BucketQuota applies individually to every bucket owned by this user.
// DefaultStorageClass preserves native representation: STANDARD on a named
// default placement is persisted and later read back as an empty string.
//
// Type is rgw (or empty) for ordinary users and root for account root users.
// The fixture never sets Admin or System. PlacementTags allow the user to
// create buckets in tagged placement targets and grant no object access;
// package rgw/placement sets the placement default and tags.
type UserInfo struct {
	ID, DisplayName, Email                      string
	Tenant, LocalID, Namespace, Type, AccountID string
	Suspended, Admin, System                    bool
	MaxBuckets                                  int
	AdminCaps                                   []AdminCapability
	UserQuota, BucketQuota                      Quota
	DefaultPlacement, DefaultStorageClass       string
	PlacementTags                               []string
}

// Admin invokes radosgw-admin inside this running gateway, preserving its
// realm, zonegroup, zone, Ceph configuration and keyring. The RGW role image
// supplies the CLI. Native arguments are passed individually, without a shell.
// Success output may contain credentials; failed command output is redacted.
// This is an explicit escape hatch for native operations beyond typed helpers;
// do not race raw user edits with owned-user creation or mutation.
//
// --format json is always added. Arguments must not contain control characters,
// so pass JSON documents (policies, trust documents) as single-line strings.
// Arguments that would change the scope or connection are refused with
// ceph.ErrInvalidRequest: --rgw-realm, --rgw-zonegroup, --rgw-zone and their ID
// and name variants, --conf/-c, --keyring/-k, --name/-n, --id, --mon-host/-m,
// --cluster and similar flags.
//
// The call is bounded by ctx alone, like Container.Ceph: the cluster's startup
// timeout bounds bootstrap, readiness and the typed operations, not a native
// command whose duration the caller chooses, so give ctx the deadline the
// command needs. No radosgw-admin outlives the call: a small sh supervisor in
// the gateway container stops the command with SIGTERM, then SIGKILL after
// 2 s, 15 s before ctx's deadline (at half the remaining time for a short one;
// ceph.ErrNativeFailure, "radosgw-admin still ran after Ns and was stopped"),
// when ctx ends first (Admin then stops
// it under its own budget of about 10 s and returns ctx's error only after the
// command is gone; a call that ends before Docker started the command makes
// sure it never starts), or when the cluster's Terminate starts. When Admin
// cannot confirm the stop, for example because the gateway is paused or the
// engine is unreachable, it returns a *ceph.NativeCommandUnstoppedError whose
// Stop retries; Terminate retries too. Stopping radosgw-admin does not undo a
// request it already sent: the outcome of a stopped mutation is unknown and
// Admin never repeats it.
//
// A failure keeps a *ceph.NativeFailure with the exit code and errno but no
// reason, so errors.Is(err, fs.ErrNotExist) works; run the same command with
// Exec to see the native text.
//
//purpose:native
func (g *Gateway) Admin(ctx context.Context, args ...string) ([]byte, error) {
	if err := validateRGWAdminArgs(args); err != nil {
		return nil, err
	}
	core := g.base()
	if core.Cluster() == nil {
		return nil, servicehook.Invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	servicehook.Lock(core.Cluster())
	defer servicehook.Unlock(core.Cluster())
	if err := core.ValidateAdmin(); err != nil {
		return nil, err
	}
	return core.AdminCommand(ctx, args...)
}

func validateRGWAdminArgs(args []string) error {
	if len(args) == 0 {
		return servicehook.Invalid(errors.New("radosgw-admin operation is required"))
	}
	for _, arg := range args {
		if strings.IndexFunc(arg, unicode.IsControl) != -1 {
			return servicehook.Invalid(errors.New("radosgw-admin arguments must not contain control characters"))
		}
		flag, _, _ := strings.Cut(strings.ReplaceAll(arg, "_", "-"), "=")
		switch flag {
		case "--rgw-realm", "--rgw-realm-id", "--realm-id", "--realm-name",
			"--rgw-zonegroup", "--rgw-zonegroup-id", "--zonegroup-id", "--zonegroup-name",
			"--rgw-zone", "--rgw-zone-id", "--zone-id", "--zone-name",
			"--conf", "--cluster", "--keyring", "--keyfile", "--key-file", "--key", "--id", "--name", "--client-id", "--mon-host", "--monmap", "--no-config-file":
			return servicehook.Invalid(errors.New("radosgw-admin cannot override the gateway scope or Ceph connection"))
		}
		for _, short := range []string{"-c", "-m", "-n", "-k"} {
			if strings.HasPrefix(arg, short) && !strings.HasPrefix(arg, "--") {
				return servicehook.Invalid(errors.New("radosgw-admin cannot override the gateway Ceph connection"))
			}
		}
	}
	return nil
}

var rgwCapTypePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]*$`)

func normalizeRGWUserConfig(config UserConfig) (UserConfig, error) {
	if config.ID == "" {
		config.ID = "tc-" + uuid.NewString()
	}
	if !naming.ClientID.MatchString(config.ID) {
		return config, servicehook.Invalid(errors.New("RGW user ID must use letters, digits, dots, underscores or hyphens"))
	}
	if err := gwbase.ValidateTenant(config.Tenant); err != nil {
		return config, err
	}
	if config.DisplayName == "" {
		config.DisplayName = "Testcontainers"
	}
	for _, value := range []string{config.DisplayName, config.Email, config.AdminCaps} {
		if strings.TrimSpace(value) != value || strings.IndexFunc(value, unicode.IsControl) != -1 {
			return config, servicehook.Invalid(errors.New("RGW user display name, email and capabilities must not contain control characters or surrounding whitespace"))
		}
	}
	if config.MaxBuckets != nil && *config.MaxBuckets < -1 {
		return config, servicehook.Invalid(errors.New("RGW max buckets must be -1, zero or positive"))
	}
	if config.MaxBuckets != nil {
		value := *config.MaxBuckets
		config.MaxBuckets = &value
	}
	if config.AdminCaps != "" {
		for _, expression := range strings.Split(config.AdminCaps, ";") {
			kind, perms, ok := strings.Cut(expression, "=")
			if !ok || !rgwCapTypePattern.MatchString(strings.TrimSpace(kind)) {
				return config, servicehook.Invalid(errors.New("invalid RGW Admin Ops capability expression"))
			}
			for _, perm := range strings.Split(perms, ",") {
				if !slices.Contains([]string{"read", "write", "*"}, strings.TrimSpace(perm)) {
					return config, servicehook.Invalid(errors.New("RGW Admin Ops permissions must be read, write or *"))
				}
			}
		}
	}
	return config, nil
}

// CreateUser creates a fresh S3 identity and, optionally, explicit Admin Ops
// capabilities. Existing IDs are rejected without changing their keys or
// policy. A non-nil handle with an error records an attempted or partial
// creation. If the generated credentials were confirmed, the handle supports
// Info and Remove even after a later capability setup failure. If the
// creation response is malformed, truncated or missing keys, credentials and
// ownership cannot be confirmed: inspect through Admin or terminate the cluster
// rather than adopting native keys or blindly retrying the same ID.
// Native user edits from external clients must not race this operation.
//
// With UserConfig.Account the user is that account's root user. The account
// must be active and created through a gateway of this gateway's cluster and
// scope; its native metadata lifetime is captured, and every later operation
// on the root user checks it again.
//
//purpose:construction,handoff
func (g *Gateway) CreateUser(ctx context.Context, config UserConfig) (*User, error) {
	var account *gwbase.Account
	if config.Account != nil {
		account = gwbase.AccountOf(config.Account)
		if !account.Owned() {
			return nil, errors.New("owned RGW account is required")
		}
		if config.Tenant == "" {
			config.Tenant = account.Tenant
		}
	}
	config, err := normalizeRGWUserConfig(config)
	if err != nil {
		return nil, err
	}
	core := g.base()
	if core.Cluster() == nil {
		return nil, servicehook.Invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	servicehook.Lock(core.Cluster())
	defer servicehook.Unlock(core.Cluster())
	if err := core.ValidateAdmin(); err != nil {
		return nil, err
	}
	ctx, cancel := gwbase.AdminContext(ctx, core.Cluster())
	defer cancel()
	if account != nil && !core.ServesScopeOf(account.Origin) {
		return nil, servicehook.Invalid(errors.New("RGW account was created in another cluster or gateway scope"))
	}
	user, err := core.CreateUser(ctx, gwbase.UserSpec{ID: config.ID, Tenant: config.Tenant, DisplayName: config.DisplayName,
		Email: config.Email, AdminCaps: config.AdminCaps, MaxBuckets: config.MaxBuckets, Account: account})
	return newUser(user), err
}

// withGateway runs fn through the gateway that serves this user's scope,
// holding the owning cluster's mutex, once the user is confirmed to be the
// active identity it created.
func (u *User) withGateway(ctx context.Context, fn func(context.Context, *gwbase.Core, *gwbase.User, *gwbase.NativeUser) error) error {
	user := u.base()
	if !user.Owned() {
		return servicehook.Invalid(errors.New("RGW user is not owned by a Ceph cluster"))
	}
	cluster := user.Origin.Cluster()
	servicehook.Lock(cluster)
	defer servicehook.Unlock(cluster)
	g, err := user.Origin.ScopeGateway(ctx)
	if err != nil {
		return err
	}
	ctx, cancel := gwbase.AdminContext(ctx, cluster)
	defer cancel()
	native, err := g.OwnedUser(ctx, user)
	if err != nil {
		return err
	}
	return fn(ctx, g, user, native)
}

// Info reads an owned user's quota, capabilities, suspension and placement
// defaults/tags. Placement tags control new bucket placement, not object access.
// It verifies the created key, original native user type and original account
// association remain present before returning the policy. Account roots also
// require the creation-owned account's native metadata lifetime to remain valid.
//
//purpose:observation
func (u *User) Info(ctx context.Context) (UserInfo, error) {
	var info UserInfo
	err := u.withGateway(ctx, func(_ context.Context, _ *gwbase.Core, _ *gwbase.User, native *gwbase.NativeUser) error {
		info = userInfo(native.Info)
		return nil
	})
	return info, err
}

// SetQuota replaces both user limits and the enabled state. If a later
// native command fails, a policy change may already have persisted; Info
// reads the authoritative state and the same request can safely be retried.
//
//purpose:construction,condition
func (u *User) SetQuota(ctx context.Context, quota Quota) error {
	return u.setQuota(ctx, "user", quota)
}

// SetBucketQuota configures the per-bucket limits for all buckets owned by this
// user. Use Gateway.Admin for a policy on a single named bucket.
//
// The limits apply to each bucket the user owns, individually. Quota
// enforcement depends on native statistics and caches, so a write right after a
// quota change, or right after the quota is reached, may still be accepted:
// poll with new writes when testing rejection.
//
//purpose:construction,condition
func (u *User) SetBucketQuota(ctx context.Context, quota Quota) error {
	return u.setQuota(ctx, "bucket", quota)
}

func (u *User) setQuota(ctx context.Context, scope string, quota Quota) error {
	if quota.MaxSizeBytes < -1 || quota.MaxObjects < -1 {
		return errors.New("RGW quota limits must be -1, zero or positive")
	}
	return u.withGateway(ctx, func(ctx context.Context, g *gwbase.Core, user *gwbase.User, native *gwbase.NativeUser) error {
		if native.Info.AccountID != "" {
			return errors.New("account-owned storage requires the account's SetQuota or SetBucketQuota")
		}
		if _, err := g.UserCommand(ctx, user, "quota", "set", "--quota-scope", scope, "--uid", user.ID,
			"--max-size", gwbase.QuotaSizeArgument(quota.MaxSizeBytes), "--max-objects", strconv.FormatInt(quota.MaxObjects, 10)); err != nil {
			return err
		}
		action := "disable"
		if quota.Enabled {
			action = "enable"
		}
		if _, err := g.UserCommand(ctx, user, "quota", action, "--quota-scope", scope, "--uid", user.ID); err != nil {
			return err
		}
		native, err := g.OwnedUser(ctx, user)
		if err != nil {
			return err
		}
		actual := native.Info.UserQuota
		if scope == "bucket" {
			actual = native.Info.BucketQuota
		}
		if Quota(actual) != quota {
			return errors.New("RGW quota readback differs from requested policy")
		}
		return nil
	})
}

// Suspend disables or re-enables S3 access for an owned user and reads back
// native state. Gateway metadata caches may delay the observed HTTP response;
// callers should poll new requests when testing suspension or re-enablement.
//
//purpose:condition
func (u *User) Suspend(ctx context.Context, suspended bool) error {
	return u.withGateway(ctx, func(ctx context.Context, g *gwbase.Core, user *gwbase.User, _ *gwbase.NativeUser) error {
		action := "enable"
		if suspended {
			action = "suspend"
		}
		if _, err := g.UserCommand(ctx, user, "user", action, "--uid", user.ID); err != nil {
			return err
		}
		native, err := g.OwnedUser(ctx, user)
		if err != nil {
			return err
		}
		if native.Info.Suspended != suspended {
			return errors.New("RGW suspension readback differs from requested policy")
		}
		return nil
	})
}

// Remove removes an owned identity without purging buckets or objects.
// The handle must contain its confirmed generated credentials; an incomplete
// creation response never permits adopting keys from a later native query.
// Ceph refuses ordinary users that still own buckets; remove those through the
// S3 client first. Account buckets belong to the account and may outlive a root
// user's removal; removing the account independently refuses a nonempty one.
// Success is idempotent across copies of a handle. After an uncertain
// native failure, an absent user is accepted as completed removal; a user still
// present remains tracked and its created key is verified before retrying.
//
//purpose:lifetime
func (u *User) Remove(ctx context.Context) error {
	user := u.base()
	if !user.Owned() {
		return servicehook.Invalid(errors.New("RGW user is not owned by a Ceph cluster"))
	}
	cluster := user.Origin.Cluster()
	servicehook.Lock(cluster)
	defer servicehook.Unlock(cluster)
	if !user.Confirmed() {
		return errors.New("RGW user was not created in this cluster")
	}
	if user.State.Removed {
		return nil
	}
	g, err := user.Origin.ScopeGateway(ctx)
	if err != nil {
		return err
	}
	ctx, cancel := gwbase.AdminContext(ctx, cluster)
	defer cancel()
	exists, err := g.UserExists(ctx, user)
	if err != nil {
		return err
	}
	if !exists {
		user.State.Removed = true
		return nil
	}
	if _, err := g.OwnedUser(ctx, user); err != nil {
		return err
	}
	if _, err := g.UserCommand(ctx, user, "user", "rm", "--uid", user.ID); err != nil {
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		exists, checkErr := g.UserExists(checkCtx, user)
		if checkErr != nil || exists {
			return errors.Join(err, checkErr)
		}
	}
	user.State.Removed = true
	return nil
}
