package rgw

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/naming"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
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
type UserConfig struct {
	ID, DisplayName, Email string
	Tenant                 string
	AdminCaps              string
	MaxBuckets             *int
}

// User is an identity created through a gateway. Credentials are returned
// explicitly by Credentials; formatting this handle never includes secrets.
// Users remain in Ceph when a gateway is removed, until explicitly removed or
// the disposable cluster is terminated. Its operations run through the first
// registered gateway, by name, in the scope of the gateway that created it, so
// they keep working after that gateway is removed if another one serves it.
type User struct {
	owner     *ceph.Container
	scope     Config
	id        string
	accessKey string
	secretKey string
	state     *rgwUserState
}

// Copies of a user handle share lifecycle state, which is accessed only while
// holding its owning cluster's mutex. Credentials themselves are immutable.
type rgwUserState struct {
	created     bool
	removed     bool
	nativeScope *rgwPlacementScope
	identity    *rgwUserCreationIdentity
}

// The original native user type and account association are immutable after a
// successful creation. An account descriptor is copied, while its lifecycle
// state remains shared with the caller's account handle.
type rgwUserCreationIdentity struct {
	originalType      string
	account           *Account
	accountID         string
	accountVersionTag string
}

// ID returns the canonical native ID, tenant$localID for a tenanted user.
// Empty-tenant identities retain their existing unprefixed IDs.
//
//purpose:observation
func (u *User) ID() string {
	if u == nil {
		return ""
	}
	return u.id
}

// Credentials returns the generated S3 access and secret keys as explicit
// copies. Previously copied credentials are not erased by user removal.
//
//purpose:handoff
func (u *User) Credentials() (accessKey, secretKey string, err error) {
	if u == nil || u.accessKey == "" || u.secretKey == "" {
		return "", "", errors.New("RGW S3 credentials are unavailable")
	}
	return u.accessKey, u.secretKey, nil
}

// servedBy reports whether g belongs to the user's cluster and serves the
// scope of the gateway that created the user, so that it can run the user's
// operations.
func (u *User) servedBy(g *Gateway) bool {
	return u != nil && g != nil && u.owner != nil && u.owner == g.owner && sameRGWScope(u.scope, g.config)
}

// String names the user without its keys.
func (u User) String() string { return "RGW user " + u.id }

// GoString formats the user like String, so %#v does not print its keys.
func (u User) GoString() string { return u.String() }

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
// create buckets in tagged placement targets and grant no object access.
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
// The call is bounded by ctx and by the cluster's startup timeout, and no
// radosgw-admin outlives it: a small sh supervisor in the gateway container
// stops the command with SIGTERM, then SIGKILL after 2 s, 15 s before the bound
// (at half the bound for a short one; ceph.ErrNativeFailure, "radosgw-admin
// still ran after Ns and was stopped"), when ctx ends first (Admin then stops
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
	if g == nil || g.owner == nil {
		return nil, servicehook.Invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	servicehook.Lock(g.owner)
	defer servicehook.Unlock(g.owner)
	if err := g.validateAdminGateway(); err != nil {
		return nil, err
	}
	ctx, cancel := adminContext(ctx, g.owner)
	defer cancel()
	return g.adminCommand(ctx, args...)
}

// adminContext bounds an admin operation by the cluster's startup timeout. A
// handle calls it before it has resolved a gateway, so it takes the owner.
func adminContext(ctx context.Context, owner *ceph.Container) (context.Context, context.CancelFunc) {
	if servicehook.StartupTimeout(owner) > 0 {
		return context.WithTimeout(ctx, servicehook.StartupTimeout(owner))
	}
	return ctx, func() {}
}

// scopeGateway returns the first registered gateway, in name order, that serves
// scope, still admits admin commands and whose container accepts Exec. A handle
// runs its operations through it. A stopped, paused or restarting gateway, or
// one whose startup failed after its container was published, stays
// registered, so it is skipped here instead of being chosen and failing the
// operation; the selection runs before any command, so no mutation is replayed
// on another gateway. The caller holds the owning cluster's mutex.
func scopeGateway(ctx context.Context, owner *ceph.Container, scope Config) (*Gateway, error) {
	if servicehook.Closed(owner) {
		return nil, servicehook.Invalid(errors.New("ceph cluster is terminated"))
	}
	ctx, cancel := adminContext(ctx, owner)
	defer cancel()
	var stopped []string
	var inspectErr error
	for _, g := range gateways(owner).Sorted() {
		if g == nil || !sameRGWScope(g.config, scope) || g.validateAdminGateway() != nil {
			continue
		}
		state, err := g.State(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			inspectErr = errors.Join(inspectErr, fmt.Errorf("inspect RGW gateway %s: %w", g.config.Name, err))
			continue
		}
		// Docker keeps Running true while a container is paused or restarting,
		// but refuses Exec in both states.
		if state != nil && state.Running && !state.Paused && !state.Restarting {
			return g, nil
		}
		stopped = append(stopped, g.config.Name)
	}
	if len(stopped) > 0 || inspectErr != nil {
		return nil, errors.Join(fmt.Errorf("no RGW gateway that accepts commands serves this handle's scope (stopped, paused or restarting: %s)", strings.Join(stopped, ", ")), inspectErr)
	}
	return nil, servicehook.Invalid(errors.New("no registered RGW gateway serves this handle's scope"))
}

func (g *Gateway) validateAdminGateway() error {
	if servicehook.Closed(g.owner) {
		return servicehook.Invalid(errors.New("ceph cluster is terminated"))
	}
	service := servicehook.Service(g.owner, rgwServiceName(g.config))
	// DiagnosticHandle also refuses a typed nil, whose ID lookup would panic.
	if servicehook.DiagnosticHandle(g.Container) == nil || servicehook.DiagnosticHandle(service) == nil || service.GetContainerID() != g.GetContainerID() {
		return servicehook.Invalid(errors.New("RGW gateway is no longer owned by this cluster"))
	}
	return nil
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

// adminCommand is called with the owning cluster's mutex held. Native output
// can include secret keys even in errors, so never wrap the original error.
func (g *Gateway) adminCommand(ctx context.Context, args ...string) ([]byte, error) {
	argv := []string{"radosgw-admin", "--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--format", "json"}
	for _, scope := range []struct{ flag, value string }{
		{"--rgw-realm", g.config.Realm}, {"--rgw-zonegroup", g.config.Zonegroup}, {"--rgw-zone", g.config.Zone},
	} {
		if scope.value != "" {
			argv = append(argv, scope.flag, scope.value)
		}
	}
	if g.config.Realm != "" {
		argv = append(argv, "--osd-pool-default-pg-num", "1", "--osd-pool-default-pgp-num", "0")
	}
	limit := rgwAdminLimit(ctx)
	report, err := servicehook.RunOwnedShell(ctx, g.owner, g.Container, "radosgw-admin", limit, append(argv, args...)...)
	var unstopped *ceph.NativeCommandUnstoppedError
	switch {
	case errors.As(err, &unstopped):
		// Its Stop, or Terminate, confirms the stop later.
		return nil, err
	case err != nil && ctx.Err() != nil:
		return nil, fmt.Errorf("radosgw-admin: the call ended before the command, which was stopped: %w", ctx.Err())
	case err != nil:
		// The command may have run: its outcome is unknown.
		return nil, servicehook.ClassifyExec(ctx, err)
	case report.Stopped:
		return nil, &ceph.OperationError{Kind: ceph.ErrNativeFailure, Op: "radosgw-admin",
			Err: fmt.Errorf("radosgw-admin still ran after %ds and was stopped", limit)}
	case report.ExitCode == 127:
		// The gateway's sh found no radosgw-admin, which never exits 127 itself.
		return nil, &ceph.OperationError{Kind: ceph.ErrImageCapability, Op: "radosgw-admin", Capability: "radosgw-admin",
			Err: errors.New("the gateway image has no radosgw-admin")}
	case report.ExitCode != 0:
		failure := servicehook.NativeFailure("radosgw-admin", report.ExitCode, report.Stdout, report.Stderr)
		return nil, clusterutil.Quiet(errors.New("radosgw-admin command failed; native output is redacted"), failure)
	}
	return report.Stdout, nil
}

// rgwAdminLimit is how long radosgw-admin may run before the gateway's
// supervisor stops it on its own: the call's bound less a margin for stopping
// it and returning, so that a command that runs too long is reported as such
// within the bound. Without a bound, it is 0. A call whose context ends first
// stops the command itself.
func rgwAdminLimit(ctx context.Context) int {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	remaining := time.Until(deadline)
	limit := remaining - 15*time.Second
	if limit < remaining/2 {
		limit = remaining / 2
	}
	return max(1, int(limit/time.Second))
}

var rgwCapTypePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]*$`)

func normalizeRGWUserConfig(config UserConfig) (UserConfig, error) {
	if config.ID == "" {
		config.ID = "tc-" + uuid.NewString()
	}
	if !naming.ClientID.MatchString(config.ID) {
		return config, servicehook.Invalid(errors.New("RGW user ID must use letters, digits, dots, underscores or hyphens"))
	}
	if err := validateRGWTenant(config.Tenant); err != nil {
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
//purpose:construction,handoff
func (g *Gateway) CreateUser(ctx context.Context, config UserConfig) (*User, error) {
	return g.createUser(ctx, config, nil)
}

func (g *Gateway) createUser(ctx context.Context, config UserConfig, account *Account) (*User, error) {
	if account != nil && config.Tenant == "" {
		config.Tenant = account.config.Tenant
	}
	config, err := normalizeRGWUserConfig(config)
	if err != nil {
		return nil, err
	}
	if g == nil || g.owner == nil {
		return nil, servicehook.Invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	servicehook.Lock(g.owner)
	defer servicehook.Unlock(g.owner)
	if err := g.validateAdminGateway(); err != nil {
		return nil, err
	}
	ctx, cancel := adminContext(ctx, g.owner)
	defer cancel()
	var nativeScope *rgwPlacementScope
	var creationAccount *Account
	var accountVersionTag string
	if account != nil {
		nativeAccount, err := g.ownedAccount(ctx, account)
		if err != nil {
			return nil, err
		}
		if config.Tenant != account.config.Tenant || !rgwIAMUserName.MatchString(config.DisplayName) {
			return nil, errors.New("account root requires its account's tenant and an IAM-compatible display name")
		}
		value := account.scope
		nativeScope = &value
		accountCopy := *account
		creationAccount, accountVersionTag = &accountCopy, nativeAccount.tag
	} else if config.Tenant != "" {
		value, err := g.placementRuntimeScope(ctx)
		if err != nil {
			return nil, err
		}
		nativeScope = &value
	}
	id := rgwFullUserID(config.Tenant, "", config.ID)
	user := &User{owner: g.owner, scope: g.config, id: id, state: &rgwUserState{nativeScope: nativeScope}}
	exists, err := g.userExistsForHandle(ctx, user)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, servicehook.Invalid(fmt.Errorf("RGW user %q already exists", id))
	}
	args := []string{"user", "create", "--uid", id, "--display-name", config.DisplayName}
	if account != nil {
		args = append(args, "--account-id", account.id, "--account-root", "--gen-secret", "--gen-access-key")
	}
	if config.Email != "" {
		args = append(args, "--email", config.Email)
	}
	if config.MaxBuckets != nil {
		args = append(args, "--max-buckets", strconv.Itoa(*config.MaxBuckets))
	}
	data, err := g.userCommand(ctx, user, args...)
	if err != nil {
		return user, err
	}
	user.state.created = true
	native, err := decodeRGWUser(data)
	if err != nil {
		return user, err
	}
	if native.info.ID != user.id || len(native.keys) != 1 || native.keys[0].AccessKey == "" || native.keys[0].SecretKey == "" || native.info.Admin || native.info.System {
		return user, errors.New("RGW did not create an ordinary user with one S3 key pair")
	}
	if account != nil && (native.info.AccountID != account.id || native.info.Type != "root") {
		return user, errors.New("RGW did not create the requested account root identity")
	}
	if account == nil && (native.info.AccountID != "" || (native.info.Type != "" && native.info.Type != "rgw")) {
		return user, errors.New("RGW did not create an ordinary non-account identity")
	}
	user.state.identity = &rgwUserCreationIdentity{originalType: native.info.Type, account: creationAccount, accountID: native.info.AccountID, accountVersionTag: accountVersionTag}
	user.accessKey, user.secretKey = native.keys[0].AccessKey, native.keys[0].SecretKey
	if config.AdminCaps != "" {
		if _, err := g.userCommand(ctx, user, "caps", "add", "--uid", user.id, "--caps", config.AdminCaps); err != nil {
			return user, err
		}
	}
	return user, nil
}

func sameRGWScope(a, b Config) bool {
	return a.Realm == b.Realm && a.Zonegroup == b.Zonegroup && a.Zone == b.Zone
}

func (g *Gateway) userExistsForHandle(ctx context.Context, user *User) (bool, error) {
	data, err := g.userCommand(ctx, user, "user", "list")
	if err != nil {
		return false, err
	}
	var users []string
	if json.Unmarshal(data, &users) != nil || users == nil {
		return false, errors.New("decode RGW user listing")
	}
	return slices.Contains(users, user.id), nil
}

func (g *Gateway) userCommand(ctx context.Context, user *User, args ...string) ([]byte, error) {
	if user.state.nativeScope != nil {
		scope, err := g.placementRuntimeScope(ctx)
		if err != nil {
			return nil, err
		}
		if scope != *user.state.nativeScope {
			return nil, errors.New("RGW user's native runtime scope changed")
		}
		return g.placementCommand(ctx, scope, args...)
	}
	return g.adminCommand(ctx, args...)
}

func (g *Gateway) ownedUser(ctx context.Context, user *User) (*rgwNativeUser, error) {
	if user.state == nil || !user.state.created || user.state.removed || user.state.identity == nil || user.accessKey == "" || user.secretKey == "" {
		return nil, errors.New("RGW user must be an active identity created in this cluster")
	}
	data, err := g.userCommand(ctx, user, "user", "info", "--uid", user.id)
	if err != nil {
		return nil, err
	}
	native, err := decodeRGWUser(data)
	if err != nil {
		return nil, err
	}
	if native.info.ID != user.id || !slices.ContainsFunc(native.keys, func(key rgwNativeKey) bool {
		return user.accessKey != "" && key.AccessKey == user.accessKey && key.SecretKey == user.secretKey
	}) {
		return nil, servicehook.Invalid(errors.New("RGW user has different credentials; refusing owned-user operation"))
	}
	if err := g.validateUserCreationIdentity(ctx, user, native); err != nil {
		return nil, err
	}
	return native, nil
}

// validateUserCreationIdentity is called with the owning cluster mutex held.
// Use the private account lookup, since Account.Info would acquire it again.
func (g *Gateway) validateUserCreationIdentity(ctx context.Context, user *User, native *rgwNativeUser) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if user.state.identity == nil {
		return errors.New("RGW user creation identity was not confirmed")
	}
	identity := user.state.identity
	if native.info.Type != identity.originalType || native.info.AccountID != identity.accountID || native.info.Namespace != "" {
		return errors.New("RGW user's original type or account association changed")
	}
	if identity.account == nil {
		if identity.accountID != "" || (identity.originalType != "" && identity.originalType != "rgw") {
			return errors.New("RGW ordinary user creation identity is invalid")
		}
		return nil
	}
	if identity.originalType != "root" || identity.accountID == "" || identity.accountVersionTag == "" || identity.account.id != identity.accountID {
		return errors.New("RGW account root creation identity is invalid")
	}
	account, err := g.ownedAccount(ctx, identity.account)
	if err != nil {
		return err
	}
	if account.info.ID != identity.accountID || account.tag != identity.accountVersionTag || native.info.Tenant != account.info.Tenant {
		return errors.New("RGW account root's original account lifetime or tenant changed")
	}
	return nil
}

// withGateway runs fn through the gateway that serves this user's scope,
// holding the owning cluster's mutex, once the user is confirmed to be the
// active identity it created.
func (u *User) withGateway(ctx context.Context, fn func(context.Context, *Gateway, *rgwNativeUser) error) error {
	if u == nil || u.owner == nil {
		return servicehook.Invalid(errors.New("RGW user is not owned by a Ceph cluster"))
	}
	servicehook.Lock(u.owner)
	defer servicehook.Unlock(u.owner)
	g, err := scopeGateway(ctx, u.owner, u.scope)
	if err != nil {
		return err
	}
	ctx, cancel := adminContext(ctx, u.owner)
	defer cancel()
	native, err := g.ownedUser(ctx, u)
	if err != nil {
		return err
	}
	return fn(ctx, g, native)
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
	err := u.withGateway(ctx, func(_ context.Context, _ *Gateway, native *rgwNativeUser) error {
		info = native.info
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

func rgwQuotaSizeArgument(bytes int64) string {
	if bytes == -1 {
		// Account CLI quota rounds signed sizes before max(-1, rounded_size).
		// -1 byte and -1 KiB truncate to zero; -2 KiB retains the disabled
		// limit. Ordinary user/bucket quota accepts this negative value too.
		return "-2K"
	}
	return strconv.FormatInt(bytes, 10) + "B"
}

func (u *User) setQuota(ctx context.Context, scope string, quota Quota) error {
	if quota.MaxSizeBytes < -1 || quota.MaxObjects < -1 {
		return errors.New("RGW quota limits must be -1, zero or positive")
	}
	return u.withGateway(ctx, func(ctx context.Context, g *Gateway, native *rgwNativeUser) error {
		if native.info.AccountID != "" {
			return errors.New("account-owned storage requires Account.SetQuota or Account.SetBucketQuota")
		}
		if _, err := g.userCommand(ctx, u, "quota", "set", "--quota-scope", scope, "--uid", u.id,
			"--max-size", rgwQuotaSizeArgument(quota.MaxSizeBytes), "--max-objects", strconv.FormatInt(quota.MaxObjects, 10)); err != nil {
			return err
		}
		action := "disable"
		if quota.Enabled {
			action = "enable"
		}
		if _, err := g.userCommand(ctx, u, "quota", action, "--quota-scope", scope, "--uid", u.id); err != nil {
			return err
		}
		native, err := g.ownedUser(ctx, u)
		if err != nil {
			return err
		}
		actual := native.info.UserQuota
		if scope == "bucket" {
			actual = native.info.BucketQuota
		}
		if actual != quota {
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
	return u.withGateway(ctx, func(ctx context.Context, g *Gateway, _ *rgwNativeUser) error {
		action := "enable"
		if suspended {
			action = "suspend"
		}
		if _, err := g.userCommand(ctx, u, "user", action, "--uid", u.id); err != nil {
			return err
		}
		native, err := g.ownedUser(ctx, u)
		if err != nil {
			return err
		}
		if native.info.Suspended != suspended {
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
// user's removal; Account.Remove independently refuses a nonempty account.
// Success is idempotent across copies of a handle. After an uncertain
// native failure, an absent user is accepted as completed removal; a user still
// present remains tracked and its created key is verified before retrying.
//
//purpose:lifetime
func (u *User) Remove(ctx context.Context) error {
	if u == nil || u.owner == nil {
		return servicehook.Invalid(errors.New("RGW user is not owned by a Ceph cluster"))
	}
	servicehook.Lock(u.owner)
	defer servicehook.Unlock(u.owner)
	if u.state == nil || !u.state.created || u.state.identity == nil || u.accessKey == "" || u.secretKey == "" {
		return errors.New("RGW user was not created in this cluster")
	}
	if u.state.removed {
		return nil
	}
	g, err := scopeGateway(ctx, u.owner, u.scope)
	if err != nil {
		return err
	}
	ctx, cancel := adminContext(ctx, u.owner)
	defer cancel()
	exists, err := g.userExistsForHandle(ctx, u)
	if err != nil {
		return err
	}
	if !exists {
		u.state.removed = true
		return nil
	}
	if _, err := g.ownedUser(ctx, u); err != nil {
		return err
	}
	if _, err := g.userCommand(ctx, u, "user", "rm", "--uid", u.id); err != nil {
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		exists, checkErr := g.userExistsForHandle(checkCtx, u)
		if checkErr != nil || exists {
			return errors.Join(err, checkErr)
		}
	}
	u.state.removed = true
	return nil
}

type rgwNativeKey struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

type rgwNativeUser struct {
	info     UserInfo
	keys     []rgwNativeKey
	document map[string]any
}

func decodeRGWUser(data []byte) (*rgwNativeUser, error) {
	var raw struct {
		ID                  string            `json:"user_id"`
		FullID              string            `json:"full_user_id"`
		Tenant              *string           `json:"tenant"`
		Namespace           *string           `json:"namespace"`
		Type                string            `json:"type"`
		AccountID           string            `json:"account_id"`
		DisplayName         string            `json:"display_name"`
		Email               string            `json:"email"`
		Suspended           json.RawMessage   `json:"suspended"`
		Admin               json.RawMessage   `json:"admin"`
		System              json.RawMessage   `json:"system"`
		MaxBuckets          int               `json:"max_buckets"`
		Caps                []AdminCapability `json:"caps"`
		Keys                []rgwNativeKey    `json:"keys"`
		UserQuota           json.RawMessage   `json:"user_quota"`
		BucketQuota         json.RawMessage   `json:"bucket_quota"`
		DefaultPlacement    string            `json:"default_placement"`
		DefaultStorageClass string            `json:"default_storage_class"`
		PlacementTags       []string          `json:"placement_tags"`
	}
	if err := json.Unmarshal(data, &raw); err != nil || raw.ID == "" {
		return nil, errors.New("decode RGW user information")
	}
	// The native CLI's UserInfo::dump emits canonical user_id alone.
	// Admin Ops dump_user_info emits local user_id plus tenant/namespace and
	// full_user_id instead. Preserve both forms and reject contradictory fields.
	tenant, namespace, localID := "", "", raw.ID
	canonicalID := strings.Contains(raw.ID, "$")
	if canonicalID {
		parts := strings.SplitN(raw.ID, "$", 3)
		tenant, localID = parts[0], parts[1]
		if len(parts) == 3 {
			namespace, localID = parts[1], parts[2]
		}
	}
	if raw.Tenant != nil {
		if canonicalID && tenant != *raw.Tenant {
			return nil, errors.New("RGW canonical user ID differs from separate tenant")
		}
		tenant = *raw.Tenant
	}
	if raw.Namespace != nil {
		if canonicalID && namespace != *raw.Namespace {
			return nil, errors.New("RGW canonical user ID differs from separate namespace")
		}
		namespace = *raw.Namespace
	}
	if localID == "" {
		return nil, errors.New("RGW user information has an empty local ID")
	}
	fullID := rgwFullUserID(tenant, namespace, localID)
	if raw.FullID != "" && raw.FullID != fullID {
		return nil, errors.New("RGW full user ID differs from tenant, namespace or local ID")
	}
	suspended, err := rgwFlag(raw.Suspended)
	if err != nil {
		return nil, err
	}
	admin, err := rgwFlag(raw.Admin)
	if err != nil {
		return nil, err
	}
	system, err := rgwFlag(raw.System)
	if err != nil {
		return nil, err
	}
	userQuota, err := decodeRGWQuota(raw.UserQuota)
	if err != nil {
		return nil, err
	}
	bucketQuota, err := decodeRGWQuota(raw.BucketQuota)
	if err != nil {
		return nil, err
	}
	tags := slices.Clone(raw.PlacementTags)
	slices.Sort(tags)
	document, err := decodeRGWPlacementObject(data)
	if err != nil {
		return nil, errors.New("decode RGW user information")
	}
	return &rgwNativeUser{keys: raw.Keys, document: document, info: UserInfo{
		ID: fullID, LocalID: localID, Tenant: tenant, Namespace: namespace, Type: raw.Type, AccountID: raw.AccountID, DisplayName: raw.DisplayName, Email: raw.Email, Suspended: suspended,
		Admin: admin, System: system, MaxBuckets: raw.MaxBuckets, AdminCaps: raw.Caps,
		UserQuota: userQuota, BucketQuota: bucketQuota,
		DefaultPlacement: raw.DefaultPlacement, DefaultStorageClass: raw.DefaultStorageClass, PlacementTags: tags,
	}}, nil
}

func rgwFlag(value json.RawMessage) (bool, error) {
	if len(value) == 0 {
		return false, nil
	}
	var boolean bool
	if json.Unmarshal(value, &boolean) == nil {
		return boolean, nil
	}
	var number int
	if json.Unmarshal(value, &number) == nil && (number == 0 || number == 1) {
		return number == 1, nil
	}
	return false, errors.New("decode RGW user policy flag")
}

func decodeRGWQuota(data json.RawMessage) (Quota, error) {
	if len(data) == 0 {
		return Quota{}, errors.New("RGW user information is missing quota policy")
	}
	var raw struct {
		Enabled    bool   `json:"enabled"`
		MaxSize    *int64 `json:"max_size"`
		MaxSizeKB  *int64 `json:"max_size_kb"`
		MaxObjects *int64 `json:"max_objects"`
	}
	if json.Unmarshal(data, &raw) != nil || raw.MaxObjects == nil || (raw.MaxSize == nil && raw.MaxSizeKB == nil) {
		return Quota{}, errors.New("decode RGW quota policy")
	}
	size := int64(-1)
	if raw.MaxSize != nil {
		size = *raw.MaxSize
	} else if *raw.MaxSizeKB >= 0 {
		if *raw.MaxSizeKB > (int64(^uint64(0)>>1) / 1024) {
			return Quota{}, errors.New("RGW legacy quota size exceeds byte range")
		}
		size = *raw.MaxSizeKB * 1024
	}
	return Quota{Enabled: raw.Enabled, MaxSizeBytes: size, MaxObjects: *raw.MaxObjects}, nil
}
