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
type UserConfig struct {
	ID, DisplayName, Email string
	Tenant                 string
	AdminCaps              string
	MaxBuckets             *int
}

// User is an identity created through a gateway. Credentials are returned
// explicitly by Credentials; formatting this handle never includes secrets.
// Users remain in Ceph when a gateway is removed, until explicitly removed or
// the disposable cluster is terminated. Use another gateway in the same scope
// for an owned user's operations if the original gateway has been removed.
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
func (u *User) ID() string {
	if u == nil {
		return ""
	}
	return u.id
}

// Credentials returns the generated S3 access and secret keys as explicit
// copies. Previously copied credentials are not erased by user removal.
func (u *User) Credentials() (accessKey, secretKey string, err error) {
	if u == nil || u.accessKey == "" || u.secretKey == "" {
		return "", "", errors.New("RGW S3 credentials are unavailable")
	}
	return u.accessKey, u.secretKey, nil
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

type AdminCapability struct {
	Type       string `json:"type"`
	Permission string `json:"perm"`
}

// UserInfo is a current native policy snapshot, deliberately excluding keys.
// BucketQuota applies individually to every bucket owned by this user.
// DefaultStorageClass preserves native representation: STANDARD on a named
// default placement is persisted and later read back as an empty string.
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
func (g *Gateway) Admin(ctx context.Context, args ...string) ([]byte, error) {
	if err := validateRGWAdminArgs(args); err != nil {
		return nil, err
	}
	if g == nil || g.owner == nil {
		return nil, invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	servicehook.Lock(g.owner)
	defer servicehook.Unlock(g.owner)
	if err := g.validateAdminGateway(); err != nil {
		return nil, err
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	return g.adminCommand(ctx, args...)
}

func (g *Gateway) adminContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if servicehook.StartupTimeout(g.owner) > 0 {
		return context.WithTimeout(ctx, servicehook.StartupTimeout(g.owner))
	}
	return ctx, func() {}
}

func (g *Gateway) validateAdminGateway() error {
	if servicehook.Closed(g.owner) {
		return invalid(errors.New("ceph cluster is terminated"))
	}
	service := servicehook.Service(g.owner, rgwServiceName(g.config))
	if g.Container == nil || service == nil || service.GetContainerID() != g.GetContainerID() {
		return invalid(errors.New("RGW gateway is no longer owned by this cluster"))
	}
	return nil
}

func validateRGWAdminArgs(args []string) error {
	if len(args) == 0 {
		return invalid(errors.New("radosgw-admin operation is required"))
	}
	for _, arg := range args {
		if strings.IndexFunc(arg, unicode.IsControl) != -1 {
			return invalid(errors.New("radosgw-admin arguments must not contain control characters"))
		}
		flag, _, _ := strings.Cut(strings.ReplaceAll(arg, "_", "-"), "=")
		switch flag {
		case "--rgw-realm", "--rgw-realm-id", "--realm-id", "--realm-name",
			"--rgw-zonegroup", "--rgw-zonegroup-id", "--zonegroup-id", "--zonegroup-name",
			"--rgw-zone", "--rgw-zone-id", "--zone-id", "--zone-name",
			"--conf", "--cluster", "--keyring", "--keyfile", "--key-file", "--key", "--id", "--name", "--client-id", "--mon-host", "--monmap", "--no-config-file":
			return invalid(errors.New("radosgw-admin cannot override the gateway scope or Ceph connection"))
		}
		for _, short := range []string{"-c", "-m", "-n", "-k"} {
			if strings.HasPrefix(arg, short) && !strings.HasPrefix(arg, "--") {
				return invalid(errors.New("radosgw-admin cannot override the gateway Ceph connection"))
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
		return config, invalid(errors.New("RGW user ID must use letters, digits, dots, underscores or hyphens"))
	}
	if err := validateRGWTenant(config.Tenant); err != nil {
		return config, err
	}
	if config.DisplayName == "" {
		config.DisplayName = "Testcontainers"
	}
	for _, value := range []string{config.DisplayName, config.Email, config.AdminCaps} {
		if strings.TrimSpace(value) != value || strings.IndexFunc(value, unicode.IsControl) != -1 {
			return config, invalid(errors.New("RGW user display name, email and capabilities must not contain control characters or surrounding whitespace"))
		}
	}
	if config.MaxBuckets != nil && *config.MaxBuckets < -1 {
		return config, invalid(errors.New("RGW max buckets must be -1, zero or positive"))
	}
	if config.MaxBuckets != nil {
		value := *config.MaxBuckets
		config.MaxBuckets = &value
	}
	if config.AdminCaps != "" {
		for _, expression := range strings.Split(config.AdminCaps, ";") {
			kind, perms, ok := strings.Cut(expression, "=")
			if !ok || !rgwCapTypePattern.MatchString(strings.TrimSpace(kind)) {
				return config, invalid(errors.New("invalid RGW Admin Ops capability expression"))
			}
			for _, perm := range strings.Split(perms, ",") {
				if !slices.Contains([]string{"read", "write", "*"}, strings.TrimSpace(perm)) {
					return config, invalid(errors.New("RGW Admin Ops permissions must be read, write or *"))
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
// UserInfo and RemoveUser even after a later capability setup failure. If the
// creation response is malformed, truncated or missing keys, credentials and
// ownership cannot be confirmed: inspect through Admin or terminate the cluster
// rather than adopting native keys or blindly retrying the same ID.
// Native user edits from external clients must not race this operation.
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
		return nil, invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	servicehook.Lock(g.owner)
	defer servicehook.Unlock(g.owner)
	if err := g.validateAdminGateway(); err != nil {
		return nil, err
	}
	ctx, cancel := g.adminContext(ctx)
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
		return nil, invalid(fmt.Errorf("RGW user %q already exists", id))
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
	if user == nil || user.owner != g.owner || !sameRGWScope(user.scope, g.config) || user.state == nil || !user.state.created || user.state.removed || user.state.identity == nil || user.accessKey == "" || user.secretKey == "" {
		return nil, errors.New("RGW user must be an active identity created in this cluster and gateway scope")
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
		return nil, invalid(errors.New("RGW user has different credentials; refusing owned-user operation"))
	}
	if err := g.validateUserCreationIdentity(ctx, user, native); err != nil {
		return nil, err
	}
	return native, nil
}

// validateUserCreationIdentity is called with the owning cluster mutex held.
// Use the private account lookup, since AccountInfo would acquire it again.
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

func (g *Gateway) withOwnedUser(ctx context.Context, user *User, fn func(context.Context, *rgwNativeUser) error) error {
	if g == nil || g.owner == nil {
		return invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	servicehook.Lock(g.owner)
	defer servicehook.Unlock(g.owner)
	if err := g.validateAdminGateway(); err != nil {
		return err
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	native, err := g.ownedUser(ctx, user)
	if err != nil {
		return err
	}
	return fn(ctx, native)
}

// UserInfo reads an owned user's quota, capabilities, suspension and placement
// defaults/tags. Placement tags control new bucket placement, not object access.
// It verifies the created key, original native user type and original account
// association remain present before returning the policy. Account roots also
// require the creation-owned account's native metadata lifetime to remain valid.
func (g *Gateway) UserInfo(ctx context.Context, user *User) (UserInfo, error) {
	var info UserInfo
	err := g.withOwnedUser(ctx, user, func(_ context.Context, native *rgwNativeUser) error {
		info = native.info
		return nil
	})
	return info, err
}

// SetUserQuota replaces both user limits and the enabled state. If a later
// native command fails, a policy change may already have persisted; UserInfo
// reads the authoritative state and the same request can safely be retried.
func (g *Gateway) SetUserQuota(ctx context.Context, user *User, quota Quota) error {
	return g.setUserQuota(ctx, user, "user", quota)
}

// SetBucketQuota configures the per-bucket limits for all buckets owned by this
// user. Use Admin for a policy on a single named bucket.
func (g *Gateway) SetBucketQuota(ctx context.Context, user *User, quota Quota) error {
	return g.setUserQuota(ctx, user, "bucket", quota)
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

func (g *Gateway) setUserQuota(ctx context.Context, user *User, scope string, quota Quota) error {
	if quota.MaxSizeBytes < -1 || quota.MaxObjects < -1 {
		return errors.New("RGW quota limits must be -1, zero or positive")
	}
	return g.withOwnedUser(ctx, user, func(ctx context.Context, native *rgwNativeUser) error {
		if native.info.AccountID != "" {
			return errors.New("account-owned storage requires SetAccountQuota or SetAccountBucketQuota")
		}
		if _, err := g.userCommand(ctx, user, "quota", "set", "--quota-scope", scope, "--uid", user.id,
			"--max-size", rgwQuotaSizeArgument(quota.MaxSizeBytes), "--max-objects", strconv.FormatInt(quota.MaxObjects, 10)); err != nil {
			return err
		}
		action := "disable"
		if quota.Enabled {
			action = "enable"
		}
		if _, err := g.userCommand(ctx, user, "quota", action, "--quota-scope", scope, "--uid", user.id); err != nil {
			return err
		}
		native, err := g.ownedUser(ctx, user)
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

// SuspendUser disables or re-enables S3 access for an owned user and reads back
// native state. Gateway metadata caches may delay the observed HTTP response;
// callers should poll new requests when testing suspension or re-enablement.
func (g *Gateway) SuspendUser(ctx context.Context, user *User, suspended bool) error {
	return g.withOwnedUser(ctx, user, func(ctx context.Context, _ *rgwNativeUser) error {
		action := "enable"
		if suspended {
			action = "suspend"
		}
		if _, err := g.userCommand(ctx, user, "user", action, "--uid", user.id); err != nil {
			return err
		}
		native, err := g.ownedUser(ctx, user)
		if err != nil {
			return err
		}
		if native.info.Suspended != suspended {
			return errors.New("RGW suspension readback differs from requested policy")
		}
		return nil
	})
}

// RemoveUser removes an owned identity without purging buckets or objects.
// The handle must contain its confirmed generated credentials; an incomplete
// creation response never permits adopting keys from a later native query.
// Ceph refuses ordinary users that still own buckets; remove those through the
// S3 client first. Account buckets belong to the account and may outlive a root
// user's removal; RemoveAccount independently refuses a nonempty account.
// Success is idempotent across copies of a handle. After an uncertain
// native failure, an absent user is accepted as completed removal; a user still
// present remains tracked and its created key is verified before retrying.
func (g *Gateway) RemoveUser(ctx context.Context, user *User) error {
	if g == nil || g.owner == nil {
		return invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	servicehook.Lock(g.owner)
	defer servicehook.Unlock(g.owner)
	if user == nil || user.owner != g.owner || !sameRGWScope(user.scope, g.config) || user.state == nil || !user.state.created || user.state.identity == nil || user.accessKey == "" || user.secretKey == "" {
		return errors.New("RGW user was not created in this cluster and gateway scope")
	}
	if user.state.removed {
		return nil
	}
	if err := g.validateAdminGateway(); err != nil {
		return err
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	exists, err := g.userExistsForHandle(ctx, user)
	if err != nil {
		return err
	}
	if !exists {
		user.state.removed = true
		return nil
	}
	if _, err := g.ownedUser(ctx, user); err != nil {
		return err
	}
	if _, err := g.userCommand(ctx, user, "user", "rm", "--uid", user.id); err != nil {
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		exists, checkErr := g.userExistsForHandle(checkCtx, user)
		if checkErr != nil || exists {
			return errors.Join(err, checkErr)
		}
	}
	user.state.removed = true
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
