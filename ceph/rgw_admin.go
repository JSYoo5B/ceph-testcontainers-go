package ceph

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

	"github.com/google/uuid"
)

// RGWUserConfig describes a new S3 identity. Empty ID generates a unique ID;
// DisplayName defaults to Testcontainers. AdminCaps is an explicit native
// expression such as "users=read;usage=read" for the RGW Admin Ops REST API.
// Neither the global admin nor the multisite system flag is granted. MaxBuckets
// nil preserves Ceph's default; zero allows unlimited buckets and -1 forbids
// creating buckets. This helper currently creates users without a tenant.
type RGWUserConfig struct {
	ID, DisplayName, Email string
	AdminCaps              string
	MaxBuckets             *int
}

// RGWUser is an identity created through a gateway. Credentials are returned
// explicitly by Credentials; formatting this handle never includes secrets.
// Users remain in Ceph when a gateway is removed, until explicitly removed or
// the disposable cluster is terminated. Use another gateway in the same scope
// for an owned user's operations if the original gateway has been removed.
type RGWUser struct {
	owner     *Container
	scope     RGWConfig
	id        string
	accessKey string
	secretKey string
	state     *rgwUserState
}

// Copies of a user handle share lifecycle state, which is accessed only while
// holding its owning cluster's mutex. Credentials themselves are immutable.
type rgwUserState struct {
	created bool
	removed bool
}

func (u *RGWUser) ID() string {
	if u == nil {
		return ""
	}
	return u.id
}

// Credentials returns the generated S3 access and secret keys as explicit
// copies. Previously copied credentials are not erased by user removal.
func (u *RGWUser) Credentials() (accessKey, secretKey string, err error) {
	if u == nil || u.accessKey == "" || u.secretKey == "" {
		return "", "", errors.New("RGW S3 credentials are unavailable")
	}
	return u.accessKey, u.secretKey, nil
}

func (u RGWUser) String() string   { return "RGW user " + u.id }
func (u RGWUser) GoString() string { return u.String() }

// RGWQuota uses bytes, matching modern radosgw-admin output. For each limit,
// -1 disables that limit; zero is an actual zero limit. Enabled controls whether
// the configured limits are enforced. Native RGW quota caches and asynchronous
// accounting can delay enforcement; reading this policy does not flush caches.
type RGWQuota struct {
	Enabled      bool
	MaxSizeBytes int64
	MaxObjects   int64
}

type RGWAdminCapability struct {
	Type       string `json:"type"`
	Permission string `json:"perm"`
}

// RGWUserInfo is a current native policy snapshot, deliberately excluding keys.
// BucketQuota applies individually to every bucket owned by this user.
type RGWUserInfo struct {
	ID, DisplayName, Email   string
	Suspended, Admin, System bool
	MaxBuckets               int
	AdminCaps                []RGWAdminCapability
	UserQuota, BucketQuota   RGWQuota
}

// Admin invokes radosgw-admin inside this running gateway, preserving its
// realm, zonegroup, zone, Ceph configuration and keyring. The RGW role image
// supplies the CLI. Native arguments are passed individually, without a shell.
// Success output may contain credentials; failed command output is redacted.
// This is an explicit escape hatch for native operations beyond typed helpers;
// do not race raw user edits with owned-user creation or mutation.
func (g *RGWContainer) Admin(ctx context.Context, args ...string) ([]byte, error) {
	if err := validateRGWAdminArgs(args); err != nil {
		return nil, err
	}
	if g == nil || g.owner == nil {
		return nil, errors.New("RGW gateway is not owned by a Ceph cluster")
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	if err := g.validateAdminGateway(); err != nil {
		return nil, err
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	return g.adminCommand(ctx, args...)
}

func (g *RGWContainer) adminContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if g.owner.settings.startupTimeout > 0 {
		return context.WithTimeout(ctx, g.owner.settings.startupTimeout)
	}
	return ctx, func() {}
}

func (g *RGWContainer) validateAdminGateway() error {
	if g.owner.closed {
		return errors.New("ceph cluster is terminated")
	}
	service := g.owner.services[rgwServiceName(g.config)]
	if g.Container == nil || service == nil || service.GetContainerID() != g.GetContainerID() {
		return errors.New("RGW gateway is no longer owned by this cluster")
	}
	return nil
}

func validateRGWAdminArgs(args []string) error {
	if len(args) == 0 {
		return errors.New("radosgw-admin operation is required")
	}
	for _, arg := range args {
		if strings.IndexFunc(arg, unicode.IsControl) != -1 {
			return errors.New("radosgw-admin arguments must not contain control characters")
		}
		flag, _, _ := strings.Cut(strings.ReplaceAll(arg, "_", "-"), "=")
		switch flag {
		case "--rgw-realm", "--rgw-realm-id", "--realm-id", "--realm-name",
			"--rgw-zonegroup", "--rgw-zonegroup-id", "--zonegroup-id", "--zonegroup-name",
			"--rgw-zone", "--rgw-zone-id", "--zone-id", "--zone-name",
			"--conf", "--cluster", "--keyring", "--keyfile", "--key-file", "--key", "--id", "--name", "--client-id", "--mon-host", "--monmap", "--no-config-file":
			return errors.New("radosgw-admin cannot override the gateway scope or Ceph connection")
		}
		for _, short := range []string{"-c", "-m", "-n", "-k"} {
			if strings.HasPrefix(arg, short) && !strings.HasPrefix(arg, "--") {
				return errors.New("radosgw-admin cannot override the gateway Ceph connection")
			}
		}
	}
	return nil
}

// adminCommand is called with the owning cluster's mutex held. Native output
// can include secret keys even in errors, so never wrap the original error.
func (g *RGWContainer) adminCommand(ctx context.Context, args ...string) ([]byte, error) {
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
	data, err := command(ctx, g.Container, append(argv, args...)...)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("radosgw-admin: %w", ctx.Err())
		}
		return nil, errors.New("radosgw-admin command failed; native output is redacted")
	}
	return data, nil
}

var rgwCapTypePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]*$`)

func normalizeRGWUserConfig(config RGWUserConfig) (RGWUserConfig, error) {
	if config.ID == "" {
		config.ID = "tc-" + uuid.NewString()
	}
	if !clientIDPattern.MatchString(config.ID) {
		return config, errors.New("RGW user ID must use letters, digits, dots, underscores or hyphens")
	}
	if config.DisplayName == "" {
		config.DisplayName = "Testcontainers"
	}
	for _, value := range []string{config.DisplayName, config.Email, config.AdminCaps} {
		if strings.TrimSpace(value) != value || strings.IndexFunc(value, unicode.IsControl) != -1 {
			return config, errors.New("RGW user display name, email and capabilities must not contain control characters or surrounding whitespace")
		}
	}
	if config.MaxBuckets != nil && *config.MaxBuckets < -1 {
		return config, errors.New("RGW max buckets must be -1, zero or positive")
	}
	if config.MaxBuckets != nil {
		value := *config.MaxBuckets
		config.MaxBuckets = &value
	}
	if config.AdminCaps != "" {
		for _, expression := range strings.Split(config.AdminCaps, ";") {
			kind, perms, ok := strings.Cut(expression, "=")
			if !ok || !rgwCapTypePattern.MatchString(strings.TrimSpace(kind)) {
				return config, errors.New("invalid RGW Admin Ops capability expression")
			}
			for _, perm := range strings.Split(perms, ",") {
				if !slices.Contains([]string{"read", "write", "*"}, strings.TrimSpace(perm)) {
					return config, errors.New("RGW Admin Ops permissions must be read, write or *")
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
func (g *RGWContainer) CreateUser(ctx context.Context, config RGWUserConfig) (*RGWUser, error) {
	config, err := normalizeRGWUserConfig(config)
	if err != nil {
		return nil, err
	}
	if g == nil || g.owner == nil {
		return nil, errors.New("RGW gateway is not owned by a Ceph cluster")
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	if err := g.validateAdminGateway(); err != nil {
		return nil, err
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	exists, err := g.userExists(ctx, config.ID)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("RGW user %q already exists", config.ID)
	}
	user := &RGWUser{owner: g.owner, scope: g.config, id: config.ID, state: &rgwUserState{}}
	args := []string{"user", "create", "--uid", config.ID, "--display-name", config.DisplayName}
	if config.Email != "" {
		args = append(args, "--email", config.Email)
	}
	if config.MaxBuckets != nil {
		args = append(args, "--max-buckets", strconv.Itoa(*config.MaxBuckets))
	}
	data, err := g.adminCommand(ctx, args...)
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
	user.accessKey, user.secretKey = native.keys[0].AccessKey, native.keys[0].SecretKey
	if config.AdminCaps != "" {
		if _, err := g.adminCommand(ctx, "caps", "add", "--uid", user.id, "--caps", config.AdminCaps); err != nil {
			return user, err
		}
	}
	return user, nil
}

func sameRGWScope(a, b RGWConfig) bool {
	return a.Realm == b.Realm && a.Zonegroup == b.Zonegroup && a.Zone == b.Zone
}

func (g *RGWContainer) userExists(ctx context.Context, id string) (bool, error) {
	data, err := g.adminCommand(ctx, "user", "list")
	if err != nil {
		return false, err
	}
	var users []string
	if err := json.Unmarshal(data, &users); err != nil || users == nil {
		return false, errors.New("decode RGW user listing")
	}
	return slices.Contains(users, id), nil
}

func (g *RGWContainer) ownedUser(ctx context.Context, user *RGWUser) (*rgwNativeUser, error) {
	if user == nil || user.owner != g.owner || !sameRGWScope(user.scope, g.config) || user.state == nil || !user.state.created || user.state.removed || user.accessKey == "" || user.secretKey == "" {
		return nil, errors.New("RGW user must be an active identity created in this cluster and gateway scope")
	}
	data, err := g.adminCommand(ctx, "user", "info", "--uid", user.id)
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
		return nil, errors.New("RGW user has different credentials; refusing owned-user operation")
	}
	return native, nil
}

func (g *RGWContainer) withOwnedUser(ctx context.Context, user *RGWUser, fn func(context.Context, *rgwNativeUser) error) error {
	if g == nil || g.owner == nil {
		return errors.New("RGW gateway is not owned by a Ceph cluster")
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
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

// UserInfo reads an owned user's current quota, capabilities and suspension.
// It verifies the created key remains present before returning the policy.
func (g *RGWContainer) UserInfo(ctx context.Context, user *RGWUser) (RGWUserInfo, error) {
	var info RGWUserInfo
	err := g.withOwnedUser(ctx, user, func(_ context.Context, native *rgwNativeUser) error {
		info = native.info
		return nil
	})
	return info, err
}

// SetUserQuota replaces both user limits and the enabled state. If a later
// native command fails, a policy change may already have persisted; UserInfo
// reads the authoritative state and the same request can safely be retried.
func (g *RGWContainer) SetUserQuota(ctx context.Context, user *RGWUser, quota RGWQuota) error {
	return g.setUserQuota(ctx, user, "user", quota)
}

// SetBucketQuota configures the per-bucket limits for all buckets owned by this
// user. Use Admin for a policy on a single named bucket.
func (g *RGWContainer) SetBucketQuota(ctx context.Context, user *RGWUser, quota RGWQuota) error {
	return g.setUserQuota(ctx, user, "bucket", quota)
}

func (g *RGWContainer) setUserQuota(ctx context.Context, user *RGWUser, scope string, quota RGWQuota) error {
	if quota.MaxSizeBytes < -1 || quota.MaxObjects < -1 {
		return errors.New("RGW quota limits must be -1, zero or positive")
	}
	return g.withOwnedUser(ctx, user, func(ctx context.Context, _ *rgwNativeUser) error {
		size := strconv.FormatInt(quota.MaxSizeBytes, 10)
		if quota.MaxSizeBytes >= 0 {
			size += "B"
		}
		if _, err := g.adminCommand(ctx, "quota", "set", "--quota-scope", scope, "--uid", user.id,
			"--max-size", size, "--max-objects", strconv.FormatInt(quota.MaxObjects, 10)); err != nil {
			return err
		}
		action := "disable"
		if quota.Enabled {
			action = "enable"
		}
		if _, err := g.adminCommand(ctx, "quota", action, "--quota-scope", scope, "--uid", user.id); err != nil {
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
func (g *RGWContainer) SuspendUser(ctx context.Context, user *RGWUser, suspended bool) error {
	return g.withOwnedUser(ctx, user, func(ctx context.Context, _ *rgwNativeUser) error {
		action := "enable"
		if suspended {
			action = "suspend"
		}
		if _, err := g.adminCommand(ctx, "user", action, "--uid", user.id); err != nil {
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
// Ceph refuses users that still own buckets; remove those through the S3 client
// first. Success is idempotent across copies of a handle. After an uncertain
// native failure, an absent user is accepted as completed removal; a user still
// present remains tracked and its created key is verified before retrying.
func (g *RGWContainer) RemoveUser(ctx context.Context, user *RGWUser) error {
	if g == nil || g.owner == nil {
		return errors.New("RGW gateway is not owned by a Ceph cluster")
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	if user == nil || user.owner != g.owner || !sameRGWScope(user.scope, g.config) || user.state == nil || !user.state.created || user.accessKey == "" || user.secretKey == "" {
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
	exists, err := g.userExists(ctx, user.id)
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
	if _, err := g.adminCommand(ctx, "user", "rm", "--uid", user.id); err != nil {
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		exists, checkErr := g.userExists(checkCtx, user.id)
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
	info RGWUserInfo
	keys []rgwNativeKey
}

func decodeRGWUser(data []byte) (*rgwNativeUser, error) {
	var raw struct {
		ID          string               `json:"user_id"`
		DisplayName string               `json:"display_name"`
		Email       string               `json:"email"`
		Suspended   json.RawMessage      `json:"suspended"`
		Admin       json.RawMessage      `json:"admin"`
		System      json.RawMessage      `json:"system"`
		MaxBuckets  int                  `json:"max_buckets"`
		Caps        []RGWAdminCapability `json:"caps"`
		Keys        []rgwNativeKey       `json:"keys"`
		UserQuota   json.RawMessage      `json:"user_quota"`
		BucketQuota json.RawMessage      `json:"bucket_quota"`
	}
	if err := json.Unmarshal(data, &raw); err != nil || raw.ID == "" {
		return nil, errors.New("decode RGW user information")
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
	return &rgwNativeUser{keys: raw.Keys, info: RGWUserInfo{
		ID: raw.ID, DisplayName: raw.DisplayName, Email: raw.Email, Suspended: suspended,
		Admin: admin, System: system, MaxBuckets: raw.MaxBuckets, AdminCaps: raw.Caps,
		UserQuota: userQuota, BucketQuota: bucketQuota,
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

func decodeRGWQuota(data json.RawMessage) (RGWQuota, error) {
	if len(data) == 0 {
		return RGWQuota{}, errors.New("RGW user information is missing quota policy")
	}
	var raw struct {
		Enabled    bool   `json:"enabled"`
		MaxSize    *int64 `json:"max_size"`
		MaxSizeKB  *int64 `json:"max_size_kb"`
		MaxObjects *int64 `json:"max_objects"`
	}
	if json.Unmarshal(data, &raw) != nil || raw.MaxObjects == nil || (raw.MaxSize == nil && raw.MaxSizeKB == nil) {
		return RGWQuota{}, errors.New("decode RGW quota policy")
	}
	size := int64(-1)
	if raw.MaxSize != nil {
		size = *raw.MaxSize
	} else if *raw.MaxSizeKB >= 0 {
		if *raw.MaxSizeKB > (int64(^uint64(0)>>1) / 1024) {
			return RGWQuota{}, errors.New("RGW legacy quota size exceeds byte range")
		}
		size = *raw.MaxSizeKB * 1024
	}
	return RGWQuota{Enabled: raw.Enabled, MaxSizeBytes: size, MaxObjects: *raw.MaxObjects}, nil
}
