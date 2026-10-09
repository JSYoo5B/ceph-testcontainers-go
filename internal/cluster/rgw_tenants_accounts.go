package cluster

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

var rgwTenantName = regexp.MustCompile(`^[A-Za-z0-9_]{1,128}$`)
var rgwAccountID = regexp.MustCompile(`^RGW[0-9]{17}$`)
var rgwIAMUserName = regexp.MustCompile(`^[A-Za-z0-9_+=,.@-]{1,64}$`)

func validateRGWTenant(tenant string) error {
	if tenant != "" && (!rgwTenantName.MatchString(tenant) || rgwAccountID.MatchString(tenant)) {
		return errors.New("RGW tenant must use letters, digits or underscores and must not be formatted as an account ID")
	}
	return nil
}

func rgwFullUserID(tenant, namespace, id string) string {
	if namespace != "" {
		return tenant + "$" + namespace + "$" + id
	}
	if tenant != "" {
		return tenant + "$" + id
	}
	return id
}

// RGWAccountConfig creates a new native account. Empty ID generates a unique
// RGW-prefixed 17-digit ID; Name defaults to a unique fixture name. Tenant is
// optional and isolates bucket names. Account IDs and email are globally unique.
// MaxBuckets nil preserves the native default. No user migration is performed.
type RGWAccountConfig struct {
	ID, Name, Tenant, Email string
	MaxBuckets              *int
}

// RGWAccount is a fresh account owned by this fixture. Its native metadata
// version tag distinguishes its lifetime from a deleted/recreated account.
// Copies share lifecycle state. Removing a gateway does not remove the account.
type RGWAccount struct {
	owner  *Container
	scope  rgwPlacementScope
	config RGWAccountConfig
	id     string
	state  *rgwAccountState
}

type rgwAccountState struct {
	confirmed, removed bool
	versionTag         string
}

func (a *RGWAccount) ID() string {
	if a == nil {
		return ""
	}
	return a.id
}

func (a RGWAccount) String() string   { return "RGW account " + a.id }
func (a RGWAccount) GoString() string { return a.String() }

// RGWAccountInfo contains account-wide limits and the individual bucket policy.
// AccountQuota applies across all account users, buckets and roles, rather than
// to one user's storage. Quota readback does not flush native accounting caches.
type RGWAccountInfo struct {
	ID, Name, Tenant, Email                            string
	MaxUsers, MaxRoles, MaxGroups, MaxBuckets, MaxKeys int
	AccountQuota, BucketQuota                          RGWQuota
}

type rgwNativeAccount struct {
	info     RGWAccountInfo
	document map[string]any
	tag      string
}

func normalizeRGWAccountConfig(config RGWAccountConfig) (RGWAccountConfig, error) {
	if config.ID == "" {
		number, err := rand.Int(rand.Reader, big.NewInt(100000000000000000))
		if err != nil {
			return config, errors.New("generate RGW account ID")
		}
		config.ID = fmt.Sprintf("RGW%017d", number)
	}
	if !rgwAccountID.MatchString(config.ID) {
		return config, errors.New("RGW account ID must be RGW followed by 17 digits")
	}
	if config.Name == "" {
		config.Name = "tc-" + uuid.NewString()
	}
	if err := validateRGWTenant(config.Tenant); err != nil {
		return config, err
	}
	for _, value := range []string{config.Name, config.Email} {
		if !utf8.ValidString(value) || strings.TrimSpace(value) != value || strings.IndexFunc(value, unicode.IsControl) != -1 {
			return config, errors.New("RGW account name and email must be valid UTF-8 without control characters or surrounding whitespace")
		}
	}
	if strings.ContainsAny(config.Name, "$:") {
		return config, errors.New("RGW account name must not contain $ or :")
	}
	if config.MaxBuckets != nil {
		if *config.MaxBuckets < -1 {
			return config, errors.New("RGW account max buckets must be -1, zero or positive")
		}
		value := *config.MaxBuckets
		config.MaxBuckets = &value
	}
	return config, nil
}

// CreateAccount creates a fresh account exclusively in the actual native
// gateway scope. Existing IDs are rejected and never adopted. A partial handle
// may be returned after a native failure, but ownership requires a validated
// creation response and native metadata version tag. Inspect uncertain results
// through Admin or terminate the disposable cluster; do not migrate users into
// them or blindly delete an identity whose creation was not confirmed.
func (g *RGWContainer) CreateAccount(ctx context.Context, config RGWAccountConfig) (*RGWAccount, error) {
	config, err := normalizeRGWAccountConfig(config)
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	scope, err := g.placementRuntimeScope(ctx)
	if err != nil {
		return nil, err
	}
	a := &RGWAccount{owner: g.owner, scope: scope, config: config, id: config.ID, state: &rgwAccountState{}}
	exists, err := g.accountExists(ctx, a)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, errors.New("RGW account ID already exists")
	}
	args := []string{"account", "create", "--account-id", a.id, "--account-name", config.Name}
	if config.Tenant != "" {
		args = append(args, "--tenant", config.Tenant)
	}
	if config.Email != "" {
		args = append(args, "--email", config.Email)
	}
	if config.MaxBuckets != nil {
		args = append(args, "--max-buckets", strconv.Itoa(*config.MaxBuckets))
	}
	data, err := g.placementCommand(ctx, scope, args...)
	if err != nil {
		return a, err
	}
	created, err := decodeRGWAccount(data)
	if err != nil || !rgwAccountCreationIdentity(created, config) {
		return a, errors.New("RGW account creation identity could not be confirmed")
	}
	native, err := g.readNativeAccount(ctx, a)
	if err != nil {
		return a, err
	}
	if !rgwAccountCreationIdentity(native, config) || native.tag == "" || !reflect.DeepEqual(created.document, rgwAccountPolicyWithoutAttrs(native.document)) {
		return a, errors.New("RGW account metadata ownership could not be confirmed")
	}
	a.state.versionTag, a.state.confirmed = native.tag, true
	return a, nil
}

func rgwAccountCreationIdentity(native *rgwNativeAccount, config RGWAccountConfig) bool {
	return native != nil && native.info.ID == config.ID && native.info.Name == config.Name && native.info.Tenant == config.Tenant && native.info.Email == config.Email && (config.MaxBuckets == nil || native.info.MaxBuckets == *config.MaxBuckets)
}

func rgwAccountPolicyWithoutAttrs(document map[string]any) map[string]any {
	policy := make(map[string]any, len(document))
	for key, value := range document {
		if key != "attrs" {
			policy[key] = value
		}
	}
	return policy
}

func decodeRGWAccount(data []byte) (*rgwNativeAccount, error) {
	var raw struct {
		ID, Name, Tenant, Email string
		MaxUsers                int             `json:"max_users"`
		MaxRoles                int             `json:"max_roles"`
		MaxGroups               int             `json:"max_groups"`
		MaxBuckets              int             `json:"max_buckets"`
		MaxKeys                 int             `json:"max_access_keys"`
		Quota                   json.RawMessage `json:"quota"`
		BucketQuota             json.RawMessage `json:"bucket_quota"`
	}
	if json.Unmarshal(data, &raw) != nil || !rgwAccountID.MatchString(raw.ID) {
		return nil, errors.New("decode RGW account information")
	}
	quota, err := decodeRGWQuota(raw.Quota)
	if err != nil {
		return nil, err
	}
	bucket, err := decodeRGWQuota(raw.BucketQuota)
	if err != nil {
		return nil, err
	}
	document, err := decodeRGWPlacementObject(data)
	if err != nil {
		return nil, errors.New("decode RGW account policy")
	}
	return &rgwNativeAccount{document: document, info: RGWAccountInfo{ID: raw.ID, Name: raw.Name, Tenant: raw.Tenant, Email: raw.Email, MaxUsers: raw.MaxUsers, MaxRoles: raw.MaxRoles, MaxGroups: raw.MaxGroups, MaxBuckets: raw.MaxBuckets, MaxKeys: raw.MaxKeys, AccountQuota: quota, BucketQuota: bucket}}, nil
}

func (g *RGWContainer) readNativeAccount(ctx context.Context, a *RGWAccount) (*rgwNativeAccount, error) {
	data, err := g.placementCommand(ctx, a.scope, "metadata", "get", "account:"+a.id)
	if err != nil {
		return nil, err
	}
	var metadata struct {
		Key     string `json:"key"`
		Version struct {
			Tag string `json:"tag"`
		} `json:"ver"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(data, &metadata) != nil || metadata.Key != "account:"+a.id || metadata.Version.Tag == "" {
		return nil, errors.New("decode RGW account metadata identity")
	}
	native, err := decodeRGWAccount(metadata.Data)
	if err != nil {
		return nil, err
	}
	native.tag = metadata.Version.Tag
	return native, nil
}

func (g *RGWContainer) accountExists(ctx context.Context, a *RGWAccount) (bool, error) {
	data, err := g.placementCommand(ctx, a.scope, "account", "list")
	if err != nil {
		return false, err
	}
	var ids []string
	if json.Unmarshal(data, &ids) != nil || ids == nil {
		return false, errors.New("decode RGW account listing")
	}
	return slices.Contains(ids, a.id), nil
}

func (g *RGWContainer) ownedAccount(ctx context.Context, a *RGWAccount) (*rgwNativeAccount, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.validateAdminGateway(); err != nil {
		return nil, err
	}
	if a == nil || a.owner != g.owner || a.state == nil || !a.state.confirmed || a.state.removed {
		return nil, errors.New("RGW account must be active and created by this cluster")
	}
	scope, err := g.placementRuntimeScope(ctx)
	if err != nil {
		return nil, err
	}
	if scope != a.scope {
		return nil, errors.New("RGW account native runtime scope changed")
	}
	native, err := g.readNativeAccount(ctx, a)
	if err != nil {
		return nil, err
	}
	if native.info.ID != a.id || native.info.Tenant != a.config.Tenant || native.tag != a.state.versionTag {
		return nil, errors.New("RGW account identity or native metadata lifetime changed")
	}
	return native, nil
}

// AccountInfo reads the owned account's authoritative native policy.
func (g *RGWContainer) AccountInfo(ctx context.Context, a *RGWAccount) (RGWAccountInfo, error) {
	if g == nil || g.owner == nil {
		return RGWAccountInfo{}, errors.New("RGW gateway is not owned by a Ceph cluster")
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	native, err := g.ownedAccount(ctx, a)
	if err != nil {
		return RGWAccountInfo{}, err
	}
	return native.info, nil
}

// CreateAccountRootUser creates a fresh root identity inside an owned account.
// Empty Tenant inherits the account tenant; a different tenant is rejected.
// Account root grants permissions within the account; global admin and multisite
// system flags remain false. IAM operations remain on the consuming client.
func (g *RGWContainer) CreateAccountRootUser(ctx context.Context, a *RGWAccount, config RGWUserConfig) (*RGWUser, error) {
	if a == nil {
		return nil, errors.New("owned RGW account is required")
	}
	return g.createUser(ctx, config, a)
}

// SetAccountQuota replaces aggregate account limits and their enabled state.
// Native account quota size is rounded to KiB, so byte limits must be -1, zero
// or a positive multiple of 1024. Readback does not promise cache enforcement.
func (g *RGWContainer) SetAccountQuota(ctx context.Context, a *RGWAccount, quota RGWQuota) error {
	return g.setAccountQuota(ctx, a, "account", quota)
}

// SetAccountBucketQuota applies individual bucket limits to every bucket owned
// by this account, independent of which account identity created the bucket.
func (g *RGWContainer) SetAccountBucketQuota(ctx context.Context, a *RGWAccount, quota RGWQuota) error {
	return g.setAccountQuota(ctx, a, "bucket", quota)
}

func (g *RGWContainer) setAccountQuota(ctx context.Context, a *RGWAccount, scope string, quota RGWQuota) error {
	if quota.MaxObjects < -1 || quota.MaxSizeBytes < -1 || (quota.MaxSizeBytes > 0 && quota.MaxSizeBytes%1024 != 0) {
		return errors.New("RGW account quota limits must be -1, zero or positive; byte limits must be multiples of 1024")
	}
	if g == nil || g.owner == nil {
		return errors.New("RGW gateway is not owned by a Ceph cluster")
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	before, err := g.ownedAccount(ctx, a)
	if err != nil {
		return err
	}
	if _, err := g.placementCommand(ctx, a.scope, "quota", "set", "--quota-scope", scope, "--account-id", a.id, "--max-size", rgwQuotaSizeArgument(quota.MaxSizeBytes), "--max-objects", strconv.FormatInt(quota.MaxObjects, 10)); err != nil {
		return err
	}
	action := "disable"
	if quota.Enabled {
		action = "enable"
	}
	if _, err := g.placementCommand(ctx, a.scope, "quota", action, "--quota-scope", scope, "--account-id", a.id); err != nil {
		return err
	}
	after, err := g.ownedAccount(ctx, a)
	if err != nil {
		return err
	}
	actual, field := after.info.AccountQuota, "quota"
	if scope == "bucket" {
		actual, field = after.info.BucketQuota, "bucket_quota"
	}
	if actual != quota {
		return errors.New("RGW account quota readback differs from requested policy")
	}
	oldPolicy, newPolicy := rgwAccountPolicyWithoutAttrs(before.document), rgwAccountPolicyWithoutAttrs(after.document)
	delete(oldPolicy, field)
	delete(newPolicy, field)
	if !reflect.DeepEqual(oldPolicy, newPolicy) || !reflect.DeepEqual(before.document["attrs"], after.document["attrs"]) {
		return errors.New("RGW unrelated account policy changed during quota update")
	}
	return nil
}

// RemoveAccount removes only an owned, empty account, without purge-data. Native
// Ceph refuses remaining users, buckets, roles, groups, OIDC providers or topics.
// Remove these through their ordinary clients first. Success is idempotent across
// copies. Native external account edits must not race these fixture helpers.
func (g *RGWContainer) RemoveAccount(ctx context.Context, a *RGWAccount) error {
	if g == nil || g.owner == nil {
		return errors.New("RGW gateway is not owned by a Ceph cluster")
	}
	g.owner.mu.Lock()
	defer g.owner.mu.Unlock()
	if a == nil || a.owner != g.owner || a.state == nil || !a.state.confirmed {
		return errors.New("RGW account was not confirmed as created by this cluster")
	}
	if a.state.removed {
		return nil
	}
	ctx, cancel := g.adminContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := g.validateAdminGateway(); err != nil {
		return err
	}
	current, err := g.placementRuntimeScope(ctx)
	if err != nil {
		return err
	}
	if current != a.scope {
		return errors.New("RGW account native runtime scope changed")
	}
	exists, err := g.accountExists(ctx, a)
	if err != nil {
		return err
	}
	if !exists {
		a.state.removed = true
		return nil
	}
	if _, err := g.ownedAccount(ctx, a); err != nil {
		return err
	}
	if _, err := g.placementCommand(ctx, a.scope, "account", "rm", "--account-id", a.id); err != nil {
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		exists, checkErr := g.accountExists(checkCtx, a)
		if checkErr != nil || exists {
			return errors.Join(err, checkErr)
		}
	}
	a.state.removed = true
	return nil
}
