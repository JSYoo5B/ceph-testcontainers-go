package account

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/google/uuid"
)

// Config creates a new native account. Empty ID generates a unique
// RGW-prefixed 17-digit ID; Name defaults to a unique fixture name. Tenant is
// optional and isolates bucket names. Account IDs and email are globally unique.
// MaxBuckets nil preserves the native default. No user migration is performed.
type Config struct {
	ID, Name, Tenant, Email string
	MaxBuckets              *int
}

// Account is a fresh account owned by this fixture. Its native metadata
// version tag distinguishes its lifetime from a deleted/recreated account.
// Copies share lifecycle state. Removing a gateway does not remove the account.
// Its operations run through the first running registered gateway, by name, in the
// scope of the gateway that created it. Pass it as rgw.UserConfig.Account to
// create its root user through a gateway of that cluster and scope.
type Account struct {
	handle
}

// handle binds an Account to its private identity (see gwbase.Account). The
// alias keeps the embedded field unexported, so *Account satisfies
// rgw.RootAccount without promoting anything into the public API.
type handle = gwbase.AccountHandle

// Metadata contains account-wide limits and the individual bucket policy.
// AccountQuota applies across all account users, buckets and roles, rather than
// to one user's storage. Quota readback does not flush native accounting caches.
type Metadata struct {
	ID, Name, Tenant, Email                            string
	MaxUsers, MaxRoles, MaxGroups, MaxBuckets, MaxKeys int
	AccountQuota, BucketQuota                          rgw.Quota
}

// ID returns the native account ID, or "" for a nil handle.
//
//purpose:observation
func (a *Account) ID() string {
	if account := a.base(); account != nil {
		return account.ID
	}
	return ""
}

// String names the account by its native ID.
func (a Account) String() string { return "RGW account " + a.ID() }

// GoString formats the account like String.
func (a Account) GoString() string { return a.String() }

// Format prints String for every verb and flag, so no verb, such as %d or %t,
// walks the fields.
func (a Account) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, a.String()) }

func (a *Account) base() *gwbase.Account {
	if a == nil {
		return nil
	}
	return gwbase.AccountOf(a)
}

func newAccount(a *gwbase.Account) *Account {
	if a == nil {
		return nil
	}
	return &Account{handle: gwbase.NewAccountHandle(a)}
}

func metadata(info gwbase.AccountInfo) Metadata {
	return Metadata{ID: info.ID, Name: info.Name, Tenant: info.Tenant, Email: info.Email,
		MaxUsers: info.MaxUsers, MaxRoles: info.MaxRoles, MaxGroups: info.MaxGroups, MaxBuckets: info.MaxBuckets, MaxKeys: info.MaxKeys,
		AccountQuota: rgw.Quota(info.AccountQuota), BucketQuota: rgw.Quota(info.BucketQuota)}
}

func normalizeConfig(config Config) (Config, error) {
	if config.ID == "" {
		number, err := rand.Int(rand.Reader, big.NewInt(100000000000000000))
		if err != nil {
			return config, servicehook.Invalid(errors.New("generate RGW account ID"))
		}
		config.ID = fmt.Sprintf("RGW%017d", number)
	}
	if !gwbase.ValidAccountID(config.ID) {
		return config, servicehook.Invalid(errors.New("RGW account ID must be RGW followed by 17 digits"))
	}
	if config.Name == "" {
		config.Name = "tc-" + uuid.NewString()
	}
	if err := gwbase.ValidateTenant(config.Tenant); err != nil {
		return config, err
	}
	for _, value := range []string{config.Name, config.Email} {
		if !utf8.ValidString(value) || strings.TrimSpace(value) != value || strings.IndexFunc(value, unicode.IsControl) != -1 {
			return config, servicehook.Invalid(errors.New("RGW account name and email must be valid UTF-8 without control characters or surrounding whitespace"))
		}
	}
	if strings.ContainsAny(config.Name, "$:") {
		return config, servicehook.Invalid(errors.New("RGW account name must not contain $ or a colon"))
	}
	if config.MaxBuckets != nil {
		if *config.MaxBuckets < -1 {
			return config, servicehook.Invalid(errors.New("RGW account max buckets must be -1, zero or positive"))
		}
		value := *config.MaxBuckets
		config.MaxBuckets = &value
	}
	return config, nil
}

// Create creates a fresh account exclusively in the actual native scope of
// gateway g. Existing IDs are rejected and never adopted. A partial handle
// may be returned after a native failure, but ownership requires a validated
// creation response and native metadata version tag. Inspect uncertain results
// through Gateway.Admin or terminate the disposable cluster; do not migrate
// users into them or blindly delete an identity whose creation was not
// confirmed.
//
//purpose:construction,handoff
func Create(ctx context.Context, g *rgw.Gateway, config Config) (*Account, error) {
	var core *gwbase.Core
	if g != nil {
		core = gwbase.Of(g)
	}
	a, err := create(ctx, core, config)
	return newAccount(a), err
}

func create(ctx context.Context, g *gwbase.Core, config Config) (*gwbase.Account, error) {
	config, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	if g.Cluster() == nil {
		return nil, servicehook.Invalid(errors.New("RGW gateway is not owned by a Ceph cluster"))
	}
	servicehook.Lock(g.Cluster())
	defer servicehook.Unlock(g.Cluster())
	if err := g.ValidateAdmin(); err != nil {
		return nil, err
	}
	ctx, cancel := gwbase.AdminContext(ctx, g.Cluster())
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	scope, err := g.RuntimeScope(ctx)
	if err != nil {
		return nil, err
	}
	a := &gwbase.Account{Origin: g, Scope: scope, ID: config.ID, Tenant: config.Tenant, State: &gwbase.AccountState{}}
	exists, err := g.AccountExists(ctx, a)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, servicehook.Invalid(errors.New("RGW account ID already exists"))
	}
	args := []string{"account", "create", "--account-id", a.ID, "--account-name", config.Name}
	if config.Tenant != "" {
		args = append(args, "--tenant", config.Tenant)
	}
	if config.Email != "" {
		args = append(args, "--email", config.Email)
	}
	if config.MaxBuckets != nil {
		args = append(args, "--max-buckets", strconv.Itoa(*config.MaxBuckets))
	}
	data, err := g.Command(ctx, scope, args...)
	if err != nil {
		return a, err
	}
	created, err := gwbase.DecodeAccount(data)
	if err != nil || !creationIdentity(created, config) {
		return a, errors.New("RGW account creation identity could not be confirmed")
	}
	native, err := g.ReadNativeAccount(ctx, a)
	if err != nil {
		return a, err
	}
	if !creationIdentity(native, config) || native.Tag == "" || !reflect.DeepEqual(created.Document, policyWithoutAttrs(native.Document)) {
		return a, errors.New("RGW account metadata ownership could not be confirmed")
	}
	a.State.VersionTag, a.State.Confirmed = native.Tag, true
	return a, nil
}

func creationIdentity(native *gwbase.NativeAccount, config Config) bool {
	return native != nil && native.Info.ID == config.ID && native.Info.Name == config.Name && native.Info.Tenant == config.Tenant && native.Info.Email == config.Email && (config.MaxBuckets == nil || native.Info.MaxBuckets == *config.MaxBuckets)
}

func policyWithoutAttrs(document map[string]any) map[string]any {
	policy := make(map[string]any, len(document))
	for key, value := range document {
		if key != "attrs" {
			policy[key] = value
		}
	}
	return policy
}

// Info reads the owned account's authoritative native policy.
//
//purpose:observation
func (a *Account) Info(ctx context.Context) (Metadata, error) {
	info, err := accountInfo(ctx, a.base())
	return metadata(info), err
}

func accountInfo(ctx context.Context, a *gwbase.Account) (gwbase.AccountInfo, error) {
	if !a.Owned() {
		return gwbase.AccountInfo{}, servicehook.Invalid(errors.New("RGW account is not owned by a Ceph cluster"))
	}
	cluster := a.Origin.Cluster()
	servicehook.Lock(cluster)
	defer servicehook.Unlock(cluster)
	ctx, cancel := gwbase.AdminContext(ctx, cluster)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return gwbase.AccountInfo{}, err
	}
	g, err := a.Origin.ScopeGateway(ctx)
	if err != nil {
		return gwbase.AccountInfo{}, err
	}
	native, err := g.OwnedAccount(ctx, a)
	if err != nil {
		return gwbase.AccountInfo{}, err
	}
	return native.Info, nil
}

// SetQuota replaces aggregate account limits and their enabled state.
// Native account quota size is rounded to KiB, so byte limits must be -1, zero
// or a positive multiple of 1024. Readback does not promise cache enforcement.
//
//purpose:construction,condition
func (a *Account) SetQuota(ctx context.Context, quota rgw.Quota) error {
	return setQuota(ctx, a.base(), "account", gwbase.Quota(quota))
}

// SetBucketQuota applies individual bucket limits to every bucket owned
// by this account, independent of which account identity created the bucket.
//
//purpose:construction,condition
func (a *Account) SetBucketQuota(ctx context.Context, quota rgw.Quota) error {
	return setQuota(ctx, a.base(), "bucket", gwbase.Quota(quota))
}

func setQuota(ctx context.Context, a *gwbase.Account, scope string, quota gwbase.Quota) error {
	if quota.MaxObjects < -1 || quota.MaxSizeBytes < -1 || (quota.MaxSizeBytes > 0 && quota.MaxSizeBytes%1024 != 0) {
		return errors.New("RGW account quota limits must be -1, zero or positive; byte limits must be multiples of 1024")
	}
	if !a.Owned() {
		return servicehook.Invalid(errors.New("RGW account is not owned by a Ceph cluster"))
	}
	cluster := a.Origin.Cluster()
	servicehook.Lock(cluster)
	defer servicehook.Unlock(cluster)
	ctx, cancel := gwbase.AdminContext(ctx, cluster)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	g, err := a.Origin.ScopeGateway(ctx)
	if err != nil {
		return err
	}
	before, err := g.OwnedAccount(ctx, a)
	if err != nil {
		return err
	}
	if _, err := g.Command(ctx, a.Scope, "quota", "set", "--quota-scope", scope, "--account-id", a.ID, "--max-size", gwbase.QuotaSizeArgument(quota.MaxSizeBytes), "--max-objects", strconv.FormatInt(quota.MaxObjects, 10)); err != nil {
		return err
	}
	action := "disable"
	if quota.Enabled {
		action = "enable"
	}
	if _, err := g.Command(ctx, a.Scope, "quota", action, "--quota-scope", scope, "--account-id", a.ID); err != nil {
		return err
	}
	after, err := g.OwnedAccount(ctx, a)
	if err != nil {
		return err
	}
	actual, field := after.Info.AccountQuota, "quota"
	if scope == "bucket" {
		actual, field = after.Info.BucketQuota, "bucket_quota"
	}
	if actual != quota {
		return errors.New("RGW account quota readback differs from requested policy")
	}
	oldPolicy, newPolicy := policyWithoutAttrs(before.Document), policyWithoutAttrs(after.Document)
	delete(oldPolicy, field)
	delete(newPolicy, field)
	if !reflect.DeepEqual(oldPolicy, newPolicy) || !reflect.DeepEqual(before.Document["attrs"], after.Document["attrs"]) {
		return errors.New("RGW unrelated account policy changed during quota update")
	}
	return nil
}

// Remove removes only an owned, empty account, without purge-data. Native
// Ceph refuses remaining users, buckets, roles, groups, OIDC providers or topics.
// Remove these through their ordinary clients first. Success is idempotent across
// copies. Native external account edits must not race these fixture helpers.
//
//purpose:lifetime
func (a *Account) Remove(ctx context.Context) error { return remove(ctx, a.base()) }

func remove(ctx context.Context, a *gwbase.Account) error {
	if !a.Owned() {
		return servicehook.Invalid(errors.New("RGW account is not owned by a Ceph cluster"))
	}
	cluster := a.Origin.Cluster()
	servicehook.Lock(cluster)
	defer servicehook.Unlock(cluster)
	if a.State == nil || !a.State.Confirmed {
		return servicehook.Invalid(errors.New("RGW account was not confirmed as created by this cluster"))
	}
	if a.State.Removed {
		return nil
	}
	ctx, cancel := gwbase.AdminContext(ctx, cluster)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	g, err := a.Origin.ScopeGateway(ctx)
	if err != nil {
		return err
	}
	current, err := g.RuntimeScope(ctx)
	if err != nil {
		return err
	}
	if current != a.Scope {
		return errors.New("RGW account native runtime scope changed")
	}
	exists, err := g.AccountExists(ctx, a)
	if err != nil {
		return err
	}
	if !exists {
		a.State.Removed = true
		return nil
	}
	if _, err := g.OwnedAccount(ctx, a); err != nil {
		return err
	}
	if _, err := g.Command(ctx, a.Scope, "account", "rm", "--account-id", a.ID); err != nil {
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		exists, checkErr := g.AccountExists(checkCtx, a)
		if checkErr != nil || exists {
			return errors.Join(err, checkErr)
		}
	}
	a.State.Removed = true
	return nil
}
