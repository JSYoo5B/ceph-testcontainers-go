package gwbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

var (
	tenantName  = regexp.MustCompile(`^[A-Za-z0-9_]{1,128}$`)
	accountID   = regexp.MustCompile(`^RGW[0-9]{17}$`)
	iamUserName = regexp.MustCompile(`^[A-Za-z0-9_+=,.@-]{1,64}$`)
)

// Quota is the foundation's copy of a native quota policy in bytes; the
// public packages convert it to rgw.Quota.
type Quota struct {
	Enabled      bool
	MaxSizeBytes int64
	MaxObjects   int64
}

// AdminCapability is the foundation's copy of one native admin capability.
type AdminCapability struct {
	Type       string `json:"type"`
	Permission string `json:"perm"`
}

// UserInfo is the foundation's copy of a native user policy snapshot,
// without keys; package rgw converts it to rgw.UserInfo.
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

// NativeKey is one native S3 key pair.
type NativeKey struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

// NativeUser is one decoded native user: its policy, its keys and the whole
// native document, so that a mutation can check unrelated fields.
type NativeUser struct {
	Info     UserInfo
	Keys     []NativeKey
	Document map[string]any
}

// User is the private state of one user created through a gateway. Every copy
// of a public user handle shares one User. The identity fields are written
// once, before the handle is returned; State is written with the owning
// cluster's mutex held.
type User struct {
	// Origin is the gateway that created the user. Its cluster and scope
	// select the gateway that runs the user's operations.
	Origin                   *Core
	ID, AccessKey, SecretKey string
	State                    *UserState
}

// UserState is the lifecycle state of a user.
type UserState struct {
	Created, Removed bool
	// NativeScope, when set, is the exact native scope that the user's
	// commands use: that of a tenanted user's gateway or of a root user's
	// account.
	NativeScope *NativeScope
	Identity    *CreationIdentity
}

// CreationIdentity is the native user type and account association confirmed
// at creation; they are immutable afterwards. Account is a copy of the account
// descriptor, whose lifecycle state stays shared with the account handle.
type CreationIdentity struct {
	OriginalType      string
	Account           *Account
	AccountID         string
	AccountVersionTag string
}

// UserHandle binds a public user handle to its User. It has only unexported
// members, so embedding it promotes nothing into a public API.
type UserHandle struct{ u *User }

// NewUserHandle returns the handle of u.
func NewUserHandle(u *User) UserHandle { return UserHandle{u: u} }

func (h UserHandle) user() *User { return h.u }

// UserOwner is a type that embeds a UserHandle. It is sealed: only types that
// embed UserHandle satisfy it.
type UserOwner interface{ user() *User }

// UserOf returns the User of o; nil for a nil o or a handle that package rgw
// did not create.
func UserOf(o UserOwner) *User {
	if isNil(o) {
		return nil
	}
	return o.user()
}

// Owned reports whether u is a user created by a gateway of a cluster.
func (u *User) Owned() bool { return u != nil && u.Origin.Cluster() != nil }

// Confirmed reports whether the creation of u and its generated credentials
// were confirmed.
func (u *User) Confirmed() bool {
	return u.State != nil && u.State.Created && u.State.Identity != nil && u.AccessKey != "" && u.SecretKey != ""
}

// Active reports whether u is a confirmed user that was not removed.
func (u *User) Active() bool { return u.Confirmed() && !u.State.Removed }

// ServedBy reports whether g belongs to the user's cluster and serves the
// scope of the gateway that created the user, so that it can run the user's
// operations.
func (u *User) ServedBy(g *Core) bool { return u != nil && g.ServesScopeOf(u.Origin) }

// FullUserID is the canonical native user ID.
func FullUserID(tenant, namespace, id string) string {
	if namespace != "" {
		return tenant + "$" + namespace + "$" + id
	}
	if tenant != "" {
		return tenant + "$" + id
	}
	return id
}

// ValidateTenant refuses a tenant that is not letters, digits and
// underscores, or that is formatted as an account ID.
func ValidateTenant(tenant string) error {
	if tenant != "" && (!tenantName.MatchString(tenant) || accountID.MatchString(tenant)) {
		return servicehook.Invalid(errors.New("RGW tenant must use letters, digits or underscores and must not be formatted as an account ID"))
	}
	return nil
}

// ValidAccountID reports whether id is formatted as a native account ID.
func ValidAccountID(id string) bool { return accountID.MatchString(id) }

// QuotaSizeArgument is the native --max-size argument of a byte limit.
func QuotaSizeArgument(bytes int64) string {
	if bytes == -1 {
		// Account CLI quota rounds signed sizes before max(-1, rounded_size).
		// -1 byte and -1 KiB truncate to zero; -2 KiB retains the disabled
		// limit. Ordinary user/bucket quota accepts this negative value too.
		return "-2K"
	}
	return strconv.FormatInt(bytes, 10) + "B"
}

// UserSpec is a normalized user creation request. Account, when set, is the
// owned account whose root user is created.
type UserSpec struct {
	ID, Tenant, DisplayName, Email, AdminCaps string
	MaxBuckets                                *int
	Account                                   *Account
}

// CreateUser creates a fresh user through g, refusing an existing ID. A non-nil
// result with an error records an attempted or partial creation. The caller
// holds the owning cluster's mutex, has validated g and bounds ctx with
// AdminContext.
func (g *Core) CreateUser(ctx context.Context, spec UserSpec) (*User, error) {
	var nativeScope *NativeScope
	var creationAccount *Account
	var accountVersionTag string
	if spec.Account != nil {
		nativeAccount, err := g.OwnedAccount(ctx, spec.Account)
		if err != nil {
			return nil, err
		}
		if spec.Tenant != spec.Account.Tenant || !iamUserName.MatchString(spec.DisplayName) {
			return nil, errors.New("account root requires its account's tenant and an IAM-compatible display name")
		}
		value := spec.Account.Scope
		nativeScope = &value
		accountCopy := *spec.Account
		creationAccount, accountVersionTag = &accountCopy, nativeAccount.Tag
	} else if spec.Tenant != "" {
		value, err := g.RuntimeScope(ctx)
		if err != nil {
			return nil, err
		}
		nativeScope = &value
	}
	id := FullUserID(spec.Tenant, "", spec.ID)
	user := &User{Origin: g, ID: id, State: &UserState{NativeScope: nativeScope}}
	exists, err := g.UserExists(ctx, user)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, servicehook.Invalid(fmt.Errorf("RGW user %q already exists", id))
	}
	args := []string{"user", "create", "--uid", id, "--display-name", spec.DisplayName}
	if spec.Account != nil {
		args = append(args, "--account-id", spec.Account.ID, "--account-root", "--gen-secret", "--gen-access-key")
	}
	if spec.Email != "" {
		args = append(args, "--email", spec.Email)
	}
	if spec.MaxBuckets != nil {
		args = append(args, "--max-buckets", strconv.Itoa(*spec.MaxBuckets))
	}
	data, err := g.UserCommand(ctx, user, args...)
	if err != nil {
		return user, err
	}
	user.State.Created = true
	native, err := DecodeUser(data)
	if err != nil {
		return user, err
	}
	if native.Info.ID != user.ID || len(native.Keys) != 1 || native.Keys[0].AccessKey == "" || native.Keys[0].SecretKey == "" || native.Info.Admin || native.Info.System {
		return user, errors.New("RGW did not create an ordinary user with one S3 key pair")
	}
	if spec.Account != nil && (native.Info.AccountID != spec.Account.ID || native.Info.Type != "root") {
		return user, errors.New("RGW did not create the requested account root identity")
	}
	if spec.Account == nil && (native.Info.AccountID != "" || (native.Info.Type != "" && native.Info.Type != "rgw")) {
		return user, errors.New("RGW did not create an ordinary non-account identity")
	}
	user.State.Identity = &CreationIdentity{OriginalType: native.Info.Type, Account: creationAccount, AccountID: native.Info.AccountID, AccountVersionTag: accountVersionTag}
	user.AccessKey, user.SecretKey = native.Keys[0].AccessKey, native.Keys[0].SecretKey
	if spec.AdminCaps != "" {
		if _, err := g.UserCommand(ctx, user, "caps", "add", "--uid", user.ID, "--caps", spec.AdminCaps); err != nil {
			return user, err
		}
	}
	return user, nil
}

// UserExists reports whether the native user listing contains u.
func (g *Core) UserExists(ctx context.Context, u *User) (bool, error) {
	data, err := g.UserCommand(ctx, u, "user", "list")
	if err != nil {
		return false, err
	}
	var users []string
	if json.Unmarshal(data, &users) != nil || users == nil {
		return false, errors.New("decode RGW user listing")
	}
	return slices.Contains(users, u.ID), nil
}

// UserCommand runs a radosgw-admin command of u through g: in the user's
// captured native scope, after checking that g still runs in it, or in g's
// configured scope.
func (g *Core) UserCommand(ctx context.Context, u *User, args ...string) ([]byte, error) {
	if u.State.NativeScope != nil {
		scope, err := g.RuntimeScope(ctx)
		if err != nil {
			return nil, err
		}
		if scope != *u.State.NativeScope {
			return nil, errors.New("RGW user's native runtime scope changed")
		}
		return g.Command(ctx, scope, args...)
	}
	return g.AdminCommand(ctx, args...)
}

// OwnedUser reads u through g and confirms that it is still the active
// identity it created: the same ID, the created key, and the original type and
// account association.
func (g *Core) OwnedUser(ctx context.Context, u *User) (*NativeUser, error) {
	if !u.Active() {
		return nil, errors.New("RGW user must be an active identity created in this cluster")
	}
	data, err := g.UserCommand(ctx, u, "user", "info", "--uid", u.ID)
	if err != nil {
		return nil, err
	}
	native, err := DecodeUser(data)
	if err != nil {
		return nil, err
	}
	if native.Info.ID != u.ID || !slices.ContainsFunc(native.Keys, func(key NativeKey) bool {
		return u.AccessKey != "" && key.AccessKey == u.AccessKey && key.SecretKey == u.SecretKey
	}) {
		return nil, servicehook.Invalid(errors.New("RGW user has different credentials; refusing owned-user operation"))
	}
	if err := g.ValidateCreationIdentity(ctx, u, native); err != nil {
		return nil, err
	}
	return native, nil
}

// ValidateCreationIdentity refuses a native user whose type or account
// association changed since creation and, for an account root, an account
// whose native metadata lifetime or tenant changed. It is called with the
// owning cluster's mutex held.
func (g *Core) ValidateCreationIdentity(ctx context.Context, u *User, native *NativeUser) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if u.State.Identity == nil {
		return errors.New("RGW user creation identity was not confirmed")
	}
	identity := u.State.Identity
	if native.Info.Type != identity.OriginalType || native.Info.AccountID != identity.AccountID || native.Info.Namespace != "" {
		return errors.New("RGW user's original type or account association changed")
	}
	if identity.Account == nil {
		if identity.AccountID != "" || (identity.OriginalType != "" && identity.OriginalType != "rgw") {
			return errors.New("RGW ordinary user creation identity is invalid")
		}
		return nil
	}
	if identity.OriginalType != "root" || identity.AccountID == "" || identity.AccountVersionTag == "" || identity.Account.ID != identity.AccountID {
		return errors.New("RGW account root creation identity is invalid")
	}
	account, err := g.OwnedAccount(ctx, identity.Account)
	if err != nil {
		return err
	}
	if account.Info.ID != identity.AccountID || account.Tag != identity.AccountVersionTag || native.Info.Tenant != account.Info.Tenant {
		return errors.New("RGW account root's original account lifetime or tenant changed")
	}
	return nil
}

// DecodeUser decodes the native user information of the CLI or Admin Ops.
func DecodeUser(data []byte) (*NativeUser, error) {
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
		Keys                []NativeKey       `json:"keys"`
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
	fullID := FullUserID(tenant, namespace, localID)
	if raw.FullID != "" && raw.FullID != fullID {
		return nil, errors.New("RGW full user ID differs from tenant, namespace or local ID")
	}
	suspended, err := decodeFlag(raw.Suspended)
	if err != nil {
		return nil, err
	}
	admin, err := decodeFlag(raw.Admin)
	if err != nil {
		return nil, err
	}
	system, err := decodeFlag(raw.System)
	if err != nil {
		return nil, err
	}
	userQuota, err := DecodeQuota(raw.UserQuota)
	if err != nil {
		return nil, err
	}
	bucketQuota, err := DecodeQuota(raw.BucketQuota)
	if err != nil {
		return nil, err
	}
	tags := slices.Clone(raw.PlacementTags)
	slices.Sort(tags)
	document, err := DecodeObject(data)
	if err != nil {
		return nil, errors.New("decode RGW user information")
	}
	return &NativeUser{Keys: raw.Keys, Document: document, Info: UserInfo{
		ID: fullID, LocalID: localID, Tenant: tenant, Namespace: namespace, Type: raw.Type, AccountID: raw.AccountID, DisplayName: raw.DisplayName, Email: raw.Email, Suspended: suspended,
		Admin: admin, System: system, MaxBuckets: raw.MaxBuckets, AdminCaps: raw.Caps,
		UserQuota: userQuota, BucketQuota: bucketQuota,
		DefaultPlacement: raw.DefaultPlacement, DefaultStorageClass: raw.DefaultStorageClass, PlacementTags: tags,
	}}, nil
}

func decodeFlag(value json.RawMessage) (bool, error) {
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

// DecodeQuota decodes a native quota policy, converting a legacy KiB size to
// bytes.
func DecodeQuota(data json.RawMessage) (Quota, error) {
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
