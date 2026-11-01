package gwbase

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
)

// Account is the private identity of one account created through a gateway:
// the creating gateway, the exact native scope it was created in, its ID and
// tenant. Every copy of a public account handle shares one Account; a root
// user's creation identity keeps a copy whose State stays shared.
type Account struct {
	// Origin is the gateway that created the account. Its cluster and scope
	// select the gateway that runs the account's operations.
	Origin     *Core
	Scope      NativeScope
	ID, Tenant string
	State      *AccountState
}

// AccountState is the lifecycle state of an account: whether its creation
// and native metadata version tag were confirmed, and whether it was removed.
type AccountState struct {
	Confirmed, Removed bool
	VersionTag         string
}

// AccountInfo is the foundation's copy of an account's native policy;
// package rgw/account converts it to account.Metadata.
type AccountInfo struct {
	ID, Name, Tenant, Email                            string
	MaxUsers, MaxRoles, MaxGroups, MaxBuckets, MaxKeys int
	AccountQuota, BucketQuota                          Quota
}

// NativeAccount is one decoded native account: its policy, the whole native
// document and the metadata version tag of its lifetime.
type NativeAccount struct {
	Info     AccountInfo
	Document map[string]any
	Tag      string
}

// AccountHandle binds a public account handle to its Account. It has only
// unexported members, so embedding it promotes nothing into a public API.
type AccountHandle struct{ a *Account }

// NewAccountHandle returns the handle of a.
func NewAccountHandle(a *Account) AccountHandle { return AccountHandle{a: a} }

func (h AccountHandle) account() *Account { return h.a }

// AccountOwner is a type that embeds an AccountHandle. It is sealed: only
// types that embed AccountHandle satisfy it.
type AccountOwner interface{ account() *Account }

// AccountOf returns the Account of o; nil for a nil o or a handle that
// package rgw/account did not create.
func AccountOf(o AccountOwner) *Account {
	if isNil(o) {
		return nil
	}
	return o.account()
}

// Owned reports whether a is an account created by a gateway of a cluster.
func (a *Account) Owned() bool { return a != nil && a.Origin.Cluster() != nil }

// DecodeAccount decodes a native account document.
func DecodeAccount(data []byte) (*NativeAccount, error) {
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
	if json.Unmarshal(data, &raw) != nil || !accountID.MatchString(raw.ID) {
		return nil, errors.New("decode RGW account information")
	}
	quota, err := DecodeQuota(raw.Quota)
	if err != nil {
		return nil, err
	}
	bucket, err := DecodeQuota(raw.BucketQuota)
	if err != nil {
		return nil, err
	}
	document, err := DecodeObject(data)
	if err != nil {
		return nil, errors.New("decode RGW account policy")
	}
	return &NativeAccount{Document: document, Info: AccountInfo{ID: raw.ID, Name: raw.Name, Tenant: raw.Tenant, Email: raw.Email, MaxUsers: raw.MaxUsers, MaxRoles: raw.MaxRoles, MaxGroups: raw.MaxGroups, MaxBuckets: raw.MaxBuckets, MaxKeys: raw.MaxKeys, AccountQuota: quota, BucketQuota: bucket}}, nil
}

// ReadNativeAccount reads the native metadata of a, with its version tag,
// through g in the account's native scope.
func (g *Core) ReadNativeAccount(ctx context.Context, a *Account) (*NativeAccount, error) {
	data, err := g.Command(ctx, a.Scope, "metadata", "get", "account:"+a.ID)
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
	if json.Unmarshal(data, &metadata) != nil || metadata.Key != "account:"+a.ID || metadata.Version.Tag == "" {
		return nil, errors.New("decode RGW account metadata identity")
	}
	native, err := DecodeAccount(metadata.Data)
	if err != nil {
		return nil, err
	}
	native.Tag = metadata.Version.Tag
	return native, nil
}

// AccountExists reports whether the native account listing contains a.
func (g *Core) AccountExists(ctx context.Context, a *Account) (bool, error) {
	data, err := g.Command(ctx, a.Scope, "account", "list")
	if err != nil {
		return false, err
	}
	var ids []string
	if json.Unmarshal(data, &ids) != nil || ids == nil {
		return false, errors.New("decode RGW account listing")
	}
	return slices.Contains(ids, a.ID), nil
}

// OwnedAccount reads a through g and confirms that it is the active account
// that was created: the same native scope, ID, tenant and metadata lifetime.
// It is called with the owning cluster's mutex held.
func (g *Core) OwnedAccount(ctx context.Context, a *Account) (*NativeAccount, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.ValidateAdmin(); err != nil {
		return nil, err
	}
	if a == nil || a.State == nil || !a.State.Confirmed || a.State.Removed {
		return nil, errors.New("RGW account must be active and created by this cluster")
	}
	scope, err := g.RuntimeScope(ctx)
	if err != nil {
		return nil, err
	}
	if scope != a.Scope {
		return nil, errors.New("RGW account native runtime scope changed")
	}
	native, err := g.ReadNativeAccount(ctx, a)
	if err != nil {
		return nil, err
	}
	if native.Info.ID != a.ID || native.Info.Tenant != a.Tenant || native.Tag != a.State.VersionTag {
		return nil, errors.New("RGW account identity or native metadata lifetime changed")
	}
	return native, nil
}
