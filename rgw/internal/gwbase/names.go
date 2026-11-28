package gwbase

import (
	"strings"
	"unicode"
)

// nameRule is one admission rule for a native name: its byte bound, the runes
// it may hold and the runes it may start with. The bucket and tenant rules of
// the RGW entry points are nameRule values, so their differences are these
// parameters; each entry point keeps the rule that it admitted with before.
type nameRule struct {
	maxBytes       int
	allowed, first func(rune) bool
}

func (r nameRule) valid(name string) bool {
	if name == "" || len(name) > r.maxBytes {
		return false
	}
	for i, c := range name {
		if !r.allowed(c) || i == 0 && !r.first(c) {
			return false
		}
	}
	return true
}

func asciiAlphanumeric(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}

// syncName is the rule of a sync policy ID: at most 128 bytes, no control or
// space character and none of ",/:$", and no leading "-".
var syncName = nameRule{
	maxBytes: 128,
	allowed: func(r rune) bool {
		return !unicode.IsControl(r) && !unicode.IsSpace(r) && !strings.ContainsRune(",/:$", r)
	},
	first: func(r rune) bool { return r != '-' },
}

// ValidSyncID reports whether id is a valid sync group, flow or pipe ID.
func ValidSyncID(id string) bool { return syncName.valid(id) }

// BucketRule is the bucket name rule of one entry point.
type BucketRule struct{ name nameRule }

// The bucket name rules of the entry points.
var (
	// SyncBucket is the rule of a sync policy's bucket scope and bucket
	// selectors: the sync policy ID rule.
	SyncBucket = BucketRule{syncName}
	// RateLimitBucket is the rule of a rate-limit bucket scope: at most 255
	// ASCII letters, digits, dots, underscores and hyphens, starting with a
	// letter or a digit, so that radosgw-admin cannot read it as a flag or
	// as another tenant's bucket.
	RateLimitBucket = BucketRule{nameRule{
		maxBytes: 255,
		allowed:  func(r rune) bool { return asciiAlphanumeric(r) || strings.ContainsRune("._-", r) },
		first:    asciiAlphanumeric,
	}}
)

// Valid reports whether bucket is a bucket name under r.
func (r BucketRule) Valid(bucket string) bool { return r.name.valid(bucket) }

// TenantRule is the tenant rule of one entry point. The empty tenant, no
// tenant, is valid under every rule.
type TenantRule struct {
	name nameRule
	// noAccountID refuses a tenant formatted as an account ID.
	noAccountID bool
}

// The tenant rules of the entry points.
var (
	// SyncTenant is the rule of the tenant of a sync policy's bucket scope
	// and bucket selectors: the sync policy ID rule.
	SyncTenant = TenantRule{name: syncName}
	// UserTenant is the rule of a user's tenant: at most 128 ASCII letters,
	// digits and underscores, not formatted as an account ID.
	UserTenant = TenantRule{name: nameRule{
		maxBytes: 128,
		allowed:  func(r rune) bool { return asciiAlphanumeric(r) || r == '_' },
		first:    func(rune) bool { return true },
	}, noAccountID: true}
)

// Valid reports whether tenant is a tenant under r.
func (r TenantRule) Valid(tenant string) bool {
	return tenant == "" || r.name.valid(tenant) && !(r.noAccountID && ValidAccountID(tenant))
}
