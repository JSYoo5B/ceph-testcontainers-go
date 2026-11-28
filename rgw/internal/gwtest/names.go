package gwtest

import "strings"

// NameCase is a bucket or tenant input with the verdict each entry point
// gives it: SyncScope is a sync policy's bucket scope (CreateSyncGroup),
// SyncSelector a sync pipe's bucket selector, RateLimit a rate-limit bucket
// scope and User a user's tenant. A false verdict is a refusal before any
// native command. The verdicts are those the entry points gave in v1.0.2.
//
//testdouble:shared-double
type NameCase struct {
	Name                                     string
	SyncScope, SyncSelector, RateLimit, User bool
}

// BucketNames are bucket inputs for the sync scope, the sync selector and the
// rate-limit scope; User is unused.
//
//testdouble:shared-double
var BucketNames = []NameCase{
	{Name: "bucket", SyncScope: true, SyncSelector: true, RateLimit: true},
	{Name: "a-b", SyncScope: true, SyncSelector: true, RateLimit: true},
	{Name: "a.b_c", SyncScope: true, SyncSelector: true, RateLimit: true},
	{Name: "a!b", SyncScope: true, SyncSelector: true},
	{Name: "_a", SyncScope: true, SyncSelector: true},
	{Name: "Ünï", SyncScope: true, SyncSelector: true},
	{Name: "*", SyncScope: true},
	{Name: strings.Repeat("a", 129), RateLimit: true},
	{Name: strings.Repeat("a", 255), RateLimit: true},
	{Name: strings.Repeat("a", 256)},
	{Name: "-a"},
	{Name: "a b"},
	{Name: "a/b"},
	{Name: "a:b"},
	{Name: "a$b"},
	{Name: "a,b"},
	{Name: "a\tb"},
}

// TenantNames are tenant inputs for the sync scope, the sync selector and a
// user; RateLimit is unused, since a rate-limit scope is in its user's
// tenant.
//
//testdouble:shared-double
var TenantNames = []NameCase{
	{Name: "team_a", SyncScope: true, SyncSelector: true, User: true},
	{Name: "a-b", SyncScope: true, SyncSelector: true},
	{Name: "a!b", SyncScope: true, SyncSelector: true},
	{Name: "RGW12345678901234567", SyncScope: true, SyncSelector: true},
	{Name: strings.Repeat("a", 128), SyncScope: true, SyncSelector: true, User: true},
	{Name: strings.Repeat("a", 129)},
	{Name: "-a"},
	{Name: "a b"},
	{Name: "a$b"},
	{Name: "a:b"},
}
