package ceph

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type rgwAccountTestContainer struct {
	*rgwPlacementTestContainer
	users          map[string]map[string]any
	account        map[string]any
	versionTag     string
	missingTag     bool
	failRemove     bool
	unrelatedQuota bool
}

func (f *rgwAccountTestContainer) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if args[0] != "radosgw-admin" {
		return f.rgwPlacementTestContainer.Exec(ctx, args, opts...)
	}
	f.calls = append(f.calls, slices.Clone(args))
	flag := func(name string) string {
		for i, arg := range args {
			if arg == name && i+1 < len(args) {
				return args[i+1]
			}
		}
		return ""
	}
	var result any
	code := 0
	switch {
	case slices.Contains(args, "metadata"):
		tag := f.versionTag
		if f.missingTag {
			tag = ""
		}
		result = map[string]any{"key": "account:" + f.account["id"].(string), "ver": map[string]any{"tag": tag, "ver": 2}, "data": f.account}
	case slices.Contains(args, "account") && !slices.Contains(args, "quota"):
		switch {
		case slices.Contains(args, "list"):
			ids := []string{}
			if f.account != nil {
				ids = append(ids, f.account["id"].(string))
			}
			result = ids
		case slices.Contains(args, "create"):
			f.account = map[string]any{"id": flag("--account-id"), "name": flag("--account-name"), "tenant": flag("--tenant"), "email": flag("--email"), "max_users": 1000, "max_roles": 1000, "max_groups": 1000, "max_buckets": 1000, "max_access_keys": 4, "quota": map[string]any{"enabled": false, "max_size": -1, "max_objects": -1}, "bucket_quota": map[string]any{"enabled": false, "max_size": -1, "max_objects": -1}}
			if flag("--max-buckets") != "" {
				value, _ := strconv.Atoi(flag("--max-buckets"))
				f.account["max_buckets"] = value
			}
			result = f.account
		case slices.Contains(args, "rm"):
			if f.failRemove {
				code = 13
				result = "PRIVATE-SECRET native nonempty account"
			} else {
				f.account = nil
			}
		}
	case slices.Contains(args, "quota") && flag("--account-id") != "":
		field := "quota"
		if flag("--quota-scope") == "bucket" {
			field = "bucket_quota"
		}
		quota := f.account[field].(map[string]any)
		if slices.Contains(args, "set") {
			rawSize := flag("--max-size")
			size, _ := strconv.ParseInt(strings.TrimRight(rawSize, "BK"), 10, 64)
			if strings.HasSuffix(rawSize, "K") {
				size *= 1024
			}
			// Native account quota chooses the int64_t overload. Both -1 byte
			// and -1 KiB truncate to zero, so the disabled size needs -2 KiB.
			size = ((size + 1023) / 1024) * 1024
			if size < -1 {
				size = -1
			}
			objects, _ := strconv.ParseInt(flag("--max-objects"), 10, 64)
			quota["max_size"], quota["max_objects"] = size, objects
		} else {
			quota["enabled"] = slices.Contains(args, "enable")
		}
		if f.unrelatedQuota {
			f.account["max_roles"] = 2
		}
		result = f.account
	case slices.Contains(args, "user") && slices.Contains(args, "list"):
		ids := []string{}
		for id := range f.users {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		result = ids
	case slices.Contains(args, "user") && slices.Contains(args, "create"):
		id := flag("--uid")
		local := id
		if _, right, found := strings.Cut(id, "$"); found {
			local = right
		}
		var user map[string]any
		_ = json.Unmarshal([]byte(rgwAdminFixture(local, "SECRET-"+id, false, RGWQuota{MaxSizeBytes: -1, MaxObjects: -1})), &user)
		user["keys"] = []rgwNativeKey{{AccessKey: "ACCESS-" + id, SecretKey: "SECRET-" + id}}
		// Native CLI RGWUserInfo::dump emits canonical user_id without separate
		// tenant/full_user_id fields; Admin Ops uses a different serializer.
		user["user_id"], user["type"], user["account_id"] = id, "rgw", ""
		if slices.Contains(args, "--account-root") {
			user["type"], user["account_id"] = "root", flag("--account-id")
		}
		user["display_name"] = flag("--display-name")
		f.users[id] = user
		result = user
	default:
		id := flag("--uid")
		user := f.users[id]
		if user == nil {
			code = 2
			result = "PRIVATE-SECRET missing user"
			break
		}
		switch {
		case slices.Contains(args, "rm"):
			delete(f.users, id)
		case slices.Contains(args, "suspend"):
			user["suspended"] = 1
		case slices.Contains(args, "user") && slices.Contains(args, "enable") && !slices.Contains(args, "quota"):
			user["suspended"] = 0
		case slices.Contains(args, "caps"):
			user["caps"] = []RGWAdminCapability{{Type: "users", Permission: "read"}}
		case slices.Contains(args, "quota"):
			field := "user_quota"
			if flag("--quota-scope") == "bucket" {
				field = "bucket_quota"
			}
			quota := user[field].(map[string]any)
			if slices.Contains(args, "set") {
				rawSize := flag("--max-size")
				size, _ := strconv.ParseInt(strings.TrimRight(rawSize, "BK"), 10, 64)
				if strings.HasSuffix(rawSize, "K") {
					size *= 1024
				}
				// Native set_quota_info explicitly normalizes any negative value,
				// unlike the account modify path's pre-clamp signed rounding.
				if size < 0 {
					size = -1
				} else {
					size = ((size + 1023) / 1024) * 1024
				}
				objects, _ := strconv.ParseInt(flag("--max-objects"), 10, 64)
				quota["max_size"], quota["max_objects"] = size, objects
			} else {
				quota["enabled"] = slices.Contains(args, "enable")
			}
		}
		result = user
	}
	data, _ := json.Marshal(result)
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	if code != 0 {
		header[0] = byte(stdcopy.Stderr)
	}
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.Write(data)
	return code, &stream, nil
}

func newRGWAccountTestGateway() (*RGWContainer, *rgwAccountTestContainer) {
	g, base := newRGWPlacementFixture()
	f := &rgwAccountTestContainer{rgwPlacementTestContainer: base, users: map[string]map[string]any{}, versionTag: "native-account-lifetime"}
	g.Container = f
	g.owner.services = map[string]testcontainers.Container{"rgw": f}
	return g, f
}

func TestRGWTenantUsersPreserveCanonicalIDForEveryOperation(t *testing.T) {
	g, f := newRGWAccountTestGateway()
	users := make([]*RGWUser, 0, 3)
	for _, tenant := range []string{"", "team_a", "team_b"} {
		user, err := g.CreateUser(t.Context(), RGWUserConfig{ID: "same", Tenant: tenant, AdminCaps: "users=read"})
		if err != nil {
			t.Fatal(err)
		}
		if user.ID() != rgwFullUserID(tenant, "", "same") {
			t.Fatal("canonical ID missing tenant")
		}
		users = append(users, user)
	}
	if _, err := g.CreateUser(t.Context(), RGWUserConfig{ID: "same", Tenant: "team_a"}); err == nil {
		t.Fatal("existing tenanted user accepted")
	}
	f.calls = nil
	for i, user := range users {
		quota := RGWQuota{Enabled: true, MaxSizeBytes: 1024, MaxObjects: int64(i + 1)}
		if err := g.SetUserQuota(t.Context(), user, quota); err != nil {
			t.Fatal(err)
		}
		if err := g.SetBucketQuota(t.Context(), user, quota); err != nil {
			t.Fatal(err)
		}
		unlimited := RGWQuota{MaxSizeBytes: -1, MaxObjects: -1}
		if err := g.SetUserQuota(t.Context(), user, unlimited); err != nil {
			t.Fatal(err)
		}
		if err := g.SetBucketQuota(t.Context(), user, unlimited); err != nil {
			t.Fatal(err)
		}
		info, err := g.UserInfo(t.Context(), user)
		if err != nil || info.UserQuota != unlimited || info.BucketQuota != unlimited {
			t.Fatal("ordinary or tenant negative user/bucket limits changed to zero")
		}
		if err := g.SetUserQuota(t.Context(), user, quota); err != nil {
			t.Fatal(err)
		}
		if err := g.SetBucketQuota(t.Context(), user, quota); err != nil {
			t.Fatal(err)
		}
		if err := g.SuspendUser(t.Context(), user, true); err != nil {
			t.Fatal(err)
		}
		info, err = g.UserInfo(t.Context(), user)
		if err != nil || info.ID != user.ID() || info.LocalID != "same" || info.UserQuota != quota || !info.Suspended {
			t.Fatal("tenant user native policy wrong")
		}
		if err := g.SuspendUser(t.Context(), user, false); err != nil {
			t.Fatal(err)
		}
		if err := g.RemoveUser(t.Context(), user); err != nil {
			t.Fatal(err)
		}
		if err := g.RemoveUser(t.Context(), user); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.users) != 0 {
		t.Fatal("tenant cleanup left another user")
	}
	for _, args := range f.calls {
		for i, arg := range args {
			if arg == "--uid" && strings.Contains(args[i+1], "team_") && !strings.Contains(args[i+1], "$") {
				t.Fatal("tenant user method used local ID")
			}
		}
	}
}

func TestRGWTenantDecoderRejectsConflictingFullIdentity(t *testing.T) {
	data := []byte(rgwAdminFixture("same", "PRIVATE-SECRET", false, RGWQuota{MaxSizeBytes: -1, MaxObjects: -1}))
	var user map[string]any
	_ = json.Unmarshal(data, &user)
	user["tenant"], user["full_user_id"] = "team_a", "team_b$same"
	data, _ = json.Marshal(user)
	if _, err := decodeRGWUser(data); err == nil {
		t.Fatal("mismatched canonical identity accepted")
	}
	user["full_user_id"], user["namespace"] = "team_a$space$same", "space"
	data, _ = json.Marshal(user)
	if native, err := decodeRGWUser(data); err != nil || native.info.ID != "team_a$space$same" {
		t.Fatal("native namespace identity decoded incorrectly")
	}
}

func TestRGWTenantDecoderNativeCLICanonicalAndSeparateComponents(t *testing.T) {
	for _, test := range []struct {
		id, tenant, namespace, local string
	}{
		{"same", "", "", "same"},
		{"team_a$same", "team_a", "", "same"},
		{"team_a$space$same", "team_a", "space", "same"},
		{"$space$same", "", "space", "same"},
	} {
		var user map[string]any
		_ = json.Unmarshal([]byte(rgwAdminFixture(test.id, "PRIVATE-SECRET", false, RGWQuota{MaxSizeBytes: -1, MaxObjects: -1})), &user)
		native, err := decodeRGWUser(mustRGWUserPlacementJSON(t, user))
		if err != nil || native.info.ID != test.id || native.info.Tenant != test.tenant || native.info.Namespace != test.namespace || native.info.LocalID != test.local {
			t.Fatal("native CLI canonical ID components were not decoded")
		}
		// Separate fields may be supplied, but cannot contradict the canonical ID.
		user["tenant"], user["namespace"], user["full_user_id"] = test.tenant, test.namespace, test.id
		if _, err := decodeRGWUser(mustRGWUserPlacementJSON(t, user)); err != nil {
			t.Fatal(err)
		}
		user["tenant"] = "other"
		if _, err := decodeRGWUser(mustRGWUserPlacementJSON(t, user)); err == nil {
			t.Fatal("conflicting canonical and separate tenant accepted")
		}
		user["tenant"], user["namespace"] = test.tenant, "other"
		if _, err := decodeRGWUser(mustRGWUserPlacementJSON(t, user)); err == nil {
			t.Fatal("conflicting canonical and separate namespace accepted")
		}
	}
}

func TestRGWAccountFreshRootQuotaAndNonpurgeLifecycle(t *testing.T) {
	g, f := newRGWAccountTestGateway()
	a, err := g.CreateAccount(t.Context(), RGWAccountConfig{ID: "RGW12345678901234567", Name: "fixture account", Tenant: "account_team"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.CreateAccount(t.Context(), RGWAccountConfig{ID: a.ID()}); err == nil {
		t.Fatal("existing account adopted")
	}
	root, err := g.CreateAccountRootUser(t.Context(), a, RGWUserConfig{ID: "fixture-root", DisplayName: "FixtureRoot"})
	if err != nil {
		t.Fatal(err)
	}
	info, err := g.UserInfo(t.Context(), root)
	if err != nil || info.AccountID != a.ID() || info.Type != "root" || info.Tenant != "account_team" || info.Admin || info.System {
		t.Fatal("root account identity wrong")
	}
	q := RGWQuota{Enabled: true, MaxSizeBytes: 2048, MaxObjects: 3}
	if err := g.SetAccountQuota(t.Context(), a, q); err != nil {
		t.Fatal(err)
	}
	if err := g.SetAccountBucketQuota(t.Context(), a, q); err != nil {
		t.Fatal(err)
	}
	account, err := g.AccountInfo(t.Context(), a)
	if err != nil || account.AccountQuota != q || account.BucketQuota != q {
		t.Fatal("account quota not read back")
	}
	q = RGWQuota{Enabled: true, MaxSizeBytes: -1, MaxObjects: 1}
	if err := g.SetAccountQuota(t.Context(), a, q); err != nil {
		t.Fatal(err)
	}
	account, err = g.AccountInfo(t.Context(), a)
	if err != nil || account.AccountQuota != q {
		t.Fatal("disabled account size was rounded to zero")
	}
	var disabledSizeArgument bool
	for _, args := range f.calls {
		if slices.Contains(args, "quota") && slices.Contains(args, "set") && slices.Contains(args, "-2K") {
			disabledSizeArgument = true
		}
	}
	if !disabledSizeArgument {
		t.Fatal("disabled account size did not use native negative KiB")
	}
	if err := g.SetUserQuota(t.Context(), root, q); err == nil {
		t.Fatal("ineffective per-user account quota accepted")
	}
	f.failRemove = true
	if err := g.RemoveAccount(t.Context(), a); err == nil || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("nonempty account removal hidden or secret leaked")
	}
	if _, err := g.UserInfo(t.Context(), root); err != nil {
		t.Fatal("failed account removal changed root")
	}
	if err := g.RemoveUser(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	f.failRemove = false
	copyHandle := *a
	if err := g.RemoveAccount(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	if err := g.RemoveAccount(t.Context(), &copyHandle); err != nil {
		t.Fatal(err)
	}
	for _, args := range f.calls {
		if slices.Contains(args, "--purge-data") || slices.Contains(args, "--yes-i-really-mean-it") {
			t.Fatal("account helper used native destructive bypass")
		}
	}
}

func TestRGWAccountRejectsUnconfirmedForeignAndReplacedLifetimes(t *testing.T) {
	for _, edit := range []func(*RGWAccount, *rgwAccountTestContainer){
		func(a *RGWAccount, _ *rgwAccountTestContainer) { a.owner = &Container{} },
		func(a *RGWAccount, _ *rgwAccountTestContainer) { a.state.confirmed = false },
		func(_ *RGWAccount, f *rgwAccountTestContainer) { f.versionTag = "replacement-lifetime" },
		func(_ *RGWAccount, f *rgwAccountTestContainer) { f.account["tenant"] = "different" },
	} {
		g, f := newRGWAccountTestGateway()
		a, err := g.CreateAccount(t.Context(), RGWAccountConfig{Name: "account"})
		if err != nil {
			t.Fatal(err)
		}
		edit(a, f)
		f.calls = nil
		if err := g.SetAccountQuota(t.Context(), a, RGWQuota{MaxSizeBytes: -1, MaxObjects: 1}); err == nil {
			t.Fatal("foreign/replaced account mutated")
		}
		for _, args := range f.calls {
			if slices.Contains(args, "quota") {
				t.Fatal("failed ownership changed quota")
			}
		}
	}
	g, f := newRGWAccountTestGateway()
	f.missingTag = true
	a, err := g.CreateAccount(t.Context(), RGWAccountConfig{Name: "partial"})
	if err == nil || a == nil || a.state.confirmed {
		t.Fatal("missing metadata lifetime adopted")
	}
	if err := g.RemoveAccount(t.Context(), a); err == nil {
		t.Fatal("unconfirmed account deleted")
	}
}

func TestRGWAccountValidationAndQuotaPreservation(t *testing.T) {
	for _, tenant := range []string{"bad$tenant", "bad:tenant", "bad-tenant", "RGW12345678901234567"} {
		g, f := newRGWAccountTestGateway()
		if _, err := g.CreateUser(t.Context(), RGWUserConfig{ID: "same", Tenant: tenant}); err == nil || len(f.calls) != 0 {
			t.Fatal("invalid tenant reached native command")
		}
	}
	g, f := newRGWAccountTestGateway()
	a, err := g.CreateAccount(t.Context(), RGWAccountConfig{Name: "account", Tenant: "team"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.CreateAccountRootUser(t.Context(), a, RGWUserConfig{ID: "root", Tenant: "other"}); err == nil {
		t.Fatal("cross-tenant account membership accepted")
	}
	f.calls = nil
	if err := g.SetAccountQuota(t.Context(), a, RGWQuota{MaxSizeBytes: 1, MaxObjects: 1}); err == nil || len(f.calls) != 0 {
		t.Fatal("native KiB rounding silently changed caller byte limit")
	}
	f.unrelatedQuota = true
	if err := g.SetAccountQuota(t.Context(), a, RGWQuota{Enabled: true, MaxSizeBytes: 1024, MaxObjects: 1}); err == nil {
		t.Fatal("unrelated account policy drift hidden")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	f.calls = nil
	if _, err := g.CreateAccount(ctx, RGWAccountConfig{Name: "cancelled"}); !errors.Is(err, context.Canceled) || len(f.calls) != 0 {
		t.Fatal("canceled account creation lost context")
	}
	if _, err := g.AccountInfo(ctx, a); !errors.Is(err, context.Canceled) || len(f.calls) != 0 {
		t.Fatal("canceled account info reached native command")
	}
	if err := g.SetAccountQuota(ctx, a, RGWQuota{MaxSizeBytes: -1, MaxObjects: -1}); !errors.Is(err, context.Canceled) || len(f.calls) != 0 {
		t.Fatal("canceled account quota reached native command")
	}
	if _, err := g.CreateAccountRootUser(ctx, a, RGWUserConfig{ID: "cancelled"}); !errors.Is(err, context.Canceled) || len(f.calls) != 0 {
		t.Fatal("canceled account root creation reached native command")
	}
	if err := g.RemoveAccount(ctx, a); !errors.Is(err, context.Canceled) || len(f.calls) != 0 {
		t.Fatal("canceled account removal reached native command")
	}
}
