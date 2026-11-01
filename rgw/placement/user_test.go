package placement

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type rgwUserPlacementTestContainer struct {
	*gwtest.PlacementContainer
	user       map[string]any
	onModify   func(map[string]any)
	failModify bool
}

func (f *rgwUserPlacementTestContainer) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	original := args
	args = gwtest.Argv(args)
	flag := gwtest.Flag(args)
	if args[0] != "radosgw-admin" || !slices.Contains(args, "user") {
		code, stream, err := f.PlacementContainer.Exec(ctx, original, opts...)
		if err == nil && code == 0 && slices.Contains(args, "zonegroup") && slices.Contains(args, "add") && flag("--tags") != "" {
			target := groupTarget(f.Group, flag("--placement-id"))
			tags := strings.Split(flag("--tags"), ",")
			values := make([]any, len(tags))
			for i := range tags {
				values[i] = tags[i]
			}
			target["tags"] = values
		}
		return code, stream, err
	}
	f.Calls = append(f.Calls, slices.Clone(args))
	if flag("--uid") != "test-user" || flag("--zonegroup-id") != "group-id" || flag("--zone-id") != "zone-id" {
		return 0, nil, errors.New("unexpected native user scope")
	}
	code := 0
	var data []byte
	if slices.Contains(args, "modify") {
		if f.failModify {
			code = 1
			data = []byte("PRIVATE-ACCESS PRIVATE-SECRET modify failed")
		} else {
			f.user["default_placement"] = flag("--placement-id")
			f.user["default_storage_class"] = flag("--storage-class")
			if flag("--storage-class") == "STANDARD" {
				// Native rgw_placement_rule persists STANDARD without a suffix,
				// then from_str clears the raw class on a later user info read.
				f.user["default_storage_class"] = ""
			}
			if flag("--tags") != "" {
				f.user["placement_tags"] = strings.Split(flag("--tags"), ",")
			}
			if f.onModify != nil {
				f.onModify(f.user)
			}
		}
	}
	if code == 0 {
		data, _ = json.Marshal(f.user)
	}
	return gwtest.Reply(original, code, gwtest.Stream(code != 0, data))
}

func newRGWUserPlacementFixture(t *testing.T) (*gwtest.Gateway, *rgwUserPlacementTestContainer, *Placement, *gwbase.User) {
	t.Helper()
	g, base := newRGWPlacementFixture()
	f := &rgwUserPlacementTestContainer{PlacementContainer: base}
	g.Replace(f)
	_ = json.Unmarshal([]byte(gwtest.UserDocument("test-user", "PRIVATE-SECRET", false, gwbase.Quota{Enabled: true, MaxSizeBytes: 98304, MaxObjects: 8})), &f.user)
	f.user["default_placement"] = ""
	f.user["default_storage_class"] = ""
	f.user["placement_tags"] = []string{}
	f.user["op_mask"] = "read, write, delete"
	f.user["caps"] = []map[string]any{{"type": "users", "perm": "read"}}
	f.user["type"] = "rgw"
	f.user["path"] = "/"
	f.user["mfa_ids"] = []string{"device-one"}
	f.user["unknown_future_attribute"] = map[string]any{"nested": "preserve"}
	config := testConfig()
	config.Tags = []string{"writer", "premium"}
	p, err := create(t.Context(), g.Core, config)
	if err != nil {
		t.Fatal(err)
	}
	f.Calls = nil
	user := &gwbase.User{Origin: g.Core, ID: "test-user", AccessKey: "PRIVATE-ACCESS", SecretKey: "PRIVATE-SECRET",
		State: &gwbase.UserState{Created: true, Identity: &gwbase.CreationIdentity{OriginalType: "rgw"}}}
	return g, f, p, user
}

func TestRGWUserPlacementGrantsDefaultsPreservesTagsAndNativePolicy(t *testing.T) {
	_, f, p, user := newRGWUserPlacementFixture(t)
	p.Name, p.ZoneID, p.LocationConstraint = "foreign", "foreign", "foreign:target"
	if err := setUserDefault(t.Context(), p, user, UserDefault{StorageClass: "STANDARD_IA", Tags: []string{"writer", "premium"}}); err != nil {
		t.Fatal(err)
	}
	native, err := gwbase.DecodeUser(mustRGWUserPlacementJSON(t, f.user))
	if err != nil || native.Info.DefaultPlacement != "tiered" || native.Info.DefaultStorageClass != "STANDARD_IA" || !slices.Equal(native.Info.PlacementTags, []string{"premium", "writer"}) {
		t.Fatal("requested user placement policy was not read back")
	}
	for _, args := range f.Calls {
		if args[0] == "radosgw-admin" && slices.Contains(args, "user") && (!slices.Contains(args, "--zone-id") || slices.Contains(args, "foreign")) {
			t.Fatal("mutable placement descriptor redirected native user operation")
		}
	}
	before := unrelatedUserPolicy(native.Document)
	if err := setUserDefault(t.Context(), p, user, UserDefault{}); err != nil {
		t.Fatal(err)
	}
	after, err := gwbase.DecodeUser(mustRGWUserPlacementJSON(t, f.user))
	if err != nil || after.Info.DefaultStorageClass != "" || !slices.Equal(after.Info.PlacementTags, []string{"premium", "writer"}) || !reflect.DeepEqual(before, unrelatedUserPolicy(after.Document)) {
		t.Fatal("nil tags or unrelated type, caps, quotas, keys and native attributes were not preserved")
	}
	if err := setUserDefault(t.Context(), p, user, UserDefault{Tags: []string{"unmatched"}}); err != nil {
		t.Fatal(err)
	}
	after, _ = gwbase.DecodeUser(mustRGWUserPlacementJSON(t, f.user))
	if !slices.Equal(after.Info.PlacementTags, []string{"unmatched"}) {
		t.Fatal("old grant tags survived exact nonmatching replacement")
	}
}

func TestRGWUserPlacementRejectsMigratedAccountIdentity(t *testing.T) {
	_, f, p, user := newRGWUserPlacementFixture(t)
	f.user["type"], f.user["account_id"] = "root", "RGW12345678901234567"
	before := mustRGWUserPlacementJSON(t, f.user)
	f.Calls = nil
	if err := setUserDefault(t.Context(), p, user, UserDefault{Tags: []string{"premium"}}); err == nil {
		t.Fatal("ordinary user's original key adopted an externally migrated account root")
	}
	for _, args := range f.Calls {
		if slices.Contains(args, "modify") {
			t.Fatal("account migration reached native placement mutation")
		}
	}
	if !bytes.Equal(before, mustRGWUserPlacementJSON(t, f.user)) {
		t.Fatal("rejected account migration changed native user policy")
	}
}

func mustRGWUserPlacementJSON(t *testing.T, document map[string]any) []byte {
	t.Helper()
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal("encode native fixture")
	}
	return data
}

func TestRGWUserPlacementRejectsInvalidConfigurationBeforeNativeCalls(t *testing.T) {
	for _, config := range []UserDefault{
		{Tags: []string{}}, {Tags: []string{""}}, {Tags: []string{"writer,other"}}, {Tags: []string{"writer\n"}}, {Tags: []string{"writer other"}}, {Tags: []string{"same", "same"}}, {StorageClass: "standard_ia"},
	} {
		_, f, p, user := newRGWUserPlacementFixture(t)
		if err := setUserDefault(t.Context(), p, user, config); err == nil || len(f.Calls) != 0 {
			t.Fatal("invalid/unsupported user placement request reached native command")
		}
	}
}

func TestRGWUserPlacementRejectsOwnershipScopePoolMappingAndKeyChanges(t *testing.T) {
	for _, check := range []struct {
		name string
		edit func(*gwtest.Gateway, *rgwUserPlacementTestContainer, *Placement, *gwbase.User)
	}{
		{"foreign user", func(_ *gwtest.Gateway, _ *rgwUserPlacementTestContainer, _ *Placement, user *gwbase.User) {
			user.Origin = gwtest.Foreign()
		}},
		{"foreign scope", func(g *gwtest.Gateway, _ *rgwUserPlacementTestContainer, _ *Placement, user *gwbase.User) {
			user.Origin = gwbase.NewCore(gwbase.Params{Cluster: g.Cluster, Name: "default", Scope: gwbase.Scope{Zone: "elsewhere"},
				Gateways: func() []*gwbase.Core { return g.Registered }})
		}},
		{"different captured native scope", func(_ *gwtest.Gateway, _ *rgwUserPlacementTestContainer, p *Placement, user *gwbase.User) {
			scope := p.scope
			scope.ZoneID = "elsewhere"
			user.State.NativeScope = &scope
		}},
		{"removed user", func(_ *gwtest.Gateway, _ *rgwUserPlacementTestContainer, _ *Placement, user *gwbase.User) {
			user.State.Removed = true
		}},
		{"unconfirmed placement", func(_ *gwtest.Gateway, _ *rgwUserPlacementTestContainer, p *Placement, _ *gwbase.User) {
			p.confirmed = false
		}},
		{"foreign placement", func(_ *gwtest.Gateway, _ *rgwUserPlacementTestContainer, p *Placement, _ *gwbase.User) {
			p.origin = gwbase.NewOrigin(gwtest.Foreign())
		}},
		{"replaced pool", func(_ *gwtest.Gateway, f *rgwUserPlacementTestContainer, _ *Placement, _ *gwbase.User) { f.PoolID = 99 }},
		{"changed native scope", func(_ *gwtest.Gateway, f *rgwUserPlacementTestContainer, _ *Placement, _ *gwbase.User) {
			f.Zone["id"] = "elsewhere"
		}},
		{"changed mapping", func(_ *gwtest.Gateway, f *rgwUserPlacementTestContainer, _ *Placement, _ *gwbase.User) {
			zonePlacement(f.Zone, "tiered")["data_extra_pool"] = "elsewhere"
		}},
		{"changed target tags", func(_ *gwtest.Gateway, f *rgwUserPlacementTestContainer, _ *Placement, _ *gwbase.User) {
			groupTarget(f.Group, "tiered")["tags"] = []any{"foreign"}
		}},
		{"replaced native keys", func(_ *gwtest.Gateway, f *rgwUserPlacementTestContainer, _ *Placement, _ *gwbase.User) {
			f.user["keys"] = []gwbase.NativeKey{{AccessKey: "REPLACED", SecretKey: "SECRET"}}
		}},
	} {
		t.Run(check.name, func(t *testing.T) {
			g, f, p, user := newRGWUserPlacementFixture(t)
			check.edit(g, f, p, user)
			if err := setUserDefault(t.Context(), p, user, UserDefault{StorageClass: "STANDARD_IA", Tags: []string{"writer"}}); err == nil {
				t.Fatal("changed resource accepted")
			}
			for _, args := range f.Calls {
				if args[0] == "radosgw-admin" && slices.Contains(args, "modify") {
					t.Fatal("ownership/preflight failure changed native user")
				}
			}
		})
	}
	// A class with valid spelling still needs a mapping on this exact target.
	_, f, p, user := newRGWUserPlacementFixture(t)
	if err := setUserDefault(t.Context(), p, user, UserDefault{StorageClass: "COLD"}); err == nil {
		t.Fatal("absent class accepted")
	}
	for _, args := range f.Calls {
		if slices.Contains(args, "modify") {
			t.Fatal("unmapped class changed native user")
		}
	}
}

func TestRGWUserPlacementReportsNativeReadbackDriftAndRedactsFailures(t *testing.T) {
	for _, edit := range []func(map[string]any){
		func(user map[string]any) { user["default_storage_class"] = "STANDARD" },
		func(user map[string]any) { user["placement_tags"] = []string{"foreign"} },
		func(user map[string]any) { user["op_mask"] = "read" },
		func(user map[string]any) { user["type"] = "root" },
		func(user map[string]any) { user["unknown_future_attribute"] = nil },
	} {
		_, f, p, user := newRGWUserPlacementFixture(t)
		f.onModify = edit
		if err := setUserDefault(t.Context(), p, user, UserDefault{StorageClass: "STANDARD_IA", Tags: []string{"writer"}}); err == nil || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatalf("native drift was hidden or keys were exposed: %v", err)
		}
	}
	_, f, p, user := newRGWUserPlacementFixture(t)
	f.failModify = true
	if err := setUserDefault(t.Context(), p, user, UserDefault{Tags: []string{"writer"}}); err == nil || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("native failure was hidden or keys were exposed: %v", err)
	}
}

func TestRGWFreshPlacementTagsAreCopiedAndGuarded(t *testing.T) {
	g, _, p, _ := newRGWUserPlacementFixture(t)
	state, err := p.Status(t.Context())
	if err != nil || !slices.Equal(state.Tags, []string{"premium", "writer"}) {
		t.Fatal("fresh target native tags missing")
	}
	state.Tags[0] = "foreign"
	state, err = p.Status(t.Context())
	if err != nil || !slices.Equal(state.Tags, []string{"premium", "writer"}) {
		t.Fatal("target tag status aliases private configuration")
	}
	if err := apply(t.Context(), g.Core, p); err != nil {
		t.Fatal(err)
	}
}

func TestRGWUserPlacementUsesRunningScopeAfterNativeDefaultsChange(t *testing.T) {
	_, f, p, user := newRGWUserPlacementFixture(t)
	f.ForeignDefault = true
	if err := setUserDefault(t.Context(), p, user, UserDefault{StorageClass: "STANDARD_IA", Tags: []string{"premium"}}); err != nil {
		t.Fatal(err)
	}
	for _, args := range f.Calls {
		if args[0] == "radosgw-admin" && slices.Contains(args, "user") && !slices.Contains(args, "--zonegroup-id") {
			t.Fatal("user placement operation resolved changed native defaults")
		}
	}
}

func TestRGWUserPlacementPreservesCancellationBeforeNativeCalls(t *testing.T) {
	_, f, p, user := newRGWUserPlacementFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := setUserDefault(ctx, p, user, UserDefault{Tags: []string{"writer"}}); !errors.Is(err, context.Canceled) || len(f.Calls) != 0 {
		t.Fatalf("canceled request ran native commands or lost context error: %v", err)
	}
}
