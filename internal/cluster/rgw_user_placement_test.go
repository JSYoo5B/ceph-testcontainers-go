package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type rgwUserPlacementTestContainer struct {
	*rgwPlacementTestContainer
	user       map[string]any
	onModify   func(map[string]any)
	failModify bool
}

func (f *rgwUserPlacementTestContainer) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	flag := func(name string) string {
		for i, arg := range args {
			if arg == name && i+1 < len(args) {
				return args[i+1]
			}
		}
		return ""
	}
	if args[0] != "radosgw-admin" || !slices.Contains(args, "user") {
		code, stream, err := f.rgwPlacementTestContainer.Exec(ctx, args, opts...)
		if err == nil && code == 0 && slices.Contains(args, "zonegroup") && slices.Contains(args, "add") && flag("--tags") != "" {
			target := rgwPlacementTarget(f.group, flag("--placement-id"))
			tags := strings.Split(flag("--tags"), ",")
			values := make([]any, len(tags))
			for i := range tags {
				values[i] = tags[i]
			}
			target["tags"] = values
		}
		return code, stream, err
	}
	f.calls = append(f.calls, slices.Clone(args))
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

func newRGWUserPlacementFixture(t *testing.T) (*RGWContainer, *rgwUserPlacementTestContainer, *RGWPlacement, *RGWUser) {
	t.Helper()
	g, base := newRGWPlacementFixture()
	f := &rgwUserPlacementTestContainer{rgwPlacementTestContainer: base}
	g.Container = f
	g.owner.services = map[string]testcontainers.Container{"rgw": f}
	_ = json.Unmarshal([]byte(rgwAdminFixture("test-user", "PRIVATE-SECRET", false, RGWQuota{Enabled: true, MaxSizeBytes: 98304, MaxObjects: 8})), &f.user)
	f.user["default_placement"] = ""
	f.user["default_storage_class"] = ""
	f.user["placement_tags"] = []string{}
	f.user["op_mask"] = "read, write, delete"
	f.user["caps"] = []map[string]any{{"type": "users", "perm": "read"}}
	f.user["type"] = "rgw"
	f.user["path"] = "/"
	f.user["mfa_ids"] = []string{"device-one"}
	f.user["unknown_future_attribute"] = map[string]any{"nested": "preserve"}
	config := rgwPlacementTestConfig()
	config.Tags = []string{"writer", "premium"}
	p, err := g.CreatePlacement(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	f.calls = nil
	return g, f, p, rgwAdminOwnedTestUser(g)
}

func TestRGWUserPlacementGrantsDefaultsPreservesTagsAndNativePolicy(t *testing.T) {
	g, f, p, user := newRGWUserPlacementFixture(t)
	p.Name, p.ZoneID, p.LocationConstraint = "foreign", "foreign", "foreign:target"
	if err := g.SetUserPlacement(t.Context(), user, p, RGWUserPlacementConfig{StorageClass: "STANDARD_IA", Tags: []string{"writer", "premium"}}); err != nil {
		t.Fatal(err)
	}
	native, err := decodeRGWUser(mustRGWUserPlacementJSON(t, f.user))
	if err != nil || native.info.DefaultPlacement != "tiered" || native.info.DefaultStorageClass != "STANDARD_IA" || !slices.Equal(native.info.PlacementTags, []string{"premium", "writer"}) {
		t.Fatal("requested user placement policy was not read back")
	}
	for _, args := range f.calls {
		if args[0] == "radosgw-admin" && slices.Contains(args, "user") && (!slices.Contains(args, "--zone-id") || slices.Contains(args, "foreign")) {
			t.Fatal("mutable placement descriptor redirected native user operation")
		}
	}
	before := rgwUserUnrelatedPlacementPolicy(native.document)
	if err := g.SetUserPlacement(t.Context(), user, p, RGWUserPlacementConfig{}); err != nil {
		t.Fatal(err)
	}
	after, err := decodeRGWUser(mustRGWUserPlacementJSON(t, f.user))
	if err != nil || after.info.DefaultStorageClass != "" || !slices.Equal(after.info.PlacementTags, []string{"premium", "writer"}) || !reflect.DeepEqual(before, rgwUserUnrelatedPlacementPolicy(after.document)) {
		t.Fatal("nil tags or unrelated type, caps, quotas, keys and native attributes were not preserved")
	}
	if err := g.SetUserPlacement(t.Context(), user, p, RGWUserPlacementConfig{Tags: []string{"unmatched"}}); err != nil {
		t.Fatal(err)
	}
	after, _ = decodeRGWUser(mustRGWUserPlacementJSON(t, f.user))
	if !slices.Equal(after.info.PlacementTags, []string{"unmatched"}) {
		t.Fatal("old grant tags survived exact nonmatching replacement")
	}
}

func TestRGWUserPlacementRejectsMigratedAccountIdentity(t *testing.T) {
	g, f, p, user := newRGWUserPlacementFixture(t)
	f.user["type"], f.user["account_id"] = "root", "RGW12345678901234567"
	before := mustRGWUserPlacementJSON(t, f.user)
	f.calls = nil
	if err := g.SetUserPlacement(t.Context(), user, p, RGWUserPlacementConfig{Tags: []string{"premium"}}); err == nil {
		t.Fatal("ordinary user's original key adopted an externally migrated account root")
	}
	for _, args := range f.calls {
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
	for _, config := range []RGWUserPlacementConfig{
		{Tags: []string{}}, {Tags: []string{""}}, {Tags: []string{"writer,other"}}, {Tags: []string{"writer\n"}}, {Tags: []string{"writer other"}}, {Tags: []string{"same", "same"}}, {StorageClass: "standard_ia"},
	} {
		g, f, p, user := newRGWUserPlacementFixture(t)
		if err := g.SetUserPlacement(t.Context(), user, p, config); err == nil || len(f.calls) != 0 {
			t.Fatal("invalid/unsupported user placement request reached native command")
		}
	}
}

func TestRGWUserPlacementRejectsOwnershipScopePoolMappingAndKeyChanges(t *testing.T) {
	for _, check := range []struct {
		name string
		edit func(*RGWContainer, *rgwUserPlacementTestContainer, *RGWPlacement, *RGWUser)
	}{
		{"foreign user", func(_ *RGWContainer, _ *rgwUserPlacementTestContainer, _ *RGWPlacement, user *RGWUser) {
			user.owner = &Container{}
		}},
		{"foreign scope", func(_ *RGWContainer, _ *rgwUserPlacementTestContainer, _ *RGWPlacement, user *RGWUser) {
			user.scope.Zone = "elsewhere"
		}},
		{"different captured native scope", func(_ *RGWContainer, _ *rgwUserPlacementTestContainer, p *RGWPlacement, user *RGWUser) {
			scope := p.scope
			scope.zoneID = "elsewhere"
			user.state.nativeScope = &scope
		}},
		{"removed user", func(_ *RGWContainer, _ *rgwUserPlacementTestContainer, _ *RGWPlacement, user *RGWUser) {
			user.state.removed = true
		}},
		{"unconfirmed placement", func(_ *RGWContainer, _ *rgwUserPlacementTestContainer, p *RGWPlacement, _ *RGWUser) {
			p.confirmed = false
		}},
		{"foreign placement", func(_ *RGWContainer, _ *rgwUserPlacementTestContainer, p *RGWPlacement, _ *RGWUser) {
			p.owner = &Container{}
		}},
		{"replaced pool", func(_ *RGWContainer, f *rgwUserPlacementTestContainer, _ *RGWPlacement, _ *RGWUser) { f.poolID = 99 }},
		{"changed native scope", func(_ *RGWContainer, f *rgwUserPlacementTestContainer, _ *RGWPlacement, _ *RGWUser) {
			f.zone["id"] = "elsewhere"
		}},
		{"changed mapping", func(_ *RGWContainer, f *rgwUserPlacementTestContainer, _ *RGWPlacement, _ *RGWUser) {
			rgwZonePlacement(f.zone, "tiered")["data_extra_pool"] = "elsewhere"
		}},
		{"changed target tags", func(_ *RGWContainer, f *rgwUserPlacementTestContainer, _ *RGWPlacement, _ *RGWUser) {
			rgwPlacementTarget(f.group, "tiered")["tags"] = []any{"foreign"}
		}},
		{"replaced native keys", func(_ *RGWContainer, f *rgwUserPlacementTestContainer, _ *RGWPlacement, _ *RGWUser) {
			f.user["keys"] = []rgwNativeKey{{AccessKey: "REPLACED", SecretKey: "SECRET"}}
		}},
	} {
		t.Run(check.name, func(t *testing.T) {
			g, f, p, user := newRGWUserPlacementFixture(t)
			check.edit(g, f, p, user)
			if err := g.SetUserPlacement(t.Context(), user, p, RGWUserPlacementConfig{StorageClass: "STANDARD_IA", Tags: []string{"writer"}}); err == nil {
				t.Fatal("changed resource accepted")
			}
			for _, args := range f.calls {
				if args[0] == "radosgw-admin" && slices.Contains(args, "modify") {
					t.Fatal("ownership/preflight failure changed native user")
				}
			}
		})
	}
	// A class with valid spelling still needs a mapping on this exact target.
	g, f, p, user := newRGWUserPlacementFixture(t)
	if err := g.SetUserPlacement(t.Context(), user, p, RGWUserPlacementConfig{StorageClass: "COLD"}); err == nil {
		t.Fatal("absent class accepted")
	}
	for _, args := range f.calls {
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
		g, f, p, user := newRGWUserPlacementFixture(t)
		f.onModify = edit
		if err := g.SetUserPlacement(t.Context(), user, p, RGWUserPlacementConfig{StorageClass: "STANDARD_IA", Tags: []string{"writer"}}); err == nil || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatalf("native drift was hidden or keys were exposed: %v", err)
		}
	}
	g, f, p, user := newRGWUserPlacementFixture(t)
	f.failModify = true
	if err := g.SetUserPlacement(t.Context(), user, p, RGWUserPlacementConfig{Tags: []string{"writer"}}); err == nil || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("native failure was hidden or keys were exposed: %v", err)
	}
}

func TestRGWFreshPlacementTagsAreCopiedAndGuarded(t *testing.T) {
	g, _, p, _ := newRGWUserPlacementFixture(t)
	state, err := g.PlacementStatus(t.Context(), p)
	if err != nil || !slices.Equal(state.Tags, []string{"premium", "writer"}) {
		t.Fatal("fresh target native tags missing")
	}
	state.Tags[0] = "foreign"
	state, err = g.PlacementStatus(t.Context(), p)
	if err != nil || !slices.Equal(state.Tags, []string{"premium", "writer"}) {
		t.Fatal("target tag status aliases private configuration")
	}
	if err := g.ApplyPlacement(t.Context(), p); err != nil {
		t.Fatal(err)
	}
}

func TestRGWUserPlacementUsesRunningScopeAfterNativeDefaultsChange(t *testing.T) {
	g, f, p, user := newRGWUserPlacementFixture(t)
	f.foreignDefault = true
	if err := g.SetUserPlacement(t.Context(), user, p, RGWUserPlacementConfig{StorageClass: "STANDARD_IA", Tags: []string{"premium"}}); err != nil {
		t.Fatal(err)
	}
	for _, args := range f.calls {
		if args[0] == "radosgw-admin" && slices.Contains(args, "user") && !slices.Contains(args, "--zonegroup-id") {
			t.Fatal("user placement operation resolved changed native defaults")
		}
	}
}

func TestRGWUserPlacementPreservesCancellationBeforeNativeCalls(t *testing.T) {
	g, f, p, user := newRGWUserPlacementFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := g.SetUserPlacement(ctx, user, p, RGWUserPlacementConfig{Tags: []string{"writer"}}); !errors.Is(err, context.Canceled) || len(f.calls) != 0 {
		t.Fatalf("canceled request ran native commands or lost context error: %v", err)
	}
}
