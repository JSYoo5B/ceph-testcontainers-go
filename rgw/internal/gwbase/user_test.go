package gwbase_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
)

func userJSON(t *testing.T, document map[string]any) []byte {
	t.Helper()
	data, err := json.Marshal(document)
	if err != nil {
		t.Fatal("encode native fixture")
	}
	return data
}

// The user configuration half of this test is in rgw
// (TestRGWUserConfigRejectsInvalidIdentityAndCapabilities).
func TestDecodeUserAcceptsModernAndLegacyLimits(t *testing.T) {
	modern := gwtest.UserDocument("test-user", "PRIVATE-SECRET", false, gwbase.Quota{Enabled: true, MaxSizeBytes: 987654, MaxObjects: 3})
	native, err := gwbase.DecodeUser([]byte(modern))
	if err != nil || native.Info.UserQuota.MaxSizeBytes != 987654 || native.Info.Suspended {
		t.Fatal("modern RGW policy was not decoded")
	}
	quota, err := gwbase.DecodeQuota([]byte(`{"enabled":true,"max_size_kb":1024,"max_objects":-1}`))
	if err != nil || quota != (gwbase.Quota{Enabled: true, MaxSizeBytes: 1048576, MaxObjects: -1}) {
		t.Fatal("legacy quota KiB was not converted to bytes")
	}
	for _, data := range []string{`{"max_objects":1}`, `{"max_size_kb":9223372036854775807,"max_objects":1}`, `{"PRIVATE-SECRET":"invalid"}`} {
		if _, err := gwbase.DecodeQuota([]byte(data)); err == nil || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatal("invalid quota accepted or secret included in decode error")
		}
	}
}

func TestRGWTenantDecoderRejectsConflictingFullIdentity(t *testing.T) {
	data := []byte(gwtest.UserDocument("same", "PRIVATE-SECRET", false, gwbase.Quota{MaxSizeBytes: -1, MaxObjects: -1}))
	var user map[string]any
	_ = json.Unmarshal(data, &user)
	user["tenant"], user["full_user_id"] = "team_a", "team_b$same"
	data, _ = json.Marshal(user)
	if _, err := gwbase.DecodeUser(data); err == nil {
		t.Fatal("mismatched canonical identity accepted")
	}
	user["full_user_id"], user["namespace"] = "team_a$space$same", "space"
	data, _ = json.Marshal(user)
	if native, err := gwbase.DecodeUser(data); err != nil || native.Info.ID != "team_a$space$same" {
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
		_ = json.Unmarshal([]byte(gwtest.UserDocument(test.id, "PRIVATE-SECRET", false, gwbase.Quota{MaxSizeBytes: -1, MaxObjects: -1})), &user)
		native, err := gwbase.DecodeUser(userJSON(t, user))
		if err != nil || native.Info.ID != test.id || native.Info.Tenant != test.tenant || native.Info.Namespace != test.namespace || native.Info.LocalID != test.local {
			t.Fatal("native CLI canonical ID components were not decoded")
		}
		// Separate fields may be supplied, but cannot contradict the canonical ID.
		user["tenant"], user["namespace"], user["full_user_id"] = test.tenant, test.namespace, test.id
		if _, err := gwbase.DecodeUser(userJSON(t, user)); err != nil {
			t.Fatal(err)
		}
		user["tenant"] = "other"
		if _, err := gwbase.DecodeUser(userJSON(t, user)); err == nil {
			t.Fatal("conflicting canonical and separate tenant accepted")
		}
		user["tenant"], user["namespace"] = test.tenant, "other"
		if _, err := gwbase.DecodeUser(userJSON(t, user)); err == nil {
			t.Fatal("conflicting canonical and separate namespace accepted")
		}
	}
}
