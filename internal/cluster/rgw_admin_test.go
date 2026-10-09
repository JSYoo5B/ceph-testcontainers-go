package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type rgwAdminResponse struct {
	data string
	code int
	err  error
}

type rgwAdminTestContainer struct {
	testcontainers.Container
	id        string
	responses []rgwAdminResponse
	calls     [][]string
}

func (ctr *rgwAdminTestContainer) GetContainerID() string { return ctr.id }

func (ctr *rgwAdminTestContainer) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	ctr.calls = append(ctr.calls, slices.Clone(args))
	if len(ctr.responses) == 0 {
		return 1, nil, errors.New("unexpected radosgw-admin invocation")
	}
	response := ctr.responses[0]
	ctr.responses = ctr.responses[1:]
	if response.err != nil {
		return 0, nil, response.err
	}
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	if response.code != 0 {
		header[0] = byte(stdcopy.Stderr)
	}
	binary.BigEndian.PutUint32(header[4:], uint32(len(response.data)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(response.data)
	return response.code, &stream, nil
}

func newRGWAdminTestGateway(config RGWConfig, responses ...rgwAdminResponse) (*RGWContainer, *rgwAdminTestContainer) {
	if config.Name == "" {
		config.Name = "default"
	}
	ctr := &rgwAdminTestContainer{id: "gateway", responses: responses}
	owner := &Container{services: map[string]testcontainers.Container{rgwServiceName(config): ctr}}
	return &RGWContainer{Container: ctr, owner: owner, config: config, GatewayName: config.Name}, ctr
}

func rgwAdminFixture(id, secret string, suspended bool, quota RGWQuota) string {
	suspendedInt := 0
	if suspended {
		suspendedInt = 1
	}
	return fmt.Sprintf(`{"user_id":%q,"type":"rgw","display_name":"Testcontainers","email":"test@example.org","suspended":%d,"admin":false,"system":false,"max_buckets":1000,"caps":[],"keys":[{"access_key":"PRIVATE-ACCESS","secret_key":%q}],"user_quota":{"enabled":%t,"max_size":%d,"max_objects":%d},"bucket_quota":{"enabled":false,"max_size":-1,"max_objects":-1}}`, id, suspendedInt, secret, quota.Enabled, quota.MaxSizeBytes, quota.MaxObjects)
}

func rgwAdminOwnedTestUser(gateway *RGWContainer) *RGWUser {
	return &RGWUser{owner: gateway.owner, scope: gateway.config, id: "test-user", accessKey: "PRIVATE-ACCESS", secretKey: "PRIVATE-SECRET", state: &rgwUserState{created: true, identity: &rgwUserCreationIdentity{originalType: "rgw"}}}
}

func TestRGWAdminPreservesDeclaredScopeAndConnection(t *testing.T) {
	config := RGWConfig{Name: "named", Realm: "realm-a", Zonegroup: "group-a", Zone: "zone-a"}
	gateway, ctr := newRGWAdminTestGateway(config, rgwAdminResponse{data: `{"native":true}`})
	if _, err := gateway.Admin(t.Context(), "zone", "get"); err != nil {
		t.Fatal(err)
	}
	want := []string{"radosgw-admin", "--conf", "/etc/ceph/ceph.conf", "--keyring", "/etc/ceph/ceph.client.admin.keyring", "--format", "json",
		"--rgw-realm", "realm-a", "--rgw-zonegroup", "group-a", "--rgw-zone", "zone-a", "--osd-pool-default-pg-num", "1", "--osd-pool-default-pgp-num", "0", "zone", "get"}
	if !slices.Equal(ctr.calls[0], want) {
		t.Fatalf("gateway CLI scope changed: %v", ctr.calls[0])
	}
	for _, override := range []string{"--rgw-zone=foreign", "--rgw_zone", "--zone-id", "--zonegroup-id=x", "--realm-id", "--conf", "--mon_host=x", "--keyring", "--key-file", "-c/tmp/other.conf", "-m", "-nclient.foreign", "-ksecret"} {
		if _, err := gateway.Admin(t.Context(), "user", "list", override); err == nil || len(ctr.calls) != 1 {
			t.Fatalf("scope override %q reached native CLI", override)
		}
	}
	if _, err := gateway.Admin(t.Context()); err == nil {
		t.Fatal("empty operation accepted")
	}
	if _, err := gateway.Admin(t.Context(), "user", "list", "bad\nargument"); err == nil {
		t.Fatal("control character accepted")
	}
}

func TestRGWAdminRedactsNativeFailuresAndPreservesCancellation(t *testing.T) {
	gateway, _ := newRGWAdminTestGateway(RGWConfig{}, rgwAdminResponse{data: "PRIVATE-ACCESS PRIVATE-SECRET", code: 1})
	if _, err := gateway.Admin(t.Context(), "user", "info", "--uid", "test-user"); err == nil || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("native failure was not redacted: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	gateway, _ = newRGWAdminTestGateway(RGWConfig{}, rgwAdminResponse{err: errors.New("PRIVATE-SECRET transport failure")})
	if _, err := gateway.Admin(ctx, "user", "list"); !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("cancellation lost or secret exposed: %v", err)
	}
}

func TestRGWUserCreationRejectsExistingIdentityAndKeepsPartialCapsFailure(t *testing.T) {
	gateway, ctr := newRGWAdminTestGateway(RGWConfig{}, rgwAdminResponse{data: `["test-user"]`})
	if user, err := gateway.CreateUser(t.Context(), RGWUserConfig{ID: "test-user"}); err == nil || user != nil || len(ctr.calls) != 1 {
		t.Fatal("existing RGW user could be adopted or changed")
	}
	native := rgwAdminFixture("test-user", "PRIVATE-SECRET", false, RGWQuota{MaxSizeBytes: -1, MaxObjects: -1})
	gateway, ctr = newRGWAdminTestGateway(RGWConfig{}, rgwAdminResponse{data: `[]`}, rgwAdminResponse{data: native}, rgwAdminResponse{data: "PRIVATE-SECRET cap failure", code: 1})
	user, err := gateway.CreateUser(t.Context(), RGWUserConfig{ID: "test-user", AdminCaps: "users=read;usage=read"})
	if err == nil || user == nil || !user.state.created || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("caps failure lost the created owned user or exposed credentials")
	}
	key, secret, err := user.Credentials()
	if err != nil || key != "PRIVATE-ACCESS" || secret != "PRIVATE-SECRET" {
		t.Fatal("partial successful creation lost generated credentials")
	}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		if value := fmt.Sprintf(format, user); strings.Contains(value, "PRIVATE") {
			t.Fatalf("formatted handle leaked secret: %s", value)
		}
	}
	for _, args := range ctr.calls {
		if slices.Contains(args, "--admin") || slices.Contains(args, "--system") {
			t.Fatal("ordinary user acquired global admin or system permissions")
		}
	}
	ctr.responses = []rgwAdminResponse{{data: `["test-user"]`}, {data: native}, {}}
	if err := gateway.RemoveUser(t.Context(), user); err != nil || !user.state.removed {
		t.Fatal("capability setup failure prevented removing a confirmed owned identity")
	}
}

func TestRGWCreationWithoutConfirmedKeysCannotAdoptNativeIdentity(t *testing.T) {
	native := rgwAdminFixture("test-user", "PRIVATE-SECRET", false, RGWQuota{MaxSizeBytes: -1, MaxObjects: -1})
	for _, test := range []struct{ name, output string }{
		{"truncated", `{"user_id":"test-user","keys":[{"secret_key":"PRIVATE-SECRET"`},
		{"malformed policy", strings.Replace(native, `"suspended":0`, `"suspended":"PRIVATE-SECRET"`, 1)},
		{"missing keys", strings.Replace(native, `"keys":[{"access_key":"PRIVATE-ACCESS","secret_key":"PRIVATE-SECRET"}]`, `"keys":[]`, 1)},
		{"missing secret", rgwAdminFixture("test-user", "", false, RGWQuota{MaxSizeBytes: -1, MaxObjects: -1})},
	} {
		t.Run(test.name, func(t *testing.T) {
			gateway, ctr := newRGWAdminTestGateway(RGWConfig{}, rgwAdminResponse{data: `[]`}, rgwAdminResponse{data: test.output}, rgwAdminResponse{data: native})
			user, err := gateway.CreateUser(t.Context(), RGWUserConfig{ID: "test-user"})
			if err == nil || user == nil || !user.state.created || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatal("unconfirmed successful native creation lost its partial handle or exposed keys")
			}
			if access, secret, err := user.Credentials(); err == nil || access != "" || secret != "" {
				t.Fatal("incomplete native result exposed unconfirmed credentials")
			}
			if _, err := gateway.UserInfo(t.Context(), user); err == nil || len(ctr.calls) != 2 {
				t.Fatal("unconfirmed handle adopted credentials from a later native query")
			}
			if err := gateway.RemoveUser(t.Context(), user); err == nil || len(ctr.calls) != 2 {
				t.Fatal("unconfirmed handle attempted native identity removal")
			}
			if data, err := gateway.Admin(t.Context(), "user", "info", "--uid", user.ID()); err != nil || string(data) != native {
				t.Fatal("explicit native inspection was unavailable for a partial creation")
			}
		})
	}
}

func TestRGWOwnedUserGuardsClusterScopeAndReplacedKeys(t *testing.T) {
	native := rgwAdminFixture("test-user", "PRIVATE-SECRET", false, RGWQuota{MaxSizeBytes: -1, MaxObjects: -1})
	gateway, ctr := newRGWAdminTestGateway(RGWConfig{Realm: "a", Zonegroup: "b", Zone: "c"}, rgwAdminResponse{data: native})
	user := rgwAdminOwnedTestUser(gateway)
	foreign := *user
	foreign.owner = &Container{}
	if err := gateway.SuspendUser(t.Context(), &foreign, true); err == nil || len(ctr.calls) != 0 {
		t.Fatal("foreign cluster handle reached native mutation")
	}
	foreign = *user
	foreign.scope.Zone = "other"
	if err := gateway.SetUserQuota(t.Context(), &foreign, RGWQuota{}); err == nil || len(ctr.calls) != 0 {
		t.Fatal("foreign gateway scope reached native mutation")
	}
	foreign = *user
	foreign.secretKey = "REPLACED"
	if err := gateway.SuspendUser(t.Context(), &foreign, true); err == nil || len(ctr.calls) != 1 {
		t.Fatal("replaced native credentials allowed a user mutation")
	}

	// Ownership belongs to the Ceph scope, allowing a replacement gateway.
	replacement := &rgwAdminTestContainer{id: "replacement", responses: []rgwAdminResponse{{data: native}}}
	gateway.owner.services["rgw:replacement"] = replacement
	other := &RGWContainer{Container: replacement, owner: gateway.owner, config: RGWConfig{Name: "replacement", Realm: "a", Zonegroup: "b", Zone: "c"}}
	if info, err := other.UserInfo(t.Context(), user); err != nil || info.ID != user.ID() {
		t.Fatal("same-scope replacement gateway could not use an owned identity")
	}
}

func TestRGWQuotaUsesByteLimitsAndVerifiesReadback(t *testing.T) {
	old := rgwAdminFixture("test-user", "PRIVATE-SECRET", false, RGWQuota{MaxSizeBytes: -1, MaxObjects: -1})
	quota := RGWQuota{Enabled: true, MaxSizeBytes: 1048576, MaxObjects: 12}
	updated := rgwAdminFixture("test-user", "PRIVATE-SECRET", false, quota)
	gateway, ctr := newRGWAdminTestGateway(RGWConfig{}, rgwAdminResponse{data: old}, rgwAdminResponse{}, rgwAdminResponse{}, rgwAdminResponse{data: updated})
	user := rgwAdminOwnedTestUser(gateway)
	if err := gateway.SetUserQuota(t.Context(), user, quota); err != nil {
		t.Fatal(err)
	}
	lastArgs := ctr.calls[1][len(ctr.calls[1])-12:]
	want := []string{"quota", "set", "--quota-scope", "user", "--uid", "test-user", "--max-size", "1048576B", "--max-objects", "12"}
	if !slices.Equal(lastArgs[len(lastArgs)-len(want):], want) || !slices.Contains(ctr.calls[2], "enable") {
		t.Fatalf("incorrect quota units or activation: %v", ctr.calls)
	}
	if err := gateway.SetUserQuota(t.Context(), user, RGWQuota{MaxSizeBytes: -2}); err == nil || len(ctr.calls) != 4 {
		t.Fatal("invalid quota reached native mutation")
	}
	gateway, _ = newRGWAdminTestGateway(RGWConfig{}, rgwAdminResponse{data: old}, rgwAdminResponse{}, rgwAdminResponse{}, rgwAdminResponse{data: old})
	if err := gateway.SetUserQuota(t.Context(), rgwAdminOwnedTestUser(gateway), quota); err == nil {
		t.Fatal("stale quota readback was accepted")
	}
}

func TestRGWRemovalRetainsUserOnFailureAndNeverPurgesData(t *testing.T) {
	native := rgwAdminFixture("test-user", "PRIVATE-SECRET", false, RGWQuota{MaxSizeBytes: -1, MaxObjects: -1})
	gateway, ctr := newRGWAdminTestGateway(RGWConfig{},
		rgwAdminResponse{data: `["test-user"]`}, rgwAdminResponse{data: native}, rgwAdminResponse{data: "user still owns buckets PRIVATE-SECRET", code: 1}, rgwAdminResponse{data: `["test-user"]`},
		rgwAdminResponse{data: native}, rgwAdminResponse{data: `["test-user"]`}, rgwAdminResponse{data: native}, rgwAdminResponse{})
	user := rgwAdminOwnedTestUser(gateway)
	if err := gateway.RemoveUser(t.Context(), user); err == nil || user.state.removed {
		t.Fatal("failed user removal lost its handle")
	}
	if _, err := gateway.UserInfo(t.Context(), user); err != nil {
		t.Fatal("failed removal prevented further native inspection")
	}
	if err := gateway.RemoveUser(t.Context(), user); err != nil || !user.state.removed {
		t.Fatal("empty owned user could not be removed")
	}
	if err := gateway.RemoveUser(t.Context(), user); err != nil || len(ctr.calls) != 8 {
		t.Fatal("repeat removal was not idempotent")
	}
	for _, args := range ctr.calls {
		if slices.Contains(args, "--purge-data") || slices.Contains(args, "--purge-objects") {
			t.Fatal("identity cleanup purged caller-owned objects")
		}
	}
}

func TestRGWRemovalHandlesLostReplyAndSharedHandleCopies(t *testing.T) {
	native := rgwAdminFixture("test-user", "PRIVATE-SECRET", false, RGWQuota{MaxSizeBytes: -1, MaxObjects: -1})
	gateway, ctr := newRGWAdminTestGateway(RGWConfig{},
		rgwAdminResponse{data: `["test-user"]`}, rgwAdminResponse{data: native}, rgwAdminResponse{err: errors.New("reply lost after native removal")}, rgwAdminResponse{data: `[]`})
	user := rgwAdminOwnedTestUser(gateway)
	copy := *user
	if err := gateway.RemoveUser(t.Context(), &copy); err != nil || !user.state.removed {
		t.Fatal("native removal with lost reply did not reconcile absence across handle copies")
	}
	if err := gateway.RemoveUser(t.Context(), user); err != nil || len(ctr.calls) != 4 {
		t.Fatal("original handle retried a removal already completed through a copy")
	}
	if _, err := gateway.UserInfo(t.Context(), user); err == nil || len(ctr.calls) != 4 {
		t.Fatal("copied-handle removal did not prevent subsequent policy operations")
	}
	// A prior unknown outcome may be resolved by the next retry's first listing.
	gateway, ctr = newRGWAdminTestGateway(RGWConfig{}, rgwAdminResponse{data: `[]`})
	if err := gateway.RemoveUser(t.Context(), rgwAdminOwnedTestUser(gateway)); err != nil || len(ctr.calls) != 1 {
		t.Fatal("retry did not accept a confirmed absent owned user")
	}
}

func TestRGWNativePolicyDecoderAcceptsModernAndLegacyLimits(t *testing.T) {
	modern := rgwAdminFixture("test-user", "PRIVATE-SECRET", false, RGWQuota{Enabled: true, MaxSizeBytes: 987654, MaxObjects: 3})
	native, err := decodeRGWUser([]byte(modern))
	if err != nil || native.info.UserQuota.MaxSizeBytes != 987654 || native.info.Suspended {
		t.Fatal("modern RGW policy was not decoded")
	}
	quota, err := decodeRGWQuota([]byte(`{"enabled":true,"max_size_kb":1024,"max_objects":-1}`))
	if err != nil || quota != (RGWQuota{Enabled: true, MaxSizeBytes: 1048576, MaxObjects: -1}) {
		t.Fatal("legacy quota KiB was not converted to bytes")
	}
	for _, data := range []string{`{"max_objects":1}`, `{"max_size_kb":9223372036854775807,"max_objects":1}`, `{"PRIVATE-SECRET":"invalid"}`} {
		if _, err := decodeRGWQuota([]byte(data)); err == nil || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatal("invalid quota accepted or secret included in decode error")
		}
	}
	for _, config := range []RGWUserConfig{{ID: "--foreign"}, {DisplayName: "bad\nname"}, {AdminCaps: "users=admin"}, {AdminCaps: "--admin=*"}, {AdminCaps: "users="}} {
		if _, err := normalizeRGWUserConfig(config); err == nil {
			t.Fatalf("invalid user configuration accepted: %v", config)
		}
	}
}
