package rgw

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// testRole is one role of the roleTestContainer model.
type testRole struct {
	id, name, path, trust string
	// policies are the inline permission policies, in put order.
	policies [][2]string
	tags     bool
}

// roleTestContainer is a small model of one gateway's radosgw-admin role
// commands: role get, role create, role delete, role-policy put and
// role-policy delete change or read the roles, and nothing else. A missing
// role answers role get with ENOENT, printed like Squid or, with silent, not
// at all like Tentacle. faults holds one fault per operation, consumed by
// its next call; settle is applied to the next role get after a mutation. It
// records every command line.
type roleTestContainer struct {
	testcontainers.Container
	roles  map[string]*testRole
	calls  [][]string
	faults map[string]keyFault
	settle *keyFault
	silent bool
	ids    int
}

func (ctr *roleTestContainer) GetContainerID() string { return "gateway" }

func (ctr *roleTestContainer) State(context.Context) (*container.State, error) {
	return &container.State{Running: true}, nil
}

func (ctr *roleTestContainer) add(name, path, trust string) *testRole {
	ctr.ids++
	role := &testRole{id: fmt.Sprintf("role-id-%d", ctr.ids), name: name, path: path, trust: trust}
	ctr.roles[name] = role
	return role
}

func (ctr *roleTestContainer) document(role *testRole) []byte {
	native := map[string]any{"RoleId": role.id, "RoleName": role.name, "Path": role.path, "Arn": "arn:aws:iam:::role" + role.path + role.name,
		"CreateDate": "2026-11-03T00:00:00.000Z", "MaxSessionDuration": 3600, "AssumeRolePolicyDocument": role.trust}
	if len(role.policies) > 0 {
		var policies []any
		for _, policy := range role.policies {
			policies = append(policies, map[string]any{"PolicyName": policy[0], "PolicyValue": policy[1]})
		}
		native["PermissionPolicies"] = policies
	}
	if role.tags {
		native["Tags"] = []any{map[string]any{"Key": "owner", "Value": "test"}}
	}
	data, _ := json.Marshal(native)
	return data
}

func (ctr *roleTestContainer) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if gwtest.IsCancel(args) {
		return 0, nil, nil
	}
	original, args := args, gwtest.Argv(args)
	ctr.calls = append(ctr.calls, slices.Clone(args))
	flag := gwtest.Flag(args)
	operation := roleOperation(args)
	fault, faulted := ctr.faults[operation]
	delete(ctr.faults, operation)
	if operation == "role get" && ctr.settle != nil {
		fault, faulted, ctr.settle = *ctr.settle, true, nil
	}
	apply := !faulted || fault.apply
	role := ctr.roles[flag("--role-name")]
	var output []byte
	switch operation {
	case "role get":
		if role == nil && !faulted {
			if ctr.silent {
				return gwtest.Reply(original, 2, gwtest.Stream(true, nil))
			}
			return gwtest.Reply(original, 2, gwtest.Stream(true, []byte("2026-11-03T00:00:00.000+0000 7f0 -1 ERROR: failed reading role name from Role pool: "+flag("--role-name")+": (2) No such file or directory\n")))
		}
		if role != nil {
			output = ctr.document(role)
		}
	case "role create":
		if apply {
			role = ctr.add(flag("--role-name"), flag("--path"), flag("--assume-role-policy-doc"))
			output = ctr.document(role)
		}
	case "role delete":
		if apply && role != nil {
			delete(ctr.roles, role.name)
		}
	case "role-policy put":
		if apply && role != nil {
			name := flag("--policy-name")
			role.policies = slices.DeleteFunc(role.policies, func(p [2]string) bool { return p[0] == name })
			role.policies = append(role.policies, [2]string{name, flag("--policy-doc")})
		}
	case "role-policy delete":
		if apply && role != nil {
			role.policies = slices.DeleteFunc(role.policies, func(p [2]string) bool { return p[0] == flag("--policy-name") })
		}
	default:
		return 1, nil, errors.New("unexpected radosgw-admin invocation")
	}
	if fault.err != nil {
		return 0, nil, fault.err
	}
	if fault.code != 0 {
		return gwtest.Reply(original, fault.code, gwtest.Stream(true, []byte("error")))
	}
	return gwtest.Reply(original, 0, gwtest.Stream(false, output))
}

// roleOperation names the role command of args.
func roleOperation(args []string) string {
	for _, candidate := range []string{"role get", "role create", "role delete", "role-policy put", "role-policy delete"} {
		words := strings.Fields(candidate)
		if i := slices.Index(args, words[0]); i >= 0 && i+1 < len(args) && args[i+1] == words[1] {
			return candidate
		}
	}
	return ""
}

// count returns how many recorded calls ran operation.
func (ctr *roleTestContainer) count(operation string) int {
	n := 0
	for _, call := range ctr.calls {
		if roleOperation(call) == operation {
			n++
		}
	}
	return n
}

// mutations returns how many recorded calls changed roles.
func (ctr *roleTestContainer) mutations() int {
	return len(ctr.calls) - ctr.count("role get")
}

func newRoleTestGateway() (*Gateway, *roleTestContainer) {
	ctr := &roleTestContainer{roles: map[string]*testRole{}, faults: map[string]keyFault{}}
	owner := testCluster(nil, testhelper.TestConfig{}, map[string]testcontainers.Container{rgwServiceName(Config{Name: "default"}): ctr})
	return registerTestGateway(testGateway(owner, ctr, Config{Name: "default"})), ctr
}

const (
	testTrust = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["arn:aws:iam:::user/assumer"]},"Action":["sts:AssumeRole"]}]}`
	testRead  = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket/allowed"]}]}`
	testList  = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:ListBucket"],"Resource":["arn:aws:s3:::bucket"]}]}`
)

func testRoleConfig() RoleConfig {
	return RoleConfig{Name: "reader", Path: "/fixture/", TrustPolicy: testTrust,
		Policies: []RolePolicy{{Name: "read", Document: testRead}, {Name: "list", Document: testList}}}
}

func roleARN(t *testing.T, role *Role) string {
	t.Helper()
	arn, err := role.ARN()
	if err != nil {
		t.Fatal(err)
	}
	return arn
}

func TestRoleCreationBindsTheNativeRoleAndRemovesOnlyIt(t *testing.T) {
	gateway, ctr := newRoleTestGateway()
	// A role the test created natively is not the fixture's.
	other := ctr.add("caller-role", "/", testTrust)
	other.policies = [][2]string{{"caller", testRead}}
	role, err := gateway.CreateRole(t.Context(), testRoleConfig())
	if err != nil {
		t.Fatal(err)
	}
	if role.Name() != "reader" || roleARN(t, role) != "arn:aws:iam:::role/fixture/reader" {
		t.Fatalf("role %s, ARN %s", role.Name(), roleARN(t, role))
	}
	native := ctr.roles["reader"]
	if native.trust != testTrust || !slices.Equal(native.policies, [][2]string{{"read", testRead}, {"list", testList}}) {
		t.Fatalf("native role: trust %q, policies %v", native.trust, native.policies)
	}
	create := ctr.calls[slices.IndexFunc(ctr.calls, func(call []string) bool { return roleOperation(call) == "role create" })]
	want := []string{"role", "create", "--role-name", "reader", "--path", "/fixture/", "--assume-role-policy-doc", testTrust}
	if !slices.Equal(create[len(create)-len(want):], want) {
		t.Fatalf("role creation command: %v", create)
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%d", "%t"} {
		if value := fmt.Sprintf(format, role); value != "RGW role reader" {
			t.Fatalf("formatted role handle: %s", value)
		}
	}
	if err := role.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	if ctr.roles["reader"] != nil || ctr.count("role-policy delete") != 2 || ctr.count("role delete") != 1 {
		t.Fatalf("Remove left the role or ran %d policy deletions", ctr.count("role-policy delete"))
	}
	if ctr.roles["caller-role"] != other || len(other.policies) != 1 {
		t.Fatal("Remove changed a role the fixture did not create")
	}
	calls := len(ctr.calls)
	if err := role.Remove(t.Context()); err != nil || len(ctr.calls) != calls {
		t.Fatal("a second Remove ran a native command")
	}
	// The removed role's ARN can still be handed to the SUT.
	if roleARN(t, role) != "arn:aws:iam:::role/fixture/reader" {
		t.Fatal("Remove changed the handed-off ARN")
	}
	// The name is free again.
	again, err := gateway.CreateRole(t.Context(), RoleConfig{Name: "reader", TrustPolicy: testTrust})
	if err != nil || roleARN(t, again) != "arn:aws:iam:::role/reader" {
		t.Fatalf("creating the removed role's name again: %v", err)
	}
}

func TestRoleCreationGeneratesANameAndAcceptsBothAbsentForms(t *testing.T) {
	for _, silent := range []bool{false, true} {
		gateway, ctr := newRoleTestGateway()
		ctr.silent = silent
		role, err := gateway.CreateRole(t.Context(), RoleConfig{TrustPolicy: testTrust})
		if err != nil {
			t.Fatalf("silent ENOENT %v: %v", silent, err)
		}
		if !strings.HasPrefix(role.Name(), "tc-") || ctr.roles[role.Name()] == nil || roleARN(t, role) != "arn:aws:iam:::role/"+role.Name() {
			t.Fatalf("generated role %q", role.Name())
		}
	}
}

func TestRoleCreationRefusesExistingAndInvalidRoles(t *testing.T) {
	gateway, ctr := newRoleTestGateway()
	caller := ctr.add("reader", "/fixture/", testTrust)
	if role, err := gateway.CreateRole(t.Context(), testRoleConfig()); !errors.Is(err, ceph.ErrInvalidRequest) || role != nil || ctr.mutations() != 0 {
		t.Fatalf("an existing role: %v after %d mutations", err, ctr.mutations())
	}
	if ctr.roles["reader"] != caller || len(caller.policies) != 0 {
		t.Fatal("CreateRole changed an existing role")
	}
	for name, config := range map[string]RoleConfig{
		"name":               {Name: "-flag", TrustPolicy: testTrust},
		"long name":          {Name: strings.Repeat("a", 65), TrustPolicy: testTrust},
		"path":               {Name: "a", Path: "fixture", TrustPolicy: testTrust},
		"missing trust":      {Name: "a"},
		"trust not object":   {Name: "a", TrustPolicy: `["sts:AssumeRole"]`},
		"multi-line trust":   {Name: "a", TrustPolicy: "{\n}"},
		"duplicate policies": {Name: "a", TrustPolicy: testTrust, Policies: []RolePolicy{{"p", testRead}, {"p", testList}}},
		"policy name":        {Name: "a", TrustPolicy: testTrust, Policies: []RolePolicy{{"", testRead}}},
		"policy document":    {Name: "a", TrustPolicy: testTrust, Policies: []RolePolicy{{"p", "not json"}}},
	} {
		calls := len(ctr.calls)
		if role, err := gateway.CreateRole(t.Context(), config); !errors.Is(err, ceph.ErrInvalidRequest) || role != nil || len(ctr.calls) != calls {
			t.Errorf("%s: %v", name, err)
		}
	}
	// A role get that fails for another reason than a missing role is not
	// absence.
	ctr.faults["role get"] = keyFault{code: 1}
	if role, err := gateway.CreateRole(t.Context(), RoleConfig{Name: "other", TrustPolicy: testTrust}); err == nil || role != nil || ctr.mutations() != 0 {
		t.Fatalf("a failed absence check: %v", err)
	}
	// Canceled before any command.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := len(ctr.calls)
	if role, err := gateway.CreateRole(ctx, RoleConfig{Name: "other", TrustPolicy: testTrust}); !errors.Is(err, context.Canceled) || role != nil || len(ctr.calls) != calls {
		t.Fatalf("a canceled CreateRole: %v", err)
	}
	if _, err := (&Gateway{}).CreateRole(t.Context(), RoleConfig{TrustPolicy: testTrust}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a gateway not owned by a cluster: %v", err)
	}
}

func TestRoleCreationFailureIsSettledAndNeverReplayed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fault keyFault
		// created is a role that native state shows; absent one it does not.
		created bool
	}{
		{"native failure after the change", keyFault{code: 1, apply: true}, true},
		{"native failure without the change", keyFault{code: 17}, false},
		{"exec failure after the change", keyFault{err: errors.New("connection reset"), apply: true}, true},
		{"exec failure without the change", keyFault{err: errors.New("connection reset")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gateway, ctr := newRoleTestGateway()
			ctr.faults["role create"] = tc.fault
			role, err := gateway.CreateRole(t.Context(), testRoleConfig())
			if err == nil || ctr.count("role create") != 1 {
				t.Fatalf("CreateRole = %v after %d creations", err, ctr.count("role create"))
			}
			if !tc.created {
				if role != nil || len(ctr.roles) != 0 || ctr.count("role-policy put") != 0 {
					t.Fatal("a role that was not created was returned")
				}
				// The name is not held by the attempt.
				if _, err := gateway.CreateRole(t.Context(), testRoleConfig()); err != nil {
					t.Fatal(err)
				}
				return
			}
			// The confirmed role is handed off; its policies were not put.
			if role == nil || roleARN(t, role) != "arn:aws:iam:::role/fixture/reader" || ctr.count("role-policy put") != 0 {
				t.Fatal("a created role was not handed off")
			}
			if err := role.Remove(t.Context()); err != nil || len(ctr.roles) != 0 {
				t.Fatalf("Remove of a role whose creation call failed: %v", err)
			}
		})
	}
}

func TestRoleWithUnknownCreationIsOnlyRemovable(t *testing.T) {
	gateway, ctr := newRoleTestGateway()
	ctr.faults["role create"] = keyFault{err: errors.New("connection reset"), apply: true}
	// The readback that settles the failed call fails too.
	settle := &settleOnCreate{roleTestContainer: ctr, fault: keyFault{code: 5}}
	gateway.Container = settle
	role, err := gateway.CreateRole(t.Context(), testRoleConfig())
	gateway.Container = ctr
	if err == nil || role == nil {
		t.Fatalf("an unknown outcome returned role %v, error %v", role, err)
	}
	if _, err := role.ARN(); err == nil {
		t.Fatal("a role whose creation is unknown was handed off")
	}
	if _, err := gateway.CreateRole(t.Context(), testRoleConfig()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a second role of a tracked name: %v", err)
	}
	// Remove settles it: native state holds the requested role, so it is
	// deleted.
	if err := role.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(ctr.roles) != 0 || ctr.count("role create") != 1 {
		t.Fatalf("settled roles %d after %d creations", len(ctr.roles), ctr.count("role create"))
	}

	// A pending role whose name now holds another role is never removed.
	ctr.faults["role create"] = keyFault{err: errors.New("connection reset")}
	gateway.Container = settle
	settle.fault = keyFault{code: 5}
	role, err = gateway.CreateRole(t.Context(), testRoleConfig())
	gateway.Container = ctr
	if err == nil || role == nil {
		t.Fatalf("an unknown outcome returned role %v, error %v", role, err)
	}
	ctr.add("reader", "/other/", testTrust)
	if err := role.Remove(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || ctr.count("role delete") != 1 || ctr.roles["reader"] == nil {
		t.Fatalf("Remove of a pending role over another role: %v", err)
	}
}

// settleOnCreate answers the role get that follows a role creation with
// fault, and everything else as its roleTestContainer.
type settleOnCreate struct {
	*roleTestContainer
	fault keyFault
}

func (ctr *settleOnCreate) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if roleOperation(gwtest.Argv(args)) == "role create" {
		fault := ctr.fault
		ctr.roleTestContainer.settle = &fault
	}
	return ctr.roleTestContainer.Exec(ctx, args, opts...)
}

func TestRolePolicyFailureLeavesARemovableRole(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fault keyFault
		// put is a policy that native state shows.
		put bool
	}{
		{"native failure after the change", keyFault{code: 1, apply: true}, true},
		{"native failure without the change", keyFault{code: 22}, false},
		{"exec failure without the change", keyFault{err: errors.New("connection reset")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gateway, ctr := newRoleTestGateway()
			ctr.faults["role-policy put"] = tc.fault
			role, err := gateway.CreateRole(t.Context(), testRoleConfig())
			if err == nil || role == nil || ctr.count("role-policy put") != 1 {
				t.Fatalf("CreateRole = %v after %d puts", err, ctr.count("role-policy put"))
			}
			if roleARN(t, role) == "" || len(ctr.roles["reader"].policies) != map[bool]int{true: 1, false: 0}[tc.put] {
				t.Fatal("the partial role's state differs from native state")
			}
			if err := role.Remove(t.Context()); err != nil || len(ctr.roles) != 0 {
				t.Fatalf("Remove of a partial role: %v", err)
			}
			if want := map[bool]int{true: 1, false: 0}[tc.put]; ctr.count("role-policy delete") != want {
				t.Fatalf("Remove ran %d policy deletions, want %d", ctr.count("role-policy delete"), want)
			}
		})
	}
	// A context that ends between policies leaves the policies put so far.
	gateway, ctr := newRoleTestGateway()
	ctx, cancel := context.WithCancel(t.Context())
	cancelAfterPut := &cancelOnPut{roleTestContainer: ctr, cancel: cancel}
	gateway.Container = cancelAfterPut
	role, err := gateway.CreateRole(ctx, testRoleConfig())
	gateway.Container = ctr
	if !errors.Is(err, context.Canceled) || role == nil || ctr.count("role-policy put") != 1 {
		t.Fatalf("a CreateRole canceled between policies: %v after %d puts", err, ctr.count("role-policy put"))
	}
	if err := role.Remove(t.Context()); err != nil || len(ctr.roles) != 0 {
		t.Fatalf("Remove of a canceled creation: %v", err)
	}
}

// cancelOnPut cancels the call's context once a policy put answered.
type cancelOnPut struct {
	*roleTestContainer
	cancel context.CancelFunc
}

func (ctr *cancelOnPut) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	code, out, err := ctr.roleTestContainer.Exec(ctx, args, opts...)
	if roleOperation(gwtest.Argv(args)) == "role-policy put" {
		ctr.cancel()
	}
	return code, out, err
}

func TestRoleRemovalRefusesRolesChangedOutsideTheFixture(t *testing.T) {
	for name, edit := range map[string]func(*roleTestContainer){
		"added policy": func(ctr *roleTestContainer) {
			ctr.roles["reader"].policies = append(ctr.roles["reader"].policies, [2]string{"caller", testRead})
		},
		"changed policy": func(ctr *roleTestContainer) { ctr.roles["reader"].policies[0][1] = testList },
		"changed trust":  func(ctr *roleTestContainer) { ctr.roles["reader"].trust = `{"Statement":[]}` },
		"added tags":     func(ctr *roleTestContainer) { ctr.roles["reader"].tags = true },
		"replaced role": func(ctr *roleTestContainer) {
			delete(ctr.roles, "reader")
			ctr.add("reader", "/fixture/", testTrust)
		},
	} {
		t.Run(name, func(t *testing.T) {
			gateway, ctr := newRoleTestGateway()
			role, err := gateway.CreateRole(t.Context(), testRoleConfig())
			if err != nil {
				t.Fatal(err)
			}
			edit(ctr)
			before := ctr.document(ctr.roles["reader"])
			if err := role.Remove(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("Remove = %v", err)
			}
			if ctr.count("role-policy delete") != 0 || ctr.count("role delete") != 0 || !bytes.Equal(ctr.document(ctr.roles["reader"]), before) {
				t.Fatal("Remove changed a role edited outside the fixture")
			}
		})
	}
	// A fixture policy or the role deleted outside the fixture is simply gone.
	gateway, ctr := newRoleTestGateway()
	role, err := gateway.CreateRole(t.Context(), testRoleConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctr.roles["reader"].policies = ctr.roles["reader"].policies[1:]
	if err := role.Remove(t.Context()); err != nil || ctr.count("role-policy delete") != 1 || len(ctr.roles) != 0 {
		t.Fatalf("Remove after a fixture policy was deleted outside: %v", err)
	}
	role, err = gateway.CreateRole(t.Context(), testRoleConfig())
	if err != nil {
		t.Fatal(err)
	}
	delete(ctr.roles, "reader")
	mutations := ctr.mutations()
	if err := role.Remove(t.Context()); err != nil || ctr.mutations() != mutations {
		t.Fatalf("Remove of a role already gone: %v", err)
	}
}

func TestRoleRemovalFailureIsSettled(t *testing.T) {
	for _, tc := range []struct {
		operation string
		fault     keyFault
		applied   bool
	}{
		{"role-policy delete", keyFault{code: 1, apply: true}, true},
		{"role-policy delete", keyFault{code: 1}, false},
		{"role delete", keyFault{code: 1, apply: true}, true},
		{"role delete", keyFault{err: errors.New("connection reset")}, false},
	} {
		t.Run(fmt.Sprintf("%s applied %v", tc.operation, tc.applied), func(t *testing.T) {
			gateway, ctr := newRoleTestGateway()
			role, err := gateway.CreateRole(t.Context(), testRoleConfig())
			if err != nil {
				t.Fatal(err)
			}
			ctr.faults[tc.operation] = tc.fault
			err = role.Remove(t.Context())
			if tc.applied && tc.operation == "role delete" {
				if err != nil || len(ctr.roles) != 0 {
					t.Fatalf("an applied role deletion: %v", err)
				}
				return
			}
			if !tc.applied && err == nil {
				t.Fatal("a removal that did not apply returned nil")
			}
			if tc.applied && err != nil {
				t.Fatalf("an applied policy deletion: %v", err)
			}
			if err := role.Remove(t.Context()); err != nil || len(ctr.roles) != 0 {
				t.Fatalf("Remove after a failed removal: %v", err)
			}
		})
	}
}

// Role creation and removal wait for the cluster mutex only until the
// caller's context ends. A queued call sends no radosgw-admin command and
// changes no role or handle; the same call succeeds with a fresh context.
func TestRoleOperationsWaitForTheClusterOnlyUntilTheContextEnds(t *testing.T) {
	gateway, ctr := newRoleTestGateway()
	mutex := testhelper.TestState(gateway.base().Cluster()).Mutex
	clustertest.AssertContextLockQueued(t, mutex, func(ctx context.Context) error {
		role, err := gateway.CreateRole(ctx, testRoleConfig())
		if role != nil {
			t.Error("a queued CreateRole returned a role")
		}
		return err
	})
	if len(ctr.calls) != 0 || len(gateways(gateway.base().Cluster()).roles) != 0 {
		t.Fatalf("a queued CreateRole reached radosgw-admin: %v", ctr.calls)
	}
	role, err := gateway.CreateRole(t.Context(), testRoleConfig())
	if err != nil {
		t.Fatalf("CreateRole with a fresh context: %v", err)
	}
	calls := len(ctr.calls)
	clustertest.AssertContextLockQueued(t, mutex, role.Remove)
	if len(ctr.calls) != calls || ctr.roles["reader"] == nil {
		t.Fatalf("a queued Remove reached radosgw-admin: %v", ctr.calls[calls:])
	}
	if err := role.Remove(t.Context()); err != nil || len(ctr.roles) != 0 {
		t.Fatalf("Remove with a fresh context: %v", err)
	}
}

func TestRoleHandlesOfForeignOrUnconfirmedRoles(t *testing.T) {
	if _, err := (*Role)(nil).ARN(); err == nil {
		t.Fatal("a nil role handle has an ARN")
	}
	if err := (*Role)(nil).Remove(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a nil role handle: %v", err)
	}
	if err := (&Role{}).Remove(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a zero role handle: %v", err)
	}
	if (&Role{}).Name() != "" || fmt.Sprint(Role{}) != "RGW role" {
		t.Fatal("a zero role handle names a role")
	}
}
