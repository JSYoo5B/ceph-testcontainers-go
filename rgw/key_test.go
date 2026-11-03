package rgw

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwtest"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// keyFault makes the next native call of one operation answer with exit
// code code, or fail the Docker exec with err, after applying its change to
// the user's keys when apply is set.
type keyFault struct {
	code  int
	err   error
	apply bool
}

// keyTestContainer is a small model of one gateway's radosgw-admin for the
// user test-user: user list, user create, user info, user rm, key create and
// key rm change or read keys, and nothing else. faults holds one fault per
// operation, consumed by its next call. It records every command line.
type keyTestContainer struct {
	testcontainers.Container
	exists bool
	keys   []gwbase.NativeKey
	calls  [][]string
	faults map[string]keyFault
	// secrets counts the generated secrets, so each one differs.
	secrets int
}

func (ctr *keyTestContainer) GetContainerID() string { return "gateway" }

func (ctr *keyTestContainer) State(context.Context) (*container.State, error) {
	return &container.State{Running: true}, nil
}

func (ctr *keyTestContainer) document() string {
	keys, _ := json.Marshal(ctr.keys)
	native := rgwAdminFixture("test-user", "PRIVATE-SECRET", false, Quota{MaxSizeBytes: -1, MaxObjects: -1})
	return strings.Replace(native, `"keys":[{"access_key":"PRIVATE-ACCESS","secret_key":"PRIVATE-SECRET"}]`, `"keys":`+string(keys), 1)
}

func (ctr *keyTestContainer) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if gwtest.IsCancel(args) {
		return 0, nil, nil
	}
	original, args := args, gwtest.Argv(args)
	ctr.calls = append(ctr.calls, slices.Clone(args))
	flag := gwtest.Flag(args)
	operation := ""
	for _, candidate := range []string{"user list", "user create", "user info", "user rm", "key create", "key rm"} {
		words := strings.Fields(candidate)
		if i := slices.Index(args, words[0]); i >= 0 && i+1 < len(args) && args[i+1] == words[1] {
			operation = candidate
		}
	}
	fault, faulted := ctr.faults[operation]
	delete(ctr.faults, operation)
	apply := !faulted || fault.apply
	switch operation {
	case "user list":
		users := []string{}
		if ctr.exists {
			users = append(users, "test-user")
		}
		data, _ := json.Marshal(users)
		return gwtest.Reply(original, 0, gwtest.Stream(false, data))
	case "user create":
		ctr.exists, ctr.keys = true, []gwbase.NativeKey{{AccessKey: "PRIVATE-ACCESS", SecretKey: "PRIVATE-SECRET"}}
	case "key create":
		if apply {
			ctr.secrets++
			ctr.keys = append(ctr.keys, gwbase.NativeKey{AccessKey: flag("--access-key"), SecretKey: fmt.Sprintf("GENERATED-SECRET-%d", ctr.secrets)})
		}
	case "key rm":
		if apply {
			ctr.keys = slices.DeleteFunc(ctr.keys, func(key gwbase.NativeKey) bool { return key.AccessKey == flag("--access-key") })
		}
	case "user rm":
		ctr.exists, ctr.keys = false, nil
		return gwtest.Reply(original, 0, gwtest.Stream(false, nil))
	case "user info":
	default:
		return 1, nil, errors.New("unexpected radosgw-admin invocation")
	}
	if fault.err != nil {
		return 0, nil, fault.err
	}
	if fault.code != 0 {
		return gwtest.Reply(original, fault.code, gwtest.Stream(true, []byte("error PRIVATE-SECRET GENERATED-SECRET")))
	}
	return gwtest.Reply(original, 0, gwtest.Stream(false, []byte(ctr.document())))
}

// count returns how many recorded calls ran operation.
func (ctr *keyTestContainer) count(operation string) int {
	words := strings.Fields(operation)
	n := 0
	for _, call := range ctr.calls {
		if i := slices.Index(call, words[0]); i >= 0 && i+1 < len(call) && call[i+1] == words[1] {
			n++
		}
	}
	return n
}

// newKeyTestUser creates test-user through CreateUser on a gateway backed by
// a keyTestContainer, so the cluster's registry tracks it.
func newKeyTestUser(t *testing.T) (*Gateway, *keyTestContainer, *User) {
	t.Helper()
	ctr := &keyTestContainer{faults: map[string]keyFault{}}
	owner := testCluster(nil, testhelper.TestConfig{}, map[string]testcontainers.Container{rgwServiceName(Config{Name: "default"}): ctr})
	gateway := registerTestGateway(testGateway(owner, ctr, Config{Name: "default"}))
	user, err := gateway.CreateUser(t.Context(), UserConfig{ID: "test-user"})
	if err != nil {
		t.Fatal(err)
	}
	return gateway, ctr, user
}

func credentials(t *testing.T, key *S3Key) (string, string) {
	t.Helper()
	access, secret, err := key.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	return access, secret
}

func TestS3KeyCreationKeepsEveryOtherKeyAndExistingHandles(t *testing.T) {
	gateway, ctr, user := newKeyTestUser(t)
	// A key the test created natively is not the fixture's.
	ctr.keys = append(ctr.keys, gwbase.NativeKey{AccessKey: "UNRELATED", SecretKey: "UNRELATED-SECRET"})
	creation := user.CreationKey()
	key, err := user.CreateS3Key(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	access, secret := credentials(t, key)
	if access == "PRIVATE-ACCESS" || len(access) != 20 || strings.Trim(access, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") != "" || secret != "GENERATED-SECRET-1" {
		t.Fatalf("new key: %d-character access key, secret %q", len(access), secret)
	}
	if access, secret := credentials(t, creation); access != "PRIVATE-ACCESS" || secret != "PRIVATE-SECRET" {
		t.Fatal("creating a key changed what the creation key handle names")
	}
	if access, _ := credentials(t, user.CreationKey()); access != "PRIVATE-ACCESS" {
		t.Fatal("CreationKey named another key after a key was created")
	}
	create := ctr.calls[slices.IndexFunc(ctr.calls, func(call []string) bool { return slices.Contains(call, "create") && slices.Contains(call, "key") })]
	want := []string{"key", "create", "--uid", "test-user", "--key-type", "s3", "--access-key", access, "--gen-secret"}
	if !slices.Equal(create[len(create)-len(want):], want) {
		t.Fatalf("key creation command: %v", create)
	}
	if !slices.ContainsFunc(ctr.keys, func(k gwbase.NativeKey) bool { return k.AccessKey == "UNRELATED" }) || len(ctr.keys) != 3 {
		t.Fatal("key creation changed a key the fixture did not create")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d", "%t"} {
		if value := fmt.Sprintf(format, key); strings.Contains(value, access) || strings.Contains(value, secret) {
			t.Fatalf("formatted key handle leaked a key: %s", value)
		}
	}
	secrets := gateways(gateway.base().Cluster()).Secrets()
	for _, value := range []string{"PRIVATE-ACCESS", "PRIVATE-SECRET", access, secret} {
		if !slices.Contains(secrets, value) {
			t.Fatal("a fixture-created key is missing from the cluster's redaction inventory")
		}
	}
	if slices.Contains(secrets, "UNRELATED-SECRET") {
		t.Fatal("the redaction inventory adopted a key the fixture did not create")
	}
}

func TestS3KeyRevocationIsExplicitAndKeepsTheOwnedUser(t *testing.T) {
	_, ctr, user := newKeyTestUser(t)
	ctr.keys = append(ctr.keys, gwbase.NativeKey{AccessKey: "UNRELATED", SecretKey: "UNRELATED-SECRET"})
	creation := user.CreationKey()
	replacement, err := user.CreateS3Key(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	newAccess, _ := credentials(t, replacement)
	if err := creation.Revoke(t.Context()); err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(ctr.keys, func(k gwbase.NativeKey) bool { return k.AccessKey == "PRIVATE-ACCESS" }) {
		t.Fatal("Revoke left the key in native state")
	}
	if !slices.ContainsFunc(ctr.keys, func(k gwbase.NativeKey) bool { return k.AccessKey == "UNRELATED" || k.AccessKey == newAccess }) || len(ctr.keys) != 2 {
		t.Fatal("Revoke removed another key")
	}
	// The revoked handle still names its key pair, so a test can hand it to the SUT.
	if access, secret := credentials(t, creation); access != "PRIVATE-ACCESS" || secret != "PRIVATE-SECRET" {
		t.Fatal("revocation changed the revoked key's credentials")
	}
	if _, err := user.Info(t.Context()); err != nil {
		t.Fatalf("the replacement key did not keep the user owned: %v", err)
	}
	removals := ctr.count("key rm")
	if err := creation.Revoke(t.Context()); err != nil || ctr.count("key rm") != removals {
		t.Fatal("a second Revoke ran a native command")
	}
	if err := replacement.Revoke(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || ctr.count("key rm") != removals {
		t.Fatalf("revoking the last active fixture key: %v", err)
	}
	// A revoked key that reappears is not what the fixture recorded.
	ctr.keys = append(ctr.keys, gwbase.NativeKey{AccessKey: "PRIVATE-ACCESS", SecretKey: "PRIVATE-SECRET"})
	if _, err := user.Info(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a reappeared revoked key: %v", err)
	}
	ctr.keys = ctr.keys[:len(ctr.keys)-1]
	if err := user.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	calls := len(ctr.calls)
	if err := replacement.Revoke(t.Context()); err != nil || len(ctr.calls) != calls {
		t.Fatal("revoking a key of a removed user ran a native command")
	}
}

func TestS3KeyCreationFailureIsSettledAndNeverReplayed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fault keyFault
		// confirmed is a key whose credentials are usable; absent a key that
		// was not created.
		confirmed, absent bool
	}{
		{"native failure after the change", keyFault{code: 1, apply: true}, true, false},
		{"native failure without the change", keyFault{code: 17}, false, true},
		{"exec failure after the change", keyFault{err: errors.New("PRIVATE-SECRET connection reset"), apply: true}, true, false},
		{"exec failure without the change", keyFault{err: errors.New("connection reset")}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gateway, ctr, user := newKeyTestUser(t)
			ctr.faults["key create"] = tc.fault
			key, err := user.CreateS3Key(t.Context())
			if err == nil || strings.Contains(err.Error(), "SECRET") || ctr.count("key create") != 1 {
				t.Fatalf("CreateS3Key = %v after %d creations", err, ctr.count("key create"))
			}
			if tc.absent {
				if key != nil || len(ctr.keys) != 1 {
					t.Fatal("a key that was not created was returned")
				}
				issued := gwbase.UserOf(user).State.Issued
				if len(issued) != 1 || issued[0].Status != gwbase.KeyRevoked || !slices.Contains(gateways(gateway.base().Cluster()).Secrets(), issued[0].AccessKey) {
					t.Fatal("the attempted access key left the redaction inventory")
				}
				return
			}
			if _, _, credErr := key.Credentials(); (credErr == nil) != tc.confirmed {
				t.Fatalf("confirmed %v, credentials error %v", tc.confirmed, credErr)
			}
			if _, err := user.Info(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestS3KeyWithUnknownCreationIsOnlyRevocable(t *testing.T) {
	gateway, ctr, user := newKeyTestUser(t)
	ctr.faults["key create"] = keyFault{err: errors.New("connection reset"), apply: true}
	// The readback that settles the failed call fails too.
	key, err := createWithFailedSettle(t, ctr, user, keyFault{code: 5})
	if err == nil || key == nil {
		t.Fatalf("an unknown outcome returned key %v, error %v", key, err)
	}
	if _, _, err := key.Credentials(); err == nil {
		t.Fatal("a key whose creation is unknown was handed off")
	}
	pending := gwbase.UserOf(user).State.Issued[0]
	if pending.Status != gwbase.KeyPending || !slices.Contains(gateways(gateway.base().Cluster()).Secrets(), pending.AccessKey) {
		t.Fatal("a key of unknown outcome was not tracked for redaction and cleanup")
	}
	if _, err := user.Info(t.Context()); err != nil {
		t.Fatalf("a pending key blocked the owned user: %v", err)
	}
	// Revoke settles it: native state holds the key, so it is removed.
	if err := key.Revoke(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(ctr.keys) != 1 || ctr.count("key create") != 1 || pending.Status != gwbase.KeyRevoked {
		t.Fatalf("settled keys %v after %d creations", len(ctr.keys), ctr.count("key create"))
	}
	if !slices.Contains(gateways(gateway.base().Cluster()).Secrets(), pending.NativeSecret) || pending.NativeSecret == "" {
		t.Fatal("the settled key's secret is not in the redaction inventory")
	}
}

// createWithFailedSettle runs CreateS3Key so that its key creation applies
// but fails, and the readback after it fails with fault.
func createWithFailedSettle(t *testing.T, ctr *keyTestContainer, user *User, fault keyFault) (*S3Key, error) {
	t.Helper()
	settled := &settleFaultContainer{keyTestContainer: ctr, fault: fault}
	g := gwbase.UserOf(user).Origin
	gateway := gateways(g.Cluster()).byName["default"]
	gateway.Container = settled
	defer func() { gateway.Container = ctr }()
	return user.CreateS3Key(t.Context())
}

// settleFaultContainer answers the user info that follows a key creation with
// fault, and everything else as its keyTestContainer.
type settleFaultContainer struct {
	*keyTestContainer
	fault   keyFault
	created bool
}

func (ctr *settleFaultContainer) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	argv := gwtest.Argv(args)
	if slices.Contains(argv, "key") && slices.Contains(argv, "create") {
		ctr.created = true
	} else if ctr.created && slices.Contains(argv, "info") {
		ctr.created = false
		ctr.faults["user info"] = ctr.fault
	}
	return ctr.keyTestContainer.Exec(ctx, args, opts...)
}

func TestS3KeyRevocationFailureIsSettled(t *testing.T) {
	_, ctr, user := newKeyTestUser(t)
	creation := user.CreationKey()
	if _, err := user.CreateS3Key(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A removal that answered with an error but applied is complete.
	ctr.faults["key rm"] = keyFault{code: 1, apply: true}
	if err := creation.Revoke(t.Context()); err != nil {
		t.Fatalf("an applied removal: %v", err)
	}

	_, ctr, user = newKeyTestUser(t)
	creation = user.CreationKey()
	if _, err := user.CreateS3Key(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A removal that did not apply keeps the key tracked and usable.
	ctr.faults["key rm"] = keyFault{code: 1}
	if err := creation.Revoke(t.Context()); err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("a failed removal: %v", err)
	}
	if _, err := user.Info(t.Context()); err != nil {
		t.Fatalf("a failed removal lost the key's ownership: %v", err)
	}
	if err := creation.Revoke(t.Context()); err != nil || len(ctr.keys) != 1 {
		t.Fatalf("Revoke after a failed removal: %v", err)
	}
	// Canceled before any command.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := len(ctr.calls)
	if _, err := user.CreateS3Key(ctx); !errors.Is(err, context.Canceled) || len(ctr.calls) != calls {
		t.Fatalf("a canceled CreateS3Key: %v", err)
	}
}

func TestS3KeyRevocationRefusesKeysChangedOutsideTheFixture(t *testing.T) {
	_, ctr, user := newKeyTestUser(t)
	creation := user.CreationKey()
	key, err := user.CreateS3Key(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	access, _ := credentials(t, key)
	index := slices.IndexFunc(ctr.keys, func(k gwbase.NativeKey) bool { return k.AccessKey == access })
	ctr.keys[index].SecretKey = "CHANGED"
	if _, err := user.Info(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a fixture key with a changed secret: %v", err)
	}
	if err := key.Revoke(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) || ctr.count("key rm") != 0 {
		t.Fatalf("revoking a key changed outside the fixture: %v", err)
	}
	// A fixture key removed outside the fixture is revoked by Revoke, which
	// restores the user's ownership checks.
	ctr.keys = slices.Delete(ctr.keys, index, index+1)
	if _, err := user.Info(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a fixture key removed outside the fixture: %v", err)
	}
	if err := key.Revoke(t.Context()); err != nil || ctr.count("key rm") != 0 {
		t.Fatalf("revoking a key already gone: %v", err)
	}
	if _, err := user.Info(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := creation.Revoke(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("revoking the last active key: %v", err)
	}
}

func TestS3KeyHandlesOfForeignOrUnconfirmedUsers(t *testing.T) {
	if (&User{}).CreationKey() != nil || (*User)(nil).CreationKey() != nil {
		t.Fatal("a handle without a confirmed creation has a creation key")
	}
	if _, _, err := (*S3Key)(nil).Credentials(); err == nil {
		t.Fatal("a nil key handle has credentials")
	}
	if err := (*S3Key)(nil).Revoke(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a nil key handle: %v", err)
	}
	if _, err := (&User{}).CreateS3Key(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a user not owned by a cluster: %v", err)
	}
	if value := fmt.Sprint(S3Key{}); value != "RGW S3 key" {
		t.Fatalf("zero key handle: %s", value)
	}
}
