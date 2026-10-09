package cluster

import (
	"context"
	"errors"
	"io"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestCephFSAuthorizationContextPublicAdmissionDeadline(t *testing.T) {
	for _, operation := range []string{"authorize", "list", "deauthorize", "evict"} {
		for _, gate := range []string{"setup", "owner", "control"} {
			t.Run(operation+"/"+gate, func(t *testing.T) {
				fs, native, volume, grant := cephFSAuthorizationContextFixture(t, operation)
				beforeAuth := cephFSAuthorizationContextAuthSnapshot(native)
				beforeGrants := maps.Clone(native.grants)
				beforeState := cephFSAuthorizationContextGrantState(grant)
				beforeTracked := len(volume.identity.authorizations)
				var result *CephFSSubvolumeAuthorization
				assertCephFSContextAdmissionGate(t, fs.cluster, gate, func(ctx context.Context) error {
					var err error
					result, err = cephFSAuthorizationContextOperation(ctx, fs, volume, grant, operation)
					return err
				})
				if result != nil || len(native.calls) != 0 ||
					!reflect.DeepEqual(beforeAuth, cephFSAuthorizationContextAuthSnapshot(native)) ||
					!maps.Equal(beforeGrants, native.grants) ||
					!reflect.DeepEqual(beforeState, cephFSAuthorizationContextGrantState(grant)) ||
					len(volume.identity.authorizations) != beforeTracked || fs.cluster.closed ||
					fs.cluster.filesystems[fs.config.Name] != fs {
					t.Fatal("queued authorization mutated native state, ownership or attempted-grant flags")
				}
				assertCephFSContextAdmissionReleased(t, fs.cluster)
				result, err := cephFSAuthorizationContextOperation(t.Context(), fs, volume, grant, operation)
				if err != nil {
					t.Fatalf("fresh context could not reuse its original scope: %v", err)
				}
				switch operation {
				case "authorize":
					if result == nil || !result.identity.authorized {
						t.Fatal("fresh authorization did not confirm its original isolated subvolume")
					}
				case "deauthorize":
					if !grant.identity.deauthorized || grant.identity.client.revoked {
						t.Fatal("fresh scoped revocation lost its shared state or revoked the retained MON principal")
					}
				case "evict":
					if cephFSAuthorizationContextMutationCount(native.calls, "evict") != 1 {
						t.Fatal("fresh scoped eviction did not reach exactly one native command")
					}
				}
				cephFSAuthorizationContextAssertForeign(t, native)
			})
		}
	}
}

func TestCephFSAuthorizationContextCanceledFreeAdmission(t *testing.T) {
	for _, operation := range []string{"authorize", "list", "deauthorize", "evict"} {
		t.Run(operation, func(t *testing.T) {
			fs, native, volume, grant := cephFSAuthorizationContextFixture(t, operation)
			beforeState := cephFSAuthorizationContextGrantState(grant)
			beforeAuth := cephFSAuthorizationContextAuthSnapshot(native)
			beforeTracked := len(volume.identity.authorizations)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			result, err := cephFSAuthorizationContextOperation(ctx, fs, volume, grant, operation)
			if !errors.Is(err, context.Canceled) || result != nil || len(native.calls) != 0 ||
				!reflect.DeepEqual(beforeState, cephFSAuthorizationContextGrantState(grant)) ||
				!reflect.DeepEqual(beforeAuth, cephFSAuthorizationContextAuthSnapshot(native)) ||
				len(volume.identity.authorizations) != beforeTracked {
				t.Fatalf("canceled admission lost its causal error or changed authorization: %v", err)
			}
			assertCephFSContextAdmissionReleased(t, fs.cluster)
			if _, err := fs.SubvolumeAuthorizedClients(t.Context(), "volume", "group"); err != nil {
				t.Fatalf("canceled admission retained the setup gate: %v", err)
			}
		})
	}
}

func TestCephFSAuthorizationContextLateOwnerAdmissionCancellation(t *testing.T) {
	for _, operation := range []string{"deauthorize", "evict"} {
		t.Run(operation, func(t *testing.T) {
			fs, native, volume, grant := cephFSAuthorizationContextFixture(t, operation)
			beforeAuth := cephFSAuthorizationContextAuthSnapshot(native)
			beforeGrants := maps.Clone(native.grants)
			beforeState := cephFSAuthorizationContextGrantState(grant)
			control := &cephFSAuthorizationContextControl{cephFSAuthFixtureContainer: native}
			fs.cluster.Container = control
			assertCephFSContextLateOwnerGate(t, &fs.cluster.mu, func(hook func()) {
				// The initial begin-operation pool query precedes subvolume
				// info. The subsequent pool query completes the original
				// volume/layout readback immediately before owner admission.
				sawVolume := false
				control.afterExec = func(args []string) {
					if slices.Equal(args, []string{"fs", "subvolume", "info", "fixture", "volume", "--group_name", "group", "--format", "json"}) {
						sawVolume = true
					}
					if sawVolume && slices.Equal(args, []string{"osd", "pool", "ls", "detail", "--format", "json"}) {
						hook()
					}
				}
			}, func(ctx context.Context) error {
				_, err := cephFSAuthorizationContextOperation(ctx, fs, volume, grant, operation)
				return err
			})
			if !reflect.DeepEqual(beforeAuth, cephFSAuthorizationContextAuthSnapshot(native)) ||
				!maps.Equal(beforeGrants, native.grants) ||
				!reflect.DeepEqual(beforeState, cephFSAuthorizationContextGrantState(grant)) ||
				len(volume.identity.authorizations) != 1 {
				t.Fatal("expired later admission changed the original grant, client or shared retry state")
			}
			for _, call := range native.calls {
				if call[0] == "auth" || call[0] == "ceph-authtool" || call[0] == "rm" ||
					cephFSAuthorizationContextIsMutation(call) {
					t.Fatal("later owner admission continued into principal inspection or mutation")
				}
			}
			assertCephFSContextAdmissionReleased(t, fs.cluster)
			control.afterExec = nil
			if _, err := cephFSAuthorizationContextOperation(t.Context(), fs, volume, grant, operation); err != nil {
				t.Fatalf("fresh retry lost its original private identity: %v", err)
			}
			cephFSAuthorizationContextAssertForeign(t, native)
		})
	}
}

func TestCephFSAuthorizationContextCanceledAfterClientCreationReturnsClient(t *testing.T) {
	fs, native, volume, _ := cephFSAuthorizationContextFixture(t, "authorize")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	control := &cephFSAuthorizationContextControl{cephFSAuthFixtureContainer: native}
	var once sync.Once
	control.afterExec = func(args []string) {
		if len(args) == 3 && args[0] == "rm" && args[1] == "-f" {
			// CreateClient already confirmed and published ready before its
			// deferred keyring cleanup; it still owns c.mu at this boundary.
			once.Do(cancel)
		}
	}
	fs.cluster.Container = control
	grant, err := fs.AuthorizeSubvolume(ctx, volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"})
	if !errors.Is(err, context.Canceled) || grant == nil || grant.Client == nil ||
		grant.Client != grant.identity.client || !grant.Client.created || !grant.Client.ready || grant.Client.revoked ||
		grant.Client.owner != fs.cluster || grant.Client.Name() != "client.writer" ||
		grant.identity.authorizeAttempted || grant.identity.authorized || grant.identity.deauthAttempted ||
		len(volume.identity.authorizations) != 0 || native.grants["writer"] != "" {
		t.Fatalf("post-created cancellation discarded or adopted a partial grant: %v", err)
	}
	entry := native.auth["client.writer"]
	if entry == nil || entry.Key != clientKey(grant.Client.keyring, grant.Client.name) ||
		!equalAuthorizationCaps(entry.Caps, map[string]string{"mon": "allow r fsname=fixture"}) ||
		cephFSAuthorizationContextMutationCount(native.calls, "authorize") != 0 {
		t.Fatal("post-created cancellation changed the fresh key, granted file rights or deleted its principal")
	}
	for _, call := range native.calls {
		if len(call) > 1 && call[0] == "auth" && (call[1] == "del" || call[1] == "caps") {
			t.Fatal("post-created cancellation performed automatic principal compensation")
		}
	}
	assertCephFSContextAdmissionReleased(t, fs.cluster)
	// An unattempted grant cannot be adopted for native deauthorization.
	before := len(native.calls)
	if err := fs.DeauthorizeSubvolume(t.Context(), grant); err == nil || len(native.calls) != before {
		t.Fatal("unattempted grant was adopted by native cleanup")
	}
	entry.Caps["mgr"] = "allow r"
	if duplicate, err := fs.AuthorizeSubvolume(t.Context(), volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"}); err == nil || duplicate != nil ||
		native.auth["client.writer"] != entry || entry.Caps["mgr"] != "allow r" {
		t.Fatal("fresh retry adopted or overwrote the already-created principal")
	}
	// A new principal can use the same fixture; the original client remains
	// explicitly owned and no cleanup operation revokes its independent rights.
	fresh, err := fs.AuthorizeSubvolume(t.Context(), volume, CephFSSubvolumeAuthorizationConfig{ClientID: "fresh", Access: "r"})
	if err != nil || fresh == nil || !fresh.identity.authorized || native.auth["client.writer"] != entry {
		t.Fatalf("fresh context could not reuse the original subvolume safely: %v", err)
	}
	cephFSAuthorizationContextAssertForeign(t, native)
}

func TestCephFSAuthorizationContextFinalReadbackPublishesAuthorizedState(t *testing.T) {
	fs, native, volume, _ := cephFSAuthorizationContextFixture(t, "authorize")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	control := &cephFSAuthorizationContextControl{cephFSAuthFixtureContainer: native}
	control.afterExec = func(args []string) {
		if cephFSAuthorizationContextIsCommand(args, "authorized_list") && native.grants["writer"] == "rw" {
			cancel()
		}
	}
	fs.cluster.Container = control
	grant, err := fs.AuthorizeSubvolume(ctx, volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"})
	if err != nil || !errors.Is(ctx.Err(), context.Canceled) || grant == nil ||
		!grant.identity.authorizeAttempted || !grant.identity.authorized ||
		grant.identity.deauthorized || grant.Client.revoked || len(volume.identity.authorizations) != 1 {
		t.Fatalf("successful final native readback lost authorized bookkeeping after cancellation: %v", err)
	}
	assertCephFSContextAdmissionReleased(t, fs.cluster)
	copy := *grant
	copy.Client, copy.AuthID, copy.SubvolumeName, copy.GroupName, copy.Path = nil, "foreign", "foreign", "foreign", "/foreign"
	if err := fs.DeauthorizeSubvolume(t.Context(), &copy); err != nil || !grant.identity.deauthorized {
		t.Fatalf("copied grant could not revoke its original confirmed scope: %v", err)
	}
	cephFSAuthorizationContextAssertForeign(t, native)
}

func TestCephFSAuthorizationContextFinalReadbackPublishesDeauthorization(t *testing.T) {
	for _, retainNeighbor := range []bool{true, false} {
		name := "native principal deletion"
		if retainNeighbor {
			name = "neighbor rights and key"
		}
		t.Run(name, func(t *testing.T) {
			fs, native, _, grant := cephFSAuthorizationContextFixture(t, "deauthorize")
			entry := native.auth["client.writer"]
			originalKey := entry.Key
			remaining := map[string]string{}
			if retainNeighbor {
				entry.Caps["mgr"] = "allow r"
				entry.Caps["mds"] += ", allow r path=/neighbor"
				entry.Caps["osd"] += ", allow r pool=other namespace=neighbor"
				remaining = map[string]string{"mon": "allow r fsname=fixture", "mgr": "allow r",
					"mds": "allow r path=/neighbor", "osd": "allow r pool=other namespace=neighbor"}
			} else {
				// Native volumes removes this generic MON clause with the last
				// MDS/OSD grant, deleting only the original no-rights principal.
				entry.Caps["mon"] = "allow r"
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			control := &cephFSAuthorizationContextControl{cephFSAuthFixtureContainer: native}
			var once sync.Once
			control.afterExec = func(args []string) {
				if cephFSAuthorizationContextIsCommand(args, "authorized_list") && native.grants["writer"] == "" {
					once.Do(cancel)
				}
			}
			fs.cluster.Container = control
			copy := *grant
			copy.Client, copy.AuthID, copy.Access, copy.SubvolumeName, copy.GroupName, copy.Path = nil, "foreign", "r", "foreign", "foreign", "/foreign"
			if err := fs.DeauthorizeSubvolume(ctx, &copy); err != nil || !errors.Is(ctx.Err(), context.Canceled) ||
				!grant.identity.deauthorized || grant.identity.client.revoked != !retainNeighbor ||
				!equalAuthorizationCaps(grant.identity.remainingCaps, remaining) {
				t.Fatalf("final revoke readback lost its confirmed original state: %v", err)
			}
			if retainNeighbor {
				if native.auth["client.writer"] != entry || entry.Key != originalKey || !equalAuthorizationCaps(entry.Caps, remaining) {
					t.Fatal("scoped revoke changed the original key or independent capabilities")
				}
			} else if native.auth["client.writer"] != nil {
				t.Fatal("confirmed native principal disappearance was not observed")
			}
			assertCephFSContextAdmissionReleased(t, fs.cluster)
			native.calls = nil
			if err := fs.DeauthorizeSubvolume(t.Context(), grant); err != nil ||
				cephFSAuthorizationContextMutationCount(native.calls, "deauthorize") != 0 {
				t.Fatalf("copied confirmed state repeated native deauthorization: %v", err)
			}
			if err := fs.EvictSubvolumeClients(t.Context(), &copy); err != nil ||
				cephFSAuthorizationContextMutationCount(native.calls, "evict") != 1 {
				t.Fatalf("fresh eviction lost its original mount/principal scope: %v", err)
			}
			cephFSAuthorizationContextAssertForeign(t, native)
		})
	}
}

func TestCephFSAuthorizationContextNativeMutationCancellationRetainsRecovery(t *testing.T) {
	for _, operation := range []string{"authorize", "deauthorize"} {
		t.Run(operation, func(t *testing.T) {
			fs, native, volume, grant := cephFSAuthorizationContextFixture(t, operation)
			if operation == "deauthorize" {
				entry := native.auth["client.writer"]
				entry.Caps["mgr"] = "allow r"
				entry.Caps["mds"] += ", allow r path=/neighbor"
				entry.Caps["osd"] += ", allow r pool=other namespace=neighbor"
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			control := &cephFSAuthorizationContextControl{cephFSAuthFixtureContainer: native}
			var once sync.Once
			control.afterExec = func(args []string) {
				if cephFSAuthorizationContextIsCommand(args, operation) {
					once.Do(cancel)
				}
			}
			fs.cluster.Container = control
			switch operation {
			case "authorize":
				var err error
				grant, err = fs.AuthorizeSubvolume(ctx, volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"})
				if !errors.Is(err, context.Canceled) || grant == nil || !grant.identity.authorizeAttempted ||
					grant.identity.authorized || len(volume.identity.authorizations) != 1 || native.grants["writer"] != "rw" {
					t.Fatalf("unknown native grant success discarded its attempted ownership: %v", err)
				}
			default:
				if err := fs.DeauthorizeSubvolume(ctx, grant); !errors.Is(err, context.Canceled) ||
					!grant.identity.deauthAttempted || grant.identity.deauthorized || native.grants["writer"] != "" {
					t.Fatalf("unknown native revoke success discarded its retry state: %v", err)
				}
			}
			assertCephFSContextAdmissionReleased(t, fs.cluster)
			copy := *grant
			copy.Client, copy.AuthID, copy.SubvolumeName, copy.GroupName, copy.Path = nil, "foreign", "foreign", "foreign", "/foreign"
			native.calls = nil
			if err := fs.DeauthorizeSubvolume(t.Context(), &copy); err != nil || !grant.identity.deauthorized {
				t.Fatalf("fresh copied recovery lost the original native scope: %v", err)
			}
			wantRevoke := 0
			if operation == "authorize" {
				wantRevoke = 1
			}
			if cephFSAuthorizationContextMutationCount(native.calls, "deauthorize") != wantRevoke {
				t.Fatal("recovery repeated a persisted revoke or failed to remove the attempted grant")
			}
			if operation == "deauthorize" {
				caps := native.auth["client.writer"].Caps
				if caps["mgr"] != "allow r" || caps["mds"] != "allow r path=/neighbor" ||
					caps["osd"] != "allow r pool=other namespace=neighbor" {
					t.Fatal("uncertain revoke recovery erased independent capabilities")
				}
			}
			cephFSAuthorizationContextAssertForeign(t, native)
		})
	}
}

func TestCephFSAuthorizationContextRecoveryRejectsReplacementScope(t *testing.T) {
	for _, replacement := range []string{"key", "filesystem", "pool", "namespace", "UUID"} {
		t.Run(replacement, func(t *testing.T) {
			fs, native, volume, _ := cephFSAuthorizationContextFixture(t, "authorize")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			control := &cephFSAuthorizationContextControl{cephFSAuthFixtureContainer: native}
			var once sync.Once
			control.afterExec = func(args []string) {
				if cephFSAuthorizationContextIsCommand(args, "authorize") {
					once.Do(cancel)
				}
			}
			fs.cluster.Container = control
			grant, err := fs.AuthorizeSubvolume(ctx, volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"})
			if !errors.Is(err, context.Canceled) || grant == nil || grant.identity.authorized || !grant.identity.authorizeAttempted {
				t.Fatalf("test did not retain an uncertain original grant: %v", err)
			}
			switch replacement {
			case "key":
				native.auth["client.writer"].Key = "REPLACEMENT-KEY"
			case "filesystem":
				key := "fs dump --format json"
				native.output[key] = strings.ReplaceAll(native.output[key], `"id":41`, `"id":99`)
			case "pool":
				key := "osd pool ls detail --format json"
				native.output[key] = strings.ReplaceAll(native.output[key], `"pool_id":3`, `"pool_id":4`)
			case "namespace":
				key := "fs subvolume info fixture volume --group_name group --format json"
				native.output[key] = strings.ReplaceAll(native.output[key], "fsvolumens_group_volume", "replacement")
			case "UUID":
				key := "fs subvolume info fixture volume --group_name group --format json"
				native.output[key] = strings.ReplaceAll(native.output[key], "unique-id", "replacement")
			}
			beforeAuth := cephFSAuthorizationContextAuthSnapshot(native)
			beforeGrants := maps.Clone(native.grants)
			beforeState := cephFSAuthorizationContextGrantState(grant)
			native.calls = nil
			if err := fs.DeauthorizeSubvolume(t.Context(), grant); err == nil ||
				!reflect.DeepEqual(beforeAuth, cephFSAuthorizationContextAuthSnapshot(native)) ||
				!maps.Equal(beforeGrants, native.grants) ||
				!reflect.DeepEqual(beforeState, cephFSAuthorizationContextGrantState(grant)) {
				t.Fatal("uncertain recovery adopted or changed a replacement identity/scope")
			}
			for _, call := range native.calls {
				if cephFSAuthorizationContextIsMutation(call) || len(call) > 1 && call[0] == "auth" && (call[1] == "caps" || call[1] == "del") {
					t.Fatal("replacement scope reached native compensation or revocation")
				}
			}
			assertCephFSContextAdmissionReleased(t, fs.cluster)
			cephFSAuthorizationContextAssertForeign(t, native)
		})
	}
}

type cephFSAuthorizationContextControl struct {
	*cephFSAuthFixtureContainer
	afterExec func([]string)
}

func (control *cephFSAuthorizationContextControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	code, reader, err := control.cephFSAuthFixtureContainer.Exec(ctx, args, opts...)
	if err == nil && code == 0 && control.afterExec != nil {
		module := args
		if args[0] == "ceph" {
			module = args[3:]
		}
		control.afterExec(module)
	}
	return code, reader, err
}

func cephFSAuthorizationContextFixture(t *testing.T, operation string) (*CephFSContainer, *cephFSAuthFixtureContainer, *CephFSSubvolume, *CephFSSubvolumeAuthorization) {
	t.Helper()
	fs, native, volume := cephFSAuthorizationFixture(t)
	native.auth["client.foreign"] = &cephFSAuthFixtureEntry{
		Entity: "client.foreign", Key: "FOREIGN-KEY",
		Caps: map[string]string{"mon": "allow r", "mgr": "allow r", "osd": "allow r pool=foreign namespace=other"},
	}
	native.grants["foreign"] = "r"
	var grant *CephFSSubvolumeAuthorization
	if operation == "deauthorize" || operation == "evict" {
		var err error
		grant, err = fs.AuthorizeSubvolume(t.Context(), volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"})
		if err != nil {
			t.Fatal(err)
		}
		if operation == "evict" {
			if err := fs.DeauthorizeSubvolume(t.Context(), grant); err != nil {
				t.Fatal(err)
			}
		}
	}
	native.calls = nil
	return fs, native, volume, grant
}

func cephFSAuthorizationContextOperation(ctx context.Context, fs *CephFSContainer, volume *CephFSSubvolume, grant *CephFSSubvolumeAuthorization, operation string) (*CephFSSubvolumeAuthorization, error) {
	switch operation {
	case "authorize":
		return fs.AuthorizeSubvolume(ctx, volume, CephFSSubvolumeAuthorizationConfig{ClientID: "writer", Access: "rw"})
	case "list":
		_, err := fs.SubvolumeAuthorizedClients(ctx, "volume", "group")
		return nil, err
	case "deauthorize", "evict":
		copy := *grant
		copy.Client, copy.AuthID, copy.Access, copy.SubvolumeName, copy.GroupName, copy.Path = nil, "foreign", "r", "foreign", "foreign", "/foreign"
		if operation == "deauthorize" {
			return nil, fs.DeauthorizeSubvolume(ctx, &copy)
		}
		return nil, fs.EvictSubvolumeClients(ctx, &copy)
	default:
		panic("unknown authorization context operation")
	}
}

type cephFSAuthorizationContextState struct {
	attempted, authorized, revokeAttempted, deauthorized, revoked bool
	created, ready                                                bool
	remaining                                                     map[string]string
}

func cephFSAuthorizationContextGrantState(grant *CephFSSubvolumeAuthorization) cephFSAuthorizationContextState {
	if grant == nil {
		return cephFSAuthorizationContextState{}
	}
	identity := grant.identity
	return cephFSAuthorizationContextState{
		attempted: identity.authorizeAttempted, authorized: identity.authorized,
		revokeAttempted: identity.deauthAttempted, deauthorized: identity.deauthorized,
		revoked: identity.client.revoked, created: identity.client.created, ready: identity.client.ready,
		remaining: maps.Clone(identity.remainingCaps),
	}
}

func cephFSAuthorizationContextAuthSnapshot(native *cephFSAuthFixtureContainer) map[string]cephFSAuthFixtureEntry {
	result := make(map[string]cephFSAuthFixtureEntry, len(native.auth))
	for name, entry := range native.auth {
		result[name] = cephFSAuthFixtureEntry{Entity: entry.Entity, Key: entry.Key, Caps: maps.Clone(entry.Caps)}
	}
	return result
}

func cephFSAuthorizationContextAssertForeign(t *testing.T, native *cephFSAuthFixtureContainer) {
	t.Helper()
	entry := native.auth["client.foreign"]
	if entry == nil || entry.Entity != "client.foreign" || entry.Key != "FOREIGN-KEY" || native.grants["foreign"] != "r" ||
		!equalAuthorizationCaps(entry.Caps, map[string]string{"mon": "allow r", "mgr": "allow r", "osd": "allow r pool=foreign namespace=other"}) {
		t.Fatal("operation changed an unrelated principal, key, capabilities or native grant")
	}
}

func cephFSAuthorizationContextIsCommand(args []string, action string) bool {
	return len(args) > 2 && args[0] == "fs" && args[1] == "subvolume" && args[2] == action
}

func cephFSAuthorizationContextIsMutation(args []string) bool {
	return cephFSAuthorizationContextIsCommand(args, "authorize") ||
		cephFSAuthorizationContextIsCommand(args, "deauthorize") ||
		cephFSAuthorizationContextIsCommand(args, "evict")
}

func cephFSAuthorizationContextMutationCount(calls [][]string, action string) int {
	count := 0
	for _, call := range calls {
		if cephFSAuthorizationContextIsCommand(call, action) {
			count++
		}
	}
	return count
}
