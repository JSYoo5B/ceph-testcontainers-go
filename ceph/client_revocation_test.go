package ceph

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"testing"
)

// requireRevoked checks that client.app is absent, that client.other is
// unchanged, and that nothing of the call is left behind or can still apply.
func (w *rotationWorld) requireRevoked(t *testing.T) {
	t.Helper()
	if app, ok := w.clients["client.app"]; ok {
		t.Fatalf("client.app still holds %+v", app)
	}
	if other := w.clients["client.other"]; other.key != otherClient.key {
		t.Fatalf("client.other changed to %+v", other)
	}
	if len(w.controlFiles) != 0 || len(w.identities) != 0 {
		t.Fatalf("left behind: control files %v, identities %v", w.controlFiles, w.identities)
	}
	future := w.clone()
	future.releaseAll()
	if _, ok := future.clients["client.app"]; ok {
		t.Fatal("a late command still recreates client.app")
	}
}

// requireUsableClient checks that every operation accepts the descriptor.
func requireUsableClient(t *testing.T, c *Container, client *ClientConfig) {
	t.Helper()
	if caps, err := client.Capabilities(t.Context()); err != nil || caps != (ClientCaps{Mon: "allow r", OSD: "allow rw pool=data namespace=tenant-a"}) {
		t.Fatalf("ClientCapabilities: %+v, %v", caps, err)
	}
	if err := identityErr(c, client); err != nil {
		t.Fatal(err)
	}
}

func TestRevokeClientRestoresTheSameIdentity(t *testing.T) {
	w, c, client := clientWorld()
	c.config = []byte("[global]\n")
	original := w.clients["client.app"]
	revocation, err := client.Revoke(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	w.requireRevoked(t)
	if text := fmt.Sprintf("%v %+v %#v", revocation, revocation, revocation); strings.Contains(text, original.key) || !strings.Contains(revocation.String(), "client.app") {
		t.Fatalf("the revocation formats as %q", text)
	}
	if cc, err := client.ConnectionConfig(); err != nil || clientKey(cc.Keyring, "client.app") != original.key {
		t.Fatalf("the revoked descriptor's credentials: %v", err)
	}
	for name, err := range map[string]error{
		"ClientCapabilities": func() error { _, err := client.Capabilities(t.Context()); return err }(),
		"WithClientIdentity": identityErr(c, client),
		"UpdateClientCaps":   client.UpdateCaps(t.Context(), ClientCaps{Mon: "allow *"}),
		"ReplaceClientKey":   func() error { _, err := client.ReplaceKey(t.Context()); return err }(),
		"RevokeClient":       func() error { _, err := client.Revoke(t.Context()); return err }(),
		"CreateClient":       func() error { _, err := c.CreateClient(t.Context(), "app", ClientCaps{Mon: "allow r"}); return err }(),
	} {
		if err == nil || !strings.Contains(err.Error(), "revoked temporarily") {
			t.Fatalf("%s while revoked returned %v", name, err)
		}
	}
	w.requireRevoked(t)
	for range 2 {
		// A repeated Restore confirms the same result.
		if err := revocation.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
		w.requireClient(t, original.key)
		requireUsableClient(t, c, client)
	}
	if w.counts["add"] != 1 {
		t.Fatalf("the identity was imported %d times", w.counts["add"])
	}
	// The descriptor can be revoked again.
	again, err := client.Revoke(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := revocation.Restore(t.Context()); err == nil {
		t.Fatal("the earlier revocation restored the identity of a later one")
	}
	if err := again.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	w.requireClient(t, original.key)
}

func TestRevokeClientRefusesAChangedIdentity(t *testing.T) {
	w, _, client := clientWorld()
	w.clients["client.app"] = fakeClient{key: fakeCephxKey(CipherAES256K, 950), caps: maps.Clone(replacedCaps)}
	if revocation, err := client.Revoke(t.Context()); err == nil || revocation != nil {
		t.Fatalf("returned %v, %v", revocation, err)
	}
	if w.counts["remove"] != 0 || w.clients["client.app"].key != fakeCephxKey(CipherAES256K, 950) {
		t.Fatal("the refused revocation changed the identity")
	}
}

// A failed removal or restoration is settled and read back, or its outcome is
// resolved by the next Restore.
func TestRevokeClientSettlesFailures(t *testing.T) {
	for name, test := range map[string]struct {
		op     string
		effect faultEffect
		// unsettled makes settling fail, and canceled cancels the call while
		// its command hangs.
		unsettled, canceled bool
		// removed says whether the removal applies, for a fault in the
		// removal.
		removed bool
	}{
		"removal failed before reaching a MON":     {"remove", failsBefore, false, false, false},
		"reply lost after the removal applied":     {"remove", failsAfter, false, false, true},
		"canceled after the MON prepared it":       {"remove", hangsPrepared, false, true, true},
		"canceled before the MON prepared it":      {"remove", hangsQueued, false, true, false},
		"removal in flight":                        {"remove", hangsInFlight, false, false, true},
		"removal answered without applying":        {"remove", answersWithoutApplying, false, false, false},
		"prepared removal, settling failed":        {"remove", hangsPrepared, true, false, true},
		"queued removal, settling failed":          {"remove", hangsQueued, true, false, false},
		"restoration failed before reaching a MON": {"add", failsBefore, false, false, true},
		"reply lost after the restoration applied": {"add", failsAfter, false, false, true},
		"canceled restoration, prepared":           {"add", hangsPrepared, false, true, true},
		"canceled restoration, queued":             {"add", hangsQueued, false, true, true},
		"prepared restoration, settling failed":    {"add", hangsPrepared, true, false, true},
		"queued restoration, settling failed":      {"add", hangsQueued, true, false, true},
		"restoration input copy failed":            {"control-copy", failsBefore, false, false, true},
	} {
		t.Run(name, func(t *testing.T) {
			w, c, client := clientWorld()
			original := w.clients["client.app"]
			w.faults[test.op] = rotationFault{occurrence: 1, effect: test.effect}
			if test.unsettled && test.op == "remove" {
				w.faults["barrier"] = rotationFault{occurrence: 1}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if test.canceled {
				w.onHang = cancel
			}
			w.route = "b"
			revocation, err := client.Revoke(ctx)
			if test.op == "remove" {
				w.onHang = nil
				switch {
				case test.unsettled:
					if err == nil || revocation == nil || !strings.Contains(err.Error(), "not known yet") {
						t.Fatalf("an unsettled removal returned %v, %v", revocation, err)
					}
				case test.removed:
					if err != nil || revocation == nil {
						t.Fatalf("a removal that applied returned %v, %v", revocation, err)
					}
					w.requireRevoked(t)
				default:
					if err == nil || revocation != nil {
						t.Fatalf("a removal that did not apply returned %v, %v", revocation, err)
					}
					w.requireClient(t, original.key)
					requireUsableClient(t, c, client)
					return
				}
				if err := revocation.Restore(t.Context()); err != nil {
					t.Fatal(err)
				}
				w.requireClient(t, original.key)
				requireUsableClient(t, c, client)
				return
			}
			// The fault is in the restoration.
			if err != nil {
				t.Fatal(err)
			}
			if test.unsettled {
				w.faults["barrier"] = rotationFault{occurrence: w.counts["barrier"] + 1}
			}
			err = revocation.Restore(ctx)
			w.onHang = nil
			restored := !test.unsettled && (test.effect == failsAfter || test.effect == hangsPrepared)
			if restored != (err == nil) {
				t.Fatalf("the restoration returned %v", err)
			}
			if !restored {
				if identityErr(c, client) == nil {
					t.Fatal("the descriptor works before the restoration was confirmed")
				}
				if err := revocation.Restore(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			w.requireClient(t, original.key)
			requireUsableClient(t, c, client)
			// The first import applies late or not at all; only in the latter
			// case does a second one run. A failed input copy sends none.
			want := 2
			if test.effect == failsAfter || test.effect == hangsPrepared || test.op == "control-copy" {
				want = 1
			}
			if w.counts["add"] != want {
				t.Fatalf("the identity was imported %d times, want %d", w.counts["add"], want)
			}
		})
	}
}

func TestRevokeClientRefusesToOverwrite(t *testing.T) {
	w, _, client := clientWorld()
	original := w.clients["client.app"]
	revocation, err := client.Revoke(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for name, recreated := range map[string]fakeClient{
		"another key":  {key: fakeCephxKey(CipherAES256K, 970), caps: maps.Clone(replacedCaps)},
		"broader caps": {key: original.key, caps: map[string]string{"mon": "allow *"}},
	} {
		w.clients["client.app"] = recreated
		if err := revocation.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "refusing to overwrite") {
			t.Fatalf("%s: Restore returned %v", name, err)
		}
		if got := w.clients["client.app"]; got.key != recreated.key || !maps.Equal(got.caps, recreated.caps) {
			t.Fatalf("%s: Restore overwrote the identity", name)
		}
	}
}

func TestRevokeClientEndsWithDeleteClient(t *testing.T) {
	for name, restoredOutside := range map[string]bool{"identity absent": false, "identity restored outside": true} {
		t.Run(name, func(t *testing.T) {
			w, c, client := clientWorld()
			original := w.clients["client.app"]
			revocation, err := client.Revoke(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if restoredOutside {
				w.clients["client.app"] = original
			}
			if err := client.Delete(t.Context()); err != nil {
				t.Fatal(err)
			}
			w.requireRevoked(t)
			if err := revocation.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "deleted for good") {
				t.Fatalf("Restore after DeleteClient returned %v", err)
			}
			if err := client.Delete(t.Context()); err != nil {
				t.Fatalf("a repeated DeleteClient returned %v", err)
			}
			if c.clientRevocations["client.app"] != nil {
				t.Fatal("the name stays reserved")
			}
		})
	}
}

// An identity without capabilities, which auth import refuses, is restored
// exactly too.
func TestRevokeClientRestoresAnIdentityWithoutCaps(t *testing.T) {
	w, _, client := clientWorld()
	original := fakeClient{key: w.clients["client.app"].key, caps: map[string]string{}}
	w.clients["client.app"] = original
	revocation, err := client.Revoke(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := revocation.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, ok := w.clients["client.app"]; !ok || got.key != original.key || len(got.caps) != 0 {
		t.Fatalf("restored %+v", got)
	}
}

// Copies of a revocation share its state, like the other handles.
func TestClientRevocationCopiesShareState(t *testing.T) {
	w, c, client := clientWorld()
	original := w.clients["client.app"]
	revocation, err := client.Revoke(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restoring := *revocation
	if err := restoring.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	w.requireClient(t, original.key)
	requireUsableClient(t, c, client)
	if c.clientRevocations["client.app"] != nil || revocation.Restore(t.Context()) != nil {
		t.Fatal("the original handle does not see the restoration of its copy")
	}
	again, err := client.Revoke(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	deleting := *again
	if err := client.Delete(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := deleting.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "deleted for good") {
		t.Fatalf("a copy restored a deleted identity: %v", err)
	}
	w.requireRevoked(t)
}

// A permanent removal that settling confirms during Restore is final.
func TestClientRevocationRestoreAfterAnUncertainDeletion(t *testing.T) {
	w, _, client := clientWorld()
	w.faults["remove"] = rotationFault{occurrence: 1, effect: hangsQueued}
	w.faults["barrier"] = rotationFault{occurrence: 1}
	revocation, err := client.Revoke(t.Context())
	if err == nil || revocation == nil {
		t.Fatalf("an unsettled removal returned %v, %v", revocation, err)
	}
	// DeleteClient finds that the temporary removal was dropped and removes
	// the identity for good, uncertainly again.
	w.faults["remove"] = rotationFault{occurrence: 2, effect: hangsPrepared}
	w.faults["barrier"] = rotationFault{occurrence: w.counts["barrier"] + 4}
	if err := client.Delete(t.Context()); err == nil || !strings.Contains(err.Error(), "not known yet") {
		t.Fatalf("an unsettled permanent removal returned %v", err)
	}
	if err := revocation.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "deleted for good") {
		t.Fatalf("Restore after the permanent removal returned %v", err)
	}
	w.requireRevoked(t)
	if w.counts["add"] != 0 {
		t.Fatal("Restore recreated the identity")
	}
}

// A revocation belongs to the identity: every descriptor of the revoked key
// sees it, and a permanent removal through any of them is final.
func TestClientRevocationIsSharedByEveryDescriptor(t *testing.T) {
	w, _, client := clientWorld()
	copied := *client
	revocation, err := client.Revoke(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (&copied).Capabilities(t.Context()); err == nil || !strings.Contains(err.Error(), "revoked temporarily") {
		t.Fatalf("a copied descriptor returned %v while revoked", err)
	}
	if err := (&copied).Delete(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := revocation.Restore(t.Context()); err == nil || !strings.Contains(err.Error(), "deleted for good") {
		t.Fatalf("Restore after a removal through a copied descriptor returned %v", err)
	}
	w.requireRevoked(t)
}

// The descriptor that an uncertain replacement returns stands for no
// confirmed key, so no operation, a removal included, accepts it.
func TestUncertainReplacementDescriptorIsRefused(t *testing.T) {
	w, _, client := clientWorld()
	w.faults["import"] = rotationFault{occurrence: 1, effect: hangsPrepared}
	w.faults["barrier"] = rotationFault{occurrence: 1}
	attempted, err := client.ReplaceKey(t.Context())
	if err == nil || attempted == nil {
		t.Fatalf("an unsettled import returned %v, %v", attempted, err)
	}
	current, err := client.ReplaceKey(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// A temporary removal that turns out not to have applied leaves the
	// identity with the key of both descriptors.
	w.faults["remove"] = rotationFault{occurrence: 1, effect: hangsQueued}
	w.faults["barrier"] = rotationFault{occurrence: w.counts["barrier"] + 1}
	revocation, err := current.Revoke(t.Context())
	if err == nil || revocation == nil {
		t.Fatalf("an unsettled removal returned %v, %v", revocation, err)
	}
	if err := attempted.Delete(t.Context()); err == nil {
		t.Fatal("DeleteClient accepted the uncertain descriptor")
	}
	if err := revocation.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	w.requireClient(t, clientKey(current.keyring, "client.app"))
}

// Diagnostics redact the original key that a revocation keeps for Restore,
// while revoked and after it.
func TestDiagnosticsRedactRevokedClientKeys(t *testing.T) {
	w, c, client := clientWorld()
	original := w.clients["client.app"].key
	revocation, err := client.Revoke(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	requireRedacted(t, c, original)
	c.Container = &rotationControl{w: w}
	if err := revocation.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	requireRedacted(t, c, original)
}
