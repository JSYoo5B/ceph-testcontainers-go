package ceph

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

var (
	replacedCaps = map[string]string{"mon": "allow r", "osd": "allow rw pool=data namespace=tenant-a"}
	otherClient  = fakeClient{key: fakeCephxKey(CipherAES, 901), caps: map[string]string{"mon": "allow r"}}
)

// clientWorld returns a fake cluster that owns client.app, whose key is
// AES256K, and holds client.other, which no test changes.
func clientWorld() (*rotationWorld, *Container, *ClientConfig) {
	w := newRotationWorld()
	w.clients["client.app"] = fakeClient{key: fakeCephxKey(CipherAES256K, 900), caps: maps.Clone(replacedCaps)}
	w.clients["client.other"] = otherClient
	c := rotationCluster(w)
	c.startup.recordKeyTypeOption(true)
	client := &ClientConfig{owner: c, name: "client.app", config: []byte("[global]\n"),
		keyring: []byte(fakeClientKeyring("client.app", w.clients["client.app"])), created: true, ready: true}
	c.authRecords.confirmKey("client.app", w.clients["client.app"].key)
	return w, c, client
}

// requireClient checks that client.app has key and the original caps, that
// client.other is unchanged, that the call left no file, identity or MON
// command behind, and that no late command can still change an entry.
func (w *rotationWorld) requireClient(t *testing.T, key string) {
	t.Helper()
	app, ok := w.clients["client.app"]
	if !ok || app.key != key || !maps.Equal(app.caps, replacedCaps) {
		t.Fatalf("client.app holds %+v, want key %s with %v", app, key, replacedCaps)
	}
	if other := w.clients["client.other"]; other.key != otherClient.key || !maps.Equal(other.caps, otherClient.caps) {
		t.Fatalf("client.other changed to %+v", other)
	}
	if len(w.controlFiles) != 0 || len(w.identities) != 0 {
		t.Fatalf("left behind: control files %v, identities %v", w.controlFiles, w.identities)
	}
	future := w.clone()
	future.releaseAll()
	if !maps.EqualFunc(future.clients, w.clients, func(a, b fakeClient) bool { return a.key == b.key && maps.Equal(a.caps, b.caps) }) {
		t.Fatalf("a late command still changes the clients: %+v, then %+v", w.clients, future.clients)
	}
}

func identityErr(client *ClientConfig) error {
	return client.WithIdentity()(&testcontainers.GenericContainerRequest{})
}

func TestReplaceClientKeyKeepsIdentityAndCaps(t *testing.T) {
	w, _, client := clientWorld()
	old := w.clients["client.app"].key
	next, err := client.ReplaceKey(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key := clientKey(next.keyring, "client.app")
	if key == old || !next.ready || next.Name() != "client.app" {
		t.Fatalf("new descriptor: key %s (old %s), ready %t", key, old, next.ready)
	}
	w.requireClient(t, key)
	if cipher, _ := keyCipher(key); cipher != CipherAES256K {
		t.Fatalf("the new key is %s, not the old key's type", cipher)
	}
	if cc, err := client.ConnectionConfig(); err != nil || clientKey(cc.Keyring, "client.app") != old {
		t.Fatalf("the old descriptor's credentials changed: %v", err)
	}
	for name, err := range map[string]error{
		"ClientCapabilities": func() error { _, err := client.Capabilities(t.Context()); return err }(),
		"WithIdentity":       identityErr(client),
		"DeleteClient":       client.Delete(t.Context()),
		"UpdateClientCaps":   client.UpdateCaps(t.Context(), ClientCaps{Mon: "allow *"}),
	} {
		if err == nil || !strings.Contains(err.Error(), "has a new key") {
			t.Fatalf("%s with the old descriptor returned %v", name, err)
		}
	}
	if caps, err := next.Capabilities(t.Context()); err != nil || caps != (ClientCaps{Mon: "allow r", OSD: "allow rw pool=data namespace=tenant-a"}) {
		t.Fatalf("ClientCapabilities with the new descriptor: %+v, %v", caps, err)
	}
	if err := identityErr(next); err != nil {
		t.Fatal(err)
	}
	// Replacing the old key again finds it replaced.
	imports := w.counts["import"]
	if again, err := client.ReplaceKey(t.Context()); err != nil || again != next || w.counts["import"] != imports {
		t.Fatalf("a repeated replacement returned %p (want %p), %v, after %d imports", again, next, err, w.counts["import"]-imports)
	}
	third, err := next.ReplaceKey(t.Context(), ClientOptions{}.WithKeyType(CipherAES))
	if err != nil {
		t.Fatal(err)
	}
	w.requireClient(t, clientKey(third.keyring, "client.app"))
	if cipher, _ := keyCipher(clientKey(third.keyring, "client.app")); cipher != CipherAES {
		t.Fatalf("ClientOptions{}.WithKeyType(aes) gave a %s key", cipher)
	}
	if err := third.Delete(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ok := w.clients["client.app"]; ok {
		t.Fatal("DeleteClient with the newest descriptor kept the identity")
	}
}

func TestReplaceClientKeyRefusesBeforeAnyChange(t *testing.T) {
	for name, setup := range map[string]func(*rotationWorld, *Container, *ClientConfig) *ClientConfig{
		"key changed outside": func(w *rotationWorld, _ *Container, client *ClientConfig) *ClientConfig {
			w.clients["client.app"] = fakeClient{key: fakeCephxKey(CipherAES256K, 950), caps: maps.Clone(replacedCaps)}
			return client
		},
		"identity removed outside": func(w *rotationWorld, _ *Container, client *ClientConfig) *ClientConfig {
			delete(w.clients, "client.app")
			return client
		},
		"another cluster's client": func(_ *rotationWorld, _ *Container, client *ClientConfig) *ClientConfig {
			foreign := *client
			foreign.owner = &Container{}
			return &foreign
		},
		"revoked client": func(_ *rotationWorld, _ *Container, client *ClientConfig) *ClientConfig {
			client.revoked = true
			return client
		},
	} {
		t.Run(name, func(t *testing.T) {
			w, c, client := clientWorld()
			client = setup(w, c, client)
			before := maps.Clone(w.clients)
			if next, err := client.ReplaceKey(t.Context()); err == nil || next != nil {
				t.Fatalf("returned %v, %v", next, err)
			}
			if w.counts["import"] != 0 || !maps.EqualFunc(before, w.clients, func(a, b fakeClient) bool { return a.key == b.key }) {
				t.Fatalf("the refused call changed the clients after %d imports", w.counts["import"])
			}
		})
	}
}

// A failed import is settled and read back: the call reports what the MONs
// hold once nothing can change it any more.
func TestReplaceClientKeyReadsTheOutcomeOfAFailedImport(t *testing.T) {
	for name, test := range map[string]struct {
		op       string
		effect   faultEffect
		canceled bool
		// applied says whether the import applies; otherwise the error must
		// contain want.
		applied bool
		want    string
	}{
		"import failed before reaching a MON":    {"import", failsBefore, false, false, "did not apply"},
		"reply lost after the import applied":    {"import", failsAfter, false, true, ""},
		"import hung after the MON prepared it":  {"import", hangsPrepared, false, true, ""},
		"import hung before the MON prepared it": {"import", hangsQueued, false, false, "did not apply"},
		"canceled after the MON prepared it":     {"import", hangsPrepared, true, true, ""},
		"canceled before the MON prepared it":    {"import", hangsQueued, true, false, "did not apply"},
		"canceled before the import was sent":    {"import", hangsUnsent, true, false, "did not apply"},
		"import in flight":                       {"import", hangsInFlight, false, true, ""},
		"answered with a copy in flight":         {"import", duplicatedInFlight, false, true, ""},
		"answered without applying":              {"import", answersWithoutApplying, false, false, "answered without applying"},
		"input copy failed":                      {"control-copy", failsBefore, false, false, "did not apply"},
		"input copy completed after it failed":   {"control-copy", failsAfter, false, false, "did not apply"},
		"connection failed":                      {"connect", failsBefore, false, false, "did not apply"},
	} {
		t.Run(name, func(t *testing.T) {
			w, _, client := clientWorld()
			old := w.clients["client.app"].key
			w.faults[test.op] = rotationFault{occurrence: 1, effect: test.effect}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if test.canceled {
				w.onHang = cancel
			}
			w.route = "b"
			next, err := client.ReplaceKey(ctx)
			w.onHang = nil
			if test.applied {
				if err != nil || next == nil || !next.ready {
					t.Fatalf("an import that applied returned %v, %v", next, err)
				}
				w.requireClient(t, clientKey(next.keyring, "client.app"))
				return
			}
			if err == nil || next != nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("returned %v, %v; want an error with %q", next, err, test.want)
			}
			w.requireClient(t, old)
			// The descriptor still works, and so does a new replacement.
			if next, err = client.ReplaceKey(t.Context()); err != nil {
				t.Fatal(err)
			}
			w.requireClient(t, clientKey(next.keyring, "client.app"))
		})
	}
}

// When settling fails, the outcome stays open until a later operation on the
// identity settles it: a later replacement then finishes with the same new
// descriptor or starts anew.
func TestReplaceClientKeyResolvesAnUncertainImportLater(t *testing.T) {
	for name, test := range map[string]struct {
		effect      faultEffect
		cancelFails int
		// applies says whether the late import applies.
		applies bool
		// resolve is the operation that resolves the outcome.
		resolve string
	}{
		"prepared import, resolved by ReplaceClientKey":       {hangsPrepared, 0, true, "replace"},
		"prepared import, resolved by ClientCapabilities":     {hangsPrepared, 0, true, "caps"},
		"queued import, resolved by ReplaceClientKey":         {hangsQueued, 0, false, "replace"},
		"queued import, resolved by ClientCapabilities":       {hangsQueued, 0, false, "caps"},
		"unstopped CLI of a prepared import":                  {hangsPrepared, 2, true, "replace"},
		"unstopped CLI of an unsent import":                   {hangsUnsent, 2, false, "replace"},
		"prepared import, resolved by DeleteClient":           {hangsPrepared, 0, true, "delete"},
		"queued import, resolved by DeleteClient":             {hangsQueued, 0, false, "delete"},
		"prepared import in flight, resolved by Replace":      {hangsInFlight, 0, true, "replace"},
		"unstopped CLI of a queued import, resolved by caps":  {hangsQueued, 2, false, "caps"},
		"unstopped CLI of a prepared import, resolved by del": {hangsPrepared, 2, true, "delete"},
	} {
		t.Run(name, func(t *testing.T) {
			w, _, client := clientWorld()
			old := w.clients["client.app"].key
			w.faults["import"] = rotationFault{occurrence: 1, effect: test.effect}
			w.cancelFails = test.cancelFails
			if test.cancelFails == 0 {
				w.faults["barrier"] = rotationFault{occurrence: 1}
			}
			w.route = "b"
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			w.onHang = cancel
			next, err := client.ReplaceKey(ctx)
			w.onHang = nil
			if err == nil || next == nil || next.ready || !strings.Contains(err.Error(), "not known yet") {
				t.Fatalf("an unsettled import returned %v, %v", next, err)
			}
			if _, err := next.ConnectionConfig(); err == nil {
				t.Fatal("the unconfirmed descriptor returned credentials")
			}
			if err := identityErr(client); err == nil || !strings.Contains(err.Error(), "not resolved yet") {
				t.Fatalf("WithIdentity while the import is uncertain returned %v", err)
			}
			if _, err := next.ReplaceKey(t.Context()); err == nil {
				t.Fatal("the unconfirmed descriptor was accepted")
			}
			if err := next.Delete(t.Context()); err == nil {
				t.Fatal("DeleteClient accepted the unconfirmed descriptor")
			}
			newKey := clientKey(next.keyring, "client.app")
			// A CLI that cannot be stopped yet keeps the identity fenced.
			for attempt := 0; attempt < test.cancelFails; attempt++ {
				if _, err := client.Capabilities(t.Context()); err == nil || !strings.Contains(err.Error(), "cannot be stopped yet") {
					break
				}
			}
			switch test.resolve {
			case "replace":
				again, err := client.ReplaceKey(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if test.applies != (clientKey(again.keyring, "client.app") == newKey) {
					t.Fatalf("the resolved replacement has the attempted key: %t, want %t", !test.applies, test.applies)
				}
				w.requireClient(t, clientKey(again.keyring, "client.app"))
				if !again.ready || next.ready {
					t.Fatalf("resolved descriptor ready: %t; the uncertain one changed to ready: %t", again.ready, next.ready)
				}
				return
			case "caps":
				_, err = client.Capabilities(t.Context())
			case "delete":
				err = client.Delete(t.Context())
			}
			if test.applies {
				if err == nil || !strings.Contains(err.Error(), "has a new key") || next.ready {
					t.Fatalf("after the import applied, the old descriptor returned %v (uncertain descriptor changed to ready: %t)", err, next.ready)
				}
				w.requireClient(t, newKey)
				if current, err := client.ReplaceKey(t.Context()); err != nil || !current.ready || clientKey(current.keyring, "client.app") != newKey {
					t.Fatalf("the old descriptor led to %v, %v", current, err)
				}
				return
			}
			if err != nil || next.ready {
				t.Fatalf("after the import was dropped, the old descriptor returned %v (new descriptor ready: %t)", err, next.ready)
			}
			if test.resolve == "delete" {
				if _, ok := w.clients["client.app"]; ok {
					t.Fatal("DeleteClient kept the identity")
				}
				return
			}
			w.requireClient(t, old)
		})
	}
}

// An entry that changed outside the module while an import was uncertain is
// reported, and the old descriptor no longer owns it.
func TestReplaceClientKeyReportsAnOutsideChangeWhileUncertain(t *testing.T) {
	w, _, client := clientWorld()
	w.faults["import"] = rotationFault{occurrence: 1, effect: hangsQueued}
	w.faults["barrier"] = rotationFault{occurrence: 1}
	if _, err := client.ReplaceKey(t.Context()); err == nil {
		t.Fatal("an unsettled import succeeded")
	}
	w.clients["client.app"] = fakeClient{key: fakeCephxKey(CipherAES256K, 960), caps: maps.Clone(replacedCaps)}
	if _, err := client.ReplaceKey(t.Context()); err == nil || !strings.Contains(err.Error(), "outside this module") {
		t.Fatalf("returned %v", err)
	}
	if _, err := client.ReplaceKey(t.Context()); err == nil || !strings.Contains(err.Error(), "different key") {
		t.Fatalf("a later call returned %v", err)
	}
	if w.clients["client.app"].key != fakeCephxKey(CipherAES256K, 960) {
		t.Fatal("the outside change was overwritten")
	}
}

// Capability changes and removals are fenced like a replacement: a later
// replacement settles them first and never races one that may still apply.
func TestClientOperationsSettleEarlierChanges(t *testing.T) {
	updated := map[string]string{"mon": "allow r", "mgr": "allow r"}
	for name, test := range map[string]struct {
		op          string
		effect      faultEffect
		cancelFails int
		// applies says whether the late command applies.
		applies bool
	}{
		"capability change applied late":            {"caps", hangsPrepared, 0, true},
		"capability change dropped":                 {"caps", hangsQueued, 0, false},
		"capability change whose CLI ran on":        {"caps", hangsPrepared, 2, true},
		"removal applied late":                      {"remove", hangsPrepared, 0, true},
		"removal dropped":                           {"remove", hangsQueued, 0, false},
		"removal whose CLI ran on, applied late":    {"remove", hangsPrepared, 2, true},
		"removal whose CLI ran on, never delivered": {"remove", hangsUnsent, 2, false},
	} {
		t.Run(name, func(t *testing.T) {
			w, _, client := clientWorld()
			w.faults[test.op] = rotationFault{occurrence: 1, effect: test.effect}
			w.cancelFails = test.cancelFails
			if test.cancelFails == 0 {
				w.faults["barrier"] = rotationFault{occurrence: 1}
			}
			w.route = "b"
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			w.onHang = cancel
			var err error
			if test.op == "caps" {
				err = client.UpdateCaps(ctx, ClientCaps{Mon: "allow r", MGR: "allow r"})
			} else {
				err = client.Delete(ctx)
			}
			w.onHang = nil
			if err == nil {
				t.Fatal("an unsettled change succeeded")
			}
			// A replacement waits while the earlier CLI may still run.
			var next *ClientConfig
			for attempt := 0; attempt <= test.cancelFails; attempt++ {
				if next, err = client.ReplaceKey(t.Context()); err == nil || !strings.Contains(err.Error(), "cannot be stopped yet") {
					break
				}
				if len(w.clients["client.app"].key) == 0 && !test.applies {
					t.Fatal("the identity vanished while the replacement waited")
				}
			}
			future := w.clone()
			future.releaseAll()
			if test.op == "remove" && test.applies {
				if err == nil || !client.revoked {
					t.Fatalf("a replacement after a removal that applied returned %v (revoked %t)", err, client.revoked)
				}
				if _, ok := future.clients["client.app"]; ok {
					t.Fatal("the identity came back")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := replacedCaps
			if test.op == "caps" && test.applies {
				want = updated
			}
			got := future.clients["client.app"]
			if got.key != clientKey(next.keyring, "client.app") || !maps.Equal(got.caps, want) || len(w.controlFiles) != 0 {
				t.Fatalf("after every late command: %+v, want the new key with %v; control files %v", got, want, w.controlFiles)
			}
		})
	}
}

func TestReplaceClientKeyNeedsCapabilities(t *testing.T) {
	w, _, client := clientWorld()
	w.clients["client.app"] = fakeClient{key: w.clients["client.app"].key, caps: map[string]string{}}
	if next, err := client.ReplaceKey(t.Context()); err == nil || next != nil || !strings.Contains(err.Error(), "no capabilities") {
		t.Fatalf("returned %v, %v", next, err)
	}
	if w.counts["import"] != 0 {
		t.Fatal("an import ran")
	}
}

// A descriptor that was replaced leads only to a successor that is still
// current.
func TestReplaceClientKeyChecksTheSuccessor(t *testing.T) {
	_, _, first := clientWorld()
	second, err := first.ReplaceKey(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	third, err := second.ReplaceKey(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if next, err := first.ReplaceKey(t.Context()); err == nil || next != nil {
		t.Fatalf("the first descriptor led to %v, %v after a second replacement", next, err)
	}
	if next, err := second.ReplaceKey(t.Context()); err != nil || next != third {
		t.Fatalf("the second descriptor led to %v, %v", next, err)
	}
	if err := third.Delete(t.Context()); err != nil {
		t.Fatal(err)
	}
	if next, err := second.ReplaceKey(t.Context()); err == nil || next != nil {
		t.Fatalf("the second descriptor led to %v, %v after a removal", next, err)
	}
}

// A key file that Docker writes after its copy failed, and after the call
// removed it, is removed by the next operation after it landed, however late
// that is.
func TestClientKeyFilesDoNotOutliveLateCopies(t *testing.T) {
	for _, delay := range []int{1, 40} {
		t.Run(fmt.Sprintf("lands after %d commands", delay), func(t *testing.T) {
			w, _, client := clientWorld()
			w.faults["control-copy"] = rotationFault{occurrence: 1, effect: hangsPrepared}
			w.lateCopyDelay = delay
			if next, err := client.ReplaceKey(t.Context()); err == nil || next != nil {
				t.Fatalf("returned %v, %v", next, err)
			}
			for len(w.lateCopies) > 0 {
				if _, err := client.Capabilities(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if len(w.controlFiles) != 1 {
				t.Fatalf("the late copy did not land after the removals: %v", w.controlFiles)
			}
			if _, err := client.Capabilities(t.Context()); err != nil {
				t.Fatal(err)
			}
			if len(w.controlFiles) != 0 {
				t.Fatalf("a key file stayed behind: %v", w.controlFiles)
			}
		})
	}
}

// Services that change a client's grants, such as CephFS subvolume
// authorization, settle the identity first and refuse a replaced descriptor.
func TestServicesSettleClientsFirst(t *testing.T) {
	w, c, client := clientWorld()
	w.faults["import"] = rotationFault{occurrence: 1, effect: hangsPrepared}
	w.faults["barrier"] = rotationFault{occurrence: 1}
	if _, err := client.ReplaceKey(t.Context()); err == nil {
		t.Fatal("an unsettled import succeeded")
	}
	err := serviceHooks{}.SettleClient(t.Context(), c, client)
	if err == nil || !strings.Contains(err.Error(), "has a new key") {
		t.Fatalf("SettleClient with the replaced descriptor returned %v", err)
	}
	future := w.clone()
	future.releaseAll()
	if future.clients["client.app"].key != w.clients["client.app"].key || len(w.late) != 0 {
		t.Fatal("the import can still apply after SettleClient")
	}
	current, err := client.ReplaceKey(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := (serviceHooks{}).SettleClient(t.Context(), c, current); err != nil {
		t.Fatal(err)
	}
}

// A replacement or a removal through one descriptor makes every copy of an
// earlier descriptor stale.
func TestCopiedClientDescriptorsGoStale(t *testing.T) {
	t.Run("replacement", func(t *testing.T) {
		_, _, client := clientWorld()
		copied := *client
		if _, err := client.ReplaceKey(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := identityErr(&copied); err == nil || !strings.Contains(err.Error(), "stale") {
			t.Fatalf("WithIdentity with a copy of the replaced descriptor returned %v", err)
		}
		if _, err := (&copied).Capabilities(t.Context()); err == nil {
			t.Fatal("ClientCapabilities accepted a copy of the replaced descriptor")
		}
	})
	t.Run("removal", func(t *testing.T) {
		w, _, client := clientWorld()
		copied := *client
		if err := (&copied).Delete(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := identityErr(client); err == nil || !strings.Contains(err.Error(), "stale") {
			t.Fatalf("WithIdentity after a removal through a copy returned %v", err)
		}
		removals := w.counts["remove"]
		if err := client.Delete(t.Context()); err != nil || !client.revoked || w.counts["remove"] != removals {
			t.Fatalf("DeleteClient after a removal through a copy returned %v", err)
		}
	})
}

// requireRedacted checks that diagnostics redact each owned client key
// even where it appears without a key= label.
func requireRedacted(t *testing.T, c *Container, keys ...string) {
	t.Helper()
	// The fake cluster has no Docker containers to list.
	c.Container = nil
	c.osdHandles.replace(nil)
	snapshot, err := c.diagnosticsSnapshot(t.Context(), diagnosticsUnitConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	redact := diagnosticsRedactor(snapshot.secrets)
	for i, key := range keys {
		if strings.Contains(redact("opaque observation "+key+" end"), key) {
			t.Errorf("diagnostics keep owned client key %d of %d", i+1, len(keys))
		}
	}
}

func TestDiagnosticsRedactOwnedClientKeys(t *testing.T) {
	t.Run("confirmed replacement", func(t *testing.T) {
		w, c, client := clientWorld()
		old := w.clients["client.app"].key
		next, err := client.ReplaceKey(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		requireRedacted(t, c, old, clientKey(next.keyring, "client.app"))
	})
	t.Run("uncertain replacement", func(t *testing.T) {
		w, c, client := clientWorld()
		old := w.clients["client.app"].key
		w.faults["import"] = rotationFault{occurrence: 1, effect: hangsPrepared}
		w.faults["barrier"] = rotationFault{occurrence: 1}
		attempted, err := client.ReplaceKey(t.Context())
		if err == nil || attempted == nil {
			t.Fatalf("an unsettled import returned %v, %v", attempted, err)
		}
		requireRedacted(t, c, old, clientKey(attempted.keyring, "client.app"))
	})
	t.Run("replacement that failed after generating its key", func(t *testing.T) {
		w, c, client := clientWorld()
		old := w.clients["client.app"].key
		w.faults["control-copy"] = rotationFault{occurrence: 1}
		if _, err := client.ReplaceKey(t.Context()); err == nil {
			t.Fatal("a failed input copy succeeded")
		}
		requireRedacted(t, c, old, fakeCephxKey(CipherAES256K, w.generated))
	})
	t.Run("creation", func(t *testing.T) {
		ctr := &authFixtureContainer{}
		c := authFixtureCluster(ctr)
		if _, err := c.CreateClient(t.Context(), "reader", ClientCaps{Mon: "allow r"}); err != nil {
			t.Fatal(err)
		}
		requireRedacted(t, c, "PRIVATE-KEY")
	})
}
