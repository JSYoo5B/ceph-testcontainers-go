//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// testClientRevocation revokes clients of each key type the image supports
// while a session that reconnects after a failure is held, and checks native
// authentication: a fresh client fails, the held session fails when it next
// authenticates, and after Restore the same key and caps are back, the
// unchanged descriptor and keyring authenticate for MON and MGR, and the held
// session reconnects. On three MONs every MON agrees, and calls canceled at
// different times leave either no change or a revocation that Restore undoes.
func testClientRevocation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	image, opts := integrationImages(t)
	ciphers, auth := clientKeyCiphers(t, ctx, image)
	cluster, _ := newServiceClusterWithOptions(t, image, append(slices.Clone(opts), ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1), ceph.WithAuth(auth))...)
	payload := prepareHeldObject(t, ctx, cluster)
	if _, err := cluster.CreateClient(ctx, "bystander", ceph.ClientCaps{Mon: "allow r"}); err != nil {
		t.Fatal(err)
	}
	bystander := authEntry(t, ctx, cluster, "client.bystander")
	for _, cipher := range ciphers {
		t.Run(string(cipher), func(t *testing.T) {
			caps := ceph.ClientCaps{Mon: "allow r", MGR: "allow r", OSD: "allow rw pool=data namespace=tenant-a"}
			client, err := cluster.CreateClient(ctx, "recoverable-"+string(cipher), caps, ceph.WithClientKeyType(cipher))
			if err != nil {
				t.Fatal(err)
			}
			before := authEntry(t, ctx, cluster, client.Name())
			holder := startClientContainer(t, ctx, cluster, client)
			startHeldSession(t, ctx, holder, client, "reconnect")
			revokedAt := containerTime(t, ctx, holder)
			revocation, err := cluster.TemporaryClientRevocation(ctx, client)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cluster.Ceph(ctx, "auth", "get", client.Name()); err == nil {
				t.Fatal("the revoked identity is still in the auth database")
			}
			if code, out := clientCommand(t, ctx, holder, client, "mon", "stat"); code == 0 {
				t.Fatalf("a fresh client of the revoked identity authenticated: %s", out)
			}
			if code, _ := osdRead(t, ctx, holder, client); code == 0 {
				t.Fatal("a fresh client of the revoked identity read from the OSD")
			}
			if _, err := cluster.ClientCapabilities(ctx, client); err == nil {
				t.Fatal("ClientCapabilities accepted the revoked descriptor")
			}
			if err := cluster.WithClientIdentity(client)(&testcontainers.GenericContainerRequest{}); err == nil {
				t.Fatal("WithClientIdentity accepted the revoked descriptor")
			}
			if _, err := cluster.CreateClient(ctx, client.Name(), caps); err == nil {
				t.Fatal("CreateClient took the revoked name")
			}
			rejected, failed := waitHeldRenewalRejected(t, ctx, holder, revokedAt, clientKeyMonTicketTTL+30*time.Second)
			revokedReads := heldOSDReads(t, ctx, holder, revokedAt)
			restoredAt := containerTime(t, ctx, holder)
			if err := revocation.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			if after := authEntry(t, ctx, cluster, client.Name()); after.Key != before.Key || !maps.Equal(after.Caps, before.Caps) {
				t.Fatalf("the restored entry has caps %v (before %v), same key %t", after.Caps, before.Caps, after.Key == before.Key)
			}
			// The holder still has the keyring attached before the revocation.
			for _, command := range [][]string{{"mon", "stat"}, {"pg", "stat"}} {
				if code, out := clientCommand(t, ctx, holder, client, command...); code != 0 {
					t.Fatalf("%v with the original keyring after Restore: exit %d: %s", command, code, out)
				}
			}
			if code, read := osdRead(t, ctx, holder, client); code != 0 || !bytes.Equal(read, payload) {
				t.Fatalf("OSD read with the original keyring after Restore: exit %d, same bytes %t", code, bytes.Equal(read, payload))
			}
			if got, err := cluster.ClientCapabilities(ctx, client); err != nil || got != caps {
				t.Fatalf("ClientCapabilities after Restore: %+v, %v", got, err)
			}
			startClientContainer(t, ctx, cluster, client)
			recovered := waitHeldSessionRecovered(t, ctx, holder, restoredAt, time.Minute)
			if err := revocation.Restore(ctx); err != nil {
				t.Fatalf("a repeated Restore: %v", err)
			}
			if entry := authEntry(t, ctx, cluster, "client.bystander"); entry.Key != bystander.Key || !maps.Equal(entry.Caps, bystander.Caps) {
				t.Fatal("client.bystander changed")
			}
			t.Logf("%s: the MONs rejected the held session %.1fs after the revocation (its MON reads failed after that: %t; its OSD reads while revoked: %s); it authenticated again and read MON, MGR and OSD %.1fs after Restore",
				cipher, rejected, failed, revokedReads, recovered)
		})
	}

	t.Run("no capabilities", func(t *testing.T) {
		// auth import refuses an entry without capabilities; Restore puts
		// back exactly such an identity too.
		client, err := cluster.CreateClient(ctx, "capless", ceph.ClientCaps{})
		if err != nil {
			t.Fatal(err)
		}
		before := authEntry(t, ctx, cluster, client.Name())
		revocation, err := cluster.TemporaryClientRevocation(ctx, client)
		if err != nil {
			t.Fatal(err)
		}
		if err := revocation.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		if after := authEntry(t, ctx, cluster, client.Name()); after.Key != before.Key || len(after.Caps) != 0 {
			t.Fatalf("restored caps %v, same key %t", after.Caps, after.Key == before.Key)
		}
		if _, err := cluster.ReplaceClientKey(ctx, client); err == nil {
			t.Fatal("a key replacement of an identity without capabilities succeeded")
		}
	})

	t.Run("three MONs", func(t *testing.T) {
		cluster, _ := newServiceClusterWithOptions(t, image, append(slices.Clone(opts), ceph.WithNoInitialOSDs(), ceph.WithMonitorCount(3))...)
		client, err := cluster.CreateClient(ctx, "recoverable", ceph.ClientCaps{Mon: "allow r", MGR: "allow r"})
		if err != nil {
			t.Fatal(err)
		}
		original := authEntry(t, ctx, cluster, client.Name())
		holder := startClientContainer(t, ctx, cluster, client)
		mons := monitorOverrides(t, ctx, cluster)
		revocation, err := cluster.TemporaryClientRevocation(ctx, client)
		if err != nil {
			t.Fatal(err)
		}
		for _, mon := range mons {
			if code, out := clientCommand(t, ctx, holder, client, "--mon_host_override", mon, "mon", "stat"); code == 0 {
				t.Fatalf("the revoked identity authenticated through %s: %s", mon, out)
			}
		}
		if err := revocation.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		for _, mon := range mons {
			if code, out := clientCommand(t, ctx, holder, client, "--mon_host_override", mon, "mon", "stat"); code != 0 {
				t.Fatalf("the restored identity through %s: exit %d: %s", mon, code, out)
			}
		}
		// Calls canceled at different points either change nothing or leave
		// a revocation that Restore undoes, also after a canceled Restore.
		for _, delay := range []time.Duration{50 * time.Millisecond, 300 * time.Millisecond, time.Second, 3 * time.Second} {
			callCtx, callCancel := context.WithTimeout(ctx, delay)
			revocation, err := cluster.TemporaryClientRevocation(callCtx, client)
			callCancel()
			if revocation == nil {
				t.Logf("a revocation canceled after %v did not apply: %v", delay, err)
			} else {
				restoreCtx, restoreCancel := context.WithTimeout(ctx, delay)
				canceledErr := revocation.Restore(restoreCtx)
				restoreCancel()
				if restoreErr := revocation.Restore(ctx); restoreErr != nil {
					t.Fatalf("Restore after a revocation (%v) and a Restore (%v) canceled after %v: %v", err, canceledErr, delay, restoreErr)
				}
				t.Logf("with %v: revocation returned %v, the canceled Restore %v; the next Restore finished", delay, err, canceledErr)
			}
			if entry := authEntry(t, ctx, cluster, client.Name()); entry.Key != original.Key || !maps.Equal(entry.Caps, original.Caps) {
				t.Fatalf("after a call canceled after %v, the identity differs from the original", delay)
			}
			if _, err := cluster.ClientCapabilities(ctx, client); err != nil {
				t.Fatalf("after a call canceled after %v, the descriptor is unusable: %v", delay, err)
			}
		}
	})
}

// waitHeldSessionRecovered waits until the held session authenticates or
// renews its tickets after since and then reads from the MON, the MGR and the
// OSD, and returns how many seconds after since it authenticated.
func waitHeldSessionRecovered(t *testing.T, ctx context.Context, ctr testcontainers.Container, since float64, limit time.Duration) float64 {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		for _, result := range heldAuthResults(t, ctx, ctr) {
			if result.at <= since || result.mon != 0 {
				continue
			}
			for _, line := range heldSessionLog(t, ctx, ctr) {
				if line.at > result.at && line.mon == 0 && line.mgr == 0 && line.osd == 0 {
					return result.at - since
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("the held session did not recover within %v:\n%s", limit, containerFile(t, ctx, ctr, "/tmp/held.log"))
	return 0
}
