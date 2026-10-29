//go:build all || integration

package integration_test

import (
	"bytes"
	"context"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
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
	cluster, _ := newServiceClusterWithOptions(t, image, opts.WithCustomizers(ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1), ceph.WithAuth(auth)))
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
			revocation, err := client.Revoke(ctx)
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
			if _, err := client.Capabilities(ctx); err == nil {
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
			if got, err := client.Capabilities(ctx); err != nil || got != caps {
				t.Fatalf("ClientCapabilities after Restore: %+v, %v", got, err)
			}
			startClientContainer(t, ctx, cluster, client)
			t.Logf("%s: container times: revocation %.3f, rejection %.3f, Restore %.3f; MON ticket TTL %v",
				cipher, revokedAt, revokedAt+rejected, restoredAt, clientKeyMonTicketTTL)
			// The open session renews its tickets after Restore only while
			// the MONs keep the rotating secret of its last auth ticket, so
			// it connects anew, as a recovering client does.
			renewed := heldRenewedWithin(t, ctx, holder, restoredAt, 5*time.Second)
			recovered := waitHeldSessionReconnected(t, ctx, holder, reconnectHeldSession(t, ctx, holder), time.Minute)
			if err := revocation.Restore(ctx); err != nil {
				t.Fatalf("a repeated Restore: %v", err)
			}
			if entry := authEntry(t, ctx, cluster, "client.bystander"); entry.Key != bystander.Key || !maps.Equal(entry.Caps, bystander.Caps) {
				t.Fatal("client.bystander changed")
			}
			t.Logf("%s: the MONs rejected the held session %.1fs after the revocation (its MON reads failed after that: %t; its OSD reads while revoked: %s); after Restore it renewed by itself within 5s: %t, and on a new connection it authenticated and read MON, MGR and OSD %.1fs after the request",
				cipher, rejected, failed, revokedReads, renewed, recovered)
		})
	}

	// The MONs renew an open session's tickets only with an auth ticket whose
	// rotating secret they still keep, for a few MonTicketTTL periods; with
	// auth_allow_insecure_global_id_reclaim off they refuse an older one. A
	// Restore after that leaves the open session unable to renew. Squid keeps
	// its MON session answering and Tentacle drops it; a new connection
	// authenticates either way.
	t.Run("late restore", func(t *testing.T) {
		caps := ceph.ClientCaps{Mon: "allow r", MGR: "allow r", OSD: "allow rw pool=data namespace=tenant-a"}
		client, err := cluster.CreateClient(ctx, "late-restore", caps)
		if err != nil {
			t.Fatal(err)
		}
		holder := startClientContainer(t, ctx, cluster, client)
		startHeldSession(t, ctx, holder, client, "hold")
		revokedAt := containerTime(t, ctx, holder)
		revocation, err := client.Revoke(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rejected, lost := waitHeldRenewalRejected(t, ctx, holder, revokedAt, clientKeyMonTicketTTL+30*time.Second)
		// Outlast the rotating secrets of the session's last auth ticket.
		time.Sleep(3 * clientKeyMonTicketTTL)
		restoredAt := containerTime(t, ctx, holder)
		if err := revocation.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		t.Logf("container times: revocation %.3f, rejection %.3f, Restore %.3f; MON ticket TTL %v",
			revokedAt, revokedAt+rejected, restoredAt, clientKeyMonTicketTTL)
		if renewed := heldRenewedWithin(t, ctx, holder, restoredAt, 10*time.Second); renewed {
			t.Fatalf("the open session renewed its tickets %.0fs after the rejection; the MONs kept an older secret than expected:\n%s",
				restoredAt-revokedAt-rejected, heldAuthTrace(t, ctx, holder))
		}
		// A session that kept its MON connection asks for renewals, which the
		// MONs refuse for its ticket; one that lost it asks for none.
		refusals := monitorLogLines(t, ctx, cluster, client.Name()+":  attempt to reclaim global_id", "using bad ticket")
		if !lost && refusals == 0 {
			t.Fatalf("the open session kept its MON connection, but the MONs logged no refusal of its ticket:\n%s", heldAuthTrace(t, ctx, holder))
		}
		recovered := waitHeldSessionReconnected(t, ctx, holder, reconnectHeldSession(t, ctx, holder), time.Minute)
		t.Logf("a Restore %.0fs after the rejection left the open session unable to renew (its MON session lost at the rejection: %t; MON log lines about its bad ticket: %d); on a new connection it authenticated and read MON, MGR and OSD %.1fs after the request",
			restoredAt-revokedAt-rejected, lost, refusals, recovered)
	})

	t.Run("no capabilities", func(t *testing.T) {
		// auth import refuses an entry without capabilities; Restore puts
		// back exactly such an identity too.
		client, err := cluster.CreateClient(ctx, "capless", ceph.ClientCaps{})
		if err != nil {
			t.Fatal(err)
		}
		before := authEntry(t, ctx, cluster, client.Name())
		revocation, err := client.Revoke(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := revocation.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		if after := authEntry(t, ctx, cluster, client.Name()); after.Key != before.Key || len(after.Caps) != 0 {
			t.Fatalf("restored caps %v, same key %t", after.Caps, after.Key == before.Key)
		}
		if _, err := client.ReplaceKey(ctx); err == nil {
			t.Fatal("a key replacement of an identity without capabilities succeeded")
		}
	})

	t.Run("three MONs", func(t *testing.T) {
		cluster, _ := newServiceClusterWithOptions(t, image, opts.WithCustomizers(ceph.WithNoInitialOSDs(), ceph.WithMonitorCount(3)))
		client, err := cluster.CreateClient(ctx, "recoverable", ceph.ClientCaps{Mon: "allow r", MGR: "allow r"})
		if err != nil {
			t.Fatal(err)
		}
		original := authEntry(t, ctx, cluster, client.Name())
		holder := startClientContainer(t, ctx, cluster, client)
		mons := monitorOverrides(t, ctx, cluster)
		revocation, err := client.Revoke(ctx)
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
			revocation, err := client.Revoke(callCtx)
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
			if _, err := client.Capabilities(ctx); err != nil {
				t.Fatalf("after a call canceled after %v, the descriptor is unusable: %v", delay, err)
			}
		}
	})
}

// heldRenewedWithin reports whether the held session renewed its tickets
// or authenticated after since, waiting at most limit for it.
func heldRenewedWithin(t *testing.T, ctx context.Context, ctr testcontainers.Container, since float64, limit time.Duration) bool {
	t.Helper()
	for deadline := time.Now().Add(limit); ; time.Sleep(time.Second) {
		for _, result := range heldAuthResults(t, ctx, ctr) {
			if result.at > since && result.mon == 0 {
				return true
			}
		}
		if time.Now().After(deadline) {
			return false
		}
	}
}

// monitorLogLines counts the lines of the MONs' logs that contain every one
// of texts.
func monitorLogLines(t *testing.T, ctx context.Context, cluster *ceph.Container, texts ...string) int {
	t.Helper()
	count := 0
	for _, mon := range clusteraccess.Monitors(t, cluster) {
		logs, err := mon.Logs(ctx)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(io.LimitReader(logs, 64<<20))
		_ = logs.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			if !slices.ContainsFunc(texts, func(text string) bool { return !strings.Contains(line, text) }) {
				count++
			}
		}
	}
	return count
}
