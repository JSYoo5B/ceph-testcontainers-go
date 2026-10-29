package harness

import (
	"context"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// createdOverride wraps a sharedBackend and reports its own created value for
// every client, whatever the client's private field says.
type createdOverride struct {
	sharedBackend
	created func(*ceph.ClientConfig) bool
}

func (o createdOverride) createClient(ctx context.Context, entity string, caps ceph.ClientCaps) (*ceph.ClientConfig, bool, error) {
	client, _, err := o.sharedBackend.createClient(ctx, entity, caps)
	return client, o.created(client), err
}

// The tenant registers a client for deletion on the backend's report, not on
// the private field of ClientConfig, which a harness in another package could
// not read.
func TestTenantRegistersClientsOnTheBackendsCreatedReport(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reported   bool
		registered int
	}{
		{"reported created", true, 1},
		{"reported not created", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shared := newFakeShared(&fakeFleet{})
			tb := newFakeTB(t)
			tenant := shared.Lease(tb)
			cluster := backendOf(tenant)
			tenant.inst.backend = createdOverride{sharedBackend: cluster, created: func(*ceph.ClientConfig) bool { return tc.reported }}
			tenant.mu.Lock()
			before := len(tenant.clients)
			tenant.mu.Unlock()
			if _, err := tenant.CreateClient(t.Context(), "extra", TenantReadWrite); err != nil {
				t.Fatal(err)
			}
			tenant.mu.Lock()
			got := len(tenant.clients) - before
			tenant.mu.Unlock()
			if got != tc.registered {
				t.Fatalf("registered clients = %d, want %d", got, tc.registered)
			}
			if !tc.reported {
				// Nothing deletes it, so remove it by hand to let the release verify.
				cluster.change(func(f *fakeShared) { delete(f.auth, "client."+tenant.ID()+"-extra") })
			}
			tb.finish()
			if tb.Failed() {
				t.Fatalf("release failed: %s", tb.errorText())
			}
		})
	}
}
