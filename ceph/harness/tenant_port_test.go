package harness

import (
	"context"
	"errors"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

func TestTenantPortFollowsTheTenantLifetime(t *testing.T) {
	shared := newFakeShared(&fakeFleet{})
	tb := newFakeTB(t)
	tenant := shared.Lease(tb)

	// The fake backend has no cluster, so a port cannot be taken.
	if _, err := (tenantHooks{}).TenantPort(tenant); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a tenant without a cluster returned a port: %v", err)
	}

	port := tenantPort(tenantLease{t: tenant, c: &recordingOps{}})
	if port.ID() != tenant.ID() || port.Live() != nil {
		t.Fatalf("live port: id %q, live %v", port.ID(), port.Live())
	}
	if err := port.AttachScope("fake", func(context.Context) ([]string, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	if err := port.AttachScope("fake", func(context.Context) ([]string, error) { return nil, nil }); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a second attachment of one key returned %v", err)
	}
	client, err := port.CreateClient(t.Context(), "fake", ceph.ClientCaps{Mon: "allow r"})
	if err != nil || client.Name() != "client."+tenant.ID()+"-fake" || !tenant.owns(client) {
		t.Fatalf("port client %v, %v", client, err)
	}

	tb.finish()
	if err := port.Live(); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a released tenant's port is live: %v", err)
	}
	if err := port.AttachScope("late", func(context.Context) ([]string, error) { return nil, nil }); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("an attachment through a released port returned %v", err)
	}
	if _, err := port.CreateClient(t.Context(), "late", ceph.ClientCaps{Mon: "allow r"}); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a client through a released port returned %v", err)
	}
}
