package ceph

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// fakeService models a service package on a fakeShared cluster: a lease
// attaches a scope and creates a resource owned by the tenant, as rbd.Lease
// does with its namespace, and the scope's release removes the tenant's
// resources unless the model leaves some behind.
type fakeService struct {
	// keep names resources that the release leaves behind; fail makes the
	// release fail before it removes anything.
	keep map[string]bool
	fail error
	// clientsAtRelease records whether the tenant's clients still existed
	// when the scope was released.
	clientsAtRelease bool
}

func (s *fakeService) lease(t *testing.T, tenant *Tenant, resource string) {
	t.Helper()
	cluster := backendOf(tenant)
	if err := tenant.attachScope("fake "+resource, func(context.Context) ([]string, error) {
		cluster.mu.Lock()
		defer cluster.mu.Unlock()
		_, s.clientsAtRelease = cluster.auth["client."+tenant.ID()]
		if s.fail != nil {
			return nil, s.fail
		}
		var remains []string
		for key, owner := range cluster.services {
			if owner != tenant.ID() {
				continue
			}
			if s.keep[key] {
				remains = append(remains, key)
				continue
			}
			delete(cluster.services, key)
		}
		return remains, nil
	}); err != nil {
		t.Fatal(err)
	}
	cluster.change(func(cluster *fakeShared) { cluster.services[resource] = tenant.ID() })
}

func TestSharedReleaseRemovesServiceScopesAndClients(t *testing.T) {
	shared := newFakeShared(&fakeFleet{})
	tb := newFakeTB(t)
	tenant := shared.Lease(tb)
	cluster := backendOf(tenant)
	service := &fakeService{}
	service.lease(t, tenant, "fake namespace "+tenant.ID())
	client, err := tenant.serviceClient(t.Context(), "fake", ClientCaps{Mon: "allow r", OSD: "profile rbd pool=p namespace=" + tenant.ID()})
	if err != nil || client.Name() != "client."+tenant.ID()+"-fake" || !tenant.owns(client) {
		t.Fatalf("service client %v, %v", client, err)
	}
	if _, err := tenant.serviceClient(t.Context(), "bad name", ClientCaps{Mon: "allow r"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("an invalid service client name returned %v", err)
	}
	tb.finish()
	if tb.Failed() || shared.Stats().Poisonings != 0 {
		t.Fatalf("a clean service release failed: %s", tb.errorText())
	}
	if len(cluster.services) != 0 {
		t.Fatalf("the release left %v", cluster.services)
	}
	if _, ok := cluster.auth[client.Name()]; ok {
		t.Fatal("the release left the service client")
	}
	if !service.clientsAtRelease {
		t.Fatal("the scope was released after the tenant's clients were deleted")
	}
}

func TestSharedServiceScopeFailuresPoison(t *testing.T) {
	t.Run("remainder", func(t *testing.T) {
		shared := newFakeShared(&fakeFleet{})
		tb := newFakeTB(t)
		tenant := shared.Lease(tb)
		resource := "fake image " + tenant.ID() + "/held"
		(&fakeService{keep: map[string]bool{resource: true}}).lease(t, tenant, resource)
		first := backendOf(tenant)
		tb.finish()
		if !strings.Contains(tb.errorText(), "fake "+resource+": "+resource) || !first.isTerminated() {
			t.Fatalf("a remainder did not poison the cluster: %q", tb.errorText())
		}
	})
	t.Run("error", func(t *testing.T) {
		shared := newFakeShared(&fakeFleet{})
		tb := newFakeTB(t)
		tenant := shared.Lease(tb)
		(&fakeService{fail: errors.New("gateway gone")}).lease(t, tenant, "fake user "+tenant.ID())
		first := backendOf(tenant)
		tb.finish()
		if !strings.Contains(strings.Join(tb.logs, "\n"), "gateway gone") || !first.isTerminated() || shared.Stats().Poisonings != 1 {
			t.Fatalf("a failed release did not poison the cluster: %q", tb.logs)
		}
		if _, ok := first.auth["client."+tenant.ID()]; ok {
			t.Fatal("a failed service release kept the tenant's client")
		}
	})
}

func TestSharedReleaseComparesServiceResources(t *testing.T) {
	for name, dirty := range map[string]func(cluster *fakeShared, self, other *Tenant){
		"resource of no tenant": func(cluster *fakeShared, _, _ *Tenant) { cluster.services["fake namespace stray"] = "stray" },
		"own resource without a scope": func(cluster *fakeShared, self, _ *Tenant) {
			cluster.services["fake group "+self.ID()] = self.ID()
		},
		"baseline resource removed": func(cluster *fakeShared, _, _ *Tenant) { delete(cluster.services, "fake user legacy") },
	} {
		t.Run(name, func(t *testing.T) {
			fleet := &fakeFleet{prepare: func(cluster *fakeShared) { cluster.services["fake user legacy"] = "legacy" }}
			shared := newFakeShared(fleet)
			selfTB, otherTB := newFakeTB(t), newFakeTB(t)
			self, other := shared.Lease(selfTB), shared.Lease(otherTB)
			backendOf(self).change(func(cluster *fakeShared) { dirty(cluster, self, other) })
			selfTB.finish()
			if !strings.Contains(selfTB.errorText(), "outside its scope") || shared.Stats().Poisonings != 1 {
				t.Fatalf("the release did not report the change: %q", selfTB.errorText())
			}
			otherTB.finish()
		})
	}
	t.Run("another tenant's resource", func(t *testing.T) {
		shared := newFakeShared(&fakeFleet{})
		selfTB, otherTB := newFakeTB(t), newFakeTB(t)
		self, other := shared.Lease(selfTB), shared.Lease(otherTB)
		(&fakeService{}).lease(t, other, "fake namespace "+other.ID())
		selfTB.finish()
		otherTB.finish()
		if selfTB.Failed() || otherTB.Failed() || shared.Stats().Poisonings != 0 {
			t.Fatalf("a tenant was blamed for another's resource: %s %s", selfTB.errorText(), otherTB.errorText())
		}
		_ = self
	})
}

func TestSharedServicesArePreparedBeforeTheBaseline(t *testing.T) {
	// A service creates what it would otherwise create on first use, such
	// as RGW's bucket pools, so that a tenant's first use changes nothing
	// outside its scope.
	fleet := &fakeFleet{prepareServices: func(cluster *fakeShared) error {
		cluster.pools["default.rgw.buckets.index"] = "rgw"
		return nil
	}}
	shared := newFakeShared(fleet)
	tb := newFakeTB(t)
	tenant := shared.Lease(tb)
	tb.finish()
	if tb.Failed() || shared.Stats().Poisonings != 0 {
		t.Fatalf("a prepared cluster poisoned: %s", tb.errorText())
	}
	if _, ok := backendOf(tenant).pools["default.rgw.buckets.index"]; !ok {
		t.Fatal("the services were not prepared")
	}

	failing := newFakeShared(&fakeFleet{prepareServices: func(*fakeShared) error { return errors.New("no zone") }})
	tb = newFakeTB(t)
	if failing.Lease(tb) != nil || !strings.Contains(tb.errorText(), "no zone") {
		t.Fatalf("a failed preparation was not reported: %q", tb.errorText())
	}
}

func TestSharedTenantScopesEndWithTheRelease(t *testing.T) {
	shared := newFakeShared(&fakeFleet{})
	tb := newFakeTB(t)
	tenant := shared.Lease(tb)
	release := func(context.Context) ([]string, error) { return nil, nil }
	if err := tenant.attachScope("fake", release); err != nil {
		t.Fatal(err)
	}
	if err := tenant.attachScope("fake", release); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a second attachment of one key returned %v", err)
	}
	tb.finish()
	if err := tenant.attachScope("late", release); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("an attachment after the release returned %v", err)
	}
	if _, err := tenant.serviceClient(t.Context(), "late", ClientCaps{Mon: "allow r"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("a service client after the release returned %v", err)
	}
}

func TestSharedAdmissionWaitsForPGsToSettle(t *testing.T) {
	fleet := &fakeFleet{}
	shared := newFakeShared(fleet)
	tb := newFakeTB(t)
	first := backendOf(shared.Lease(tb))
	tb.finish()
	first.change(func(cluster *fakeShared) {
		cluster.unsettled, cluster.unsettledClearsAfter = "PGs are not all active+clean", cluster.unsettledReads+3
	})
	tb = newFakeTB(t)
	if backendOf(shared.Lease(tb)) != first || shared.Stats().Poisonings != 0 {
		t.Fatal("a lease did not wait for PGs that settled")
	}
	tb.finish()
	first.change(func(cluster *fakeShared) {
		cluster.unsettled, cluster.unsettledClearsAfter = "PGs are not all active+clean", 0
	})
	tb = newFakeTB(t)
	second := backendOf(shared.Lease(tb))
	tb.finish()
	if second == first || !strings.Contains(strings.Join(tb.logs, "\n"), "PGs are not all active+clean") {
		t.Fatalf("a lease was admitted to a cluster whose PGs did not settle: %q", tb.logs)
	}
}
