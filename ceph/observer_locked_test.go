package ceph

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

const (
	observerOSDDumpKey = "osd dump --format json"
	observerOSDDump    = `{"flags":"noout,pause","osds":[]}`
	observerOSDDump2   = `{"flags":"","osds":[]}`
)

// lockedQueries are the observations that take the topology lock first. Each
// entry runs one query of an Observer and reports its error.
func lockedQueries(observer *Observer) map[string]func(context.Context) error {
	return map[string]func(context.Context) error{
		"Configuration":    func(ctx context.Context) error { _, err := observer.Configuration(ctx); return err },
		"BlocklistEntries": func(ctx context.Context) error { _, err := observer.BlocklistEntries(ctx); return err },
		"MGRModules":       func(ctx context.Context) error { _, err := observer.MGRModules(ctx); return err },
		"OSDFlags":         func(ctx context.Context) error { _, err := observer.OSDFlags(ctx); return err },
		"OSDStates":        func(ctx context.Context) error { _, err := observer.OSDStates(ctx); return err },
		"ManagerServices":  func(ctx context.Context) error { _, err := observer.ManagerServices(ctx); return err },
	}
}

func newLockedObserverCluster() (*Container, *observerControl) {
	cluster, ctr := newObserverCluster()
	ctr.replies[observerOSDDumpKey] = observerOSDDump
	ctr.replies["mgr services --format json"] = `{"dashboard":"http://mgr:8443/"}`
	ctr.replies["osd blocklist ls --format json"] = "[]\n[]\n"
	ctr.replies["config dump --format json"] = "[]"
	return cluster, ctr
}

func TestObserverLockedQueriesReadActualStateOnEveryQuery(t *testing.T) {
	cluster, ctr := newLockedObserverCluster()
	observer, _ := Observe(cluster)
	if len(ctr.commands()) != 0 {
		t.Fatal("construction ran commands")
	}
	flags, err := observer.OSDFlags(t.Context())
	if err != nil || !reflect.DeepEqual(flags, []OSDFlag{"noout", "pause"}) {
		t.Fatalf("flags = %v, %v", flags, err)
	}
	ctr.set(observerOSDDumpKey, observerOSDDump2)
	flags, err = observer.OSDFlags(t.Context())
	if err != nil || len(flags) != 0 {
		t.Fatalf("a retained observer returned earlier flags: %v, %v", flags, err)
	}
	services, err := observer.ManagerServices(t.Context())
	if err != nil || services["dashboard"] != "http://mgr:8443/" {
		t.Fatalf("services = %v, %v", services, err)
	}
	if entries, err := observer.BlocklistEntries(t.Context()); err != nil || len(entries) != 0 {
		t.Fatalf("blocklist = %v, %v", entries, err)
	}
	if entries, err := observer.Configuration(t.Context()); err != nil || len(entries) != 0 {
		t.Fatalf("configuration = %v, %v", entries, err)
	}
	if states, err := observer.OSDStates(t.Context()); err != nil || len(states) != 0 {
		t.Fatalf("states = %v, %v", states, err)
	}
}

func TestObserverLockedQueriesRefuseATerminatedClusterBeforeAnyCommand(t *testing.T) {
	cluster, ctr := newLockedObserverCluster()
	observer, _ := Observe(cluster)
	cluster.mu.Lock()
	cluster.closed = true
	cluster.mu.Unlock()
	for name, query := range lockedQueries(observer) {
		if err := query(t.Context()); !errors.Is(err, errClusterTerminated) {
			t.Errorf("%s on a terminated cluster returned %v", name, err)
		}
	}
	if got := ctr.commands(); len(got) != 0 {
		t.Fatalf("a terminated cluster ran commands: %q", got)
	}
}

func TestObserverLockedQueriesKeepTheirOwnTopologyLockAcquisition(t *testing.T) {
	cluster, ctr := newLockedObserverCluster()
	observer, _ := Observe(cluster)
	cluster.mu.Lock()
	// ManagerServices waits for the topology lock under the caller's context.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := observer.ManagerServices(ctx); !errors.Is(err, context.DeadlineExceeded) {
		cluster.mu.Unlock()
		t.Fatalf("ManagerServices did not honor its deadline while the lock was held: %v", err)
	}
	// The others take the mutex without a context: they wait for the holder.
	done := make(chan string, 5)
	for name, query := range lockedQueries(observer) {
		if name == "ManagerServices" {
			continue
		}
		go func() { _ = query(t.Context()); done <- name }()
	}
	select {
	case name := <-done:
		cluster.mu.Unlock()
		t.Fatalf("%s ran while the topology lock was held", name)
	case <-time.After(50 * time.Millisecond):
	}
	if got := ctr.commands(); len(got) != 0 {
		cluster.mu.Unlock()
		t.Fatalf("commands ran under a held lock: %q", got)
	}
	cluster.mu.Unlock()
	for range 5 {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("a query did not resume after the lock was released")
		}
	}
}

func TestObserverLockedQueriesValidateTheTenantLeaseOnEveryQuery(t *testing.T) {
	shared, tenant, tb, ctr := leaseObservable(t)
	ctr.set(observerOSDDumpKey, observerOSDDump)
	observer, _ := ObserveTenant(tenant)
	if flags, err := observer.OSDFlags(t.Context()); err != nil || len(flags) != 2 {
		t.Fatalf("live tenant flags = %v, %v", flags, err)
	}
	before := len(ctr.commands())
	tb.finish()
	check := func(when string) {
		for name, query := range lockedQueries(observer) {
			if err := query(t.Context()); !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("%s %s returned %v, want ErrInvalidRequest", name, when, err)
			}
		}
		if after := len(ctr.commands()); after != before {
			t.Fatalf("%s: %d commands ran", when, after-before)
		}
	}
	check("after release")
	if err := shared.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	check("after release and Close")
}
