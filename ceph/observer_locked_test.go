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

func TestLockedQueriesWaitForTheTopologyLockUnderTheCallersContext(t *testing.T) {
	cluster, ctr := newLockedObserverCluster()
	observer, _ := Observe(cluster)
	pool := cluster.poolOf(PoolState{Name: "rbd", ID: 1, Type: "replicated", Size: 1, MinSize: 1})
	queries := lockedQueries(observer)
	queries["Pools"] = func(ctx context.Context) error { _, err := observer.Pools(ctx); return err }
	queries["ObjectWatchers"] = func(ctx context.Context) error {
		_, err := observer.ObjectWatchers(ctx, "rbd", "", "object")
		return err
	}
	queries["Container.Pool"] = func(ctx context.Context) error { _, err := cluster.Pool(ctx, "rbd"); return err }
	queries["Pool.Status"] = func(ctx context.Context) error { _, err := pool.Status(ctx); return err }
	queries["Pool.SetQuota"] = func(ctx context.Context) error { return pool.SetQuota(ctx, PoolQuota{}) }
	queries["Pool.SetReplication"] = func(ctx context.Context) error { return pool.SetReplication(ctx, 1, 1) }
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	for name, query := range queries {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		err := query(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s did not honor its deadline while the lock was held: %v", name, err)
		}
	}
	if got := ctr.commands(); len(got) != 0 {
		t.Fatalf("commands ran under a held lock: %q", got)
	}
}
