package ceph

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/evidence"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type diagnosticsTestContainer struct {
	testcontainers.Container
	id   string
	exec func(context.Context, []string, ...tcexec.ProcessOption) (int, io.Reader, error)
}

func (c *diagnosticsTestContainer) GetContainerID() string { return c.id }
func (c *diagnosticsTestContainer) Exec(ctx context.Context, argv []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	if c.exec == nil {
		return 1, nil, errors.New("unexpected exec")
	}
	return c.exec(ctx, argv, opts...)
}

// The inventory of a partially started, closed cluster keeps every tracked
// handle, uncreated ones included, the bridge networks and every known
// secret; a caller's container that the cluster tracks keeps the cluster's
// description when a collection selects it.
func TestEvidenceInventoryOfAPartialCluster(t *testing.T) {
	primary := &diagnosticsTestContainer{id: "primary"}
	mgr := &diagnosticsTestContainer{id: "manager"}
	service := &diagnosticsTestContainer{id: "partial-gateway"}
	cluster := &Container{
		Container:    primary,
		controlPlane: controlPlaneState{handle: primary},
		closed:       true,
		extensions:   extensionStore{states: map[string]any{"rgw": testExtension{secrets: []string{"access-private", "s3-private"}}}},
		nets:         networkState{public: &testcontainers.DockerNetwork{ID: "public-id", Name: "public"}},
		bootstrap:    bootstrapRecord{keyring: []byte("[client.admin]\n key = cephx-private\n")},
		fleet:        osdFleetState{osdHandles: osdRegistry{owned: map[int]*OSDContainer{0: {ID: 0}, 1: nil}}},
		members: membershipState{monitorTerminated: true, roster: memberRoster{
			firstMgr: mgr,
			monitors: map[string]*MonitorContainer{"partial": {}, "uncreated": nil},
			managers: map[string]*ManagerContainer{"a": {Container: mgr}},
			services: map[string]testcontainers.Container{"rgw:bootstrapping": service, "mds.tenant-0": nil},
		}},
	}
	inv, err := cluster.evidenceInventory(t.Context())
	if err != nil || !inv.Closed || inv.Control != primary || inv.ControlPlane != primary {
		t.Fatalf("partial inventory failed: %+v, %v", inv, err)
	}
	members, omitted := inv.Select([]evidence.Member{{Container: primary}, {Role: "mirror", Name: "external", Container: &diagnosticsTestContainer{id: "external"}}, {}}, 128)
	ids := make(map[string]int)
	missing := 0
	foundGateway := false
	for _, target := range members {
		if target.Container == nil {
			missing++
			continue
		}
		ids[target.Container.GetContainerID()]++
		if target.Container.GetContainerID() == "primary" && (target.Role != "mon" || target.Name != "a") {
			t.Fatalf("additional label replaced the owned MON identity: %+v", target)
		}
		foundGateway = foundGateway || target.Name == "rgw:bootstrapping" && target.Role == "rgw"
	}
	if omitted != 0 || ids["primary"] != 1 || ids["manager"] != 1 || ids["external"] != 1 || !foundGateway || missing != 4 {
		t.Fatalf("lost partial handles or duplicated native IDs: ids=%v, missing=%d, gateway=%v", ids, missing, foundGateway)
	}
	for _, secret := range []string{"access-private", "s3-private", "cephx-private"} {
		if !slices.Contains(inv.Secrets, secret) {
			t.Fatalf("known secret missing from the inventory: %q", secret)
		}
	}
	if len(inv.Networks) != 1 || inv.Networks[0].ID != "public-id" {
		t.Fatalf("network-only bootstrap information lost: %v", inv.Networks)
	}
	var nilCluster *Container
	if _, err := nilCluster.evidenceInventory(t.Context()); err == nil {
		t.Fatal("nil cluster accepted")
	}
	if empty, err := (&Container{}).evidenceInventory(t.Context()); err != nil || len(empty.Members) != 0 {
		t.Fatalf("zero-value partial cluster panicked or fabricated handles: %+v, %v", empty, err)
	}
	t.Run("dedicated-control", func(t *testing.T) {
		control := &diagnosticsTestContainer{id: "dedicated-control"}
		cluster.controlPlane.handle = control
		inv, err := cluster.evidenceInventory(t.Context())
		if err != nil || inv.Control != control || inv.ControlPlane != control {
			t.Fatalf("dedicated CLI target lost: %+v, %v", inv, err)
		}
		matches := 0
		for _, target := range inv.Members {
			if target.Container == control && target.Role == "control" && target.Name == "cli" {
				matches++
			}
		}
		if matches != 1 {
			t.Fatalf("dedicated control inventory count: %d", matches)
		}
	})
}

// Each lock that the inventory takes is polled under ctx, so a held mutex
// never blocks it past its deadline and it keeps no lock afterwards.
func TestEvidenceInventoryLockDeadline(t *testing.T) {
	for _, lock := range []string{"topology", "control", "config"} {
		t.Run(lock, func(t *testing.T) {
			cluster := &Container{}
			var unlock func()
			switch lock {
			case "topology":
				cluster.mu.Lock()
				unlock = cluster.mu.Unlock
			case "control":
				cluster.controlPlane.mu.Lock()
				unlock = cluster.controlPlane.mu.Unlock
			case "config":
				cluster.bootstrap.mu.Lock()
				unlock = cluster.bootstrap.mu.Unlock
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			started := time.Now()
			_, err := cluster.evidenceInventory(ctx)
			unlock()
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 500*time.Millisecond {
				t.Fatalf("held mutex ignored deadline: %v", err)
			}
			if !cluster.mu.TryLock() {
				t.Fatal("inventory retained topology mutex after cancellation")
			}
			cluster.mu.Unlock()
		})
	}
	cluster := &Container{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := cluster.evidenceInventory(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("already-canceled inventory acquired an available lock: %v", err)
	}
}

// Typed nil handles of the cluster are kept as uncreated members, never as
// a handle that a collector would call.
func TestEvidenceInventoryNormalizesTypedNilHandles(t *testing.T) {
	var missing *diagnosticsTestContainer
	cluster := &Container{
		Container:    missing,
		controlPlane: controlPlaneState{handle: missing},
		fleet:        osdFleetState{osdHandles: osdRegistry{owned: map[int]*OSDContainer{0: {Container: missing, ID: 0}}}},
		members: membershipState{roster: memberRoster{
			firstMgr: missing,
			monitors: map[string]*MonitorContainer{"joining": {Container: missing}},
			services: map[string]testcontainers.Container{"rgw:partial": missing},
		}},
	}
	inv, err := cluster.evidenceInventory(t.Context())
	if err != nil || inv.Control != nil || inv.ControlPlane != nil {
		t.Fatalf("typed-nil control was not normalized: %+v, %v", inv, err)
	}
	foundOwned := false
	for _, target := range inv.Members {
		if target.Container != nil {
			t.Fatalf("inventory retained a typed-nil handle: %+v", target)
		}
		foundOwned = foundOwned || target.Role == "rgw" && target.Name == "rgw:partial"
	}
	if !foundOwned {
		t.Fatalf("an owned creation attempt was discarded: %+v", inv.Members)
	}
}

// The owner hook reports the ID that labels the cluster's containers.
func TestEvidenceHooksOwner(t *testing.T) {
	if got := (evidenceHooks{}).Owner(&Container{startup: newStartupIdentity("owner")}); got != "owner" {
		t.Fatalf("owner = %q", got)
	}
}
