package harness

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/hookreg"
)

// These controls cover the harness side of the tenant Observer seam with a
// real Tenant and a real *ceph.Container whose control container is a fake that
// answers the CLI. The query implementation and its per-query behavior are
// covered in ceph (observer_*_test.go) with a stand-in for the lease.

// statusControl answers "status --format json" and records every command.
type statusControl struct {
	testcontainers.Container
	mu    sync.Mutex
	calls []string
}

func (c *statusControl) Exec(_ context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.mu.Lock()
	c.calls = append(c.calls, strings.Join(args[3:], " "))
	c.mu.Unlock()
	out := `{"fsid":"f1","health":{"status":"HEALTH_OK"}}`
	var header [8]byte
	header[0] = byte(stdcopy.Stdout)
	binary.BigEndian.PutUint32(header[4:], uint32(len(out)))
	var stream bytes.Buffer
	stream.Write(header[:])
	stream.WriteString(out)
	return 0, &stream, nil
}

func (c *statusControl) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls)
}

// realCluster builds a *ceph.Container over the fake control container through
// the core's unit-test seam.
func realCluster(t *testing.T, control testcontainers.Container) *ceph.Container {
	t.Helper()
	access, ok := hookreg.Test.(interface {
		UnitCluster(control testcontainers.Container, startupTimeout time.Duration, closed, hostNetwork bool,
			publicAddress, networkName string, shared bool, osds int, placement ceph.OSDConfig, memStore bool,
			mode ceph.MessengerMode, images *ceph.ImageSet) *ceph.Container
	})
	if !ok {
		t.Fatal("package ceph registered no unit-test access with UnitCluster")
	}
	return access.UnitCluster(control, time.Second, false, false, "", "", true, 0, ceph.OSDConfig{}, false, 0, nil)
}

func TestObserveTenantRefusesANilTenant(t *testing.T) {
	observer, err := ObserveTenant(nil)
	if observer != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("ObserveTenant(nil) = %v, %v", observer, err)
	}
}

func TestObserveTenantReadsThroughTheLeaseAndRefusesAfterRelease(t *testing.T) {
	control := &statusControl{}
	shared := newOpsShared(&recordingOps{rawValue: realCluster(t, control)})
	tb := newFakeTB(t)
	tenant := shared.Lease(tb)
	observer, err := ObserveTenant(tenant)
	if err != nil {
		t.Fatal(err)
	}
	if control.count() != 0 {
		t.Fatal("construction ran a command")
	}
	if status, err := observer.Status(t.Context()); err != nil || status.FSID != "f1" {
		t.Fatalf("live tenant status = %+v, %v", status, err)
	}
	before := control.count()
	tb.finish() // releases the lease; the cluster lives
	if _, err := observer.Status(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("after release: %v, want ErrInvalidRequest", err)
	}
	if control.count() != before {
		t.Fatal("a released tenant's observer still ran a command")
	}
}

func TestObserveTenantRefusesAfterTheClusterEnds(t *testing.T) {
	control := &statusControl{}
	shared := newOpsShared(&recordingOps{rawValue: realCluster(t, control)})
	tb := newFakeTB(t)
	observer, err := ObserveTenant(shared.Lease(tb))
	if err != nil {
		t.Fatal(err)
	}
	if err := shared.Close(t.Context()); err != nil { // the lease is still held
		t.Fatal(err)
	}
	before := control.count()
	if _, err := observer.Status(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("after termination: %v, want ErrInvalidRequest", err)
	}
	if control.count() != before {
		t.Fatal("a terminated cluster's tenant observer still ran a command")
	}
	tb.finish()
}

func TestObserveTenantOfAModelWithoutAClusterIsRefused(t *testing.T) {
	shared := newFakeShared(&fakeFleet{})
	tb := newFakeTB(t)
	observer, err := ObserveTenant(shared.Lease(tb)) // the model backend has no *ceph.Container
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observer.Status(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("a tenant without a cluster answered: %v", err)
	}
	tb.finish()
}

func TestTenantHasNoStaticFactAccessors(t *testing.T) {
	// The facts are read through ceph.FactsOf, never from a Tenant.
	for _, name := range []string{"AddressFamily", "CephVersion", "ClusterNetworkName", "EssentialImage",
		"HasSeparateClusterNetwork", "Images", "MessengerMode", "NetworkName", "PublicAddress", "UsesHostNetwork"} {
		if hasMethod(&Tenant{}, name) {
			t.Errorf("Tenant has the static fact accessor %s; read it through FactsOf", name)
		}
	}
}

func hasMethod(v any, name string) bool {
	_, ok := reflect.TypeOf(v).MethodByName(name)
	return ok
}
