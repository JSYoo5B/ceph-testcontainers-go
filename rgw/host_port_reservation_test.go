package rgw

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// A reservation serves one gateway: the start that takes it, or a release.
// A release after the start does not free the port the gateway now binds.
func TestRGWHostPortReservationIsUsedOnce(t *testing.T) {
	releases := 0
	release := func(context.Context) error { releases++; return nil }
	taken := &hostPortReservation{ports: []int{40001}, release: release}
	ports, got, err := taken.take()
	if err != nil || len(ports) != 1 || ports[0] != 40001 || got == nil {
		t.Fatalf("first take: %v %v", ports, err)
	}
	if _, _, err := taken.take(); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("second take: %v", err)
	}
	if err := taken.releaseUnused(t.Context()); err != nil || releases != 0 {
		t.Fatalf("release after a take: %v, %d releases", err, releases)
	}
	unused := &hostPortReservation{ports: []int{40002}, release: release}
	if err := unused.releaseUnused(t.Context()); err != nil || releases != 1 {
		t.Fatalf("release: %v, %d releases", err, releases)
	}
	if err := unused.releaseUnused(t.Context()); err != nil || releases != 1 {
		t.Fatalf("second release: %v, %d releases", err, releases)
	}
	if _, _, err := unused.take(); err == nil {
		t.Fatal("a released reservation was taken")
	}
}

// A reserved port is refused before any container exists when the cluster
// is not on the host network, for a TLS gateway that would need a second
// port, and when another start already took it.
func TestRGWReservedPortStartRefusesBeforeCreatingAnything(t *testing.T) {
	bridge := testCluster(nil, testhelper.TestConfig{StartupTimeout: time.Second}, nil)
	host := testCluster(nil, testhelper.TestConfig{StartupTimeout: time.Second, HostNetwork: true, PublicAddress: "127.0.0.1"}, nil)
	noRelease := func(context.Context) error { t.Error("a refused start released the port"); return nil }
	for _, tc := range []struct {
		name    string
		cluster *ceph.Container
		config  Config
		used    bool
	}{
		{"bridge", bridge, Config{Name: "z"}, false},
		{"tls", host, Config{Name: "z", TLS: &TLSConfig{}}, false},
		{"used", host, Config{Name: "z"}, true},
	} {
		reserved := &hostPortReservation{ports: []int{40003}, release: noRelease, used: tc.used}
		ctr, _, _, err := startNamedDaemon(t.Context(), tc.cluster, testFacts(t, tc.cluster), tc.config, reserved)
		if ctr != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v %v", tc.name, ctr, err)
		}
	}
}
