package mirror

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

func TestResolveDaemonImageUsesExplicitImageAsGiven(t *testing.T) {
	// Even a cluster pair without any role image accepts an explicit image, and
	// an explicit image is never replaced by the role image.
	image, err := resolveDaemonImage("explicit", Config{})
	if err != nil || image != "explicit" {
		t.Fatalf("explicit image: %q %v", image, err)
	}
	// The empty image derives, so it needs both clusters.
	if image, err := resolveDaemonImage("", Config{}); image != "" || err == nil || !strings.Contains(err.Error(), "roles clusters are required") {
		t.Fatalf("empty image did not take the derivation path: %q %v", image, err)
	}
	// A blank image is an explicit selection, not "derive": it is passed on so
	// pair validation refuses it.
	image, err = resolveDaemonImage(" ", Config{})
	if err != nil || image != " " {
		t.Fatalf("blank image was derived or refused early: %q %v", image, err)
	}
}

func TestDaemonImageFromRoleKeepsRoleAndRefusesMissingRole(t *testing.T) {
	image, err := daemonImageFromRole(ceph.ImageSet{Essential: "essential", Client: "client", RBDAux: "role-image"})
	if err != nil || image != "role-image" {
		t.Fatalf("role image: %q %v", image, err)
	}
	// Other roles never substitute for a missing rbd-aux role.
	image, err = daemonImageFromRole(ceph.ImageSet{Essential: "essential", Client: "client", CephFS: "cephfs", RGW: "rgw"})
	if image != "" || !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("missing role was not refused: %q %v", image, err)
	}
}

func TestRunEmptyImageRefusesBeforeEffects(t *testing.T) {
	var calls atomic.Int32
	source := &ceph.Container{Container: &clustertest.ControlQueryCounter{Calls: &calls}}
	destination := &ceph.Container{Container: &clustertest.ControlQueryCounter{Calls: &calls}}
	cfg := Config{Source: source, Destination: destination, Pool: "rbd"}
	for name, run := range map[string]func() (*Mirror, error){
		"role missing": func() (*Mirror, error) { return Run(context.Background(), "", cfg) },
		"clusters nil": func() (*Mirror, error) { return Run(context.Background(), "", Config{Pool: "rbd"}) },
		"blank image":  func() (*Mirror, error) { return Run(context.Background(), " ", cfg) },
	} {
		result, err := run()
		if result != nil || !errors.Is(err, ceph.ErrInvalidRequest) || calls.Load() != 0 {
			t.Fatalf("%s reached the runtime: result=%v err=%v calls=%d", name, result, err, calls.Load())
		}
	}
}
