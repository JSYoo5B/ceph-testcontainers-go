package ceph

import (
	"errors"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// testcontainers.Run returns a nil *DockerContainer when it fails before a
// container exists. startService must return a nil interface then, or callers
// that test the handle would use a nil pointer.
func TestStartServiceReturnsNoHandleWhenNoContainerWasCreated(t *testing.T) {
	cluster := &Container{
		bootstrap: bootstrapRecord{config: []byte("[global]\n"), keyring: []byte("keyring")},
		services:  map[string]testcontainers.Container{},
		settings:  options{startupTimeout: time.Second, hostNetwork: true, publicAddress: "127.0.0.1"},
	}
	cause := errors.New("rejected before create")
	ctr, err := cluster.startService(t.Context(), "mds.a", "image", testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error {
		return cause
	}))
	if ctr != nil || !errors.Is(err, cause) {
		t.Fatalf("failed creation returned handle %#v and error %v", ctr, err)
	}
	if len(cluster.services) != 0 {
		t.Fatalf("a service without a container was registered: %v", cluster.services)
	}
	if err := cluster.lockTopology(t.Context()); err != nil {
		t.Fatalf("topology lock not released: %v", err)
	}
	cluster.mu.Unlock()
}
