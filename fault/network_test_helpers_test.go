package fault

import (
	"context"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/testcontainers/testcontainers-go"
)

// Only the Docker netem backend is fake; the cluster registry, admission,
// topology lock and Terminate are the production implementations.
type delayTestCluster struct {
	*ceph.Container
	backend netemBackend
}

func newDelayTestCluster(backend netemBackend, osd testcontainers.Container, host bool) *delayTestCluster {
	c := testhelper.TestCluster(nil, testhelper.TestConfig{NetworkName: "tc-public", HostNetwork: host, StartupTimeout: time.Second})
	view := testhelper.TestState(c)
	testhelper.TestReplaceOSDs(c, map[int]*ceph.OSDContainer{0: {Container: osd}})
	*view.NetworkRemoved = true // synthetic bridge; no Docker removal
	return &delayTestCluster{Container: c, backend: backend}
}
func (c *delayTestCluster) ApplyNetworkProfile(ctx context.Context, target testcontainers.Container, delay NetworkProfile) (*NetworkProfileLease, error) {
	return temporaryNetworkDelay(ctx, c.Container, target, delay, c.backend)
}
func (c *delayTestCluster) faultsOf(kind faulthook.Kind) (result map[string]any) {
	faulthook.WithTopologyNoContext(c.Container, func() { result = testhelper.TestFaults(c.Container, kind) })
	return result
}
func nativeTerminated() error { native, _ := faulthook.Terminated(); return native }

var categories = []error{ceph.ErrInvalidRequest, ceph.ErrUnsupported, ceph.ErrImageCapability, ceph.ErrEnvironmentUnavailable, ceph.ErrNativeFailure, ceph.ErrSharedCluster}

func classifiedError(err error) bool {
	for _, kind := range []error{ceph.ErrInvalidRequest, ceph.ErrUnsupported, ceph.ErrImageCapability, ceph.ErrEnvironmentUnavailable, ceph.ErrNativeFailure, ceph.ErrSharedCluster} {
		if errors.Is(err, kind) {
			return true
		}
	}
	return false
}
func trackInterruption(c *ceph.Container, key string, link *NetworkInterruption) {
	faulthook.WithTopologyNoContext(c, func() {
		token, _, err := faulthook.Reserve(context.Background(), c, faulthook.NetworkInterruption, key)
		if err != nil {
			panic(err)
		}
		_, err = faulthook.Settle(c, token, faulthook.Held{Network: link.NetworkName, Handle: link, Undo: link.Restore, Complete: link.isComplete})
		if err != nil {
			panic(err)
		}
	})
}
