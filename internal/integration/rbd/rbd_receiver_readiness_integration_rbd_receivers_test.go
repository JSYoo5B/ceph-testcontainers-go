//go:build integration && multicluster && ci && ci_recovery && ci_batch && ci_batch_rbd_receivers

//ci: timeout=90m job-timeout=100 checker=receivers image-roles=essential,client,rbd-aux

package rbd_test

import (
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

func TestMultiClusterRBDReceiverReadiness(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		t.Run(mode.Name, func(t *testing.T) { harness.RunRBDReceiverReadiness(t, mode.Host) })
	}
}
