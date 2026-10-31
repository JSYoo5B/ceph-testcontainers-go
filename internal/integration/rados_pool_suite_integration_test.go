//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rados_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,cephfs

package integration_test

import (
	"context"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
)

// radosPoolSuite runs the RADOS cases that need only their own pools on one
// single-OSD cluster per network mode: TestClientFixtures,
// TestNativePoolReplacement and TestObjectReceipts. Each case creates and
// names its own pools, changes no daemon and restores any configuration it
// overrides, so no case depends on another having run or passed. Cases that
// stop, kill or reconfigure daemons keep their own clusters.
//
// The cluster belongs to the suite: SetupSuite starts it under the suite's
// test, whose cleanup terminates it after the last case. Its gate
// (sharedClusterGate) requires the cluster to be clean before every case and
// again after a failed one; a cluster that is not clean fails every later case
// at once.
type radosPoolSuite struct {
	suite.Suite
	host    bool
	cluster *ceph.Container
	client  testcontainers.Container
	gate    sharedClusterGate
}

func (s *radosPoolSuite) SetupSuite() {
	opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}
	if s.host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	s.cluster, s.client = harness.NewServiceCluster(s.T(), opts...)
	s.gate.check = func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		return s.cluster.WaitForClean(ctx)
	}
}

func (s *radosPoolSuite) SetupTest() { s.gate.before(s.T()) }

func (s *radosPoolSuite) TearDownTest() { s.gate.after(s.T()) }

// TestRADOSPoolCases runs the suite once per network mode.
func TestRADOSPoolCases(t *testing.T) {
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		t.Run(mode.Name, func(t *testing.T) {
			suite.Run(t, &radosPoolSuite{host: mode.Host})
		})
	}
}
